use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, oneshot};
use tonic::Status;

use super::direct_io;
use super::memtable::MemTable;

const WAL_MAGIC_BYTES: &[u8; 9] = b"ORELSMWAL";
const WAL_VERSION: u32 = 1;

/// Write request for group commit with pipelining
struct WriteRequest {
    key: String,
    value: Option<String>,
    /// Notified when data is written to OS buffer
    written_tx: Option<oneshot::Sender<()>>,
    /// Notified when data is synced to disk
    synced_tx: oneshot::Sender<Result<(), Status>>,
}

/// Batch write request - multiple entries with single notification
struct BatchWriteRequest {
    entries: Vec<(String, Option<String>)>,
    /// Notified when all data is written to OS buffer
    written_tx: Option<oneshot::Sender<()>>,
    /// Notified when all data is synced to disk
    synced_tx: oneshot::Sender<Result<(), Status>>,
}

/// Request type sent to the background flusher
enum FlushRequest {
    Single(WriteRequest),
    Batch(BatchWriteRequest),
}

/// Inner state for WAL file operations
#[derive(Debug)]
struct WalWriterInner {
    writer: File,
    path: PathBuf,
}

/// Group commit WAL writer that batches multiple writes before sync
pub struct GroupCommitWalWriter {
    /// Channel to send write requests to background flusher (shared across clones)
    request_tx: Arc<Mutex<Option<mpsc::Sender<FlushRequest>>>>,
    /// Current WAL ID
    current_id: Arc<AtomicU64>,
    /// Data directory
    data_dir: PathBuf,
    /// Inner state shared with background task
    inner: Arc<Mutex<WalWriterInner>>,
    /// Batch interval in microseconds
    batch_interval_micros: u64,
    /// Handle to the background flusher task
    flusher_handle: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
}

impl std::fmt::Debug for GroupCommitWalWriter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GroupCommitWalWriter")
            .field("current_id", &self.current_id)
            .field("data_dir", &self.data_dir)
            .field("batch_interval_micros", &self.batch_interval_micros)
            .finish()
    }
}

impl Clone for GroupCommitWalWriter {
    fn clone(&self) -> Self {
        GroupCommitWalWriter {
            request_tx: Arc::clone(&self.request_tx),
            current_id: Arc::clone(&self.current_id),
            data_dir: self.data_dir.clone(),
            inner: Arc::clone(&self.inner),
            batch_interval_micros: self.batch_interval_micros,
            flusher_handle: Arc::clone(&self.flusher_handle),
        }
    }
}

impl GroupCommitWalWriter {
    /// Create a new group commit WAL writer
    #[allow(clippy::result_large_err)]
    pub fn new(data_dir: &Path, wal_id: u64, batch_interval_micros: u64) -> Result<Self, Status> {
        let (file, path) = Self::create_wal_file(data_dir, wal_id)?;

        let inner = Arc::new(Mutex::new(WalWriterInner { writer: file, path }));

        let (tx, rx) = mpsc::channel::<FlushRequest>(1024);

        // Start background flusher
        let inner_clone = Arc::clone(&inner);
        let handle = tokio::spawn(async move {
            Self::background_flusher(rx, inner_clone, batch_interval_micros).await;
        });

        Ok(GroupCommitWalWriter {
            request_tx: Arc::new(Mutex::new(Some(tx))),
            current_id: Arc::new(AtomicU64::new(wal_id)),
            data_dir: data_dir.to_path_buf(),
            inner,
            batch_interval_micros,
            flusher_handle: Arc::new(Mutex::new(Some(handle))),
        })
    }

    /// Shutdown the WAL writer gracefully, flushing all pending writes
    pub async fn shutdown(&self) {
        println!("Shutting down WAL writer...");

        // Drop the sender to signal the background task to finish
        // This will cause the channel to close, signaling the background task to exit
        {
            let mut guard = self.request_tx.lock().unwrap();
            guard.take(); // Drop the sender
        }

        // Take the handle to wait for the background task
        let handle = {
            let mut guard = self.flusher_handle.lock().unwrap();
            guard.take()
        };

        if let Some(handle) = handle {
            // Wait for the background task to complete
            if let Err(e) = handle.await {
                eprintln!("Error waiting for WAL flusher to shutdown: {}", e);
            } else {
                println!("WAL writer shutdown complete.");
            }
        }
    }

    /// Get current WAL ID
    pub fn current_id(&self) -> u64 {
        self.current_id.load(Ordering::SeqCst)
    }

    /// Create a new WAL file and return the file handle
    #[allow(clippy::result_large_err)]
    fn create_wal_file(data_dir: &Path, wal_id: u64) -> Result<(File, PathBuf), Status> {
        let wal_path = data_dir.join(format!("wal_{:05}.log", wal_id));
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&wal_path)
            .map_err(|e| Status::internal(format!("Failed to create WAL: {}", e)))?;

        // Disable page cache for WAL to reduce memory pressure
        // and avoid double-buffering (we have our own write batching)
        if let Err(e) = direct_io::disable_page_cache(&file) {
            // Log warning but don't fail - this is an optimization
            eprintln!(
                "Warning: Failed to disable page cache for WAL: {} (using {})",
                e,
                direct_io::method_name()
            );
        }

        // Write header
        file.write_all(WAL_MAGIC_BYTES)
            .map_err(|e| Status::internal(e.to_string()))?;
        file.write_all(&WAL_VERSION.to_le_bytes())
            .map_err(|e| Status::internal(e.to_string()))?;
        file.sync_all()
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok((file, wal_path))
    }

    /// Background task that batches writes and flushes periodically
    async fn background_flusher(
        mut rx: mpsc::Receiver<FlushRequest>,
        inner: Arc<Mutex<WalWriterInner>>,
        batch_interval_micros: u64,
    ) {
        let mut pending: Vec<FlushRequest> = Vec::new();

        loop {
            // Wait for a request with timeout
            match tokio::time::timeout(Duration::from_micros(batch_interval_micros), rx.recv())
                .await
            {
                Ok(Some(req)) => {
                    pending.push(req);
                    // Collect any additional pending requests
                    while let Ok(req) = rx.try_recv() {
                        pending.push(req);
                    }
                }
                Ok(None) => {
                    // Channel closed, flush remaining and exit
                    if !pending.is_empty() {
                        Self::flush_and_notify(&inner, &mut pending);
                    }
                    break;
                }
                Err(_) => {
                    // Timeout - flush if we have pending writes
                }
            }

            if !pending.is_empty() {
                Self::flush_and_notify(&inner, &mut pending);
            }
        }
    }

    /// Flush pending writes and notify all waiting requests
    fn flush_and_notify(inner: &Arc<Mutex<WalWriterInner>>, pending: &mut Vec<FlushRequest>) {
        let mut guard = inner.lock().unwrap();

        // Stage 1: Write all to OS buffer
        for req in pending.iter() {
            match req {
                FlushRequest::Single(single) => {
                    if let Err(e) =
                        Self::write_entry_to_file(&mut guard.writer, &single.key, &single.value)
                    {
                        // If write fails, notify all about the error and return
                        Self::notify_all_error(pending, e);
                        return;
                    }
                }
                FlushRequest::Batch(batch) => {
                    for (key, value) in &batch.entries {
                        if let Err(e) = Self::write_entry_to_file(&mut guard.writer, key, value) {
                            Self::notify_all_error(pending, e);
                            return;
                        }
                    }
                }
            }
        }

        // Notify all that writes are in OS buffer
        for req in pending.iter_mut() {
            match req {
                FlushRequest::Single(single) => {
                    if let Some(tx) = single.written_tx.take() {
                        let _ = tx.send(());
                    }
                }
                FlushRequest::Batch(batch) => {
                    if let Some(tx) = batch.written_tx.take() {
                        let _ = tx.send(());
                    }
                }
            }
        }

        // Stage 2: Sync data to disk (fdatasync - skips metadata sync for better performance)
        let sync_result = guard
            .writer
            .sync_data()
            .map_err(|e| Status::internal(e.to_string()));

        // Stage 3: Notify "synced"
        for req in pending.drain(..) {
            match req {
                FlushRequest::Single(single) => {
                    let _ = single.synced_tx.send(sync_result.clone());
                }
                FlushRequest::Batch(batch) => {
                    let _ = batch.synced_tx.send(sync_result.clone());
                }
            }
        }
    }

    /// Notify all pending requests about an error
    fn notify_all_error(pending: &mut Vec<FlushRequest>, error: Status) {
        for req in pending.drain(..) {
            match req {
                FlushRequest::Single(single) => {
                    let _ = single.synced_tx.send(Err(error.clone()));
                }
                FlushRequest::Batch(batch) => {
                    let _ = batch.synced_tx.send(Err(error.clone()));
                }
            }
        }
    }

    /// Write a single entry to the file (without sync)
    #[allow(clippy::result_large_err)]
    fn write_entry_to_file(
        writer: &mut File,
        key: &str,
        value: &Option<String>,
    ) -> Result<(), Status> {
        let key_bytes = key.as_bytes();
        let key_len = key_bytes.len() as u64;
        let (val_len, val_bytes): (u64, &[u8]) = match value {
            Some(v) => (v.len() as u64, v.as_bytes()),
            None => (u64::MAX, &[]), // Tombstone
        };

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        writer
            .write_all(&timestamp.to_le_bytes())
            .map_err(|e| Status::internal(e.to_string()))?;
        writer
            .write_all(&key_len.to_le_bytes())
            .map_err(|e| Status::internal(e.to_string()))?;
        writer
            .write_all(&val_len.to_le_bytes())
            .map_err(|e| Status::internal(e.to_string()))?;
        writer
            .write_all(key_bytes)
            .map_err(|e| Status::internal(e.to_string()))?;
        if val_len != u64::MAX {
            writer
                .write_all(val_bytes)
                .map_err(|e| Status::internal(e.to_string()))?;
        }

        Ok(())
    }

    /// Append an entry to the WAL with pipelining support.
    /// Returns two receivers: one for when it's written to OS buffer, one for when it's synced to disk.
    pub async fn append_pipelined(
        &self,
        key: &str,
        value: &Option<String>,
    ) -> Result<(oneshot::Receiver<()>, oneshot::Receiver<Result<(), Status>>), Status> {
        let (written_tx, written_rx) = oneshot::channel();
        let (synced_tx, synced_rx) = oneshot::channel();

        let request = FlushRequest::Single(WriteRequest {
            key: key.to_string(),
            value: value.clone(),
            written_tx: Some(written_tx),
            synced_tx,
        });

        let sender = {
            let guard = self.request_tx.lock().unwrap();
            guard
                .as_ref()
                .ok_or_else(|| Status::internal("WAL writer is shut down"))?
                .clone()
        };

        sender
            .send(request)
            .await
            .map_err(|_| Status::internal("WAL writer channel closed"))?;

        Ok((written_rx, synced_rx))
    }

    /// Append multiple entries to the WAL as a batch with pipelining support.
    /// Returns two receivers: one for when all data is written to OS buffer, one for when it's synced to disk.
    pub async fn append_batch_pipelined(
        &self,
        entries: Vec<(String, Option<String>)>,
    ) -> Result<(oneshot::Receiver<()>, oneshot::Receiver<Result<(), Status>>), Status> {
        let (written_tx, written_rx) = oneshot::channel();
        let (synced_tx, synced_rx) = oneshot::channel();

        let request = FlushRequest::Batch(BatchWriteRequest {
            entries,
            written_tx: Some(written_tx),
            synced_tx,
        });

        let sender = {
            let guard = self.request_tx.lock().unwrap();
            guard
                .as_ref()
                .ok_or_else(|| Status::internal("WAL writer is shut down"))?
                .clone()
        };

        sender
            .send(request)
            .await
            .map_err(|_| Status::internal("WAL writer channel closed"))?;

        Ok((written_rx, synced_rx))
    }

    /// Write multiple entries to the WAL (used for recovery re-write)
    #[allow(clippy::result_large_err)]
    pub fn write_entries(&self, entries: &MemTable) -> Result<(), Status> {
        let mut guard = self.inner.lock().unwrap();

        for (key, value) in entries {
            Self::write_entry_to_file(&mut guard.writer, key, value)?;
        }

        guard
            .writer
            .sync_data()
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(())
    }

    /// Rotate WAL: create a new WAL file (old WAL is preserved for replication)
    #[allow(clippy::result_large_err)]
    pub fn rotate(&self) -> Result<u64, Status> {
        let new_wal_id = self.current_id.load(Ordering::SeqCst) + 1;
        let (new_file, new_path) = Self::create_wal_file(&self.data_dir, new_wal_id)?;

        // Swap WAL file
        {
            let mut guard = self.inner.lock().unwrap();

            // Ensure old WAL data is synced
            guard
                .writer
                .sync_data()
                .map_err(|e| Status::internal(e.to_string()))?;

            guard.writer = new_file;
            guard.path = new_path.clone();
        }

        self.current_id.store(new_wal_id, Ordering::SeqCst);

        println!("WAL rotated to {:?}", new_path);

        Ok(new_wal_id)
    }

    /// Read all entries from a WAL file
    #[allow(clippy::result_large_err)]
    pub fn read_entries(path: &Path) -> Result<MemTable, Status> {
        let mut file = match File::open(path) {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Ok(BTreeMap::new());
            }
            Err(e) => return Err(Status::internal(e.to_string())),
        };

        // Verify header
        let mut magic = [0u8; 9];
        if file.read_exact(&mut magic).is_err() {
            return Ok(BTreeMap::new()); // Empty file
        }
        if &magic != WAL_MAGIC_BYTES {
            return Err(Status::internal("Invalid WAL magic bytes"));
        }

        let mut version_bytes = [0u8; 4];
        file.read_exact(&mut version_bytes)
            .map_err(|e| Status::internal(e.to_string()))?;
        let version = u32::from_le_bytes(version_bytes);
        if version != WAL_VERSION {
            return Err(Status::internal(format!(
                "Unsupported WAL version: {}",
                version
            )));
        }

        // Read entries
        let mut memtable = BTreeMap::new();

        loop {
            let mut ts_bytes = [0u8; 8];
            if file.read_exact(&mut ts_bytes).is_err() {
                break; // End of file
            }

            let mut klen_bytes = [0u8; 8];
            let mut vlen_bytes = [0u8; 8];

            file.read_exact(&mut klen_bytes)
                .map_err(|e| Status::internal(e.to_string()))?;
            file.read_exact(&mut vlen_bytes)
                .map_err(|e| Status::internal(e.to_string()))?;

            let key_len = u64::from_le_bytes(klen_bytes);
            let val_len = u64::from_le_bytes(vlen_bytes);

            let mut key_buf = vec![0u8; key_len as usize];
            file.read_exact(&mut key_buf)
                .map_err(|e| Status::internal(e.to_string()))?;
            let key = String::from_utf8_lossy(&key_buf).to_string();

            let value = if val_len == u64::MAX {
                None // Tombstone
            } else {
                let mut val_buf = vec![0u8; val_len as usize];
                file.read_exact(&mut val_buf)
                    .map_err(|e| Status::internal(e.to_string()))?;
                Some(String::from_utf8_lossy(&val_buf).to_string())
            };

            memtable.insert(key, value);
        }

        Ok(memtable)
    }

    /// Parse WAL filename and return WAL ID
    pub fn parse_wal_filename(filename: &str) -> Option<u64> {
        if filename.starts_with("wal_") && filename.ends_with(".log") {
            filename[4..filename.len() - 4].parse::<u64>().ok()
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    /// Test-only WAL writer (synchronous, no group commit)
    struct WalWriter {
        writer: Arc<Mutex<File>>,
        path: Arc<Mutex<PathBuf>>,
        current_id: Arc<AtomicU64>,
        data_dir: PathBuf,
    }

    impl WalWriter {
        fn new(data_dir: &Path, wal_id: u64) -> Result<Self, Status> {
            let (file, path) = Self::create_wal_file(data_dir, wal_id)?;
            Ok(WalWriter {
                writer: Arc::new(Mutex::new(file)),
                path: Arc::new(Mutex::new(path)),
                current_id: Arc::new(AtomicU64::new(wal_id)),
                data_dir: data_dir.to_path_buf(),
            })
        }

        fn current_id(&self) -> u64 {
            self.current_id.load(Ordering::SeqCst)
        }

        fn create_wal_file(data_dir: &Path, wal_id: u64) -> Result<(File, PathBuf), Status> {
            let wal_path = data_dir.join(format!("wal_{:05}.log", wal_id));
            let mut file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(&wal_path)
                .map_err(|e| Status::internal(format!("Failed to create WAL: {}", e)))?;

            file.write_all(WAL_MAGIC_BYTES)
                .map_err(|e| Status::internal(e.to_string()))?;
            file.write_all(&WAL_VERSION.to_le_bytes())
                .map_err(|e| Status::internal(e.to_string()))?;
            file.sync_all()
                .map_err(|e| Status::internal(e.to_string()))?;

            Ok((file, wal_path))
        }

        fn append(&self, key: &str, value: &Option<String>) -> Result<(), Status> {
            let mut wal = self.writer.lock().unwrap();

            let key_bytes = key.as_bytes();
            let key_len = key_bytes.len() as u64;
            let (val_len, val_bytes): (u64, &[u8]) = match value {
                Some(v) => (v.len() as u64, v.as_bytes()),
                None => (u64::MAX, &[]),
            };

            let timestamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs();

            wal.write_all(&timestamp.to_le_bytes())
                .map_err(|e| Status::internal(e.to_string()))?;
            wal.write_all(&key_len.to_le_bytes())
                .map_err(|e| Status::internal(e.to_string()))?;
            wal.write_all(&val_len.to_le_bytes())
                .map_err(|e| Status::internal(e.to_string()))?;
            wal.write_all(key_bytes)
                .map_err(|e| Status::internal(e.to_string()))?;
            if val_len != u64::MAX {
                wal.write_all(val_bytes)
                    .map_err(|e| Status::internal(e.to_string()))?;
            }

            wal.sync_all()
                .map_err(|e| Status::internal(e.to_string()))?;

            Ok(())
        }

        fn rotate(&self) -> Result<u64, Status> {
            let new_wal_id = self.current_id.load(Ordering::SeqCst) + 1;
            let (new_file, new_path) = Self::create_wal_file(&self.data_dir, new_wal_id)?;

            {
                let mut wal_writer = self.writer.lock().unwrap();
                let mut wal_path = self.path.lock().unwrap();

                wal_writer
                    .sync_all()
                    .map_err(|e| Status::internal(e.to_string()))?;

                *wal_writer = new_file;
                *wal_path = new_path;
            }

            self.current_id.store(new_wal_id, Ordering::SeqCst);
            Ok(new_wal_id)
        }
    }

    #[test]
    fn test_wal_creation_and_header() {
        let dir = tempdir().unwrap();
        let wal_id = 123;
        let _wal = WalWriter::new(dir.path(), wal_id).unwrap();

        let wal_path = dir.path().join(format!("wal_{:05}.log", wal_id));
        assert!(wal_path.exists());

        let mut file = File::open(wal_path).unwrap();
        let mut magic = [0u8; 9];
        file.read_exact(&mut magic).unwrap();
        assert_eq!(&magic, WAL_MAGIC_BYTES);

        let mut version_bytes = [0u8; 4];
        file.read_exact(&mut version_bytes).unwrap();
        assert_eq!(u32::from_le_bytes(version_bytes), WAL_VERSION);
    }

    #[test]
    fn test_wal_append_and_read() {
        let dir = tempdir().unwrap();
        let wal = WalWriter::new(dir.path(), 1).unwrap();

        wal.append("k1", &Some("v1".to_string())).unwrap();
        wal.append("k2", &Some("v2".to_string())).unwrap();
        wal.append("k1", &None).unwrap(); // Tombstone

        let path = wal.path.lock().unwrap().clone();
        let entries = GroupCommitWalWriter::read_entries(&path).unwrap();

        assert_eq!(entries.len(), 2);
        assert_eq!(entries.get("k1"), Some(&None));
        assert_eq!(entries.get("k2"), Some(&Some("v2".to_string())));
    }

    #[test]
    fn test_wal_rotation() {
        let dir = tempdir().unwrap();
        let wal = WalWriter::new(dir.path(), 1).unwrap();

        wal.append("k1", &Some("v1".to_string())).unwrap();
        let old_path = wal.path.lock().unwrap().clone();

        let new_id = wal.rotate().unwrap();
        assert_eq!(new_id, 2);
        assert_eq!(wal.current_id(), 2);

        let new_path = wal.path.lock().unwrap().clone();
        assert_ne!(old_path, new_path);
        assert!(new_path.exists());

        wal.append("k2", &Some("v2".to_string())).unwrap();

        // Old WAL should still have k1
        let old_entries = GroupCommitWalWriter::read_entries(&old_path).unwrap();
        assert_eq!(old_entries.get("k1"), Some(&Some("v1".to_string())));
        assert!(!old_entries.contains_key("k2"));

        // New WAL should have k2
        let new_entries = GroupCommitWalWriter::read_entries(&new_path).unwrap();
        assert_eq!(new_entries.get("k2"), Some(&Some("v2".to_string())));
        assert!(!new_entries.contains_key("k1"));
    }

    #[test]
    fn test_parse_wal_filename() {
        assert_eq!(
            GroupCommitWalWriter::parse_wal_filename("wal_00001.log"),
            Some(1)
        );
        assert_eq!(
            GroupCommitWalWriter::parse_wal_filename("wal_12345.log"),
            Some(12345)
        );
        assert_eq!(
            GroupCommitWalWriter::parse_wal_filename("wal_00000.log"),
            Some(0)
        );
        assert_eq!(GroupCommitWalWriter::parse_wal_filename("other.log"), None);
        assert_eq!(
            GroupCommitWalWriter::parse_wal_filename("wal_123.data"),
            None
        );
    }
}
