use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, oneshot};
use tonic::Status;

use super::memtable::MemTable;

const WAL_MAGIC_BYTES: &[u8; 9] = b"ORELSMWAL";
const WAL_VERSION: u32 = 1;

/// WAL (Write-Ahead Log) writer for crash recovery
#[derive(Debug)]
pub struct WalWriter {
    /// Current active WAL file handle
    writer: Arc<Mutex<File>>,
    /// Path to current WAL file
    path: Arc<Mutex<PathBuf>>,
    /// Current WAL ID
    current_id: Arc<AtomicU64>,
    /// Data directory
    data_dir: PathBuf,
}

impl Clone for WalWriter {
    fn clone(&self) -> Self {
        WalWriter {
            writer: Arc::clone(&self.writer),
            path: Arc::clone(&self.path),
            current_id: Arc::clone(&self.current_id),
            data_dir: self.data_dir.clone(),
        }
    }
}

impl WalWriter {
    /// Create a new WAL writer
    #[allow(clippy::result_large_err)]
    pub fn new(data_dir: &Path, wal_id: u64) -> Result<Self, Status> {
        let (file, path) = Self::create_wal_file(data_dir, wal_id)?;
        Ok(WalWriter {
            writer: Arc::new(Mutex::new(file)),
            path: Arc::new(Mutex::new(path)),
            current_id: Arc::new(AtomicU64::new(wal_id)),
            data_dir: data_dir.to_path_buf(),
        })
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

        // Write header
        file.write_all(WAL_MAGIC_BYTES)
            .map_err(|e| Status::internal(e.to_string()))?;
        file.write_all(&WAL_VERSION.to_le_bytes())
            .map_err(|e| Status::internal(e.to_string()))?;
        file.sync_all()
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok((file, wal_path))
    }

    /// Append an entry to the WAL
    #[allow(clippy::result_large_err)]
    pub fn append(&self, key: &str, value: &Option<String>) -> Result<(), Status> {
        let mut wal = self.writer.lock().unwrap();

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

        // Write entry
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

        // Sync to disk for durability
        wal.sync_all()
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(())
    }

    /// Write multiple entries to the WAL (used for recovery re-write)
    #[allow(clippy::result_large_err)]
    pub fn write_entries(&self, entries: &MemTable) -> Result<(), Status> {
        let mut wal = self.writer.lock().unwrap();

        for (key, value) in entries {
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
        }

        wal.sync_all()
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
            let mut wal_writer = self.writer.lock().unwrap();
            let mut wal_path = self.path.lock().unwrap();

            // Ensure old WAL is synced
            wal_writer
                .sync_all()
                .map_err(|e| Status::internal(e.to_string()))?;

            *wal_writer = new_file;
            *wal_path = new_path.clone();
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

/// Write request for group commit
struct WriteRequest {
    key: String,
    value: Option<String>,
    response: oneshot::Sender<Result<(), Status>>,
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
    request_tx: Arc<Mutex<Option<mpsc::Sender<WriteRequest>>>>,
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

        let (tx, rx) = mpsc::channel::<WriteRequest>(1024);

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
        mut rx: mpsc::Receiver<WriteRequest>,
        inner: Arc<Mutex<WalWriterInner>>,
        batch_interval_micros: u64,
    ) {
        let mut pending: Vec<WriteRequest> = Vec::new();

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
    fn flush_and_notify(inner: &Arc<Mutex<WalWriterInner>>, pending: &mut Vec<WriteRequest>) {
        let result = Self::flush_batch(inner, pending);
        for req in pending.drain(..) {
            let _ = req.response.send(result.clone());
        }
    }

    /// Write all pending entries to WAL and sync once
    #[allow(clippy::result_large_err)]
    fn flush_batch(
        inner: &Arc<Mutex<WalWriterInner>>,
        requests: &[WriteRequest],
    ) -> Result<(), Status> {
        let mut guard = inner.lock().unwrap();

        for req in requests {
            Self::write_entry_to_file(&mut guard.writer, &req.key, &req.value)?;
        }

        // Single sync_all for all entries
        guard
            .writer
            .sync_all()
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(())
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

    /// Append an entry to the WAL asynchronously (group commit)
    pub async fn append_async(&self, key: &str, value: &Option<String>) -> Result<(), Status> {
        let (tx, rx) = oneshot::channel();

        let request = WriteRequest {
            key: key.to_string(),
            value: value.clone(),
            response: tx,
        };

        // Clone the sender while holding the lock briefly
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

        rx.await
            .map_err(|_| Status::internal("WAL response channel closed"))?
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
            .sync_all()
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

            // Ensure old WAL is synced
            guard
                .writer
                .sync_all()
                .map_err(|e| Status::internal(e.to_string()))?;

            guard.writer = new_file;
            guard.path = new_path.clone();
        }

        self.current_id.store(new_wal_id, Ordering::SeqCst);

        println!("WAL rotated to {:?}", new_path);

        Ok(new_wal_id)
    }

    /// Read all entries from a WAL file (same as WalWriter)
    #[allow(clippy::result_large_err)]
    pub fn read_entries(path: &Path) -> Result<MemTable, Status> {
        WalWriter::read_entries(path)
    }

    /// Parse WAL filename and return WAL ID (same as WalWriter)
    pub fn parse_wal_filename(filename: &str) -> Option<u64> {
        WalWriter::parse_wal_filename(filename)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

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
        let entries = WalWriter::read_entries(&path).unwrap();

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
        let old_entries = WalWriter::read_entries(&old_path).unwrap();
        assert_eq!(old_entries.get("k1"), Some(&Some("v1".to_string())));
        assert!(!old_entries.contains_key("k2"));

        // New WAL should have k2
        let new_entries = WalWriter::read_entries(&new_path).unwrap();
        assert_eq!(new_entries.get("k2"), Some(&Some("v2".to_string())));
        assert!(!new_entries.contains_key("k1"));
    }

    #[test]
    fn test_parse_wal_filename() {
        assert_eq!(WalWriter::parse_wal_filename("wal_00001.log"), Some(1));
        assert_eq!(WalWriter::parse_wal_filename("wal_12345.log"), Some(12345));
        assert_eq!(WalWriter::parse_wal_filename("wal_00000.log"), Some(0));
        assert_eq!(WalWriter::parse_wal_filename("other.log"), None);
        assert_eq!(WalWriter::parse_wal_filename("wal_123.data"), None);
    }
}
