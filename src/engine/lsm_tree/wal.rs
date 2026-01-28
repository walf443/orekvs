use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{Cursor, Read};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, oneshot};
use tonic::Status;

use super::buffer_pool::PooledBuffer;
use super::memtable::{MemTable, MemValue};
use crate::engine::wal::{
    GroupCommitConfig, SeqGenerator, WAL_FORMAT_VERSION, WalWriter, read_block, read_wal_header,
    write_block, write_wal_header,
};

/// WAL entry with sequence number for recovery filtering
#[derive(Debug, Clone)]
pub struct WalEntry {
    pub seq: u64,
    pub key: String,
    pub value: Option<String>,
    pub expire_at: u64,
}

/// Write request for group commit with pipelining
struct WriteRequest {
    seq: u64,
    key: String,
    value: Option<String>,
    expire_at: u64,
    /// Notified when data is written to OS buffer
    written_tx: Option<oneshot::Sender<()>>,
    /// Notified when data is synced to disk (returns the sequence number)
    synced_tx: oneshot::Sender<Result<u64, Status>>,
}

/// Batch write request - multiple entries with single notification
struct BatchWriteRequest {
    entries: Vec<(u64, String, Option<String>, u64)>, // (seq, key, value, expire_at)
    /// Notified when all data is written to OS buffer
    written_tx: Option<oneshot::Sender<()>>,
    /// Notified when all data is synced to disk (returns the max sequence number)
    synced_tx: oneshot::Sender<Result<u64, Status>>,
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
    /// Sequence number generator for WAL entries (LSN)
    seq_gen: Arc<SeqGenerator>,
    /// Data directory
    data_dir: PathBuf,
    /// Inner state shared with background task
    inner: Arc<Mutex<WalWriterInner>>,
    /// Group commit configuration
    config: GroupCommitConfig,
    /// Handle to the background flusher task
    flusher_handle: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
}

impl std::fmt::Debug for GroupCommitWalWriter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GroupCommitWalWriter")
            .field("current_id", &self.current_id)
            .field("seq_gen", &self.seq_gen)
            .field("data_dir", &self.data_dir)
            .field("config", &self.config)
            .finish()
    }
}

impl Clone for GroupCommitWalWriter {
    fn clone(&self) -> Self {
        GroupCommitWalWriter {
            request_tx: Arc::clone(&self.request_tx),
            current_id: Arc::clone(&self.current_id),
            seq_gen: Arc::clone(&self.seq_gen),
            data_dir: self.data_dir.clone(),
            inner: Arc::clone(&self.inner),
            config: self.config,
            flusher_handle: Arc::clone(&self.flusher_handle),
        }
    }
}

impl GroupCommitWalWriter {
    /// Create a new group commit WAL writer with initial sequence number
    #[allow(clippy::result_large_err)]
    pub fn new_with_seq(
        data_dir: &Path,
        wal_id: u64,
        config: GroupCommitConfig,
        initial_seq: u64,
    ) -> Result<Self, Status> {
        let (file, path) = Self::create_wal_file(data_dir, wal_id)?;

        let inner = Arc::new(Mutex::new(WalWriterInner { writer: file, path }));

        let (tx, rx) = mpsc::channel::<FlushRequest>(1024);

        // Start background flusher
        let inner_clone = Arc::clone(&inner);
        let batch_interval = config.batch_interval_micros;
        let handle = tokio::spawn(async move {
            Self::background_flusher(rx, inner_clone, batch_interval).await;
        });

        Ok(GroupCommitWalWriter {
            request_tx: Arc::new(Mutex::new(Some(tx))),
            current_id: Arc::new(AtomicU64::new(wal_id)),
            seq_gen: Arc::new(SeqGenerator::new(initial_seq)),
            data_dir: data_dir.to_path_buf(),
            inner,
            config,
            flusher_handle: Arc::new(Mutex::new(Some(handle))),
        })
    }

    /// Get the current (next) sequence number
    pub fn current_seq(&self) -> u64 {
        self.seq_gen.current()
    }

    /// Allocate the next sequence number
    fn allocate_seq(&self) -> u64 {
        self.seq_gen.allocate()
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

    /// Get data directory path
    #[allow(dead_code)]
    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    /// Delete WAL files up to (and including) the given ID
    pub fn delete_wals_up_to(&self, max_id: u64) -> std::io::Result<()> {
        use crate::engine::wal::parse_wal_filename;

        for entry in std::fs::read_dir(&self.data_dir)? {
            let entry = entry?;
            let filename = entry.file_name();
            let filename_str = filename.to_string_lossy();

            if let Some(id) = parse_wal_filename(&filename_str)
                && id <= max_id
            {
                std::fs::remove_file(entry.path())?;
                println!("Deleted old WAL: {:?}", entry.path());
            }
        }

        Ok(())
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

        // Write header using common format
        write_wal_header(&mut file, WAL_FORMAT_VERSION)
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

        // Stage 1: Serialize all entries to a pooled buffer (reduces allocation overhead)
        let mut entries_buf = PooledBuffer::new(4096);
        let mut max_seq = 0u64;
        for req in pending.iter() {
            match req {
                FlushRequest::Single(single) => {
                    max_seq = max_seq.max(single.seq);
                    if let Err(e) = Self::serialize_entry(
                        &mut entries_buf,
                        single.seq,
                        &single.key,
                        &single.value,
                        single.expire_at,
                    ) {
                        Self::notify_all_error(pending, e);
                        return;
                    }
                }
                FlushRequest::Batch(batch) => {
                    for (seq, key, value, expire_at) in &batch.entries {
                        max_seq = max_seq.max(*seq);
                        if let Err(e) =
                            Self::serialize_entry(&mut entries_buf, *seq, key, value, *expire_at)
                        {
                            Self::notify_all_error(pending, e);
                            return;
                        }
                    }
                }
            }
        }

        // Stage 2: Write block (compressed if large enough)
        if let Err(e) = Self::write_block_internal(&mut guard.writer, &entries_buf) {
            Self::notify_all_error(pending, e);
            return;
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

        // Stage 3: Sync data to disk (fdatasync - skips metadata sync for better performance)
        let sync_result = guard
            .writer
            .sync_data()
            .map_err(|e| Status::internal(e.to_string()));

        // Stage 4: Notify "synced" with their sequence numbers
        for req in pending.drain(..) {
            match req {
                FlushRequest::Single(single) => {
                    let result = sync_result.clone().map(|()| single.seq);
                    let _ = single.synced_tx.send(result);
                }
                FlushRequest::Batch(batch) => {
                    // Return the max sequence number for the batch
                    let batch_max_seq = batch
                        .entries
                        .iter()
                        .map(|(seq, _, _, _)| *seq)
                        .max()
                        .unwrap_or(0);
                    let result = sync_result.clone().map(|()| batch_max_seq);
                    let _ = batch.synced_tx.send(result);
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

    /// Serialize a single entry to a buffer
    /// v4 format: [seq: u64][timestamp: u64][expire_at: u64][key_len: u64][value_len: u64][key][value]
    #[allow(clippy::result_large_err)]
    fn serialize_entry(
        buf: &mut Vec<u8>,
        seq: u64,
        key: &str,
        value: &Option<String>,
        expire_at: u64,
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

        buf.extend_from_slice(&seq.to_le_bytes());
        buf.extend_from_slice(&timestamp.to_le_bytes());
        buf.extend_from_slice(&expire_at.to_le_bytes());
        buf.extend_from_slice(&key_len.to_le_bytes());
        buf.extend_from_slice(&val_len.to_le_bytes());
        buf.extend_from_slice(key_bytes);
        if val_len != u64::MAX {
            buf.extend_from_slice(val_bytes);
        }

        Ok(())
    }

    /// Write a block to the file, compressing if size exceeds threshold
    ///
    /// Uses the common block format from `crate::engine::wal::write_block`.
    #[allow(clippy::result_large_err)]
    fn write_block_internal(writer: &mut File, data: &[u8]) -> Result<(), Status> {
        write_block(writer, data, true).map_err(|e| Status::internal(e.to_string()))
    }

    /// Append an entry to the WAL with pipelining support and TTL.
    /// Returns two receivers: one for when it's written to OS buffer, one for when it's synced to disk (with seq number).
    pub async fn append_pipelined_with_ttl(
        &self,
        key: &str,
        value: &Option<String>,
        expire_at: u64,
    ) -> Result<
        (
            oneshot::Receiver<()>,
            oneshot::Receiver<Result<u64, Status>>,
        ),
        Status,
    > {
        let (written_tx, written_rx) = oneshot::channel();
        let (synced_tx, synced_rx) = oneshot::channel();

        let seq = self.allocate_seq();
        let request = FlushRequest::Single(WriteRequest {
            seq,
            key: key.to_string(),
            value: value.clone(),
            expire_at,
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

    /// Append multiple entries to the WAL as a batch with pipelining support and TTL.
    /// Returns two receivers: one for when all data is written to OS buffer, one for when it's synced to disk (with max seq number).
    pub async fn append_batch_pipelined_with_ttl(
        &self,
        entries: Vec<(String, Option<String>, u64)>,
    ) -> Result<
        (
            oneshot::Receiver<()>,
            oneshot::Receiver<Result<u64, Status>>,
        ),
        Status,
    > {
        let (written_tx, written_rx) = oneshot::channel();
        let (synced_tx, synced_rx) = oneshot::channel();

        // Allocate sequence numbers for each entry
        let entries_with_seq: Vec<(u64, String, Option<String>, u64)> = entries
            .into_iter()
            .map(|(key, value, expire_at)| {
                let seq = self.allocate_seq();
                (seq, key, value, expire_at)
            })
            .collect();

        let request = FlushRequest::Batch(BatchWriteRequest {
            entries: entries_with_seq,
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
    /// Returns the maximum sequence number written
    #[allow(clippy::result_large_err)]
    pub fn write_entries(&self, entries: &MemTable) -> Result<u64, Status> {
        let mut guard = self.inner.lock().unwrap();

        // Serialize all entries to buffer with sequence numbers
        let mut buf = Vec::new();
        let mut max_seq = 0u64;
        for (key, mem_value) in entries {
            let seq = self.allocate_seq();
            max_seq = max_seq.max(seq);
            Self::serialize_entry(&mut buf, seq, key, &mem_value.value, mem_value.expire_at)?;
        }

        // Write as a block
        Self::write_block_internal(&mut guard.writer, &buf)?;

        guard
            .writer
            .sync_data()
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(max_seq)
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

        // Verify header using common format
        let version = match read_wal_header(&mut file) {
            Ok(v) => v,
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                return Ok(BTreeMap::new()); // Empty file
            }
            Err(e) => return Err(Status::internal(e.to_string())),
        };

        if version != WAL_FORMAT_VERSION {
            return Err(Status::internal(format!(
                "Unsupported WAL version: {}. Only version {} is supported.",
                version, WAL_FORMAT_VERSION
            )));
        }

        Self::read_entries_v4(&mut file).map(|entries| {
            entries
                .into_iter()
                .map(|e| (e.key, MemValue::new_with_ttl(e.value, e.expire_at)))
                .collect()
        })
    }

    /// Read all entries from a WAL file with their sequence numbers (v4 only)
    /// Returns entries with seq > min_seq for incremental recovery
    #[allow(clippy::result_large_err)]
    pub fn read_entries_with_seq(path: &Path, min_seq: u64) -> Result<Vec<WalEntry>, Status> {
        let mut file = match File::open(path) {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Ok(Vec::new());
            }
            Err(e) => return Err(Status::internal(e.to_string())),
        };

        // Verify header using common format
        let version = match read_wal_header(&mut file) {
            Ok(v) => v,
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                return Ok(Vec::new()); // Empty file
            }
            Err(e) => return Err(Status::internal(e.to_string())),
        };

        if version != WAL_FORMAT_VERSION {
            return Err(Status::internal(format!(
                "Unsupported WAL version: {}. Only version {} is supported.",
                version, WAL_FORMAT_VERSION
            )));
        }

        let all_entries = Self::read_entries_v4(&mut file)?;
        // Filter by sequence number for incremental recovery
        Ok(all_entries
            .into_iter()
            .filter(|e| e.seq > min_seq)
            .collect())
    }

    /// Get the maximum sequence number in a WAL file
    /// Returns 0 if the file is empty or cannot be read
    pub fn get_max_seq(path: &Path) -> u64 {
        let mut file = match File::open(path) {
            Ok(f) => f,
            Err(_) => return 0,
        };

        // Verify header
        if read_wal_header(&mut file).is_err() {
            return 0;
        }

        // Read all entries and find max seq
        match Self::read_entries_v4(&mut file) {
            Ok(entries) => entries.iter().map(|e| e.seq).max().unwrap_or(0),
            Err(_) => 0,
        }
    }

    /// Read entries from WAL v4 format (with sequence numbers)
    ///
    /// Uses the common block format from `crate::engine::wal::read_block`.
    #[allow(clippy::result_large_err)]
    fn read_entries_v4(file: &mut File) -> Result<Vec<WalEntry>, Status> {
        let mut entries = Vec::new();

        loop {
            match read_block(file) {
                Ok(Some(entries_data)) => {
                    // Parse entries from buffer (v4 format with sequence numbers)
                    Self::parse_entries_from_buffer_v4(&entries_data, &mut entries)?;
                }
                Ok(None) => {
                    // End of file
                    break;
                }
                Err(e) => {
                    // Checksum mismatch or other error - stop recovery
                    eprintln!("WAL block read error: {}. Stopping recovery.", e);
                    break;
                }
            }
        }

        Ok(entries)
    }

    /// Parse entries from a v4 buffer into a list of WalEntry (with sequence numbers and TTL)
    #[allow(clippy::result_large_err)]
    fn parse_entries_from_buffer_v4(buf: &[u8], entries: &mut Vec<WalEntry>) -> Result<(), Status> {
        let mut cursor = Cursor::new(buf);

        loop {
            // v4 format: [seq: u64][timestamp: u64][expire_at: u64][key_len: u64][value_len: u64][key][value]
            let mut seq_bytes = [0u8; 8];
            if cursor.read_exact(&mut seq_bytes).is_err() {
                break; // End of buffer
            }
            let seq = u64::from_le_bytes(seq_bytes);

            let mut ts_bytes = [0u8; 8];
            cursor
                .read_exact(&mut ts_bytes)
                .map_err(|e| Status::internal(e.to_string()))?;
            // timestamp is written but not used during recovery

            let mut expire_at_bytes = [0u8; 8];
            cursor
                .read_exact(&mut expire_at_bytes)
                .map_err(|e| Status::internal(e.to_string()))?;
            let expire_at = u64::from_le_bytes(expire_at_bytes);

            let mut klen_bytes = [0u8; 8];
            let mut vlen_bytes = [0u8; 8];

            cursor
                .read_exact(&mut klen_bytes)
                .map_err(|e| Status::internal(e.to_string()))?;
            cursor
                .read_exact(&mut vlen_bytes)
                .map_err(|e| Status::internal(e.to_string()))?;

            let key_len = u64::from_le_bytes(klen_bytes);
            let val_len = u64::from_le_bytes(vlen_bytes);

            let mut key_buf = vec![0u8; key_len as usize];
            cursor
                .read_exact(&mut key_buf)
                .map_err(|e| Status::internal(e.to_string()))?;
            let key = String::from_utf8_lossy(&key_buf).to_string();

            let value = if val_len == u64::MAX {
                None // Tombstone
            } else {
                let mut val_buf = vec![0u8; val_len as usize];
                cursor
                    .read_exact(&mut val_buf)
                    .map_err(|e| Status::internal(e.to_string()))?;
                Some(String::from_utf8_lossy(&val_buf).to_string())
            };

            entries.push(WalEntry {
                seq,
                key,
                value,
                expire_at,
            });
        }

        Ok(())
    }

    /// Parse WAL filename and return WAL ID
    ///
    /// Delegates to the common WAL utility function.
    pub fn parse_wal_filename(filename: &str) -> Option<u64> {
        crate::engine::wal::parse_wal_filename(filename)
    }
}

impl WalWriter for GroupCommitWalWriter {
    fn current_id(&self) -> u64 {
        self.current_id.load(Ordering::SeqCst)
    }

    fn current_seq(&self) -> u64 {
        self.seq_gen.current()
    }

    fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    fn rotate(&self) -> std::io::Result<u64> {
        GroupCommitWalWriter::rotate(self).map_err(|e| std::io::Error::other(e.message()))
    }

    fn delete_wals_up_to(&self, max_id: u64) -> std::io::Result<()> {
        GroupCommitWalWriter::delete_wals_up_to(self, max_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::wal::{WAL_FORMAT_VERSION, WAL_MAGIC};
    use tempfile::tempdir;

    /// Test-only WAL writer (synchronous, no group commit)
    struct WalWriter {
        writer: Arc<Mutex<File>>,
        path: Arc<Mutex<PathBuf>>,
        current_id: Arc<AtomicU64>,
        next_seq: Arc<AtomicU64>,
        data_dir: PathBuf,
    }

    impl WalWriter {
        fn new(data_dir: &Path, wal_id: u64) -> Result<Self, Status> {
            let (file, path) = Self::create_wal_file(data_dir, wal_id)?;
            Ok(WalWriter {
                writer: Arc::new(Mutex::new(file)),
                path: Arc::new(Mutex::new(path)),
                current_id: Arc::new(AtomicU64::new(wal_id)),
                next_seq: Arc::new(AtomicU64::new(1)),
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

            write_wal_header(&mut file, WAL_FORMAT_VERSION)
                .map_err(|e| Status::internal(e.to_string()))?;
            file.sync_all()
                .map_err(|e| Status::internal(e.to_string()))?;

            Ok((file, wal_path))
        }

        fn append(&self, key: &str, value: &Option<String>) -> Result<(), Status> {
            self.append_with_ttl(key, value, 0)
        }

        fn append_with_ttl(
            &self,
            key: &str,
            value: &Option<String>,
            expire_at: u64,
        ) -> Result<(), Status> {
            let mut wal = self.writer.lock().unwrap();

            // Allocate sequence number
            let seq = self.next_seq.fetch_add(1, Ordering::SeqCst);

            // Serialize entry to buffer (v4 format with sequence number and TTL)
            let mut buf = Vec::new();
            GroupCommitWalWriter::serialize_entry(&mut buf, seq, key, value, expire_at)?;

            // Write as uncompressed block (test data is small)
            write_block(&mut *wal, &buf, false).map_err(|e| Status::internal(e.to_string()))?;

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
        assert_eq!(&magic, WAL_MAGIC);

        let mut version_bytes = [0u8; 4];
        file.read_exact(&mut version_bytes).unwrap();
        assert_eq!(u32::from_le_bytes(version_bytes), WAL_FORMAT_VERSION);
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
        assert_eq!(entries.get("k1"), Some(&MemValue::new(None)));
        assert_eq!(
            entries.get("k2"),
            Some(&MemValue::new(Some("v2".to_string())))
        );
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
        assert_eq!(
            old_entries.get("k1"),
            Some(&MemValue::new(Some("v1".to_string())))
        );
        assert!(!old_entries.contains_key("k2"));

        // New WAL should have k2
        let new_entries = GroupCommitWalWriter::read_entries(&new_path).unwrap();
        assert_eq!(
            new_entries.get("k2"),
            Some(&MemValue::new(Some("v2".to_string())))
        );
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
