//! Write-Ahead Log (WAL) for B-tree crash recovery.
//!
//! Records all modifications before they are applied to the B-tree.
//! On recovery, replays the WAL to restore the tree to a consistent state.

use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::engine::wal::{
    GroupCommitConfig, SeqGenerator, WAL_FORMAT_VERSION, WalWriter, read_block, read_wal_header,
    write_block, write_wal_header,
};

/// Record types
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum RecordType {
    /// Insert or update a key-value pair
    Insert = 1,
    /// Delete a key
    Delete = 2,
    /// Checkpoint marker (tree is consistent up to this point)
    Checkpoint = 3,
    /// Begin batch operation
    BeginBatch = 4,
    /// End batch operation (commit point)
    EndBatch = 5,
}

impl TryFrom<u8> for RecordType {
    type Error = io::Error;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(RecordType::Insert),
            2 => Ok(RecordType::Delete),
            3 => Ok(RecordType::Checkpoint),
            4 => Ok(RecordType::BeginBatch),
            5 => Ok(RecordType::EndBatch),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Invalid record type: {}", value),
            )),
        }
    }
}

/// WAL record structure
#[derive(Debug, Clone)]
pub struct WalRecord {
    /// Sequence number
    pub seq: u64,
    /// Record type
    pub record_type: RecordType,
    /// Key (empty for checkpoint/batch records)
    pub key: String,
    /// Value (None for delete/checkpoint/batch records)
    pub value: Option<String>,
}

impl WalRecord {
    pub fn insert(seq: u64, key: String, value: String) -> Self {
        Self {
            seq,
            record_type: RecordType::Insert,
            key,
            value: Some(value),
        }
    }

    pub fn delete(seq: u64, key: String) -> Self {
        Self {
            seq,
            record_type: RecordType::Delete,
            key,
            value: None,
        }
    }

    pub fn checkpoint(seq: u64) -> Self {
        Self {
            seq,
            record_type: RecordType::Checkpoint,
            key: String::new(),
            value: None,
        }
    }

    pub fn begin_batch(seq: u64) -> Self {
        Self {
            seq,
            record_type: RecordType::BeginBatch,
            key: String::new(),
            value: None,
        }
    }

    pub fn end_batch(seq: u64) -> Self {
        Self {
            seq,
            record_type: RecordType::EndBatch,
            key: String::new(),
            value: None,
        }
    }

    /// Serialize record to a buffer
    ///
    /// Format: [seq: u64][type: u8][key_len: u32][value_len: u32][key][value]
    /// Checksum is handled by the block writer.
    pub fn serialize(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&self.seq.to_le_bytes());
        buf.push(self.record_type as u8);
        buf.extend_from_slice(&(self.key.len() as u32).to_le_bytes());

        let value_len = self.value.as_ref().map_or(u32::MAX, |v| v.len() as u32);
        buf.extend_from_slice(&value_len.to_le_bytes());

        buf.extend_from_slice(self.key.as_bytes());
        if let Some(value) = &self.value {
            buf.extend_from_slice(value.as_bytes());
        }
    }

    /// Deserialize record from bytes
    ///
    /// Format: [seq: u64][type: u8][key_len: u32][value_len: u32][key][value]
    pub fn deserialize(data: &[u8]) -> io::Result<(Self, usize)> {
        if data.len() < 17 {
            // Minimum: 8 + 1 + 4 + 4 = 17 bytes
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "Record too short",
            ));
        }

        let seq = u64::from_le_bytes(data[0..8].try_into().unwrap());
        let record_type = RecordType::try_from(data[8])?;
        let key_len = u32::from_le_bytes(data[9..13].try_into().unwrap()) as usize;
        let value_len = u32::from_le_bytes(data[13..17].try_into().unwrap());

        let header_size = 17;
        let key_end = header_size + key_len;

        if data.len() < key_end {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "Record truncated (key)",
            ));
        }

        let key = String::from_utf8(data[header_size..key_end].to_vec())
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        let (value, value_end) = if value_len == u32::MAX {
            (None, key_end)
        } else {
            let value_end = key_end + value_len as usize;
            if data.len() < value_end {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "Record truncated (value)",
                ));
            }
            let value = String::from_utf8(data[key_end..value_end].to_vec())
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            (Some(value), value_end)
        };

        Ok((
            Self {
                seq,
                record_type,
                key,
                value,
            },
            value_end,
        ))
    }
}

// ============================================================================
// WAL Reading Functions
// ============================================================================

/// Read all records from a WAL file (v4 block-based format)
pub fn read_wal_records<P: AsRef<Path>>(path: P) -> io::Result<Vec<WalRecord>> {
    let file = File::open(path)?;
    let mut reader = BufReader::new(file);

    // Read and validate header using common function
    let version = read_wal_header(&mut reader)?;

    if version != WAL_FORMAT_VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "Unsupported WAL version: {} (expected {})",
                version, WAL_FORMAT_VERSION
            ),
        ));
    }

    read_wal_records_v4(&mut reader)
}

/// Read records in v4 format (block-based, compatible with LSM-Tree)
fn read_wal_records_v4<R: Read>(reader: &mut R) -> io::Result<Vec<WalRecord>> {
    let mut records = Vec::new();

    loop {
        match read_block(reader) {
            Ok(Some(block_data)) => {
                // Parse records from block
                let mut offset = 0;
                while offset < block_data.len() {
                    match WalRecord::deserialize(&block_data[offset..]) {
                        Ok((record, size)) => {
                            records.push(record);
                            offset += size;
                        }
                        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                            break;
                        }
                        Err(e) => {
                            eprintln!("WAL record parse error: {}. Stopping recovery.", e);
                            return Ok(records);
                        }
                    }
                }
            }
            Ok(None) => {
                // End of file
                break;
            }
            Err(e) => {
                // Block read error - stop recovery
                eprintln!("WAL block read error: {}. Stopping recovery.", e);
                break;
            }
        }
    }

    Ok(records)
}

/// Read WAL records for recovery (from last checkpoint)
pub fn recover_from_wal<P: AsRef<Path>>(path: P) -> io::Result<Vec<WalRecord>> {
    let all_records = read_wal_records(path)?;

    // Find last checkpoint
    let checkpoint_idx = all_records
        .iter()
        .rposition(|r| r.record_type == RecordType::Checkpoint);

    // Find last complete batch
    let mut last_complete_batch_end = None;
    let mut batch_stack = Vec::new();

    for (i, record) in all_records.iter().enumerate() {
        match record.record_type {
            RecordType::BeginBatch => {
                batch_stack.push(i);
            }
            RecordType::EndBatch => {
                batch_stack.pop();
                last_complete_batch_end = Some(i);
            }
            _ => {}
        }
    }

    // Determine recovery start point
    let start_idx = match (checkpoint_idx, last_complete_batch_end) {
        (Some(cp), Some(batch)) => cp.max(batch) + 1,
        (Some(cp), None) => cp + 1,
        (None, Some(batch)) => batch + 1,
        (None, None) => 0,
    };

    // Return records after recovery point, excluding incomplete batches
    let mut records = Vec::new();
    let mut in_batch = false;

    for record in all_records.into_iter().skip(start_idx) {
        match record.record_type {
            RecordType::BeginBatch => {
                in_batch = true;
            }
            RecordType::EndBatch => {
                in_batch = false;
            }
            RecordType::Checkpoint => {
                // Skip checkpoint records during recovery
            }
            _ => {
                if !in_batch {
                    records.push(record);
                }
            }
        }
    }

    Ok(records)
}

/// Delete WAL file
pub fn delete_wal<P: AsRef<Path>>(path: P) -> io::Result<()> {
    if path.as_ref().exists() {
        fs::remove_file(path)?;
    }
    Ok(())
}

// ============================================================================
// Group Commit WAL Writer
// ============================================================================

use std::sync::Arc;
use std::sync::mpsc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

/// Write request for group commit
struct WriteRequest {
    record: WalRecord,
    response_tx: mpsc::Sender<io::Result<u64>>,
}

/// Inner state for WAL writer (protected by mutex)
struct WalWriterInner {
    writer: BufWriter<File>,
    path: PathBuf,
}

/// Group commit WAL writer that batches multiple writes before sync
///
/// Supports WAL rotation and is Clone-able for sharing across threads.
pub struct GroupCommitWalWriter {
    /// Channel to send write requests to background flusher (shared across clones)
    request_tx: Arc<Mutex<Option<mpsc::Sender<WriteRequest>>>>,
    /// Sequence number generator
    seq_gen: Arc<SeqGenerator>,
    /// Current WAL ID
    current_id: Arc<AtomicU64>,
    /// Data directory for WAL files
    data_dir: PathBuf,
    /// Shared writer state
    inner: Arc<Mutex<WalWriterInner>>,
    /// Group commit configuration
    config: GroupCommitConfig,
    /// Handle to the background flusher thread
    flusher_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
    /// Flag to signal shutdown
    shutdown: Arc<std::sync::atomic::AtomicBool>,
}

impl Clone for GroupCommitWalWriter {
    fn clone(&self) -> Self {
        Self {
            request_tx: Arc::clone(&self.request_tx),
            seq_gen: Arc::clone(&self.seq_gen),
            current_id: Arc::clone(&self.current_id),
            data_dir: self.data_dir.clone(),
            inner: Arc::clone(&self.inner),
            config: self.config,
            flusher_handle: Arc::clone(&self.flusher_handle),
            shutdown: Arc::clone(&self.shutdown),
        }
    }
}

impl GroupCommitWalWriter {
    /// Generate WAL file path for a given ID
    fn wal_path(data_dir: &Path, wal_id: u64) -> PathBuf {
        data_dir.join(format!("wal_{:05}.log", wal_id))
    }

    /// Create or open a group commit WAL in a directory
    ///
    /// This will scan for existing WAL files and resume from the latest one.
    pub fn open<P: AsRef<Path>>(data_dir: P, config: GroupCommitConfig) -> io::Result<Self> {
        let data_dir = data_dir.as_ref().to_path_buf();

        // Scan for existing WAL files
        let (wal_id, max_seq) = Self::scan_existing_wals(&data_dir)?;

        Self::open_with_id(&data_dir, wal_id, config, max_seq)
    }

    /// Create a new WAL with a specific ID and initial sequence number
    pub fn open_with_id(
        data_dir: &Path,
        wal_id: u64,
        config: GroupCommitConfig,
        initial_seq: u64,
    ) -> io::Result<Self> {
        let path = Self::wal_path(data_dir, wal_id);
        let file_exists = path.exists();

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)?;

        let mut writer = BufWriter::new(file);

        let max_seq = if file_exists {
            // Read existing WAL to find last sequence
            let mut max_seq = initial_seq;
            if let Ok(records) = read_wal_records(&path) {
                for record in records {
                    max_seq = max_seq.max(record.seq);
                }
            }
            // Seek to end for appending
            writer.seek(SeekFrom::End(0))?;
            max_seq
        } else {
            // Write header using common format (v4)
            write_wal_header(&mut writer, WAL_FORMAT_VERSION)?;
            writer.flush()?;
            initial_seq
        };

        let inner = Arc::new(Mutex::new(WalWriterInner {
            writer,
            path: path.clone(),
        }));

        let (request_tx, request_rx) = mpsc::channel::<WriteRequest>();
        let shutdown = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let shutdown_clone = shutdown.clone();
        let inner_clone = inner.clone();
        let batch_interval_micros = config.batch_interval_micros;

        // Start background flusher thread
        let handle = thread::spawn(move || {
            Self::background_flusher(
                request_rx,
                inner_clone,
                batch_interval_micros,
                shutdown_clone,
            );
        });

        Ok(Self {
            request_tx: Arc::new(Mutex::new(Some(request_tx))),
            seq_gen: Arc::new(SeqGenerator::new(max_seq)),
            current_id: Arc::new(AtomicU64::new(wal_id)),
            data_dir: data_dir.to_path_buf(),
            inner,
            config,
            flusher_handle: Arc::new(Mutex::new(Some(handle))),
            shutdown,
        })
    }

    /// Scan data directory for existing WAL files and return the latest ID and max sequence
    fn scan_existing_wals(data_dir: &Path) -> io::Result<(u64, u64)> {
        use crate::engine::wal::parse_wal_filename;

        let mut max_wal_id = 0u64;
        let mut max_seq = 0u64;

        if data_dir.exists() {
            for entry in fs::read_dir(data_dir)? {
                let entry = entry?;
                let filename = entry.file_name();
                let filename_str = filename.to_string_lossy();

                if let Some(id) = parse_wal_filename(&filename_str) {
                    max_wal_id = max_wal_id.max(id);

                    // Read records to find max sequence
                    let path = entry.path();
                    if let Ok(records) = read_wal_records(&path) {
                        for record in records {
                            max_seq = max_seq.max(record.seq);
                        }
                    }
                }
            }
        } else {
            fs::create_dir_all(data_dir)?;
        }

        Ok((max_wal_id, max_seq))
    }

    /// Get current WAL ID
    pub fn current_id(&self) -> u64 {
        self.current_id.load(Ordering::SeqCst)
    }

    /// Get current sequence number
    pub fn current_seq(&self) -> u64 {
        self.seq_gen.current()
    }

    /// Rotate to a new WAL file
    ///
    /// This creates a new WAL file with an incremented ID and switches to it.
    /// The old WAL file is kept for recovery purposes.
    pub fn rotate(&self) -> io::Result<u64> {
        let new_id = self.current_id.fetch_add(1, Ordering::SeqCst) + 1;
        let new_path = Self::wal_path(&self.data_dir, new_id);

        // Create new WAL file
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&new_path)?;

        let mut new_writer = BufWriter::new(file);

        // Write header using common format (v4)
        write_wal_header(&mut new_writer, WAL_FORMAT_VERSION)?;
        new_writer.flush()?;

        // Swap writer
        {
            let mut guard = self.inner.lock().unwrap();
            // Flush old writer first
            guard.writer.flush()?;
            guard.writer.get_ref().sync_data()?;

            // Replace with new writer
            guard.writer = new_writer;
            guard.path = new_path;
        }

        println!("WAL rotated to ID {}", new_id);
        Ok(new_id)
    }

    /// Background thread that batches writes and flushes periodically
    fn background_flusher(
        request_rx: mpsc::Receiver<WriteRequest>,
        inner: Arc<Mutex<WalWriterInner>>,
        batch_interval_micros: u64,
        shutdown: Arc<std::sync::atomic::AtomicBool>,
    ) {
        let batch_interval = Duration::from_micros(batch_interval_micros);
        let mut pending: Vec<WriteRequest> = Vec::new();

        loop {
            // Wait for a request with timeout
            match request_rx.recv_timeout(batch_interval) {
                Ok(req) => {
                    pending.push(req);
                    // Collect any additional pending requests (non-blocking)
                    while let Ok(req) = request_rx.try_recv() {
                        pending.push(req);
                    }
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    // Timeout - flush if we have pending writes
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    // Channel closed, flush remaining and exit
                    if !pending.is_empty() {
                        Self::flush_and_notify(&inner, &mut pending);
                    }
                    break;
                }
            }

            if !pending.is_empty() {
                Self::flush_and_notify(&inner, &mut pending);
            }

            if shutdown.load(Ordering::SeqCst) {
                break;
            }
        }
    }

    /// Flush pending writes and notify all waiting requests
    ///
    /// Uses v4 block format: all records are serialized to a buffer and written
    /// as a single block with optional compression.
    fn flush_and_notify(inner: &Arc<Mutex<WalWriterInner>>, pending: &mut Vec<WriteRequest>) {
        let mut guard = inner.lock().unwrap();

        // Stage 1: Serialize all records to a buffer (v4 format)
        let mut buf = Vec::new();
        for req in pending.iter() {
            req.record.serialize(&mut buf);
        }

        // Stage 2: Write as a single block with optional compression
        if let Err(e) = write_block(&mut guard.writer, &buf, true) {
            for req in pending.drain(..) {
                let _ = req
                    .response_tx
                    .send(Err(io::Error::new(e.kind(), e.to_string())));
            }
            return;
        }

        // Stage 3: Flush buffer
        if let Err(e) = guard.writer.flush() {
            for req in pending.drain(..) {
                let _ = req
                    .response_tx
                    .send(Err(io::Error::new(e.kind(), e.to_string())));
            }
            return;
        }

        // Stage 4: Sync to disk (fdatasync)
        let sync_result = guard.writer.get_ref().sync_data();

        // Stage 5: Notify all
        match sync_result {
            Ok(()) => {
                for req in pending.drain(..) {
                    let _ = req.response_tx.send(Ok(req.record.seq));
                }
            }
            Err(e) => {
                for req in pending.drain(..) {
                    let _ = req
                        .response_tx
                        .send(Err(io::Error::new(e.kind(), e.to_string())));
                }
            }
        }
    }

    /// Log an insert operation (batched with group commit)
    pub fn log_insert(&self, key: &str, value: &str) -> io::Result<u64> {
        let seq = self.seq_gen.allocate();
        let record = WalRecord::insert(seq, key.to_string(), value.to_string());
        self.send_and_wait(record)
    }

    /// Log a delete operation (batched with group commit)
    pub fn log_delete(&self, key: &str) -> io::Result<u64> {
        let seq = self.seq_gen.allocate();
        let record = WalRecord::delete(seq, key.to_string());
        self.send_and_wait(record)
    }

    /// Log a checkpoint
    pub fn log_checkpoint(&self) -> io::Result<u64> {
        let seq = self.seq_gen.allocate();
        let record = WalRecord::checkpoint(seq);
        self.send_and_wait(record)
    }

    /// Send a record to the background flusher and wait for completion
    fn send_and_wait(&self, record: WalRecord) -> io::Result<u64> {
        let (response_tx, response_rx) = mpsc::channel();

        let request = WriteRequest {
            record,
            response_tx,
        };

        // Get the sender (may be None if shutdown)
        let sender = {
            let guard = self.request_tx.lock().unwrap();
            guard.clone()
        };

        match sender {
            Some(tx) => tx
                .send(request)
                .map_err(|_| io::Error::other("WAL writer channel closed"))?,
            None => return Err(io::Error::other("WAL writer is shut down")),
        }

        response_rx
            .recv()
            .map_err(|_| io::Error::other("WAL writer response channel closed"))?
    }

    /// Get current WAL file path
    pub fn path(&self) -> PathBuf {
        let guard = self.inner.lock().unwrap();
        guard.path.clone()
    }

    /// Get data directory
    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    /// Read all records from the current WAL file
    pub fn read_records(&self) -> io::Result<Vec<WalRecord>> {
        let path = self.path();
        read_wal_records(&path)
    }

    /// Read all records from all WAL files in the data directory
    pub fn read_all_records(&self) -> io::Result<Vec<WalRecord>> {
        use crate::engine::wal::parse_wal_filename;

        let mut all_records = Vec::new();
        let mut wal_files: Vec<(u64, PathBuf)> = Vec::new();

        for entry in fs::read_dir(&self.data_dir)? {
            let entry = entry?;
            let filename = entry.file_name();
            let filename_str = filename.to_string_lossy();

            if let Some(id) = parse_wal_filename(&filename_str) {
                wal_files.push((id, entry.path()));
            }
        }

        // Sort by WAL ID
        wal_files.sort_by_key(|(id, _)| *id);

        // Read records from each WAL file in order
        for (_, path) in wal_files {
            if let Ok(records) = read_wal_records(&path) {
                all_records.extend(records);
            }
        }

        Ok(all_records)
    }

    /// Sync is a no-op for group commit (always syncs after each batch)
    pub fn sync(&self) -> io::Result<()> {
        Ok(())
    }

    /// Begin a batch operation (no-op for group commit, batching is automatic)
    pub fn begin_batch(&self) -> io::Result<u64> {
        // Group commit already batches writes, so this is a no-op
        // Just return a dummy sequence number
        Ok(0)
    }

    /// End a batch operation (no-op for group commit)
    pub fn end_batch(&self) -> io::Result<u64> {
        // Group commit already syncs after each batch
        Ok(0)
    }

    /// Truncate all WAL files after checkpoint
    ///
    /// This removes all WAL files and creates a fresh one.
    pub fn truncate_after_checkpoint(&self) -> io::Result<()> {
        use crate::engine::wal::parse_wal_filename;

        // Lock the inner state to prevent concurrent writes
        let mut guard = self.inner.lock().unwrap();

        // Flush and sync any pending data
        guard.writer.flush()?;
        guard.writer.get_ref().sync_data()?;

        // Delete all existing WAL files
        for entry in fs::read_dir(&self.data_dir)? {
            let entry = entry?;
            let filename = entry.file_name();
            let filename_str = filename.to_string_lossy();

            if parse_wal_filename(&filename_str).is_some() {
                fs::remove_file(entry.path())?;
            }
        }

        // Reset WAL ID and create new file
        let new_id = 0;
        self.current_id.store(new_id, Ordering::SeqCst);

        let new_path = Self::wal_path(&self.data_dir, new_id);
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&new_path)?;

        let mut new_writer = BufWriter::new(file);
        write_wal_header(&mut new_writer, WAL_FORMAT_VERSION)?;
        new_writer.flush()?;
        new_writer.get_ref().sync_all()?;

        // Replace the writer
        guard.writer = new_writer;
        guard.path = new_path;

        // Reset sequence number
        self.seq_gen.set(1);

        Ok(())
    }

    /// Delete WAL files up to (and including) the given ID
    pub fn delete_wals_up_to(&self, max_id: u64) -> io::Result<()> {
        use crate::engine::wal::parse_wal_filename;

        for entry in fs::read_dir(&self.data_dir)? {
            let entry = entry?;
            let filename = entry.file_name();
            let filename_str = filename.to_string_lossy();

            if let Some(id) = parse_wal_filename(&filename_str)
                && id <= max_id
            {
                fs::remove_file(entry.path())?;
                println!("Deleted old WAL: {:?}", entry.path());
            }
        }

        Ok(())
    }

    /// Close the WAL writer
    pub fn close(&self) -> io::Result<()> {
        self.shutdown()
    }

    /// Shutdown the WAL writer gracefully
    pub fn shutdown(&self) -> io::Result<()> {
        self.shutdown.store(true, Ordering::SeqCst);

        // Drop the sender to signal shutdown
        {
            let mut guard = self.request_tx.lock().unwrap();
            guard.take();
        }

        // Take the handle to wait for the background thread
        let handle = {
            let mut guard = self.flusher_handle.lock().unwrap();
            guard.take()
        };

        if let Some(handle) = handle {
            // Wait for the background thread to complete
            handle
                .join()
                .map_err(|_| io::Error::other("Failed to join WAL flusher thread"))?;
        }

        Ok(())
    }
}

impl Drop for GroupCommitWalWriter {
    fn drop(&mut self) {
        // Only signal shutdown if this is the last reference
        // (Arc strong_count would be 1 for the last clone)
        if Arc::strong_count(&self.shutdown) == 1 {
            self.shutdown.store(true, Ordering::SeqCst);
            // Drop the sender to signal the receiver
            if let Ok(mut guard) = self.request_tx.lock() {
                guard.take();
            }
        }
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

    fn rotate(&self) -> io::Result<u64> {
        GroupCommitWalWriter::rotate(self)
    }

    fn delete_wals_up_to(&self, max_id: u64) -> io::Result<()> {
        GroupCommitWalWriter::delete_wals_up_to(self, max_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_wal_record_roundtrip() {
        let record = WalRecord::insert(1, "key1".to_string(), "value1".to_string());
        let mut data = Vec::new();
        record.serialize(&mut data);
        let (decoded, _) = WalRecord::deserialize(&data).unwrap();

        assert_eq!(decoded.seq, 1);
        assert_eq!(decoded.record_type, RecordType::Insert);
        assert_eq!(decoded.key, "key1");
        assert_eq!(decoded.value, Some("value1".to_string()));
    }

    #[test]
    fn test_wal_write_read() {
        let dir = tempdir().unwrap();
        let config = GroupCommitConfig::default();

        let wal = GroupCommitWalWriter::open(dir.path(), config).unwrap();
        wal.log_insert("key1", "value1").unwrap();
        wal.log_insert("key2", "value2").unwrap();
        wal.log_delete("key1").unwrap();

        let records = wal.read_records().unwrap();

        assert_eq!(records.len(), 3);
        assert_eq!(records[0].record_type, RecordType::Insert);
        assert_eq!(records[0].key, "key1");
        assert_eq!(records[1].record_type, RecordType::Insert);
        assert_eq!(records[1].key, "key2");
        assert_eq!(records[2].record_type, RecordType::Delete);
        assert_eq!(records[2].key, "key1");

        wal.close().unwrap();
    }

    #[test]
    fn test_wal_checkpoint() {
        let dir = tempdir().unwrap();
        let config = GroupCommitConfig::default();

        {
            let wal = GroupCommitWalWriter::open(dir.path(), config).unwrap();
            wal.log_insert("key1", "value1").unwrap();
            wal.log_checkpoint().unwrap();
            wal.log_insert("key2", "value2").unwrap();
            wal.close().unwrap();
        }

        let wal_path = dir.path().join("wal_00000.log");
        let records = recover_from_wal(&wal_path).unwrap();

        // Should only have records after checkpoint
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].key, "key2");
    }

    #[test]
    fn test_wal_truncate() {
        let dir = tempdir().unwrap();
        let config = GroupCommitConfig::default();

        {
            let wal = GroupCommitWalWriter::open(dir.path(), config).unwrap();
            wal.log_insert("key1", "value1").unwrap();
            wal.log_checkpoint().unwrap();
            wal.truncate_after_checkpoint().unwrap();
            wal.close().unwrap();
        }

        let wal = GroupCommitWalWriter::open(dir.path(), config).unwrap();
        let records = wal.read_records().unwrap();

        assert!(records.is_empty());
        wal.close().unwrap();
    }
}
