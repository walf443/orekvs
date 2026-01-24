mod proto {
    tonic::include_proto!("replication");
}

pub use proto::replication_client::ReplicationClient;
pub use proto::replication_server::{Replication, ReplicationServer};
pub use proto::{
    Empty, LeaderInfo, SnapshotChunk, SnapshotInfo, SnapshotRequest, StreamSnapshotRequest,
    StreamWalRequest, WalBatch,
};

use crate::engine::lsm_tree::SnapshotLock;
use std::fs::{self, File};
use std::io::{Cursor, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_stream::Stream;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

/// WAL entry parsed from file
#[derive(Debug, Clone)]
pub struct WalEntry {
    pub timestamp: u64,
    pub key: String,
    pub value: Option<String>,
}

/// Threshold for switching between catch-up and real-time modes
const CATCHUP_THRESHOLD: usize = 100;
/// Batch size for catch-up mode
const CATCHUP_BATCH_SIZE: usize = 1000;
/// Max batch size in bytes before sending
const CATCHUP_BATCH_BYTES: usize = 64 * 1024;
/// Poll interval when waiting for new entries in real-time mode
const REALTIME_POLL_INTERVAL_MS: u64 = 10;
/// Maximum allowed key/value size (1GB) to prevent memory allocation attacks
const MAX_ENTRY_SIZE: u64 = 1024 * 1024 * 1024;
/// Chunk size for snapshot file transfer (64KB)
const SNAPSHOT_CHUNK_SIZE: usize = 64 * 1024;

/// Leader-side replication service
pub struct ReplicationService {
    data_dir: PathBuf,
    /// Lock to prevent SSTable deletion during snapshot transfer
    snapshot_lock: SnapshotLock,
}

impl ReplicationService {
    pub fn new(data_dir: PathBuf, snapshot_lock: SnapshotLock) -> Self {
        Self {
            data_dir,
            snapshot_lock,
        }
    }

    /// Create a new ReplicationService without snapshot lock protection.
    /// Use this only when snapshot transfer is not needed (e.g., WAL-only streaming).
    pub fn new_without_lock(data_dir: PathBuf) -> Self {
        Self {
            data_dir,
            snapshot_lock: Arc::new(RwLock::new(())),
        }
    }

    /// Get list of WAL files sorted by ID
    fn get_wal_files(&self) -> Vec<(u64, PathBuf)> {
        let mut wal_files = Vec::new();
        if let Ok(entries) = fs::read_dir(&self.data_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if let Some(filename) = path.file_name().and_then(|n| n.to_str())
                    && let Some(id) = parse_wal_filename(filename)
                {
                    wal_files.push((id, path));
                }
            }
        }
        wal_files.sort_by_key(|(id, _)| *id);
        wal_files
    }

    /// Get current WAL position (latest WAL ID and file size)
    fn get_current_position(&self) -> (u64, u64) {
        let wal_files = self.get_wal_files();
        if let Some((id, path)) = wal_files.last() {
            let offset = fs::metadata(path).map(|m| m.len()).unwrap_or(0);
            (*id, offset)
        } else {
            (0, 0)
        }
    }

    /// Get list of SSTable files
    fn get_sstable_files(&self) -> Vec<(String, PathBuf)> {
        let mut sstable_files = Vec::new();
        if let Ok(entries) = fs::read_dir(&self.data_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if let Some(filename) = path.file_name().and_then(|n| n.to_str()) {
                    // SSTable files: sst_*.data (e.g., sst_00001.data or sst_00001_00002.data)
                    if filename.starts_with("sst_") && filename.ends_with(".data") {
                        sstable_files.push((filename.to_string(), path));
                    }
                }
            }
        }
        sstable_files.sort_by(|(a, _), (b, _)| a.cmp(b));
        sstable_files
    }

    /// Check if the requested WAL is available
    fn is_wal_available(&self, wal_id: u64) -> bool {
        if wal_id == 0 {
            // Initial sync, check if any WAL exists
            return !self.get_wal_files().is_empty();
        }
        let wal_files = self.get_wal_files();
        // WAL is available if we have it or a newer one
        wal_files.iter().any(|(id, _)| *id >= wal_id)
    }

    /// Get the oldest available WAL ID
    fn get_oldest_wal_id(&self) -> Option<u64> {
        self.get_wal_files().first().map(|(id, _)| *id)
    }

    /// Read WAL entries from a specific position
    #[allow(clippy::result_large_err)]
    fn read_wal_entries_from(
        &self,
        wal_id: u64,
        offset: u64,
        max_entries: usize,
        max_bytes: usize,
    ) -> Result<(Vec<WalEntry>, u64, u64, bool), Status> {
        let wal_files = self.get_wal_files();

        if wal_files.is_empty() {
            // No WAL files yet
            return Ok((Vec::new(), wal_id, offset, false));
        }

        // Find the WAL file
        let wal_path = wal_files
            .iter()
            .find(|(id, _)| *id == wal_id)
            .map(|(_, path)| path.clone());

        let Some(path) = wal_path else {
            // WAL file not found, check if there's a newer one
            if let Some((newer_id, _)) = wal_files.iter().find(|(id, _)| *id > wal_id) {
                // Start from beginning of newer file
                return self.read_wal_entries_from(*newer_id, 0, max_entries, max_bytes);
            }
            // Also check if there's an older file we should start from (for initial sync)
            if wal_id == 0
                && let Some((first_id, _)) = wal_files.first()
            {
                return self.read_wal_entries_from(*first_id, 0, max_entries, max_bytes);
            }
            return Ok((Vec::new(), wal_id, offset, false));
        };

        let mut file = File::open(&path).map_err(|e| Status::internal(e.to_string()))?;
        let file_len = file
            .metadata()
            .map_err(|e| Status::internal(e.to_string()))?
            .len();

        // Seek to offset
        let start_offset = if offset == 0 {
            // Skip WAL header (magic + version = 13 bytes)
            13
        } else {
            offset
        };

        if start_offset >= file_len {
            // Check if there's a newer WAL file
            if let Some((newer_id, _)) = wal_files.iter().find(|(id, _)| *id > wal_id) {
                return self.read_wal_entries_from(*newer_id, 0, max_entries, max_bytes);
            }
            // At end of current file, no more entries
            return Ok((Vec::new(), wal_id, file_len, false));
        }

        file.seek(SeekFrom::Start(start_offset))
            .map_err(|e| Status::internal(e.to_string()))?;

        let mut entries = Vec::new();
        let mut current_offset = start_offset;
        let mut bytes_read = 0usize;

        // Reasonable timestamp bounds (year 2020 to 2100)
        const MIN_TIMESTAMP: u64 = 1577836800; // 2020-01-01
        const MAX_TIMESTAMP: u64 = 4102444800; // 2100-01-01

        while entries.len() < max_entries && bytes_read < max_bytes {
            // Read timestamp
            let mut ts_bytes = [0u8; 8];
            if file.read_exact(&mut ts_bytes).is_err() {
                break;
            }
            let timestamp = u64::from_le_bytes(ts_bytes);

            // Validate timestamp to detect corrupt data early
            if !(MIN_TIMESTAMP..=MAX_TIMESTAMP).contains(&timestamp) {
                return Err(Status::internal(format!(
                    "Invalid timestamp {} at offset {}, likely corrupted WAL or wrong file format",
                    timestamp, current_offset
                )));
            }

            // Read key length
            let mut klen_bytes = [0u8; 8];
            if file.read_exact(&mut klen_bytes).is_err() {
                break;
            }
            let key_len = u64::from_le_bytes(klen_bytes);

            // Read value length
            let mut vlen_bytes = [0u8; 8];
            if file.read_exact(&mut vlen_bytes).is_err() {
                break;
            }
            let val_len = u64::from_le_bytes(vlen_bytes);

            // Validate lengths to prevent memory allocation attacks
            if key_len > MAX_ENTRY_SIZE {
                return Err(Status::internal(format!(
                    "Invalid key length {} at offset {}, likely corrupted WAL",
                    key_len, current_offset
                )));
            }
            if val_len != u64::MAX && val_len > MAX_ENTRY_SIZE {
                return Err(Status::internal(format!(
                    "Invalid value length {} at offset {}, likely corrupted WAL",
                    val_len, current_offset
                )));
            }

            // Calculate expected entry size and validate against file bounds
            let expected_entry_size = 24 + key_len + if val_len == u64::MAX { 0 } else { val_len };
            if current_offset + expected_entry_size > file_len {
                // Incomplete entry at end of file
                break;
            }

            // Read key
            let mut key_buf = vec![0u8; key_len as usize];
            if file.read_exact(&mut key_buf).is_err() {
                break;
            }
            let key = String::from_utf8_lossy(&key_buf).to_string();

            // Read value
            let value = if val_len == u64::MAX {
                None // Tombstone
            } else {
                let mut val_buf = vec![0u8; val_len as usize];
                if file.read_exact(&mut val_buf).is_err() {
                    break;
                }
                Some(String::from_utf8_lossy(&val_buf).to_string())
            };

            let entry_size = 24
                + key_len as usize
                + if val_len == u64::MAX {
                    0
                } else {
                    val_len as usize
                };
            current_offset += entry_size as u64;
            bytes_read += entry_size;

            entries.push(WalEntry {
                timestamp,
                key,
                value,
            });
        }

        // Check if there are more entries
        let has_more = current_offset < file_len || wal_files.iter().any(|(id, _)| *id > wal_id);

        Ok((entries, wal_id, current_offset, has_more))
    }
}

/// Parse WAL filename to extract ID
fn parse_wal_filename(filename: &str) -> Option<u64> {
    if filename.starts_with("wal_") && filename.ends_with(".log") {
        filename[4..filename.len() - 4].parse::<u64>().ok()
    } else {
        None
    }
}

/// Serialize WAL entries to bytes
fn serialize_entries(entries: &[WalEntry]) -> Vec<u8> {
    let mut buf = Vec::new();
    for entry in entries {
        buf.extend_from_slice(&entry.timestamp.to_le_bytes());

        let key_bytes = entry.key.as_bytes();
        buf.extend_from_slice(&(key_bytes.len() as u64).to_le_bytes());

        match &entry.value {
            Some(v) => {
                let val_bytes = v.as_bytes();
                buf.extend_from_slice(&(val_bytes.len() as u64).to_le_bytes());
                buf.extend_from_slice(key_bytes);
                buf.extend_from_slice(val_bytes);
            }
            None => {
                buf.extend_from_slice(&u64::MAX.to_le_bytes());
                buf.extend_from_slice(key_bytes);
            }
        }
    }
    buf
}

/// Deserialize WAL entries from bytes
#[allow(clippy::result_large_err)]
pub fn deserialize_entries(data: &[u8]) -> Result<Vec<WalEntry>, Status> {
    let mut entries = Vec::new();
    let mut cursor = Cursor::new(data);

    loop {
        // Read timestamp
        let mut ts_bytes = [0u8; 8];
        if cursor.read_exact(&mut ts_bytes).is_err() {
            break;
        }
        let timestamp = u64::from_le_bytes(ts_bytes);

        // Read key length
        let mut klen_bytes = [0u8; 8];
        cursor
            .read_exact(&mut klen_bytes)
            .map_err(|e| Status::internal(e.to_string()))?;
        let key_len = u64::from_le_bytes(klen_bytes);

        // Read value length
        let mut vlen_bytes = [0u8; 8];
        cursor
            .read_exact(&mut vlen_bytes)
            .map_err(|e| Status::internal(e.to_string()))?;
        let val_len = u64::from_le_bytes(vlen_bytes);

        // Validate lengths
        if key_len > MAX_ENTRY_SIZE {
            return Err(Status::internal(format!(
                "Invalid key length {} in deserialization",
                key_len
            )));
        }
        if val_len != u64::MAX && val_len > MAX_ENTRY_SIZE {
            return Err(Status::internal(format!(
                "Invalid value length {} in deserialization",
                val_len
            )));
        }

        // Read key
        let mut key_buf = vec![0u8; key_len as usize];
        cursor
            .read_exact(&mut key_buf)
            .map_err(|e| Status::internal(e.to_string()))?;
        let key = String::from_utf8_lossy(&key_buf).to_string();

        // Read value
        let value = if val_len == u64::MAX {
            None
        } else {
            let mut val_buf = vec![0u8; val_len as usize];
            cursor
                .read_exact(&mut val_buf)
                .map_err(|e| Status::internal(e.to_string()))?;
            Some(String::from_utf8_lossy(&val_buf).to_string())
        };

        entries.push(WalEntry {
            timestamp,
            key,
            value,
        });
    }

    Ok(entries)
}

#[tonic::async_trait]
impl Replication for ReplicationService {
    type StreamWALEntriesStream =
        Pin<Box<dyn Stream<Item = Result<WalBatch, Status>> + Send + 'static>>;
    type StreamSnapshotStream =
        Pin<Box<dyn Stream<Item = Result<SnapshotChunk, Status>> + Send + 'static>>;

    async fn stream_wal_entries(
        &self,
        request: Request<StreamWalRequest>,
    ) -> Result<Response<Self::StreamWALEntriesStream>, Status> {
        let peer_addr = request.remote_addr();
        let req = request.into_inner();
        let mut current_wal_id = req.from_wal_id;
        let mut current_offset = req.from_offset;
        let data_dir = self.data_dir.clone();

        println!(
            "Follower connected from {:?}, starting from WAL {} offset {}",
            peer_addr, current_wal_id, current_offset
        );

        let (tx, rx) = mpsc::channel(32);

        let peer_addr_str = peer_addr.map(|a| a.to_string()).unwrap_or_default();
        tokio::spawn(async move {
            // This service is only for WAL streaming, doesn't need snapshot lock
            let service = ReplicationService::new_without_lock(data_dir);

            loop {
                // Check how far behind we are
                let (latest_wal_id, latest_offset) = service.get_current_position();
                let is_catching_up = latest_wal_id > current_wal_id
                    || (latest_wal_id == current_wal_id
                        && latest_offset > current_offset + CATCHUP_THRESHOLD as u64 * 100);

                let (batch_size, max_bytes) = if is_catching_up {
                    // Catch-up mode: larger batches
                    (CATCHUP_BATCH_SIZE, CATCHUP_BATCH_BYTES)
                } else {
                    // Real-time mode: smaller batches for lower latency
                    (10, 4096)
                };

                match service.read_wal_entries_from(
                    current_wal_id,
                    current_offset,
                    batch_size,
                    max_bytes,
                ) {
                    Ok((entries, new_wal_id, new_offset, _has_more)) => {
                        if entries.is_empty() {
                            // No new entries, wait before polling again
                            tokio::time::sleep(Duration::from_millis(REALTIME_POLL_INTERVAL_MS))
                                .await;
                            continue;
                        }

                        let entry_count = entries.len() as u32;
                        let serialized = serialize_entries(&entries);

                        // Compress with zstd
                        let compressed = match zstd::encode_all(Cursor::new(&serialized), 3) {
                            Ok(c) => c,
                            Err(e) => {
                                eprintln!("Compression error: {}", e);
                                break;
                            }
                        };

                        let batch = WalBatch {
                            wal_id: current_wal_id,
                            start_offset: current_offset,
                            end_offset: new_offset,
                            compressed_data: compressed,
                            entry_count,
                        };

                        if tx.send(Ok(batch)).await.is_err() {
                            // Client disconnected
                            println!("Follower disconnected: {}", peer_addr_str);
                            break;
                        }

                        current_wal_id = new_wal_id;
                        current_offset = new_offset;
                    }
                    Err(e) => {
                        let _ = tx.send(Err(e)).await;
                        break;
                    }
                }
            }
        });

        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }

    async fn get_leader_info(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<LeaderInfo>, Status> {
        let (current_wal_id, current_offset) = self.get_current_position();
        Ok(Response::new(LeaderInfo {
            current_wal_id,
            current_offset,
        }))
    }

    async fn request_snapshot(
        &self,
        request: Request<SnapshotRequest>,
    ) -> Result<Response<SnapshotInfo>, Status> {
        let req = request.into_inner();
        let requested_wal_id = req.requested_wal_id;

        // Check if the requested WAL is available
        if self.is_wal_available(requested_wal_id) {
            // WAL is available, no snapshot needed
            return Ok(Response::new(SnapshotInfo {
                snapshot_required: false,
                sstable_files: vec![],
                resume_wal_id: 0,
                resume_offset: 0,
                total_bytes: 0,
            }));
        }

        // WAL is not available, snapshot required
        let sstable_files = self.get_sstable_files();
        let filenames: Vec<String> = sstable_files.iter().map(|(name, _)| name.clone()).collect();

        // Calculate total bytes
        let total_bytes: u64 = sstable_files
            .iter()
            .filter_map(|(_, path)| fs::metadata(path).ok().map(|m| m.len()))
            .sum();

        // Get the resume position (oldest available WAL or current position)
        let (resume_wal_id, resume_offset) = if let Some(oldest_id) = self.get_oldest_wal_id() {
            (oldest_id, 0)
        } else {
            self.get_current_position()
        };

        println!(
            "Snapshot requested for WAL {}, will transfer {} SSTable files ({} bytes), resume at WAL {} offset {}",
            requested_wal_id,
            filenames.len(),
            total_bytes,
            resume_wal_id,
            resume_offset
        );

        Ok(Response::new(SnapshotInfo {
            snapshot_required: true,
            sstable_files: filenames,
            resume_wal_id,
            resume_offset,
            total_bytes,
        }))
    }

    async fn stream_snapshot(
        &self,
        request: Request<StreamSnapshotRequest>,
    ) -> Result<Response<Self::StreamSnapshotStream>, Status> {
        let req = request.into_inner();
        let filename = req.filename;
        let file_path = self.data_dir.join(&filename);

        // Validate filename to prevent path traversal
        if filename.contains("..") || filename.contains('/') || filename.contains('\\') {
            return Err(Status::invalid_argument("Invalid filename"));
        }

        // Acquire read lock to prevent SSTable deletion during transfer.
        // This lock is held by the spawned task until the transfer completes.
        let snapshot_lock = Arc::clone(&self.snapshot_lock);

        // Check file exists while holding the lock
        {
            let _lock = snapshot_lock.read().unwrap();
            if !file_path.exists() {
                return Err(Status::not_found(format!("File not found: {}", filename)));
            }
        }

        let (tx, rx) = mpsc::channel(16);
        let filename_clone = filename.clone();

        println!("Starting snapshot transfer for file: {}", filename);

        tokio::spawn(async move {
            // Hold read lock for the entire transfer duration
            let _lock = snapshot_lock.read().unwrap();

            let result = (|| -> Result<(), Status> {
                let mut file =
                    File::open(&file_path).map_err(|e| Status::internal(e.to_string()))?;
                let file_len = file
                    .metadata()
                    .map_err(|e| Status::internal(e.to_string()))?
                    .len();

                let mut offset = 0u64;
                let mut buf = vec![0u8; SNAPSHOT_CHUNK_SIZE];

                while offset < file_len {
                    let bytes_read = file
                        .read(&mut buf)
                        .map_err(|e| Status::internal(e.to_string()))?;
                    if bytes_read == 0 {
                        break;
                    }

                    let is_last = offset + bytes_read as u64 >= file_len;
                    let chunk = SnapshotChunk {
                        filename: filename_clone.clone(),
                        data: buf[..bytes_read].to_vec(),
                        offset,
                        is_last,
                    };

                    offset += bytes_read as u64;

                    if tx.blocking_send(Ok(chunk)).is_err() {
                        // Receiver dropped
                        break;
                    }
                }

                Ok(())
            })();

            if let Err(e) = result {
                let _ = tx.blocking_send(Err(e));
            }
        });

        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }
}

/// Result of a replication connection
pub enum ReplicationResult {
    /// Normal stream end, should reconnect
    Continue,
    /// Snapshot was applied, engine needs to reload
    SnapshotApplied {
        resume_wal_id: u64,
        resume_offset: u64,
    },
}

/// Follower replication client
pub struct FollowerReplicator {
    leader_addr: String,
    data_dir: PathBuf,
}

impl FollowerReplicator {
    pub fn new(leader_addr: String, data_dir: PathBuf) -> Self {
        Self {
            leader_addr,
            data_dir,
        }
    }

    /// Get the last applied WAL position from persistent storage
    fn load_replication_state(&self) -> (u64, u64) {
        let state_path = self.data_dir.join("replication_state");
        if let Ok(data) = fs::read(&state_path)
            && data.len() >= 16
        {
            let wal_id = u64::from_le_bytes(data[0..8].try_into().unwrap());
            let offset = u64::from_le_bytes(data[8..16].try_into().unwrap());
            return (wal_id, offset);
        }
        (0, 0)
    }

    /// Save the current replication position
    #[allow(clippy::result_large_err)]
    fn save_replication_state(&self, wal_id: u64, offset: u64) -> Result<(), Status> {
        let state_path = self.data_dir.join("replication_state");
        let mut data = Vec::with_capacity(16);
        data.extend_from_slice(&wal_id.to_le_bytes());
        data.extend_from_slice(&offset.to_le_bytes());
        fs::write(&state_path, &data).map_err(|e| Status::internal(e.to_string()))
    }

    /// Start replication from leader
    /// Returns SnapshotApplied if a snapshot was downloaded and the caller should reload the engine
    pub async fn start<F>(&self, mut apply_fn: F) -> Result<(), Status>
    where
        F: FnMut(WalEntry) -> Result<(), Status> + Send,
    {
        let (mut wal_id, mut offset) = self.load_replication_state();
        println!("Starting replication from WAL {} offset {}", wal_id, offset);

        loop {
            match self
                .connect_and_stream(&mut wal_id, &mut offset, &mut apply_fn)
                .await
            {
                Ok(ReplicationResult::Continue) => {
                    // Stream ended normally (shouldn't happen in normal operation)
                    println!("Replication stream ended, reconnecting...");
                }
                Ok(ReplicationResult::SnapshotApplied {
                    resume_wal_id,
                    resume_offset,
                }) => {
                    // Snapshot was applied, update position and continue
                    wal_id = resume_wal_id;
                    offset = resume_offset;
                    println!(
                        "Snapshot applied, resuming from WAL {} offset {}",
                        wal_id, offset
                    );
                    // Note: caller should reload the engine after snapshot
                    return Err(Status::aborted("Snapshot applied, engine reload required"));
                }
                Err(e) => {
                    eprintln!("Replication error: {}, reconnecting in 1s...", e);
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }
    }

    /// Check if snapshot is needed and download it if so
    async fn check_and_download_snapshot(
        &self,
        client: &mut ReplicationClient<tonic::transport::Channel>,
        wal_id: u64,
    ) -> Result<Option<(u64, u64)>, Status> {
        let request = SnapshotRequest {
            requested_wal_id: wal_id,
        };

        let response = client
            .request_snapshot(request)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        let info = response.into_inner();

        if !info.snapshot_required {
            return Ok(None);
        }

        println!(
            "Snapshot required: downloading {} SSTable files ({} bytes)",
            info.sstable_files.len(),
            info.total_bytes
        );

        // Download each SSTable file
        for filename in &info.sstable_files {
            self.download_sstable_file(client, filename).await?;
        }

        println!("Snapshot download complete");

        // Save the new replication state
        self.save_replication_state(info.resume_wal_id, info.resume_offset)?;

        Ok(Some((info.resume_wal_id, info.resume_offset)))
    }

    /// Download a single SSTable file from leader
    async fn download_sstable_file(
        &self,
        client: &mut ReplicationClient<tonic::transport::Channel>,
        filename: &str,
    ) -> Result<(), Status> {
        let request = StreamSnapshotRequest {
            filename: filename.to_string(),
        };

        let mut stream = client
            .stream_snapshot(request)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .into_inner();

        let file_path = self.data_dir.join(filename);

        // Create temporary file for atomic write
        let temp_path = self.data_dir.join(format!("{}.tmp", filename));
        let mut file = File::create(&temp_path).map_err(|e| Status::internal(e.to_string()))?;

        let mut bytes_received = 0u64;

        while let Some(result) = tokio_stream::StreamExt::next(&mut stream).await {
            let chunk = result?;

            file.write_all(&chunk.data)
                .map_err(|e| Status::internal(e.to_string()))?;

            bytes_received += chunk.data.len() as u64;

            if chunk.is_last {
                break;
            }
        }

        // Sync and rename atomically
        file.sync_all()
            .map_err(|e| Status::internal(e.to_string()))?;
        drop(file);

        fs::rename(&temp_path, &file_path).map_err(|e| Status::internal(e.to_string()))?;

        println!("Downloaded {} ({} bytes)", filename, bytes_received);

        Ok(())
    }

    async fn connect_and_stream<F>(
        &self,
        wal_id: &mut u64,
        offset: &mut u64,
        apply_fn: &mut F,
    ) -> Result<ReplicationResult, Status>
    where
        F: FnMut(WalEntry) -> Result<(), Status>,
    {
        let mut client = ReplicationClient::connect(self.leader_addr.clone())
            .await
            .map_err(|e| Status::internal(format!("Failed to connect to leader: {}", e)))?;

        println!("Connected to leader at {}", self.leader_addr);

        // Check if snapshot is needed
        if let Some((resume_wal_id, resume_offset)) = self
            .check_and_download_snapshot(&mut client, *wal_id)
            .await?
        {
            return Ok(ReplicationResult::SnapshotApplied {
                resume_wal_id,
                resume_offset,
            });
        }

        let request = StreamWalRequest {
            from_wal_id: *wal_id,
            from_offset: *offset,
        };

        let mut stream = client
            .stream_wal_entries(request)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .into_inner();

        while let Some(result) = tokio_stream::StreamExt::next(&mut stream).await {
            let batch = result?;

            // Decompress
            let decompressed = zstd::decode_all(Cursor::new(&batch.compressed_data))
                .map_err(|e| Status::internal(format!("Decompression error: {}", e)))?;

            // Parse entries
            let entries = deserialize_entries(&decompressed)?;

            // Apply entries
            for entry in entries {
                apply_fn(entry)?;
            }

            // Update position
            *wal_id = batch.wal_id;
            *offset = batch.end_offset;

            // Persist state periodically (every batch)
            self.save_replication_state(*wal_id, *offset)?;
        }

        Ok(ReplicationResult::Continue)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_serialize_deserialize_entries() {
        let entries = vec![
            WalEntry {
                timestamp: 12345,
                key: "key1".to_string(),
                value: Some("value1".to_string()),
            },
            WalEntry {
                timestamp: 12346,
                key: "key2".to_string(),
                value: None, // Tombstone
            },
        ];

        let serialized = serialize_entries(&entries);
        let deserialized = deserialize_entries(&serialized).unwrap();

        assert_eq!(deserialized.len(), 2);
        assert_eq!(deserialized[0].key, "key1");
        assert_eq!(deserialized[0].value, Some("value1".to_string()));
        assert_eq!(deserialized[1].key, "key2");
        assert_eq!(deserialized[1].value, None);
    }

    #[test]
    fn test_parse_wal_filename() {
        assert_eq!(parse_wal_filename("wal_00001.log"), Some(1));
        assert_eq!(parse_wal_filename("wal_12345.log"), Some(12345));
        assert_eq!(parse_wal_filename("other.log"), None);
        assert_eq!(parse_wal_filename("wal_123.data"), None);
    }
}
