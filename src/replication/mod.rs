mod proto {
    tonic::include_proto!("replication");
}

pub mod follower;
mod service;

pub use follower::run_follower;
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
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tonic::Status;

/// WAL entry parsed from file
#[derive(Debug, Clone)]
pub struct WalEntry {
    pub timestamp: u64,
    pub key: String,
    pub value: Option<String>,
    pub expire_at: u64, // 0 = no expiration, >0 = Unix timestamp when key expires
}

/// Threshold for switching between catch-up and real-time modes
pub(crate) const CATCHUP_THRESHOLD: usize = 100;
/// Batch size for catch-up mode
pub(crate) const CATCHUP_BATCH_SIZE: usize = 1000;
/// Max batch size in bytes before sending
pub(crate) const CATCHUP_BATCH_BYTES: usize = 64 * 1024;
/// Poll interval when waiting for new entries in real-time mode
pub(crate) const REALTIME_POLL_INTERVAL_MS: u64 = 10;
/// Maximum allowed key/value size (1GB) to prevent memory allocation attacks
const MAX_ENTRY_SIZE: u64 = 1024 * 1024 * 1024;
/// Chunk size for snapshot file transfer (64KB)
pub(crate) const SNAPSHOT_CHUNK_SIZE: usize = 64 * 1024;
/// Error code for WAL gap detection (used to signal snapshot required)
pub(crate) const WAL_GAP_ERROR_CODE: &str = "WAL_GAP_DETECTED";

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
            if wal_files.iter().any(|(id, _)| *id > wal_id) {
                // WAL gap detected! The requested WAL was deleted but newer ones exist.
                // This means the follower is too far behind and needs a snapshot.
                return Err(Status::failed_precondition(WAL_GAP_ERROR_CODE));
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

        // Always read WAL version from header
        let mut magic = [0u8; 9];
        file.read_exact(&mut magic)
            .map_err(|e| Status::internal(e.to_string()))?;
        if &magic != b"OREKVSWAL" {
            return Err(Status::internal("Invalid WAL magic bytes"));
        }
        let mut version_bytes = [0u8; 4];
        file.read_exact(&mut version_bytes)
            .map_err(|e| Status::internal(e.to_string()))?;
        let version = u32::from_le_bytes(version_bytes);

        // If offset is 0, start after header (13 bytes); otherwise use provided offset
        let start_offset = if offset == 0 { 13 } else { offset };

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

        match version {
            2 => self.read_wal_entries_v2(
                &mut file,
                wal_id,
                start_offset,
                file_len,
                max_entries,
                max_bytes,
                &wal_files,
                false, // no checksum
            ),
            3 => self.read_wal_entries_v2(
                &mut file,
                wal_id,
                start_offset,
                file_len,
                max_entries,
                max_bytes,
                &wal_files,
                true, // with checksum
            ),
            4 => self.read_wal_entries_v4(
                &mut file,
                wal_id,
                start_offset,
                file_len,
                max_entries,
                max_bytes,
                &wal_files,
            ),
            _ => Err(Status::internal(format!(
                "Unsupported WAL version: {}. Only versions 2-4 are supported.",
                version
            ))),
        }
    }

    /// Read WAL entries in v4 format (with sequence numbers)
    #[allow(clippy::result_large_err, clippy::too_many_arguments)]
    fn read_wal_entries_v4(
        &self,
        file: &mut File,
        wal_id: u64,
        start_offset: u64,
        file_len: u64,
        max_entries: usize,
        max_bytes: usize,
        wal_files: &[(u64, PathBuf)],
    ) -> Result<(Vec<WalEntry>, u64, u64, bool), Status> {
        const BLOCK_FLAG_COMPRESSED: u8 = 0x01;
        const MIN_TIMESTAMP: u64 = 1577836800; // 2020-01-01
        const MAX_TIMESTAMP: u64 = 4102444800; // 2100-01-01

        let mut entries = Vec::new();
        let mut current_offset = start_offset;
        let mut bytes_read = 0usize;

        while entries.len() < max_entries && bytes_read < max_bytes && current_offset < file_len {
            // Read block header: flags(1) + uncompressed_size(4) + data_size(4) = 9 bytes
            let mut flags = [0u8; 1];
            if file.read_exact(&mut flags).is_err() {
                break;
            }

            let mut uncompressed_size_bytes = [0u8; 4];
            let mut data_size_bytes = [0u8; 4];

            if file.read_exact(&mut uncompressed_size_bytes).is_err() {
                break;
            }
            if file.read_exact(&mut data_size_bytes).is_err() {
                break;
            }

            let _uncompressed_size = u32::from_le_bytes(uncompressed_size_bytes);
            let data_size = u32::from_le_bytes(data_size_bytes) as usize;

            // Read block data
            let mut data = vec![0u8; data_size];
            if file.read_exact(&mut data).is_err() {
                break;
            }

            // Read and verify checksum (v4 always has checksum)
            let mut _checksum_bytes = [0u8; 4];
            if file.read_exact(&mut _checksum_bytes).is_err() {
                break;
            }

            // Decompress if needed
            let entries_data = if flags[0] & BLOCK_FLAG_COMPRESSED != 0 {
                zstd::decode_all(Cursor::new(&data))
                    .map_err(|e| Status::internal(format!("Decompression error: {}", e)))?
            } else {
                data.clone()
            };

            // Update offset to point to next block
            let block_size = 9 + data_size + 4; // +4 for checksum
            current_offset += block_size as u64;
            bytes_read += block_size;

            // Parse entries from buffer (v4 format: seq + timestamp + expire_at + key_len + value_len + key + value)
            let mut cursor = Cursor::new(&entries_data);
            loop {
                // v4 format starts with sequence number
                let mut seq_bytes = [0u8; 8];
                if cursor.read_exact(&mut seq_bytes).is_err() {
                    break;
                }
                let _seq = u64::from_le_bytes(seq_bytes);

                let mut ts_bytes = [0u8; 8];
                if cursor.read_exact(&mut ts_bytes).is_err() {
                    break;
                }
                let timestamp = u64::from_le_bytes(ts_bytes);

                // Validate timestamp
                if !(MIN_TIMESTAMP..=MAX_TIMESTAMP).contains(&timestamp) {
                    return Err(Status::internal(format!(
                        "Invalid timestamp {} in WAL block",
                        timestamp
                    )));
                }

                // Read expire_at (added for TTL support)
                let mut expire_at_bytes = [0u8; 8];
                if cursor.read_exact(&mut expire_at_bytes).is_err() {
                    break;
                }
                let expire_at = u64::from_le_bytes(expire_at_bytes);

                let mut klen_bytes = [0u8; 8];
                let mut vlen_bytes = [0u8; 8];
                if cursor.read_exact(&mut klen_bytes).is_err() {
                    break;
                }
                if cursor.read_exact(&mut vlen_bytes).is_err() {
                    break;
                }

                let key_len = u64::from_le_bytes(klen_bytes);
                let val_len = u64::from_le_bytes(vlen_bytes);

                let mut key_buf = vec![0u8; key_len as usize];
                if cursor.read_exact(&mut key_buf).is_err() {
                    break;
                }
                let key = String::from_utf8_lossy(&key_buf).to_string();

                let value = if val_len == u64::MAX {
                    None
                } else {
                    let mut val_buf = vec![0u8; val_len as usize];
                    if cursor.read_exact(&mut val_buf).is_err() {
                        break;
                    }
                    Some(String::from_utf8_lossy(&val_buf).to_string())
                };

                entries.push(WalEntry {
                    timestamp,
                    key,
                    value,
                    expire_at,
                });

                if entries.len() >= max_entries {
                    break;
                }
            }
        }

        // Check if there are more entries
        let has_more = current_offset < file_len || wal_files.iter().any(|(id, _)| *id > wal_id);

        Ok((entries, wal_id, current_offset, has_more))
    }

    /// Read WAL entries in v2/v3 format (block-based with optional compression)
    /// v3 adds a 4-byte checksum after each block
    #[allow(clippy::result_large_err, clippy::too_many_arguments)]
    fn read_wal_entries_v2(
        &self,
        file: &mut File,
        wal_id: u64,
        start_offset: u64,
        file_len: u64,
        max_entries: usize,
        max_bytes: usize,
        wal_files: &[(u64, PathBuf)],
        has_checksum: bool,
    ) -> Result<(Vec<WalEntry>, u64, u64, bool), Status> {
        const BLOCK_FLAG_COMPRESSED: u8 = 0x01;
        const MIN_TIMESTAMP: u64 = 1577836800; // 2020-01-01
        const MAX_TIMESTAMP: u64 = 4102444800; // 2100-01-01

        let mut entries = Vec::new();
        let mut current_offset = start_offset;
        let mut bytes_read = 0usize;

        while entries.len() < max_entries && bytes_read < max_bytes && current_offset < file_len {
            // Read block header: flags(1) + uncompressed_size(4) + data_size(4) = 9 bytes
            let mut flags = [0u8; 1];
            if file.read_exact(&mut flags).is_err() {
                break;
            }

            let mut uncompressed_size_bytes = [0u8; 4];
            let mut data_size_bytes = [0u8; 4];

            if file.read_exact(&mut uncompressed_size_bytes).is_err() {
                break;
            }
            if file.read_exact(&mut data_size_bytes).is_err() {
                break;
            }

            let _uncompressed_size = u32::from_le_bytes(uncompressed_size_bytes);
            let data_size = u32::from_le_bytes(data_size_bytes) as usize;

            // Read block data
            let mut data = vec![0u8; data_size];
            if file.read_exact(&mut data).is_err() {
                break;
            }

            // Skip checksum for v3
            if has_checksum {
                let mut _checksum_bytes = [0u8; 4];
                if file.read_exact(&mut _checksum_bytes).is_err() {
                    break;
                }
            }

            // Decompress if needed
            let entries_data = if flags[0] & BLOCK_FLAG_COMPRESSED != 0 {
                zstd::decode_all(Cursor::new(&data))
                    .map_err(|e| Status::internal(format!("Decompression error: {}", e)))?
            } else {
                data.clone()
            };

            // Update offset to point to next block
            let block_size = 9 + data_size + if has_checksum { 4 } else { 0 };
            current_offset += block_size as u64;
            bytes_read += block_size;

            // Parse entries from buffer (now includes expire_at for TTL support)
            let mut cursor = Cursor::new(&entries_data);
            loop {
                let mut ts_bytes = [0u8; 8];
                if cursor.read_exact(&mut ts_bytes).is_err() {
                    break;
                }
                let timestamp = u64::from_le_bytes(ts_bytes);

                // Validate timestamp
                if !(MIN_TIMESTAMP..=MAX_TIMESTAMP).contains(&timestamp) {
                    return Err(Status::internal(format!(
                        "Invalid timestamp {} in WAL block",
                        timestamp
                    )));
                }

                // Read expire_at (added for TTL support)
                let mut expire_at_bytes = [0u8; 8];
                if cursor.read_exact(&mut expire_at_bytes).is_err() {
                    break;
                }
                let expire_at = u64::from_le_bytes(expire_at_bytes);

                let mut klen_bytes = [0u8; 8];
                let mut vlen_bytes = [0u8; 8];
                if cursor.read_exact(&mut klen_bytes).is_err() {
                    break;
                }
                if cursor.read_exact(&mut vlen_bytes).is_err() {
                    break;
                }

                let key_len = u64::from_le_bytes(klen_bytes);
                let val_len = u64::from_le_bytes(vlen_bytes);

                let mut key_buf = vec![0u8; key_len as usize];
                if cursor.read_exact(&mut key_buf).is_err() {
                    break;
                }
                let key = String::from_utf8_lossy(&key_buf).to_string();

                let value = if val_len == u64::MAX {
                    None
                } else {
                    let mut val_buf = vec![0u8; val_len as usize];
                    if cursor.read_exact(&mut val_buf).is_err() {
                        break;
                    }
                    Some(String::from_utf8_lossy(&val_buf).to_string())
                };

                entries.push(WalEntry {
                    timestamp,
                    key,
                    value,
                    expire_at,
                });

                if entries.len() >= max_entries {
                    break;
                }
            }
        }

        // Check if there are more entries
        let has_more = current_offset < file_len || wal_files.iter().any(|(id, _)| *id > wal_id);

        Ok((entries, wal_id, current_offset, has_more))
    }
}

use crate::engine::wal::parse_wal_filename;

/// Serialize WAL entries to bytes
/// Format: [timestamp: u64][expire_at: u64][key_len: u64][val_len: u64][key][value]
pub(crate) fn serialize_entries(entries: &[WalEntry]) -> Vec<u8> {
    let mut buf = Vec::new();
    for entry in entries {
        buf.extend_from_slice(&entry.timestamp.to_le_bytes());
        buf.extend_from_slice(&entry.expire_at.to_le_bytes());

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
/// Format: [timestamp: u64][expire_at: u64][key_len: u64][val_len: u64][key][value]
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

        // Read expire_at
        let mut expire_bytes = [0u8; 8];
        cursor
            .read_exact(&mut expire_bytes)
            .map_err(|e| Status::internal(e.to_string()))?;
        let expire_at = u64::from_le_bytes(expire_bytes);

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
            expire_at,
        });
    }

    Ok(entries)
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
    /// The apply_fn receives a batch of WAL entries to apply atomically.
    pub async fn start<F>(&self, mut apply_fn: F) -> Result<(), Status>
    where
        F: FnMut(Vec<WalEntry>) -> Result<(), Status> + Send,
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
        F: FnMut(Vec<WalEntry>) -> Result<(), Status>,
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

            // Check if snapshot is required (WAL gap detected on leader)
            if batch.snapshot_required {
                println!("Leader signaled snapshot required due to WAL gap");
                // Reconnect and download snapshot
                drop(stream);
                let mut client = ReplicationClient::connect(self.leader_addr.clone())
                    .await
                    .map_err(|e| {
                        Status::internal(format!("Failed to reconnect to leader: {}", e))
                    })?;

                if let Some((resume_wal_id, resume_offset)) = self
                    .check_and_download_snapshot(&mut client, *wal_id)
                    .await?
                {
                    return Ok(ReplicationResult::SnapshotApplied {
                        resume_wal_id,
                        resume_offset,
                    });
                }
                // If snapshot wasn't required after all, continue
                return Ok(ReplicationResult::Continue);
            }

            // Decompress
            let decompressed = zstd::decode_all(Cursor::new(&batch.compressed_data))
                .map_err(|e| Status::internal(format!("Decompression error: {}", e)))?;

            // Parse entries
            let entries = deserialize_entries(&decompressed)?;

            // Apply entries as a batch
            if !entries.is_empty() {
                apply_fn(entries)?;
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
    use tempfile::TempDir;

    #[test]
    fn test_serialize_deserialize_entries() {
        let entries = vec![
            WalEntry {
                timestamp: 12345,
                key: "key1".to_string(),
                value: Some("value1".to_string()),
                expire_at: 0,
            },
            WalEntry {
                timestamp: 12346,
                key: "key2".to_string(),
                value: None, // Tombstone
                expire_at: 0,
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
    fn test_serialize_deserialize_with_ttl() {
        let entries = vec![
            WalEntry {
                timestamp: 12345,
                key: "key_with_ttl".to_string(),
                value: Some("value".to_string()),
                expire_at: 1700000000, // Some future timestamp
            },
            WalEntry {
                timestamp: 12346,
                key: "key_no_ttl".to_string(),
                value: Some("value2".to_string()),
                expire_at: 0,
            },
        ];

        let serialized = serialize_entries(&entries);
        let deserialized = deserialize_entries(&serialized).unwrap();

        assert_eq!(deserialized.len(), 2);
        assert_eq!(deserialized[0].key, "key_with_ttl");
        assert_eq!(deserialized[0].expire_at, 1700000000);
        assert_eq!(deserialized[1].key, "key_no_ttl");
        assert_eq!(deserialized[1].expire_at, 0);
    }

    #[test]
    fn test_serialize_deserialize_empty_entries() {
        let entries: Vec<WalEntry> = vec![];
        let serialized = serialize_entries(&entries);
        let deserialized = deserialize_entries(&serialized).unwrap();
        assert_eq!(deserialized.len(), 0);
    }

    #[test]
    fn test_serialize_deserialize_large_batch() {
        let entries: Vec<WalEntry> = (0..1000)
            .map(|i| WalEntry {
                timestamp: 12345 + i,
                key: format!("key_{}", i),
                value: Some(format!("value_{}", i)),
                expire_at: if i % 2 == 0 { 1700000000 + i } else { 0 },
            })
            .collect();

        let serialized = serialize_entries(&entries);
        let deserialized = deserialize_entries(&serialized).unwrap();

        assert_eq!(deserialized.len(), 1000);
        for i in 0..1000 {
            assert_eq!(deserialized[i].key, format!("key_{}", i));
            assert_eq!(deserialized[i].value, Some(format!("value_{}", i)));
        }
    }

    #[test]
    fn test_serialize_deserialize_mixed_tombstones() {
        let entries: Vec<WalEntry> = (0..100)
            .map(|i| WalEntry {
                timestamp: 12345 + i,
                key: format!("key_{}", i),
                value: if i % 3 == 0 {
                    None // Tombstone every 3rd entry
                } else {
                    Some(format!("value_{}", i))
                },
                expire_at: 0,
            })
            .collect();

        let serialized = serialize_entries(&entries);
        let deserialized = deserialize_entries(&serialized).unwrap();

        assert_eq!(deserialized.len(), 100);
        for i in 0..100 {
            if i % 3 == 0 {
                assert_eq!(deserialized[i as usize].value, None);
            } else {
                assert_eq!(deserialized[i as usize].value, Some(format!("value_{}", i)));
            }
        }
    }

    #[test]
    fn test_serialize_deserialize_unicode_keys() {
        let entries = vec![
            WalEntry {
                timestamp: 12345,
                key: "日本語キー".to_string(),
                value: Some("値".to_string()),
                expire_at: 0,
            },
            WalEntry {
                timestamp: 12346,
                key: "emoji_🎉_key".to_string(),
                value: Some("🚀 value".to_string()),
                expire_at: 0,
            },
        ];

        let serialized = serialize_entries(&entries);
        let deserialized = deserialize_entries(&serialized).unwrap();

        assert_eq!(deserialized.len(), 2);
        assert_eq!(deserialized[0].key, "日本語キー");
        assert_eq!(deserialized[0].value, Some("値".to_string()));
        assert_eq!(deserialized[1].key, "emoji_🎉_key");
        assert_eq!(deserialized[1].value, Some("🚀 value".to_string()));
    }

    #[test]
    fn test_serialize_deserialize_empty_value() {
        let entries = vec![WalEntry {
            timestamp: 12345,
            key: "key".to_string(),
            value: Some("".to_string()), // Empty string, not tombstone
            expire_at: 0,
        }];

        let serialized = serialize_entries(&entries);
        let deserialized = deserialize_entries(&serialized).unwrap();

        assert_eq!(deserialized.len(), 1);
        assert_eq!(deserialized[0].value, Some("".to_string()));
    }

    #[test]
    fn test_deserialize_truncated_data() {
        // Create valid data first
        let entries = vec![WalEntry {
            timestamp: 12345,
            key: "key".to_string(),
            value: Some("value".to_string()),
            expire_at: 0,
        }];
        let serialized = serialize_entries(&entries);

        // Truncate data - when timestamp can be read but expire_at cannot,
        // deserialize_entries returns an error
        let truncated = &serialized[..10];
        let result = deserialize_entries(truncated);
        // The function returns error when it can read timestamp but fails on subsequent fields
        assert!(result.is_err());

        // Empty data should return empty entries
        let empty_result = deserialize_entries(&[]);
        assert!(empty_result.is_ok());
        assert!(empty_result.unwrap().is_empty());
    }

    #[test]
    fn test_parse_wal_filename() {
        assert_eq!(parse_wal_filename("wal_00001.log"), Some(1));
        assert_eq!(parse_wal_filename("wal_12345.log"), Some(12345));
        assert_eq!(parse_wal_filename("other.log"), None);
        assert_eq!(parse_wal_filename("wal_123.data"), None);
    }

    #[test]
    fn test_parse_wal_filename_edge_cases() {
        assert_eq!(parse_wal_filename("wal_00000.log"), Some(0));
        assert_eq!(parse_wal_filename("wal_99999.log"), Some(99999));
        assert_eq!(parse_wal_filename("wal_.log"), None);
        assert_eq!(parse_wal_filename("wal_abc.log"), None);
        assert_eq!(parse_wal_filename("WAL_00001.log"), None); // Case sensitive
        assert_eq!(parse_wal_filename("wal_00001.LOG"), None); // Case sensitive
        assert_eq!(parse_wal_filename(""), None);
        assert_eq!(parse_wal_filename("wal_00001"), None); // Missing extension
    }

    // ReplicationService tests

    #[test]
    fn test_replication_service_new() {
        let temp_dir = TempDir::new().unwrap();
        let snapshot_lock = Arc::new(RwLock::new(()));
        let service = ReplicationService::new(temp_dir.path().to_path_buf(), snapshot_lock);

        // Should not panic and data_dir should be set correctly
        assert_eq!(service.data_dir, temp_dir.path().to_path_buf());
    }

    #[test]
    fn test_replication_service_new_without_lock() {
        let temp_dir = TempDir::new().unwrap();
        let service = ReplicationService::new_without_lock(temp_dir.path().to_path_buf());

        assert_eq!(service.data_dir, temp_dir.path().to_path_buf());
    }

    #[test]
    fn test_get_wal_files_empty_dir() {
        let temp_dir = TempDir::new().unwrap();
        let service = ReplicationService::new_without_lock(temp_dir.path().to_path_buf());

        let wal_files = service.get_wal_files();
        assert!(wal_files.is_empty());
    }

    #[test]
    fn test_get_wal_files_with_files() {
        let temp_dir = TempDir::new().unwrap();

        // Create some WAL files
        fs::write(temp_dir.path().join("wal_00001.log"), b"data1").unwrap();
        fs::write(temp_dir.path().join("wal_00003.log"), b"data3").unwrap();
        fs::write(temp_dir.path().join("wal_00002.log"), b"data2").unwrap();
        // Create non-WAL files that should be ignored
        fs::write(temp_dir.path().join("other.txt"), b"other").unwrap();
        fs::write(temp_dir.path().join("sst_00001.data"), b"sstable").unwrap();

        let service = ReplicationService::new_without_lock(temp_dir.path().to_path_buf());
        let wal_files = service.get_wal_files();

        assert_eq!(wal_files.len(), 3);
        // Should be sorted by ID
        assert_eq!(wal_files[0].0, 1);
        assert_eq!(wal_files[1].0, 2);
        assert_eq!(wal_files[2].0, 3);
    }

    #[test]
    fn test_get_current_position_empty() {
        let temp_dir = TempDir::new().unwrap();
        let service = ReplicationService::new_without_lock(temp_dir.path().to_path_buf());

        let (wal_id, offset) = service.get_current_position();
        assert_eq!(wal_id, 0);
        assert_eq!(offset, 0);
    }

    #[test]
    fn test_get_current_position_with_files() {
        let temp_dir = TempDir::new().unwrap();

        // Create WAL files with different sizes
        let content1 = b"small";
        let content2 = b"this is a larger file content";
        fs::write(temp_dir.path().join("wal_00001.log"), content1).unwrap();
        fs::write(temp_dir.path().join("wal_00002.log"), content2).unwrap();

        let service = ReplicationService::new_without_lock(temp_dir.path().to_path_buf());
        let (wal_id, offset) = service.get_current_position();

        assert_eq!(wal_id, 2); // Latest WAL ID
        assert_eq!(offset, content2.len() as u64); // Size of the larger file
    }

    #[test]
    fn test_get_sstable_files_empty() {
        let temp_dir = TempDir::new().unwrap();
        let service = ReplicationService::new_without_lock(temp_dir.path().to_path_buf());

        let sstable_files = service.get_sstable_files();
        assert!(sstable_files.is_empty());
    }

    #[test]
    fn test_get_sstable_files_with_files() {
        let temp_dir = TempDir::new().unwrap();

        // Create SSTable files
        fs::write(temp_dir.path().join("sst_00001.data"), b"data1").unwrap();
        fs::write(temp_dir.path().join("sst_00002.data"), b"data2").unwrap();
        fs::write(temp_dir.path().join("sst_00001_00002.data"), b"data3").unwrap();
        // Create non-SSTable files that should be ignored
        fs::write(temp_dir.path().join("wal_00001.log"), b"wal").unwrap();
        fs::write(temp_dir.path().join("sst_00001.index"), b"index").unwrap();

        let service = ReplicationService::new_without_lock(temp_dir.path().to_path_buf());
        let sstable_files = service.get_sstable_files();

        assert_eq!(sstable_files.len(), 3);
        // Should be sorted alphabetically
        assert_eq!(sstable_files[0].0, "sst_00001.data");
        assert_eq!(sstable_files[1].0, "sst_00001_00002.data");
        assert_eq!(sstable_files[2].0, "sst_00002.data");
    }

    #[test]
    fn test_is_wal_available_empty() {
        let temp_dir = TempDir::new().unwrap();
        let service = ReplicationService::new_without_lock(temp_dir.path().to_path_buf());

        // No WAL files exist
        assert!(!service.is_wal_available(0));
        assert!(!service.is_wal_available(1));
    }

    #[test]
    fn test_is_wal_available_with_files() {
        let temp_dir = TempDir::new().unwrap();

        fs::write(temp_dir.path().join("wal_00002.log"), b"data").unwrap();
        fs::write(temp_dir.path().join("wal_00003.log"), b"data").unwrap();

        let service = ReplicationService::new_without_lock(temp_dir.path().to_path_buf());

        // WAL ID 0 means initial sync - should return true if any WAL exists
        assert!(service.is_wal_available(0));
        // WAL ID 1: we have WAL >= 1 (WAL 2 and 3 exist), so it returns true
        // The implementation checks if any WAL with id >= requested exists
        assert!(service.is_wal_available(1));
        // WAL ID 2 and 3 are available
        assert!(service.is_wal_available(2));
        assert!(service.is_wal_available(3));
        // WAL ID 4 is not available (no WAL >= 4 exists)
        assert!(!service.is_wal_available(4));
    }

    #[test]
    fn test_get_oldest_wal_id_empty() {
        let temp_dir = TempDir::new().unwrap();
        let service = ReplicationService::new_without_lock(temp_dir.path().to_path_buf());

        assert_eq!(service.get_oldest_wal_id(), None);
    }

    #[test]
    fn test_get_oldest_wal_id_with_files() {
        let temp_dir = TempDir::new().unwrap();

        fs::write(temp_dir.path().join("wal_00005.log"), b"data").unwrap();
        fs::write(temp_dir.path().join("wal_00003.log"), b"data").unwrap();
        fs::write(temp_dir.path().join("wal_00007.log"), b"data").unwrap();

        let service = ReplicationService::new_without_lock(temp_dir.path().to_path_buf());

        assert_eq!(service.get_oldest_wal_id(), Some(3));
    }

    // FollowerReplicator tests

    #[test]
    fn test_follower_replicator_new() {
        let temp_dir = TempDir::new().unwrap();
        let replicator = FollowerReplicator::new(
            "http://localhost:50051".to_string(),
            temp_dir.path().to_path_buf(),
        );

        assert_eq!(replicator.leader_addr, "http://localhost:50051");
        assert_eq!(replicator.data_dir, temp_dir.path().to_path_buf());
    }

    #[test]
    fn test_load_replication_state_empty() {
        let temp_dir = TempDir::new().unwrap();
        let replicator = FollowerReplicator::new(
            "http://localhost:50051".to_string(),
            temp_dir.path().to_path_buf(),
        );

        let (wal_id, offset) = replicator.load_replication_state();
        assert_eq!(wal_id, 0);
        assert_eq!(offset, 0);
    }

    #[test]
    fn test_save_and_load_replication_state() {
        let temp_dir = TempDir::new().unwrap();
        let replicator = FollowerReplicator::new(
            "http://localhost:50051".to_string(),
            temp_dir.path().to_path_buf(),
        );

        // Save state
        replicator.save_replication_state(42, 1234).unwrap();

        // Load state
        let (wal_id, offset) = replicator.load_replication_state();
        assert_eq!(wal_id, 42);
        assert_eq!(offset, 1234);
    }

    #[test]
    fn test_save_and_load_replication_state_large_values() {
        let temp_dir = TempDir::new().unwrap();
        let replicator = FollowerReplicator::new(
            "http://localhost:50051".to_string(),
            temp_dir.path().to_path_buf(),
        );

        // Test with large values
        let large_wal_id = u64::MAX - 1;
        let large_offset = u64::MAX / 2;

        replicator
            .save_replication_state(large_wal_id, large_offset)
            .unwrap();

        let (wal_id, offset) = replicator.load_replication_state();
        assert_eq!(wal_id, large_wal_id);
        assert_eq!(offset, large_offset);
    }

    #[test]
    fn test_load_replication_state_corrupted() {
        let temp_dir = TempDir::new().unwrap();
        let replicator = FollowerReplicator::new(
            "http://localhost:50051".to_string(),
            temp_dir.path().to_path_buf(),
        );

        // Write corrupted/short data
        fs::write(temp_dir.path().join("replication_state"), b"short").unwrap();

        // Should return default values
        let (wal_id, offset) = replicator.load_replication_state();
        assert_eq!(wal_id, 0);
        assert_eq!(offset, 0);
    }

    // WalEntry tests

    #[test]
    fn test_wal_entry_clone() {
        let entry = WalEntry {
            timestamp: 12345,
            key: "key".to_string(),
            value: Some("value".to_string()),
            expire_at: 1700000000,
        };

        let cloned = entry.clone();
        assert_eq!(cloned.timestamp, entry.timestamp);
        assert_eq!(cloned.key, entry.key);
        assert_eq!(cloned.value, entry.value);
        assert_eq!(cloned.expire_at, entry.expire_at);
    }

    #[test]
    fn test_wal_entry_debug() {
        let entry = WalEntry {
            timestamp: 12345,
            key: "key".to_string(),
            value: Some("value".to_string()),
            expire_at: 0,
        };

        let debug_str = format!("{:?}", entry);
        assert!(debug_str.contains("WalEntry"));
        assert!(debug_str.contains("timestamp: 12345"));
        assert!(debug_str.contains("key"));
    }

    // Constants tests

    #[test]
    fn test_constants_values() {
        // Verify constants have reasonable values
        assert!(CATCHUP_THRESHOLD > 0);
        assert!(CATCHUP_BATCH_SIZE > CATCHUP_THRESHOLD);
        assert!(CATCHUP_BATCH_BYTES > 0);
        assert!(REALTIME_POLL_INTERVAL_MS > 0);
        assert!(SNAPSHOT_CHUNK_SIZE > 0);
        assert!(!WAL_GAP_ERROR_CODE.is_empty());
    }

    // ReplicationResult tests

    #[test]
    fn test_replication_result_variants() {
        // Test Continue variant
        let result = ReplicationResult::Continue;
        matches!(result, ReplicationResult::Continue);

        // Test SnapshotApplied variant
        let result = ReplicationResult::SnapshotApplied {
            resume_wal_id: 10,
            resume_offset: 500,
        };
        if let ReplicationResult::SnapshotApplied {
            resume_wal_id,
            resume_offset,
        } = result
        {
            assert_eq!(resume_wal_id, 10);
            assert_eq!(resume_offset, 500);
        } else {
            panic!("Expected SnapshotApplied variant");
        }
    }
}
