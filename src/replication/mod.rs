mod proto {
    tonic::include_proto!("replication");
}

pub use proto::replication_client::ReplicationClient;
pub use proto::replication_server::{Replication, ReplicationServer};
pub use proto::{Empty, LeaderInfo, StreamWalRequest, WalBatch};

use std::fs::{self, File};
use std::io::{Cursor, Read, Seek, SeekFrom};
use std::path::PathBuf;
use std::pin::Pin;
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

/// Leader-side replication service
pub struct ReplicationService {
    data_dir: PathBuf,
}

impl ReplicationService {
    pub fn new(data_dir: PathBuf) -> Self {
        Self { data_dir }
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
            let service = ReplicationService::new(data_dir);

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
                Ok(()) => {
                    // Stream ended normally (shouldn't happen in normal operation)
                    println!("Replication stream ended, reconnecting...");
                }
                Err(e) => {
                    eprintln!("Replication error: {}, reconnecting in 1s...", e);
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }
    }

    async fn connect_and_stream<F>(
        &self,
        wal_id: &mut u64,
        offset: &mut u64,
        apply_fn: &mut F,
    ) -> Result<(), Status>
    where
        F: FnMut(WalEntry) -> Result<(), Status>,
    {
        let mut client = ReplicationClient::connect(self.leader_addr.clone())
            .await
            .map_err(|e| Status::internal(format!("Failed to connect to leader: {}", e)))?;

        println!("Connected to leader at {}", self.leader_addr);

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

        Ok(())
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
