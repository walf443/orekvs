//! gRPC service implementation for replication.
//!
//! Implements the `Replication` trait generated from protobuf definitions.

use std::fs::{self, File};
use std::io::{Cursor, Read};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio_stream::Stream;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use super::{
    CATCHUP_BATCH_BYTES, CATCHUP_BATCH_SIZE, CATCHUP_THRESHOLD, Empty, LeaderInfo,
    REALTIME_POLL_INTERVAL_MS, Replication, ReplicationService, SNAPSHOT_CHUNK_SIZE, SnapshotChunk,
    SnapshotInfo, SnapshotRequest, StreamSnapshotRequest, StreamWalRequest, WAL_GAP_ERROR_CODE,
    WalBatch, serialize_entries,
};

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
                            snapshot_required: false,
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
                        // Check if this is a WAL gap error (follower is too far behind)
                        if e.message() == WAL_GAP_ERROR_CODE {
                            println!(
                                "WAL gap detected for follower {}, sending snapshot_required signal",
                                peer_addr_str
                            );
                            // Send a special batch to signal snapshot is required
                            let batch = WalBatch {
                                wal_id: current_wal_id,
                                start_offset: current_offset,
                                end_offset: current_offset,
                                compressed_data: vec![],
                                entry_count: 0,
                                snapshot_required: true,
                            };
                            let _ = tx.send(Ok(batch)).await;
                        } else {
                            let _ = tx.send(Err(e)).await;
                        }
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
