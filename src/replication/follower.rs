//! Follower server implementation for leader-follower replication.
//!
//! This module provides the follower-side server that:
//! - Serves read requests to clients
//! - Replicates data from the leader
//! - Supports promotion to leader

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};

use tonic::{Request, Response, Status, transport::Server};

use crate::engine::Engine;
use crate::engine::lsm_tree::{LsmTreeEngine, SnapshotLock, WalArchiveConfig};
use crate::replication::{FollowerReplicator, ReplicationServer, ReplicationService};
use crate::server::kv::key_value_server::{KeyValue, KeyValueServer};
use crate::server::kv::{
    BatchDeleteRequest, BatchDeleteResponse, BatchGetRequest, BatchGetResponse, BatchSetRequest,
    BatchSetResponse, DeleteRequest, DeleteResponse, GetExpireAtRequest, GetExpireAtResponse,
    GetMetricsRequest, GetMetricsResponse, GetRequest, GetResponse, KeyValuePair, PromoteRequest,
    PromoteResponse, SetRequest, SetResponse,
};

/// Run as a follower, replicating from a leader
pub async fn run_follower(
    leader_addr: String,
    data_dir: String,
    lsm_memtable_capacity_bytes: u64,
    lsm_compaction_trigger_file_count: usize,
    lsm_wal_batch_interval_micros: u64,
    addr: std::net::SocketAddr,
    wal_archive_config: WalArchiveConfig,
) {
    use std::fs;

    // Create data directory
    let data_path = PathBuf::from(&data_dir);
    if !data_path.exists() {
        fs::create_dir_all(&data_path).expect("Failed to create data directory");
    }

    // Create follower state for promotion support
    let follower_state = Arc::new(FollowerState::new(data_path.clone(), addr));
    let follower_state_for_server = Arc::clone(&follower_state);
    let follower_state_for_repl = Arc::clone(&follower_state);

    // Store config for engine reloads
    let wal_archive_config_for_reload = wal_archive_config.clone();
    let batch_interval_for_reload = lsm_wal_batch_interval_micros;

    // Create swappable engine holder (using std::sync::RwLock for sync access in replication closure)
    let engine_holder: Arc<RwLock<Arc<LsmTreeEngine>>> =
        Arc::new(RwLock::new(Arc::new(LsmTreeEngine::new_with_config(
            data_dir.clone(),
            lsm_memtable_capacity_bytes,
            lsm_compaction_trigger_file_count,
            lsm_wal_batch_interval_micros,
            wal_archive_config,
        ))));
    let engine_holder_for_server = Arc::clone(&engine_holder);

    println!("Starting follower, replicating from {}", leader_addr);
    println!("Follower serving read requests on {}", addr);

    // Shutdown flag
    let shutdown_flag = Arc::new(AtomicBool::new(false));
    let shutdown_flag_server = Arc::clone(&shutdown_flag);

    // Start server with swappable engine and promotion support
    let server_handle = tokio::spawn(async move {
        let key_value = SwappableFollowerKeyValue {
            engine_holder: engine_holder_for_server,
            state: follower_state_for_server,
        };
        if let Err(e) = Server::builder()
            .add_service(KeyValueServer::new(key_value))
            .serve_with_shutdown(addr, async {
                let ctrl_c = tokio::signal::ctrl_c();

                #[cfg(unix)]
                {
                    use tokio::signal::unix::{SignalKind, signal};
                    let mut sigterm =
                        signal(SignalKind::terminate()).expect("Failed to create SIGTERM handler");

                    tokio::select! {
                        _ = ctrl_c => {
                            println!("\nReceived SIGINT (Ctrl+C)...");
                        }
                        _ = sigterm.recv() => {
                            println!("\nReceived SIGTERM...");
                        }
                    }
                }

                #[cfg(not(unix))]
                {
                    ctrl_c.await.expect("Failed to listen for ctrl+c signal");
                    println!("\nReceived shutdown signal...");
                }

                shutdown_flag_server.store(true, Ordering::SeqCst);
            })
            .await
        {
            eprintln!("Follower server error: {}", e);
        }
    });

    // Replication loop with engine reload support and promotion support
    let data_path_for_repl = data_path.clone();
    let engine_holder_for_repl = Arc::clone(&engine_holder);

    // Create stop channel for promotion
    let (stop_tx, mut stop_rx) = tokio::sync::oneshot::channel::<()>();
    {
        let mut guard = follower_state_for_repl.stop_replication_tx.lock().await;
        *guard = Some(stop_tx);
    }

    loop {
        // Check if we should stop (shutdown or promoted to leader)
        if shutdown_flag.load(Ordering::SeqCst) {
            println!("Shutdown requested, stopping replication...");
            break;
        }

        if follower_state_for_repl.is_write_enabled() {
            println!("Promoted to leader, stopping replication...");
            break;
        }

        // Check for stop signal (non-blocking)
        if stop_rx.try_recv().is_ok() {
            println!("Stop signal received, stopping replication...");
            break;
        }

        let replicator = FollowerReplicator::new(leader_addr.clone(), data_path_for_repl.clone());
        let engine_holder_clone = Arc::clone(&engine_holder_for_repl);

        // Apply WAL entries to the engine
        let result = replicator
            .start(move |entry| {
                // Get current engine reference (using std::sync::RwLock)
                let engine = {
                    let guard = engine_holder_clone.read().unwrap();
                    Arc::clone(&*guard)
                };

                if let Some(value) = entry.value {
                    engine.set_with_expire_at(entry.key, value, entry.expire_at)
                } else {
                    engine.delete(entry.key)
                }
            })
            .await;

        match result {
            Ok(()) => {
                // Normal exit (shouldn't happen)
                break;
            }
            Err(e) if e.code() == tonic::Code::Aborted => {
                // Snapshot applied, reload engine
                println!("Reloading engine after snapshot...");

                // Shutdown old engine
                let old_engine = {
                    let guard = engine_holder_for_repl.read().unwrap();
                    Arc::clone(&*guard)
                };
                old_engine.shutdown().await;

                // Create new engine (will load the downloaded SSTables)
                let new_engine = Arc::new(LsmTreeEngine::new_with_config(
                    data_dir.clone(),
                    lsm_memtable_capacity_bytes,
                    lsm_compaction_trigger_file_count,
                    batch_interval_for_reload,
                    wal_archive_config_for_reload.clone(),
                ));

                // Swap engine
                {
                    let mut guard = engine_holder_for_repl.write().unwrap();
                    *guard = new_engine;
                }

                println!("Engine reloaded, continuing replication...");
                // Continue to next iteration of the loop
            }
            Err(e) if e.code() == tonic::Code::Cancelled => {
                // Replication was cancelled (promotion)
                println!("Replication cancelled");
                break;
            }
            Err(e) => {
                eprintln!("Replication error: {}", e);
                // Wait before retrying
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
        }
    }

    // Wait for server to finish
    let _ = server_handle.await;

    // Shutdown final engine
    {
        let engine = {
            let guard = engine_holder.read().unwrap();
            Arc::clone(&*guard)
        };
        engine.shutdown().await;
    }

    println!("Follower stopped.");
}

/// Shared state for follower that can be promoted to leader
pub struct FollowerState {
    /// Whether writes are enabled (false = follower, true = leader)
    write_enabled: AtomicBool,
    /// Channel to signal replication to stop
    stop_replication_tx: tokio::sync::Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
    /// Data directory for replication service
    data_dir: PathBuf,
    /// Server address (for calculating replication port)
    server_addr: std::net::SocketAddr,
    /// Replication service handle
    replication_handle: tokio::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl FollowerState {
    fn new(data_dir: PathBuf, server_addr: std::net::SocketAddr) -> Self {
        Self {
            write_enabled: AtomicBool::new(false),
            stop_replication_tx: tokio::sync::Mutex::new(None),
            data_dir,
            server_addr,
            replication_handle: tokio::sync::Mutex::new(None),
        }
    }

    fn is_write_enabled(&self) -> bool {
        self.write_enabled.load(Ordering::SeqCst)
    }

    fn enable_writes(&self) {
        self.write_enabled.store(true, Ordering::SeqCst);
    }

    async fn stop_replication(&self) -> bool {
        let mut guard = self.stop_replication_tx.lock().await;
        if let Some(tx) = guard.take() {
            let _ = tx.send(());
            true
        } else {
            false
        }
    }

    async fn start_replication_service(
        &self,
        port: u16,
        snapshot_lock: SnapshotLock,
    ) -> Result<(), Status> {
        let repl_addr = std::net::SocketAddr::new(self.server_addr.ip(), port);
        let replication_service = ReplicationService::new(self.data_dir.clone(), snapshot_lock);

        println!("Starting replication service on {}", repl_addr);

        let handle = tokio::spawn(async move {
            if let Err(e) = Server::builder()
                .add_service(ReplicationServer::new(replication_service))
                .serve(repl_addr)
                .await
            {
                eprintln!("Replication server error: {}", e);
            }
        });

        let mut guard = self.replication_handle.lock().await;
        *guard = Some(handle);

        Ok(())
    }
}

/// Key-value service for followers with swappable engine and promotion support
struct SwappableFollowerKeyValue {
    engine_holder: Arc<RwLock<Arc<LsmTreeEngine>>>,
    state: Arc<FollowerState>,
}

#[tonic::async_trait]
impl KeyValue for SwappableFollowerKeyValue {
    async fn set(&self, request: Request<SetRequest>) -> Result<Response<SetResponse>, Status> {
        if !self.state.is_write_enabled() {
            return Err(Status::failed_precondition(
                "This is a read-only follower. Writes must go to the leader, or promote this node first.",
            ));
        }
        let req = request.into_inner();
        let engine = {
            let guard = self.engine_holder.read().unwrap();
            Arc::clone(&*guard)
        };
        engine.set(req.key, req.value)?;
        Ok(Response::new(SetResponse { success: true }))
    }

    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
        let req = request.into_inner();
        let engine = {
            let guard = self.engine_holder.read().unwrap();
            Arc::clone(&*guard)
        };
        if req.include_expire_at {
            let (value, expire_at) = engine.get_with_expire_at(req.key)?;
            Ok(Response::new(GetResponse { value, expire_at }))
        } else {
            let value = engine.get(req.key)?;
            Ok(Response::new(GetResponse {
                value,
                expire_at: 0,
            }))
        }
    }

    async fn delete(
        &self,
        request: Request<DeleteRequest>,
    ) -> Result<Response<DeleteResponse>, Status> {
        if !self.state.is_write_enabled() {
            return Err(Status::failed_precondition(
                "This is a read-only follower. Writes must go to the leader, or promote this node first.",
            ));
        }
        let req = request.into_inner();
        let engine = {
            let guard = self.engine_holder.read().unwrap();
            Arc::clone(&*guard)
        };
        engine.delete(req.key)?;
        Ok(Response::new(DeleteResponse { success: true }))
    }

    async fn batch_set(
        &self,
        request: Request<BatchSetRequest>,
    ) -> Result<Response<BatchSetResponse>, Status> {
        if !self.state.is_write_enabled() {
            return Err(Status::failed_precondition(
                "This is a read-only follower. Writes must go to the leader, or promote this node first.",
            ));
        }
        let req = request.into_inner();
        let engine = {
            let guard = self.engine_holder.read().unwrap();
            Arc::clone(&*guard)
        };
        let items: Vec<(String, String)> = req
            .items
            .into_iter()
            .map(|item| (item.key, item.value))
            .collect();
        let count = engine.batch_set(items)? as i32;
        Ok(Response::new(BatchSetResponse {
            success: true,
            count,
        }))
    }

    async fn batch_get(
        &self,
        request: Request<BatchGetRequest>,
    ) -> Result<Response<BatchGetResponse>, Status> {
        let req = request.into_inner();
        let engine = {
            let guard = self.engine_holder.read().unwrap();
            Arc::clone(&*guard)
        };
        let results = engine.batch_get(req.keys);
        let items = results
            .into_iter()
            .map(|(key, value)| KeyValuePair { key, value })
            .collect();

        Ok(Response::new(BatchGetResponse { items }))
    }

    async fn batch_delete(
        &self,
        request: Request<BatchDeleteRequest>,
    ) -> Result<Response<BatchDeleteResponse>, Status> {
        if !self.state.is_write_enabled() {
            return Err(Status::failed_precondition(
                "This is a read-only follower. Writes must go to the leader, or promote this node first.",
            ));
        }
        let req = request.into_inner();
        let engine = {
            let guard = self.engine_holder.read().unwrap();
            Arc::clone(&*guard)
        };
        let count = engine.batch_delete(req.keys)? as i32;
        Ok(Response::new(BatchDeleteResponse {
            success: true,
            count,
        }))
    }

    async fn get_metrics(
        &self,
        _request: Request<GetMetricsRequest>,
    ) -> Result<Response<GetMetricsResponse>, Status> {
        let engine = {
            let guard = self.engine_holder.read().unwrap();
            Arc::clone(&*guard)
        };
        let metrics = engine.metrics();
        let cache_stats = engine.cache_stats();

        Ok(Response::new(GetMetricsResponse {
            get_count: metrics.get_count,
            set_count: metrics.set_count,
            delete_count: metrics.delete_count,
            batch_get_count: metrics.batch_get_count,
            batch_set_count: metrics.batch_set_count,
            batch_delete_count: metrics.batch_delete_count,
            sstable_searches: metrics.sstable_searches,
            bloom_filter_hits: metrics.bloom_filter_hits,
            bloom_filter_false_positives: metrics.bloom_filter_false_positives,
            bloom_effectiveness: metrics.bloom_effectiveness,
            memtable_flushes: metrics.memtable_flushes,
            memtable_flush_bytes: metrics.memtable_flush_bytes,
            compaction_count: metrics.compaction_count,
            compaction_bytes_read: metrics.compaction_bytes_read,
            compaction_bytes_written: metrics.compaction_bytes_written,
            blockcache_entries: cache_stats.entries as u64,
            blockcache_size_bytes: cache_stats.size_bytes as u64,
            blockcache_max_size_bytes: cache_stats.max_size_bytes as u64,
            blockcache_hits: cache_stats.hits,
            blockcache_misses: cache_stats.misses,
            blockcache_evictions: cache_stats.evictions,
            blockcache_hit_ratio: cache_stats.hit_ratio,
        }))
    }

    async fn get_expire_at(
        &self,
        request: Request<GetExpireAtRequest>,
    ) -> Result<Response<GetExpireAtResponse>, Status> {
        let req = request.into_inner();
        let engine = {
            let guard = self.engine_holder.read().unwrap();
            Arc::clone(&*guard)
        };
        let (exists, expire_at) = engine.get_expire_at(req.key)?;
        Ok(Response::new(GetExpireAtResponse { expire_at, exists }))
    }

    async fn promote_to_leader(
        &self,
        request: Request<PromoteRequest>,
    ) -> Result<Response<PromoteResponse>, Status> {
        if self.state.is_write_enabled() {
            return Ok(Response::new(PromoteResponse {
                success: false,
                message: "Already running as leader".to_string(),
            }));
        }

        let req = request.into_inner();

        // Stop replication
        let stopped = self.state.stop_replication().await;
        if stopped {
            println!("Replication stopped for leader promotion");
        }

        // Enable writes
        self.state.enable_writes();
        println!("Write operations enabled - now running as leader");

        // Start replication service if requested
        if req.enable_replication_service {
            let port = if req.replication_port > 0 {
                req.replication_port as u16
            } else {
                self.state.server_addr.port() + 1
            };

            // Get snapshot lock from engine
            let snapshot_lock = {
                let engine = self.engine_holder.read().unwrap();
                engine.snapshot_lock()
            };

            if let Err(e) = self
                .state
                .start_replication_service(port, snapshot_lock)
                .await
            {
                return Ok(Response::new(PromoteResponse {
                    success: true,
                    message: format!(
                        "Promoted to leader but failed to start replication service: {}",
                        e
                    ),
                }));
            }
        }

        Ok(Response::new(PromoteResponse {
            success: true,
            message: "Successfully promoted to leader".to_string(),
        }))
    }
}
