use std::path::PathBuf;
use std::sync::Arc;
use tonic::{Request, Response, Status, transport::Server};

pub mod cli;

pub mod kv {
    tonic::include_proto!("kv");
}

use kv::key_value_server::{KeyValue, KeyValueServer};
use kv::{
    BatchDeleteRequest, BatchDeleteResponse, BatchGetRequest, BatchGetResponse, BatchSetRequest,
    BatchSetResponse, CompareAndSetRequest, CompareAndSetResponse, CountRequest, CountResponse,
    DeleteRequest, DeleteResponse, GetExpireAtRequest, GetExpireAtResponse, GetMetricsRequest,
    GetMetricsResponse, GetRequest, GetResponse, KeyValuePair, PromoteRequest, PromoteResponse,
    SetRequest, SetResponse,
};

use crate::engine::{
    Engine,
    btree::{BTreeEngine, BTreeEngineHolder, BTreeEngineWrapper},
    log::{LogEngine, LogEngineHolder, LogEngineWrapper},
    lsm_tree::{
        LsmEngineHolder, LsmTreeEngine, LsmTreeEngineWrapper, SnapshotLock, WalArchiveConfig,
    },
    memory::MemoryEngine,
};
pub use crate::replication::run_follower;
use crate::replication::{ReplicationServer, ReplicationService};

// --- gRPC Service ---

pub struct MyKeyValue {
    engine: Box<dyn Engine>,
    // Optional reference to LsmTreeEngine for metrics access
    lsm_engine: Option<Arc<LsmTreeEngine>>,
}

impl MyKeyValue {
    #[allow(clippy::too_many_arguments)]
    fn new(
        engine_type: EngineType,
        data_dir: String,
        log_capacity_bytes: u64,
        lsm_memtable_capacity_bytes: u64,
        lsm_compaction_trigger_file_count: usize,
        lsm_wal_batch_interval_micros: u64,
        lsm_holder: &mut LsmEngineHolder,
        log_holder: &mut LogEngineHolder,
        btree_holder: &mut BTreeEngineHolder,
        wal_archive_config: WalArchiveConfig,
    ) -> Self {
        let mut lsm_engine_ref = None;
        let engine: Box<dyn Engine> = match engine_type {
            EngineType::Memory => Box::new(MemoryEngine::new()),
            EngineType::Log => {
                let log_engine = Arc::new(LogEngine::new(data_dir, log_capacity_bytes));
                log_holder.set(Arc::clone(&log_engine));
                Box::new(LogEngineWrapper(log_engine))
            }
            EngineType::LsmTree => {
                let lsm_engine = Arc::new(LsmTreeEngine::new_with_config(
                    data_dir,
                    lsm_memtable_capacity_bytes,
                    lsm_compaction_trigger_file_count,
                    lsm_wal_batch_interval_micros,
                    wal_archive_config,
                ));
                lsm_holder.set(Arc::clone(&lsm_engine));
                lsm_engine_ref = Some(Arc::clone(&lsm_engine));
                Box::new(LsmTreeEngineWrapper(lsm_engine))
            }
            EngineType::BTree => {
                let btree_engine =
                    Arc::new(BTreeEngine::open(data_dir).expect("Failed to open BTree engine"));
                btree_holder.set(Arc::clone(&btree_engine));
                Box::new(BTreeEngineWrapper(btree_engine))
            }
        };
        MyKeyValue {
            engine,
            lsm_engine: lsm_engine_ref,
        }
    }
}

#[tonic::async_trait]
impl KeyValue for MyKeyValue {
    async fn set(&self, request: Request<SetRequest>) -> Result<Response<SetResponse>, Status> {
        let req = request.into_inner();
        if req.ttl_seconds > 0 {
            self.engine
                .set_with_ttl(req.key, req.value, req.ttl_seconds)?;
        } else {
            self.engine.set(req.key, req.value)?;
        }
        Ok(Response::new(SetResponse { success: true }))
    }

    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
        let req = request.into_inner();
        if req.include_expire_at {
            let (value, expire_at) = self.engine.get_with_expire_at(req.key)?;
            Ok(Response::new(GetResponse { value, expire_at }))
        } else {
            let value = self.engine.get(req.key)?;
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
        let req = request.into_inner();
        self.engine.delete(req.key)?;
        Ok(Response::new(DeleteResponse { success: true }))
    }

    async fn batch_set(
        &self,
        request: Request<BatchSetRequest>,
    ) -> Result<Response<BatchSetResponse>, Status> {
        let req = request.into_inner();
        let items: Vec<(String, String)> = req
            .items
            .into_iter()
            .map(|item| (item.key, item.value))
            .collect();

        let count = self.engine.batch_set(items)? as i32;

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
        let results = self.engine.batch_get(req.keys);
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
        let req = request.into_inner();
        let count = self.engine.batch_delete(req.keys)? as i32;

        Ok(Response::new(BatchDeleteResponse {
            success: true,
            count,
        }))
    }

    async fn get_metrics(
        &self,
        _request: Request<GetMetricsRequest>,
    ) -> Result<Response<GetMetricsResponse>, Status> {
        if let Some(ref lsm_engine) = self.lsm_engine {
            let metrics = lsm_engine.metrics();
            let cache_stats = lsm_engine.cache_stats();

            Ok(Response::new(GetMetricsResponse {
                // Operation counts
                get_count: metrics.get_count,
                set_count: metrics.set_count,
                delete_count: metrics.delete_count,
                batch_get_count: metrics.batch_get_count,
                batch_set_count: metrics.batch_set_count,
                batch_delete_count: metrics.batch_delete_count,

                // SSTable metrics
                sstable_searches: metrics.sstable_searches,
                bloom_filter_hits: metrics.bloom_filter_hits,
                bloom_filter_false_positives: metrics.bloom_filter_false_positives,
                bloom_effectiveness: metrics.bloom_effectiveness,

                // MemTable metrics
                memtable_flushes: metrics.memtable_flushes,
                memtable_flush_bytes: metrics.memtable_flush_bytes,

                // Compaction metrics
                compaction_count: metrics.compaction_count,
                compaction_bytes_read: metrics.compaction_bytes_read,
                compaction_bytes_written: metrics.compaction_bytes_written,

                // Block cache metrics
                blockcache_entries: cache_stats.entries as u64,
                blockcache_size_bytes: cache_stats.size_bytes as u64,
                blockcache_max_size_bytes: cache_stats.max_size_bytes as u64,
                blockcache_hits: cache_stats.hits,
                blockcache_misses: cache_stats.misses,
                blockcache_evictions: cache_stats.evictions,
                blockcache_hit_ratio: cache_stats.hit_ratio,
            }))
        } else {
            // Return empty metrics for non-LSM engines
            Ok(Response::new(GetMetricsResponse::default()))
        }
    }

    async fn get_expire_at(
        &self,
        request: Request<GetExpireAtRequest>,
    ) -> Result<Response<GetExpireAtResponse>, Status> {
        let req = request.into_inner();
        let (exists, expire_at) = self.engine.get_expire_at(req.key)?;
        Ok(Response::new(GetExpireAtResponse { expire_at, exists }))
    }

    async fn compare_and_set(
        &self,
        request: Request<CompareAndSetRequest>,
    ) -> Result<Response<CompareAndSetResponse>, Status> {
        let req = request.into_inner();
        let expected_value = if req.expect_exists {
            Some(req.expected_value)
        } else {
            None
        };
        let expire_at = if req.ttl_seconds > 0 {
            crate::engine::current_timestamp() + req.ttl_seconds
        } else {
            0
        };
        let (success, current_value) =
            self.engine
                .compare_and_set(req.key, expected_value, req.new_value, expire_at)?;
        Ok(Response::new(CompareAndSetResponse {
            success,
            current_value: current_value.unwrap_or_default(),
        }))
    }

    async fn promote_to_leader(
        &self,
        _request: Request<PromoteRequest>,
    ) -> Result<Response<PromoteResponse>, Status> {
        // This server is already a leader
        Ok(Response::new(PromoteResponse {
            success: false,
            message: "This server is already running as a leader".to_string(),
        }))
    }

    async fn count(
        &self,
        request: Request<CountRequest>,
    ) -> Result<Response<CountResponse>, Status> {
        let req = request.into_inner();
        let count = self.engine.count(&req.prefix)?;
        Ok(Response::new(CountResponse { count }))
    }
}

#[derive(clap::ValueEnum, Clone, Debug)]
pub enum EngineType {
    Memory,
    Log,
    #[value(name = "lsm-tree")]
    LsmTree,
    #[value(name = "btree")]
    BTree,
}

#[allow(clippy::too_many_arguments)]
pub async fn run_server(
    addr: std::net::SocketAddr,
    engine_type: EngineType,
    data_dir: String,
    log_capacity_bytes: u64,
    lsm_memtable_capacity_bytes: u64,
    lsm_compaction_trigger_file_count: usize,
    lsm_wal_batch_interval_micros: u64,
    replication_addr: Option<std::net::SocketAddr>,
    wal_archive_config: WalArchiveConfig,
) {
    let mut lsm_holder = LsmEngineHolder::new();
    let mut log_holder = LogEngineHolder::new();
    let mut btree_holder = BTreeEngineHolder::new();

    let key_value = MyKeyValue::new(
        engine_type,
        data_dir.clone(),
        log_capacity_bytes,
        lsm_memtable_capacity_bytes,
        lsm_compaction_trigger_file_count,
        lsm_wal_batch_interval_micros,
        &mut lsm_holder,
        &mut log_holder,
        &mut btree_holder,
        wal_archive_config,
    );

    println!("Server listening on {}", addr);

    // Get snapshot lock from LSM engine if available
    let snapshot_lock: Option<SnapshotLock> =
        lsm_holder.engine().map(|engine| engine.snapshot_lock());

    // Start replication service if enabled
    let replication_handle = if let Some(repl_addr) = replication_addr {
        println!("Replication service listening on {}", repl_addr);
        let replication_service = if let Some(lock) = snapshot_lock {
            ReplicationService::new(PathBuf::from(&data_dir), lock)
        } else {
            ReplicationService::new_without_lock(PathBuf::from(&data_dir))
        };
        let handle = tokio::spawn(async move {
            if let Err(e) = Server::builder()
                .add_service(ReplicationServer::new(replication_service))
                .serve(repl_addr)
                .await
            {
                eprintln!("Replication server error: {}", e);
            }
        });
        Some(handle)
    } else {
        None
    };

    // Build the server with graceful shutdown
    let server = Server::builder()
        .add_service(KeyValueServer::new(key_value))
        .serve_with_shutdown(addr, async {
            // Wait for SIGINT (Ctrl+C) or SIGTERM
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
        });

    // Run the server
    if let Err(e) = server.await {
        eprintln!("Server error: {}", e);
    }

    // Abort replication service
    if let Some(handle) = replication_handle {
        handle.abort();
    }

    // Gracefully shutdown the engines (flush pending writes)
    lsm_holder.shutdown().await;
    log_holder.shutdown().await;
    btree_holder.shutdown();

    println!("Server stopped.");
}
