use std::path::PathBuf;
use std::sync::Arc;
use tonic::{Request, Response, Status, transport::Server};

pub mod kv {
    tonic::include_proto!("kv");
}

use kv::key_value_server::{KeyValue, KeyValueServer};
use kv::{
    BatchDeleteRequest, BatchDeleteResponse, BatchGetRequest, BatchGetResponse, BatchSetRequest,
    BatchSetResponse, DeleteRequest, DeleteResponse, GetMetricsRequest, GetMetricsResponse,
    GetRequest, GetResponse, KeyValuePair, SetRequest, SetResponse,
};

use crate::engine::{Engine, log::LogEngine, lsm_tree::LsmTreeEngine, memory::MemoryEngine};
use crate::replication::{FollowerReplicator, ReplicationServer, ReplicationService};

// --- gRPC Service ---

pub struct MyKeyValue {
    engine: Box<dyn Engine>,
    // Optional reference to LsmTreeEngine for metrics access
    lsm_engine: Option<Arc<LsmTreeEngine>>,
}

/// Wrapper to hold LSM engine reference for graceful shutdown
struct LsmEngineHolder {
    engine: Option<Arc<LsmTreeEngine>>,
}

impl LsmEngineHolder {
    fn new() -> Self {
        LsmEngineHolder { engine: None }
    }

    fn set(&mut self, engine: Arc<LsmTreeEngine>) {
        self.engine = Some(engine);
    }

    async fn shutdown(&self) {
        if let Some(ref engine) = self.engine {
            engine.shutdown().await;
        }
    }
}

/// Wrapper to hold Log engine reference for graceful shutdown
struct LogEngineHolder {
    engine: Option<Arc<LogEngine>>,
}

impl LogEngineHolder {
    fn new() -> Self {
        LogEngineHolder { engine: None }
    }

    fn set(&mut self, engine: Arc<LogEngine>) {
        self.engine = Some(engine);
    }

    async fn shutdown(&self) {
        if let Some(ref engine) = self.engine {
            engine.shutdown().await;
        }
    }
}

impl MyKeyValue {
    fn new(
        engine_type: EngineType,
        data_dir: String,
        log_capacity_bytes: u64,
        lsm_memtable_capacity_bytes: u64,
        lsm_compaction_trigger_file_count: usize,
        lsm_holder: &mut LsmEngineHolder,
        log_holder: &mut LogEngineHolder,
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
                let lsm_engine = Arc::new(LsmTreeEngine::new(
                    data_dir,
                    lsm_memtable_capacity_bytes,
                    lsm_compaction_trigger_file_count,
                ));
                lsm_holder.set(Arc::clone(&lsm_engine));
                lsm_engine_ref = Some(Arc::clone(&lsm_engine));
                Box::new(LsmTreeEngineWrapper(lsm_engine))
            }
        };
        MyKeyValue {
            engine,
            lsm_engine: lsm_engine_ref,
        }
    }
}

/// Wrapper to implement Engine for Arc<LogEngine>
struct LogEngineWrapper(Arc<LogEngine>);

impl Engine for LogEngineWrapper {
    fn set(&self, key: String, value: String) -> Result<(), Status> {
        self.0.set(key, value)
    }

    fn get(&self, key: String) -> Result<String, Status> {
        self.0.get(key)
    }

    fn delete(&self, key: String) -> Result<(), Status> {
        self.0.delete(key)
    }
}

/// Wrapper to implement Engine for Arc<LsmTreeEngine>
struct LsmTreeEngineWrapper(Arc<LsmTreeEngine>);

impl Engine for LsmTreeEngineWrapper {
    fn set(&self, key: String, value: String) -> Result<(), Status> {
        self.0.set(key, value)
    }

    fn get(&self, key: String) -> Result<String, Status> {
        self.0.get(key)
    }

    fn delete(&self, key: String) -> Result<(), Status> {
        self.0.delete(key)
    }

    fn batch_set(&self, items: Vec<(String, String)>) -> Result<usize, Status> {
        self.0.batch_set(items)
    }

    fn batch_get(&self, keys: Vec<String>) -> Vec<(String, String)> {
        self.0.batch_get(keys)
    }

    fn batch_delete(&self, keys: Vec<String>) -> Result<usize, Status> {
        self.0.batch_delete(keys)
    }
}

#[tonic::async_trait]
impl KeyValue for MyKeyValue {
    async fn set(&self, request: Request<SetRequest>) -> Result<Response<SetResponse>, Status> {
        let req = request.into_inner();
        self.engine.set(req.key, req.value)?;
        Ok(Response::new(SetResponse { success: true }))
    }

    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
        let req = request.into_inner();
        let value = self.engine.get(req.key)?;
        Ok(Response::new(GetResponse { value }))
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
}

#[derive(clap::ValueEnum, Clone, Debug)]
pub enum EngineType {
    Memory,
    Log,
    LsmTree,
}

pub async fn run_server(
    addr: std::net::SocketAddr,
    engine_type: EngineType,
    data_dir: String,
    log_capacity_bytes: u64,
    lsm_memtable_capacity_bytes: u64,
    lsm_compaction_trigger_file_count: usize,
    replication_addr: Option<std::net::SocketAddr>,
) {
    let mut lsm_holder = LsmEngineHolder::new();
    let mut log_holder = LogEngineHolder::new();

    let key_value = MyKeyValue::new(
        engine_type,
        data_dir.clone(),
        log_capacity_bytes,
        lsm_memtable_capacity_bytes,
        lsm_compaction_trigger_file_count,
        &mut lsm_holder,
        &mut log_holder,
    );

    println!("Server listening on {}", addr);

    // Start replication service if enabled
    let replication_handle = if let Some(repl_addr) = replication_addr {
        println!("Replication service listening on {}", repl_addr);
        let replication_service = ReplicationService::new(PathBuf::from(&data_dir));
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

    println!("Server stopped.");
}

/// Run as a follower, replicating from a leader
pub async fn run_follower(
    leader_addr: String,
    data_dir: String,
    lsm_memtable_capacity_bytes: u64,
    lsm_compaction_trigger_file_count: usize,
    addr: std::net::SocketAddr,
) {
    use std::fs;
    use std::sync::atomic::{AtomicBool, Ordering};
    use tokio::sync::RwLock;

    // Create data directory
    let data_path = PathBuf::from(&data_dir);
    if !data_path.exists() {
        fs::create_dir_all(&data_path).expect("Failed to create data directory");
    }

    // Shutdown flag
    let shutdown_flag = Arc::new(AtomicBool::new(false));
    let shutdown_flag_server = Arc::clone(&shutdown_flag);

    // Create swappable engine holder
    let engine_holder: Arc<RwLock<Arc<LsmTreeEngine>>> =
        Arc::new(RwLock::new(Arc::new(LsmTreeEngine::new(
            data_dir.clone(),
            lsm_memtable_capacity_bytes,
            lsm_compaction_trigger_file_count,
        ))));
    let engine_holder_for_server = Arc::clone(&engine_holder);

    println!("Starting follower, replicating from {}", leader_addr);
    println!("Follower serving read requests on {}", addr);

    // Start read-only server with swappable engine
    let server_handle = tokio::spawn(async move {
        let key_value = SwappableFollowerKeyValue {
            engine_holder: engine_holder_for_server,
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

    // Replication loop with engine reload support
    let data_path_for_repl = data_path.clone();
    let engine_holder_for_repl = Arc::clone(&engine_holder);

    loop {
        if shutdown_flag.load(Ordering::SeqCst) {
            break;
        }

        let replicator = FollowerReplicator::new(leader_addr.clone(), data_path_for_repl.clone());
        let engine_holder_clone = Arc::clone(&engine_holder_for_repl);

        // Apply WAL entries to the engine
        let result = replicator
            .start(move |entry| {
                // Get current engine reference
                let engine = {
                    let guard = engine_holder_clone.blocking_read();
                    Arc::clone(&*guard)
                };

                if let Some(value) = entry.value {
                    engine.set(entry.key, value)
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
                {
                    let old_engine = engine_holder_for_repl.read().await;
                    old_engine.shutdown().await;
                }

                // Create new engine (will load the downloaded SSTables)
                let new_engine = Arc::new(LsmTreeEngine::new(
                    data_dir.clone(),
                    lsm_memtable_capacity_bytes,
                    lsm_compaction_trigger_file_count,
                ));

                // Swap engine
                {
                    let mut guard = engine_holder_for_repl.write().await;
                    *guard = new_engine;
                }

                println!("Engine reloaded, continuing replication...");
                // Continue to next iteration of the loop
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
        let engine = engine_holder.read().await;
        engine.shutdown().await;
    }

    println!("Follower stopped.");
}

/// Read-only key-value service for followers with swappable engine
struct SwappableFollowerKeyValue {
    engine_holder: Arc<tokio::sync::RwLock<Arc<LsmTreeEngine>>>,
}

#[tonic::async_trait]
impl KeyValue for SwappableFollowerKeyValue {
    async fn set(&self, _request: Request<SetRequest>) -> Result<Response<SetResponse>, Status> {
        Err(Status::failed_precondition(
            "This is a read-only follower. Writes must go to the leader.",
        ))
    }

    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
        let req = request.into_inner();
        let engine = {
            let guard = self.engine_holder.read().await;
            Arc::clone(&*guard)
        };
        let value = engine.get(req.key)?;
        Ok(Response::new(GetResponse { value }))
    }

    async fn delete(
        &self,
        _request: Request<DeleteRequest>,
    ) -> Result<Response<DeleteResponse>, Status> {
        Err(Status::failed_precondition(
            "This is a read-only follower. Writes must go to the leader.",
        ))
    }

    async fn batch_set(
        &self,
        _request: Request<BatchSetRequest>,
    ) -> Result<Response<BatchSetResponse>, Status> {
        Err(Status::failed_precondition(
            "This is a read-only follower. Writes must go to the leader.",
        ))
    }

    async fn batch_get(
        &self,
        request: Request<BatchGetRequest>,
    ) -> Result<Response<BatchGetResponse>, Status> {
        let req = request.into_inner();
        let engine = {
            let guard = self.engine_holder.read().await;
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
        _request: Request<BatchDeleteRequest>,
    ) -> Result<Response<BatchDeleteResponse>, Status> {
        Err(Status::failed_precondition(
            "This is a read-only follower. Writes must go to the leader.",
        ))
    }

    async fn get_metrics(
        &self,
        _request: Request<GetMetricsRequest>,
    ) -> Result<Response<GetMetricsResponse>, Status> {
        let engine = {
            let guard = self.engine_holder.read().await;
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
}
