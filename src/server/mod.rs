use std::path::PathBuf;
use std::sync::Arc;
use tonic::{Request, Response, Status, transport::Server};
use tracing::instrument;

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

    #[instrument(skip(self, request), fields(prefix_len))]
    async fn count(
        &self,
        request: Request<CountRequest>,
    ) -> Result<Response<CountResponse>, Status> {
        let req = request.into_inner();
        tracing::Span::current().record("prefix_len", req.prefix.len());
        if req.prefix.is_empty() {
            return Err(Status::not_found("Prefix cannot be empty"));
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use kv::{
        BatchDeleteRequest, BatchGetRequest, BatchSetRequest, CompareAndSetRequest, CountRequest,
        DeleteRequest, GetExpireAtRequest, GetMetricsRequest, GetRequest, PromoteRequest,
        SetRequest,
    };

    /// Helper to create a MyKeyValue with MemoryEngine for testing
    fn create_test_key_value() -> MyKeyValue {
        let mut lsm_holder = LsmEngineHolder::new();
        let mut log_holder = LogEngineHolder::new();
        let mut btree_holder = BTreeEngineHolder::new();

        MyKeyValue::new(
            EngineType::Memory,
            String::new(),
            0,
            0,
            0,
            0,
            &mut lsm_holder,
            &mut log_holder,
            &mut btree_holder,
            WalArchiveConfig::default(),
        )
    }

    // ==================== Set Tests ====================

    #[tokio::test]
    async fn test_set_basic() {
        let key_value = create_test_key_value();

        let request = Request::new(SetRequest {
            key: "key1".to_string(),
            value: "value1".to_string(),
            ttl_seconds: 0,
        });
        let result = key_value.set(request).await;

        assert!(result.is_ok());
        assert!(result.unwrap().into_inner().success);
    }

    #[tokio::test]
    async fn test_set_with_ttl() {
        let key_value = create_test_key_value();

        let request = Request::new(SetRequest {
            key: "key_ttl".to_string(),
            value: "value_ttl".to_string(),
            ttl_seconds: 3600, // 1 hour TTL
        });
        let result = key_value.set(request).await;

        assert!(result.is_ok());
        assert!(result.unwrap().into_inner().success);
    }

    #[tokio::test]
    async fn test_set_overwrites_existing() {
        let key_value = create_test_key_value();

        // Set initial value
        let request1 = Request::new(SetRequest {
            key: "key".to_string(),
            value: "value1".to_string(),
            ttl_seconds: 0,
        });
        key_value.set(request1).await.unwrap();

        // Overwrite with new value
        let request2 = Request::new(SetRequest {
            key: "key".to_string(),
            value: "value2".to_string(),
            ttl_seconds: 0,
        });
        let result = key_value.set(request2).await;
        assert!(result.is_ok());

        // Verify the new value
        let get_request = Request::new(GetRequest {
            key: "key".to_string(),
            include_expire_at: false,
        });
        let get_result = key_value.get(get_request).await.unwrap();
        assert_eq!(get_result.into_inner().value, "value2");
    }

    #[tokio::test]
    async fn test_set_empty_key() {
        let key_value = create_test_key_value();

        let request = Request::new(SetRequest {
            key: "".to_string(),
            value: "value".to_string(),
            ttl_seconds: 0,
        });
        let result = key_value.set(request).await;

        // Empty key should still succeed (engine allows it)
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_set_empty_value() {
        let key_value = create_test_key_value();

        let request = Request::new(SetRequest {
            key: "key".to_string(),
            value: "".to_string(),
            ttl_seconds: 0,
        });
        let result = key_value.set(request).await;

        assert!(result.is_ok());

        // Verify empty value can be retrieved
        let get_request = Request::new(GetRequest {
            key: "key".to_string(),
            include_expire_at: false,
        });
        let get_result = key_value.get(get_request).await.unwrap();
        assert_eq!(get_result.into_inner().value, "");
    }

    // ==================== Get Tests ====================

    #[tokio::test]
    async fn test_get_existing_key() {
        let key_value = create_test_key_value();

        // Set a value first
        let set_request = Request::new(SetRequest {
            key: "key".to_string(),
            value: "value".to_string(),
            ttl_seconds: 0,
        });
        key_value.set(set_request).await.unwrap();

        // Get the value
        let get_request = Request::new(GetRequest {
            key: "key".to_string(),
            include_expire_at: false,
        });
        let result = key_value.get(get_request).await;

        assert!(result.is_ok());
        let response = result.unwrap().into_inner();
        assert_eq!(response.value, "value");
        assert_eq!(response.expire_at, 0);
    }

    #[tokio::test]
    async fn test_get_non_existing_key() {
        let key_value = create_test_key_value();

        let request = Request::new(GetRequest {
            key: "nonexistent".to_string(),
            include_expire_at: false,
        });
        let result = key_value.get(request).await;

        // MemoryEngine returns NotFound error for non-existing keys
        assert!(result.is_err());
        let status = result.unwrap_err();
        assert_eq!(status.code(), tonic::Code::NotFound);
    }

    #[tokio::test]
    async fn test_get_with_expire_at() {
        let key_value = create_test_key_value();

        // Set a value with TTL
        let set_request = Request::new(SetRequest {
            key: "key_ttl".to_string(),
            value: "value".to_string(),
            ttl_seconds: 3600,
        });
        key_value.set(set_request).await.unwrap();

        // Get with include_expire_at = true
        let get_request = Request::new(GetRequest {
            key: "key_ttl".to_string(),
            include_expire_at: true,
        });
        let result = key_value.get(get_request).await;

        assert!(result.is_ok());
        let response = result.unwrap().into_inner();
        assert_eq!(response.value, "value");
        assert!(response.expire_at > 0); // Should have an expiration timestamp
    }

    #[tokio::test]
    async fn test_get_unicode_key_value() {
        let key_value = create_test_key_value();

        let set_request = Request::new(SetRequest {
            key: "日本語キー".to_string(),
            value: "値🎉".to_string(),
            ttl_seconds: 0,
        });
        key_value.set(set_request).await.unwrap();

        let get_request = Request::new(GetRequest {
            key: "日本語キー".to_string(),
            include_expire_at: false,
        });
        let result = key_value.get(get_request).await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap().into_inner().value, "値🎉");
    }

    // ==================== Delete Tests ====================

    #[tokio::test]
    async fn test_delete_existing_key() {
        let key_value = create_test_key_value();

        // Set a value first
        let set_request = Request::new(SetRequest {
            key: "key".to_string(),
            value: "value".to_string(),
            ttl_seconds: 0,
        });
        key_value.set(set_request).await.unwrap();

        // Delete the key
        let delete_request = Request::new(DeleteRequest {
            key: "key".to_string(),
        });
        let result = key_value.delete(delete_request).await;

        assert!(result.is_ok());
        assert!(result.unwrap().into_inner().success);

        // Verify it's deleted (should return NotFound)
        let get_request = Request::new(GetRequest {
            key: "key".to_string(),
            include_expire_at: false,
        });
        let get_result = key_value.get(get_request).await;
        assert!(get_result.is_err());
    }

    #[tokio::test]
    async fn test_delete_non_existing_key() {
        let key_value = create_test_key_value();

        let request = Request::new(DeleteRequest {
            key: "nonexistent".to_string(),
        });
        let result = key_value.delete(request).await;

        // MemoryEngine returns NotFound error for non-existing keys
        assert!(result.is_err());
        let status = result.unwrap_err();
        assert_eq!(status.code(), tonic::Code::NotFound);
    }

    // ==================== Batch Set Tests ====================

    #[tokio::test]
    async fn test_batch_set_basic() {
        let key_value = create_test_key_value();

        let request = Request::new(BatchSetRequest {
            items: vec![
                KeyValuePair {
                    key: "key1".to_string(),
                    value: "value1".to_string(),
                },
                KeyValuePair {
                    key: "key2".to_string(),
                    value: "value2".to_string(),
                },
                KeyValuePair {
                    key: "key3".to_string(),
                    value: "value3".to_string(),
                },
            ],
        });
        let result = key_value.batch_set(request).await;

        assert!(result.is_ok());
        let response = result.unwrap().into_inner();
        assert!(response.success);
        assert_eq!(response.count, 3);

        // Verify all values were set
        for i in 1..=3 {
            let get_request = Request::new(GetRequest {
                key: format!("key{}", i),
                include_expire_at: false,
            });
            let get_result = key_value.get(get_request).await.unwrap();
            assert_eq!(get_result.into_inner().value, format!("value{}", i));
        }
    }

    #[tokio::test]
    async fn test_batch_set_empty() {
        let key_value = create_test_key_value();

        let request = Request::new(BatchSetRequest { items: vec![] });
        let result = key_value.batch_set(request).await;

        assert!(result.is_ok());
        let response = result.unwrap().into_inner();
        assert!(response.success);
        assert_eq!(response.count, 0);
    }

    // ==================== Batch Get Tests ====================

    #[tokio::test]
    async fn test_batch_get_basic() {
        let key_value = create_test_key_value();

        // Set some values first
        for i in 1..=3 {
            let set_request = Request::new(SetRequest {
                key: format!("key{}", i),
                value: format!("value{}", i),
                ttl_seconds: 0,
            });
            key_value.set(set_request).await.unwrap();
        }

        let request = Request::new(BatchGetRequest {
            keys: vec!["key1".to_string(), "key2".to_string(), "key3".to_string()],
        });
        let result = key_value.batch_get(request).await;

        assert!(result.is_ok());
        let response = result.unwrap().into_inner();
        assert_eq!(response.items.len(), 3);
    }

    #[tokio::test]
    async fn test_batch_get_mixed_existing_nonexisting() {
        let key_value = create_test_key_value();

        // Set only key1
        let set_request = Request::new(SetRequest {
            key: "key1".to_string(),
            value: "value1".to_string(),
            ttl_seconds: 0,
        });
        key_value.set(set_request).await.unwrap();

        let request = Request::new(BatchGetRequest {
            keys: vec![
                "key1".to_string(),
                "nonexistent".to_string(),
                "key2".to_string(),
            ],
        });
        let result = key_value.batch_get(request).await;

        assert!(result.is_ok());
        let response = result.unwrap().into_inner();
        // Should return items for existing keys only
        assert!(!response.items.is_empty());
    }

    #[tokio::test]
    async fn test_batch_get_empty() {
        let key_value = create_test_key_value();

        let request = Request::new(BatchGetRequest { keys: vec![] });
        let result = key_value.batch_get(request).await;

        assert!(result.is_ok());
        let response = result.unwrap().into_inner();
        assert!(response.items.is_empty());
    }

    // ==================== Batch Delete Tests ====================

    #[tokio::test]
    async fn test_batch_delete_basic() {
        let key_value = create_test_key_value();

        // Set some values first
        for i in 1..=3 {
            let set_request = Request::new(SetRequest {
                key: format!("key{}", i),
                value: format!("value{}", i),
                ttl_seconds: 0,
            });
            key_value.set(set_request).await.unwrap();
        }

        let request = Request::new(BatchDeleteRequest {
            keys: vec!["key1".to_string(), "key2".to_string()],
        });
        let result = key_value.batch_delete(request).await;

        assert!(result.is_ok());
        let response = result.unwrap().into_inner();
        assert!(response.success);
        assert_eq!(response.count, 2);

        // Verify key1 and key2 are deleted (NotFound error)
        let get1 = key_value
            .get(Request::new(GetRequest {
                key: "key1".to_string(),
                include_expire_at: false,
            }))
            .await;
        assert!(get1.is_err());

        // key3 should still exist
        let get3 = key_value
            .get(Request::new(GetRequest {
                key: "key3".to_string(),
                include_expire_at: false,
            }))
            .await
            .unwrap();
        assert_eq!(get3.into_inner().value, "value3");
    }

    #[tokio::test]
    async fn test_batch_delete_empty() {
        let key_value = create_test_key_value();

        let request = Request::new(BatchDeleteRequest { keys: vec![] });
        let result = key_value.batch_delete(request).await;

        assert!(result.is_ok());
        let response = result.unwrap().into_inner();
        assert!(response.success);
        assert_eq!(response.count, 0);
    }

    // ==================== Get Metrics Tests ====================

    #[tokio::test]
    async fn test_get_metrics_memory_engine() {
        let key_value = create_test_key_value();

        let request = Request::new(GetMetricsRequest {});
        let result = key_value.get_metrics(request).await;

        assert!(result.is_ok());
        // Memory engine returns default/empty metrics
        let response = result.unwrap().into_inner();
        assert_eq!(response.get_count, 0);
        assert_eq!(response.set_count, 0);
    }

    // ==================== Get Expire At Tests ====================

    #[tokio::test]
    async fn test_get_expire_at_no_ttl() {
        let key_value = create_test_key_value();

        // Set a value without TTL
        let set_request = Request::new(SetRequest {
            key: "key".to_string(),
            value: "value".to_string(),
            ttl_seconds: 0,
        });
        key_value.set(set_request).await.unwrap();

        let request = Request::new(GetExpireAtRequest {
            key: "key".to_string(),
        });
        let result = key_value.get_expire_at(request).await;

        assert!(result.is_ok());
        let response = result.unwrap().into_inner();
        assert!(response.exists);
        assert_eq!(response.expire_at, 0); // No expiration
    }

    #[tokio::test]
    async fn test_get_expire_at_with_ttl() {
        let key_value = create_test_key_value();

        // Set a value with TTL
        let set_request = Request::new(SetRequest {
            key: "key_ttl".to_string(),
            value: "value".to_string(),
            ttl_seconds: 3600,
        });
        key_value.set(set_request).await.unwrap();

        let request = Request::new(GetExpireAtRequest {
            key: "key_ttl".to_string(),
        });
        let result = key_value.get_expire_at(request).await;

        assert!(result.is_ok());
        let response = result.unwrap().into_inner();
        assert!(response.exists);
        assert!(response.expire_at > 0);
    }

    #[tokio::test]
    async fn test_get_expire_at_nonexistent() {
        let key_value = create_test_key_value();

        let request = Request::new(GetExpireAtRequest {
            key: "nonexistent".to_string(),
        });
        let result = key_value.get_expire_at(request).await;

        assert!(result.is_ok());
        let response = result.unwrap().into_inner();
        assert!(!response.exists);
    }

    // ==================== Compare And Set Tests ====================

    #[tokio::test]
    async fn test_compare_and_set_create_new() {
        let key_value = create_test_key_value();

        // CAS on non-existing key (expect_exists = false)
        let request = Request::new(CompareAndSetRequest {
            key: "new_key".to_string(),
            expect_exists: false,
            expected_value: "".to_string(),
            new_value: "new_value".to_string(),
            ttl_seconds: 0,
        });
        let result = key_value.compare_and_set(request).await;

        assert!(result.is_ok());
        let response = result.unwrap().into_inner();
        assert!(response.success);

        // Verify the value was set
        let get_request = Request::new(GetRequest {
            key: "new_key".to_string(),
            include_expire_at: false,
        });
        let get_result = key_value.get(get_request).await.unwrap();
        assert_eq!(get_result.into_inner().value, "new_value");
    }

    #[tokio::test]
    async fn test_compare_and_set_update_existing() {
        let key_value = create_test_key_value();

        // Set initial value
        let set_request = Request::new(SetRequest {
            key: "key".to_string(),
            value: "old_value".to_string(),
            ttl_seconds: 0,
        });
        key_value.set(set_request).await.unwrap();

        // CAS to update with correct expected value
        let request = Request::new(CompareAndSetRequest {
            key: "key".to_string(),
            expect_exists: true,
            expected_value: "old_value".to_string(),
            new_value: "new_value".to_string(),
            ttl_seconds: 0,
        });
        let result = key_value.compare_and_set(request).await;

        assert!(result.is_ok());
        let response = result.unwrap().into_inner();
        assert!(response.success);

        // Verify the value was updated
        let get_request = Request::new(GetRequest {
            key: "key".to_string(),
            include_expire_at: false,
        });
        let get_result = key_value.get(get_request).await.unwrap();
        assert_eq!(get_result.into_inner().value, "new_value");
    }

    #[tokio::test]
    async fn test_compare_and_set_fails_wrong_expected() {
        let key_value = create_test_key_value();

        // Set initial value
        let set_request = Request::new(SetRequest {
            key: "key".to_string(),
            value: "actual_value".to_string(),
            ttl_seconds: 0,
        });
        key_value.set(set_request).await.unwrap();

        // CAS with wrong expected value
        let request = Request::new(CompareAndSetRequest {
            key: "key".to_string(),
            expect_exists: true,
            expected_value: "wrong_value".to_string(),
            new_value: "new_value".to_string(),
            ttl_seconds: 0,
        });
        let result = key_value.compare_and_set(request).await;

        assert!(result.is_ok());
        let response = result.unwrap().into_inner();
        assert!(!response.success);
        assert_eq!(response.current_value, "actual_value");

        // Verify the value was not changed
        let get_request = Request::new(GetRequest {
            key: "key".to_string(),
            include_expire_at: false,
        });
        let get_result = key_value.get(get_request).await.unwrap();
        assert_eq!(get_result.into_inner().value, "actual_value");
    }

    #[tokio::test]
    async fn test_compare_and_set_fails_key_exists_unexpectedly() {
        let key_value = create_test_key_value();

        // Set a value
        let set_request = Request::new(SetRequest {
            key: "key".to_string(),
            value: "value".to_string(),
            ttl_seconds: 0,
        });
        key_value.set(set_request).await.unwrap();

        // CAS expecting key not to exist
        let request = Request::new(CompareAndSetRequest {
            key: "key".to_string(),
            expect_exists: false,
            expected_value: "".to_string(),
            new_value: "new_value".to_string(),
            ttl_seconds: 0,
        });
        let result = key_value.compare_and_set(request).await;

        assert!(result.is_ok());
        let response = result.unwrap().into_inner();
        assert!(!response.success);
    }

    #[tokio::test]
    async fn test_compare_and_set_with_ttl() {
        let key_value = create_test_key_value();

        let request = Request::new(CompareAndSetRequest {
            key: "key_ttl".to_string(),
            expect_exists: false,
            expected_value: "".to_string(),
            new_value: "value".to_string(),
            ttl_seconds: 3600,
        });
        let result = key_value.compare_and_set(request).await;

        assert!(result.is_ok());
        assert!(result.unwrap().into_inner().success);

        // Verify TTL was set
        let expire_request = Request::new(GetExpireAtRequest {
            key: "key_ttl".to_string(),
        });
        let expire_result = key_value.get_expire_at(expire_request).await.unwrap();
        assert!(expire_result.into_inner().expire_at > 0);
    }

    // ==================== Promote To Leader Tests ====================

    #[tokio::test]
    async fn test_promote_to_leader_already_leader() {
        let key_value = create_test_key_value();

        let request = Request::new(PromoteRequest {
            enable_replication_service: false,
            replication_port: 0,
        });
        let result = key_value.promote_to_leader(request).await;

        assert!(result.is_ok());
        let response = result.unwrap().into_inner();
        assert!(!response.success);
        assert!(response.message.contains("already running as a leader"));
    }

    // ==================== Count Tests ====================

    #[tokio::test]
    async fn test_count_empty_prefix_returns_not_found() {
        let key_value = create_test_key_value();

        let request = Request::new(CountRequest {
            prefix: "".to_string(),
        });
        let result = key_value.count(request).await;

        assert!(result.is_err());
        let status = result.unwrap_err();
        assert_eq!(status.code(), tonic::Code::NotFound);
        assert_eq!(status.message(), "Prefix cannot be empty");
    }

    #[tokio::test]
    async fn test_count_non_empty_prefix_succeeds() {
        let key_value = create_test_key_value();

        let request = Request::new(CountRequest {
            prefix: "test".to_string(),
        });
        let result = key_value.count(request).await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap().into_inner().count, 0);
    }

    #[tokio::test]
    async fn test_count_with_matching_keys() {
        let key_value = create_test_key_value();

        // Set keys with common prefix
        for i in 1..=5 {
            let set_request = Request::new(SetRequest {
                key: format!("prefix:{}", i),
                value: format!("value{}", i),
                ttl_seconds: 0,
            });
            key_value.set(set_request).await.unwrap();
        }

        // Set keys with different prefix
        let set_request = Request::new(SetRequest {
            key: "other:key".to_string(),
            value: "value".to_string(),
            ttl_seconds: 0,
        });
        key_value.set(set_request).await.unwrap();

        let request = Request::new(CountRequest {
            prefix: "prefix:".to_string(),
        });
        let result = key_value.count(request).await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap().into_inner().count, 5);
    }

    // ==================== Engine Type Tests ====================

    #[test]
    fn test_engine_type_debug() {
        assert_eq!(format!("{:?}", EngineType::Memory), "Memory");
        assert_eq!(format!("{:?}", EngineType::Log), "Log");
        assert_eq!(format!("{:?}", EngineType::LsmTree), "LsmTree");
        assert_eq!(format!("{:?}", EngineType::BTree), "BTree");
    }

    #[test]
    fn test_engine_type_clone() {
        let engine_type = EngineType::Memory;
        let cloned = engine_type.clone();
        assert!(matches!(cloned, EngineType::Memory));
    }
}
