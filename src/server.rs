use std::sync::Arc;
use tonic::{Request, Response, Status, transport::Server};

pub mod kv {
    tonic::include_proto!("kv");
}

use kv::key_value_server::{KeyValue, KeyValueServer};
use kv::{
    BatchDeleteRequest, BatchDeleteResponse, BatchGetRequest, BatchGetResponse, BatchSetRequest,
    BatchSetResponse, DeleteRequest, DeleteResponse, GetRequest, GetResponse, KeyValuePair,
    SetRequest, SetResponse,
};

use crate::engine::{Engine, log::LogEngine, lsm_tree::LsmTreeEngine, memory::MemoryEngine};

// --- gRPC Service ---

pub struct MyKeyValue {
    engine: Box<dyn Engine>,
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
                Box::new(LsmTreeEngineWrapper(lsm_engine))
            }
        };
        MyKeyValue { engine }
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
) {
    let mut lsm_holder = LsmEngineHolder::new();
    let mut log_holder = LogEngineHolder::new();

    let key_value = MyKeyValue::new(
        engine_type,
        data_dir,
        log_capacity_bytes,
        lsm_memtable_capacity_bytes,
        lsm_compaction_trigger_file_count,
        &mut lsm_holder,
        &mut log_holder,
    );

    println!("Server listening on {}", addr);

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

    // Gracefully shutdown the engines (flush pending writes)
    lsm_holder.shutdown().await;
    log_holder.shutdown().await;

    println!("Server stopped.");
}
