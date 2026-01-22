use tonic::{Request, Response, Status, transport::Server};

pub mod kv {
    tonic::include_proto!("kv");
}

use kv::key_value_server::{KeyValue, KeyValueServer};
use kv::{DeleteRequest, DeleteResponse, GetRequest, GetResponse, SetRequest, SetResponse};

use crate::engine::{Engine, log::LogEngine, lsm_tree::LsmTreeEngine, memory::MemoryEngine};

// --- gRPC Service ---

pub struct MyKeyValue {
    engine: Box<dyn Engine>,
}

impl MyKeyValue {
    pub fn new(
        engine_type: EngineType,
        data_dir: String,
        log_engine_compaction_threshold: u64,
        lsm_tree_engine_memtable_threshold: u64,
        lsm_tree_compaction_threshold: usize,
    ) -> Self {
        let engine: Box<dyn Engine> = match engine_type {
            EngineType::Memory => Box::new(MemoryEngine::new()),
            EngineType::Log => Box::new(LogEngine::new(data_dir, log_engine_compaction_threshold)),
            EngineType::LsmTree => Box::new(LsmTreeEngine::new(
                data_dir,
                lsm_tree_engine_memtable_threshold,
                lsm_tree_compaction_threshold,
            )),
        };
        MyKeyValue { engine }
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
    log_engine_compaction_threshold: u64,
    lsm_tree_engine_memtable_threshold: u64,
    lsm_tree_compaction_threshold: usize,
) {
    let key_value = MyKeyValue::new(
        engine_type,
        data_dir,
        log_engine_compaction_threshold,
        lsm_tree_engine_memtable_threshold,
        lsm_tree_compaction_threshold,
    );

    println!("Server listening on {}", addr);

    Server::builder()
        .add_service(KeyValueServer::new(key_value))
        .serve(addr)
        .await
        .unwrap();
}
