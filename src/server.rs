use tonic::{Request, Response, Status, transport::Server};

pub mod kv {
    tonic::include_proto!("kv");
}

use kv::key_value_server::{KeyValue, KeyValueServer};
use kv::{GetRequest, GetResponse, SetRequest, SetResponse};

use crate::engine::{Engine, log::LogEngine, memory::MemoryEngine};

// --- gRPC Service ---

pub struct MyKeyValue {
    engine: Box<dyn Engine>,
}

impl MyKeyValue {
    pub fn new(
        engine_type: EngineType,
        data_file: String,
        log_engine_compaction_threshold: u64,
    ) -> Self {
        let engine: Box<dyn Engine> = match engine_type {
            EngineType::Memory => Box::new(MemoryEngine::new()),
            EngineType::Log => Box::new(LogEngine::new(data_file, log_engine_compaction_threshold)),
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
}

#[derive(clap::ValueEnum, Clone, Debug)]
pub enum EngineType {
    Memory,
    Log,
}

pub async fn run_server(
    addr: std::net::SocketAddr,
    engine_type: EngineType,
    data_file: String,
    log_engine_compaction_threshold: u64,
) {
    let key_value = MyKeyValue::new(engine_type, data_file, log_engine_compaction_threshold);

    println!("Server listening on {}", addr);

    Server::builder()
        .add_service(KeyValueServer::new(key_value))
        .serve(addr)
        .await
        .unwrap();
}
