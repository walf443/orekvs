use tonic::{transport::Server, Request, Response, Status};

pub mod kv {
    tonic::include_proto!("kv");
}

use kv::key_value_server::{KeyValue, KeyValueServer};
use kv::{GetRequest, GetResponse, SetRequest, SetResponse};

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

#[derive(Debug, Default)]
pub struct MyKeyValue {
    db: Arc<Mutex<HashMap<String, String>>>,
}

#[tonic::async_trait]
impl KeyValue for MyKeyValue {
    async fn set(
        &self,
        request: Request<SetRequest>,
    ) -> Result<Response<SetResponse>, Status> {
        let req = request.into_inner();
        let mut db = self.db.lock().unwrap();
        db.insert(req.key, req.value);

        Ok(Response::new(SetResponse { success: true }))
    }

    async fn get(
        &self,
        request: Request<GetRequest>,
    ) -> Result<Response<GetResponse>, Status> {
        let req = request.into_inner();
        let db = self.db.lock().unwrap();

        match db.get(&req.key) {
            Some(value) => Ok(Response::new(GetResponse {
                value: value.clone(),
            })),
            None => Err(Status::not_found("Key not found")),
        }
    }
}

pub async fn run_server(addr: std::net::SocketAddr) {
    let key_value = MyKeyValue::default();

    println!("Server listening on {}", addr);

    Server::builder()
        .add_service(KeyValueServer::new(key_value))
        .serve(addr)
        .await
        .unwrap();
}