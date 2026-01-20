use axum::{
    extract::{Path, State},
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

#[derive(Clone)]
struct AppState {
    db: Arc<Mutex<HashMap<String, String>>>,
}

#[derive(Deserialize, Serialize)]
pub struct SetRequest {
    pub key: String,
    pub value: String,
}

#[derive(Serialize, Deserialize)]
pub struct GetResponse {
    pub value: String,
}

pub async fn run_server() {
    let state = AppState {
        db: Arc::new(Mutex::new(HashMap::new())),
    };

    let app = Router::new()
        .route("/set", post(set_handler))
        .route("/get/:key", get(get_handler))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    println!("Server listening on {}", listener.local_addr().unwrap());
    axum::serve(listener, app).await.unwrap();
}

async fn set_handler(
    State(state): State<AppState>,
    Json(payload): Json<SetRequest>,
) -> StatusCode {
    let mut db = state.db.lock().unwrap();
    db.insert(payload.key, payload.value);
    StatusCode::OK
}

async fn get_handler(
    Path(key): Path<String>,
    State(state): State<AppState>,
) -> Result<Json<GetResponse>, StatusCode> {
    let db = state.db.lock().unwrap();
    if let Some(value) = db.get(&key) {
        Ok(Json(GetResponse {
            value: value.clone(),
        }))
    } else {
        Err(StatusCode::NOT_FOUND)
    }
}
