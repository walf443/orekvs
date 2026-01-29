//! Wrapper types for LsmTreeEngine.
//!
//! Provides utility wrappers for managing engine lifecycle and
//! implementing the Engine trait for Arc<LsmTreeEngine>.

use std::sync::Arc;
use tonic::Status;

use super::LsmTreeEngine;
use crate::engine::Engine;

/// Wrapper to hold LSM engine reference for graceful shutdown.
///
/// Used by server code to manage engine lifecycle.
pub struct LsmEngineHolder {
    engine: Option<Arc<LsmTreeEngine>>,
}

impl LsmEngineHolder {
    pub fn new() -> Self {
        LsmEngineHolder { engine: None }
    }

    pub fn set(&mut self, engine: Arc<LsmTreeEngine>) {
        self.engine = Some(engine);
    }

    pub fn engine(&self) -> Option<&Arc<LsmTreeEngine>> {
        self.engine.as_ref()
    }

    pub async fn shutdown(&self) {
        if let Some(ref engine) = self.engine {
            engine.shutdown().await;
        }
    }
}

impl Default for LsmEngineHolder {
    fn default() -> Self {
        Self::new()
    }
}

/// Wrapper to implement Engine trait for Arc<LsmTreeEngine>.
///
/// Allows using Arc<LsmTreeEngine> where Engine trait is required.
pub struct LsmTreeEngineWrapper(pub Arc<LsmTreeEngine>);

impl Engine for LsmTreeEngineWrapper {
    fn set(&self, key: String, value: String) -> Result<(), Status> {
        self.0.set(key, value)
    }

    fn set_with_ttl(&self, key: String, value: String, ttl_secs: u64) -> Result<(), Status> {
        self.0.set_with_ttl(key, value, ttl_secs)
    }

    fn delete(&self, key: String) -> Result<(), Status> {
        self.0.delete(key)
    }

    fn batch_set(&self, items: Vec<(String, String)>) -> Result<usize, Status> {
        self.0.batch_set(items)
    }

    fn batch_set_with_expire_at(&self, items: Vec<(String, String, u64)>) -> Result<usize, Status> {
        self.0.batch_set_with_expire_at(items)
    }

    fn batch_get(&self, keys: Vec<String>) -> Vec<(String, String)> {
        self.0.batch_get(keys)
    }

    fn batch_delete(&self, keys: Vec<String>) -> Result<usize, Status> {
        self.0.batch_delete(keys)
    }

    fn get_with_expire_at(&self, key: String) -> Result<(String, u64), Status> {
        self.0.get_with_expire_at(key)
    }

    fn compare_and_set(
        &self,
        key: String,
        expected_value: Option<String>,
        new_value: String,
        expire_at: u64,
    ) -> Result<(bool, Option<String>), Status> {
        self.0
            .compare_and_set(key, expected_value, new_value, expire_at)
    }

    fn count(&self, prefix: &str) -> Result<u64, Status> {
        self.0.count(prefix)
    }
}
