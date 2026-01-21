use super::Engine;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tonic::Status;

#[derive(Debug, Default)]
pub struct MemoryEngine {
    db: Arc<Mutex<HashMap<String, String>>>,
}

impl MemoryEngine {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Engine for MemoryEngine {
    fn set(&self, key: String, value: String) -> Result<(), Status> {
        let mut db = self.db.lock().unwrap();
        db.insert(key, value);
        Ok(())
    }

    fn get(&self, key: String) -> Result<String, Status> {
        let db = self.db.lock().unwrap();
        match db.get(&key) {
            Some(value) => Ok(value.clone()),
            None => Err(Status::not_found("Key not found")),
        }
    }

    fn delete(&self, key: String) -> Result<(), Status> {
        let mut db = self.db.lock().unwrap();
        match db.remove(&key) {
            Some(_) => Ok(()),
            None => Err(Status::not_found("Key not found")),
        }
    }
}