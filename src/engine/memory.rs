use super::{Engine, current_timestamp};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tonic::Status;

/// Entry in memory store with optional expiration
#[derive(Debug, Clone)]
struct MemEntry {
    value: String,
    expire_at: u64, // 0 = no expiration
}

impl MemEntry {
    fn new(value: String) -> Self {
        Self {
            value,
            expire_at: 0,
        }
    }

    fn new_with_ttl(value: String, expire_at: u64) -> Self {
        Self { value, expire_at }
    }

    fn is_expired(&self, now: u64) -> bool {
        self.expire_at > 0 && now > self.expire_at
    }
}

#[derive(Debug, Default)]
pub struct MemoryEngine {
    db: Arc<Mutex<HashMap<String, MemEntry>>>,
}

impl MemoryEngine {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Engine for MemoryEngine {
    fn set(&self, key: String, value: String) -> Result<(), Status> {
        let mut db = self.db.lock().unwrap();
        db.insert(key, MemEntry::new(value));
        Ok(())
    }

    fn set_with_ttl(&self, key: String, value: String, ttl_secs: u64) -> Result<(), Status> {
        let expire_at = current_timestamp() + ttl_secs;
        let mut db = self.db.lock().unwrap();
        db.insert(key, MemEntry::new_with_ttl(value, expire_at));
        Ok(())
    }

    fn get_with_expire_at(&self, key: String) -> Result<(String, u64), Status> {
        let db = self.db.lock().unwrap();
        match db.get(&key) {
            Some(entry) if !entry.is_expired(current_timestamp()) => {
                Ok((entry.value.clone(), entry.expire_at))
            }
            _ => Err(Status::not_found("Key not found")),
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
