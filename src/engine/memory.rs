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

    fn compare_and_set(
        &self,
        key: String,
        expected_value: Option<String>,
        new_value: String,
        expire_at: u64,
    ) -> Result<(bool, Option<String>), Status> {
        let mut db = self.db.lock().unwrap();
        let now = current_timestamp();

        let new_entry = MemEntry::new_with_ttl(new_value, expire_at);

        match (expected_value, db.get(&key)) {
            // Key should not exist
            (None, None) => {
                db.insert(key, new_entry);
                Ok((true, None))
            }
            (None, Some(entry)) if entry.is_expired(now) => {
                db.insert(key, new_entry);
                Ok((true, None))
            }
            (None, Some(entry)) => Ok((false, Some(entry.value.clone()))),
            // Key should exist with expected value
            (Some(expected), Some(entry)) if !entry.is_expired(now) && entry.value == expected => {
                db.insert(key, new_entry);
                Ok((true, Some(expected)))
            }
            (Some(_), Some(entry)) if !entry.is_expired(now) => {
                Ok((false, Some(entry.value.clone())))
            }
            (Some(_), _) => Ok((false, None)),
        }
    }
}
