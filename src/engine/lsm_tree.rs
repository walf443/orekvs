use super::Engine;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use tonic::Status;

#[derive(Debug)]
pub struct LsmTreeEngine {
    // Current in-memory write buffer
    memtable: Arc<Mutex<BTreeMap<String, Option<String>>>>,
    // Path where SSTables will be stored
    _data_dir: String,
    // Maximum size of MemTable before flushing to SSTable (currently placeholder)
    _memtable_threshold: u64,
}

impl LsmTreeEngine {
    pub fn new(data_dir: String, memtable_threshold: u64) -> Self {
        LsmTreeEngine {
            memtable: Arc::new(Mutex::new(BTreeMap::new())),
            _data_dir: data_dir,
            _memtable_threshold: memtable_threshold,
        }
    }
}

impl Engine for LsmTreeEngine {
    fn set(&self, key: String, value: String) -> Result<(), Status> {
        let mut memtable = self.memtable.lock().unwrap();
        memtable.insert(key, Some(value));
        
        // TODO: Check memtable size and flush to SSTable if it exceeds threshold
        
        Ok(())
    }

    fn get(&self, key: String) -> Result<String, Status> {
        let memtable = self.memtable.lock().unwrap();
        
        match memtable.get(&key) {
            Some(Some(value)) => Ok(value.clone()),
            Some(None) => Err(Status::not_found("Key has been deleted")),
            None => {
                // TODO: Search in SSTables on disk
                Err(Status::not_found("Key not found"))
            }
        }
    }

    fn delete(&self, key: String) -> Result<(), Status> {
        let mut memtable = self.memtable.lock().unwrap();
        // Insert a tombstone (None)
        memtable.insert(key, None);
        
        // TODO: Check memtable size and flush if needed
        
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_lsm_basic_operations() {
        let dir = tempdir().unwrap();
        let data_dir = dir.path().to_str().unwrap().to_string();
        let engine = LsmTreeEngine::new(data_dir, 1024);

        // Set and Get
        engine.set("k1".to_string(), "v1".to_string()).unwrap();
        assert_eq!(engine.get("k1".to_string()).unwrap(), "v1");

        // Update
        engine.set("k1".to_string(), "v2".to_string()).unwrap();
        assert_eq!(engine.get("k1".to_string()).unwrap(), "v2");

        // Delete
        engine.delete("k1".to_string()).unwrap();
        let res = engine.get("k1".to_string());
        assert!(res.is_err());
        assert_eq!(res.unwrap_err().code(), tonic::Code::NotFound);

        // Get non-existent
        let res = engine.get("k2".to_string());
        assert!(res.is_err());
    }
}
