use std::time::{SystemTime, UNIX_EPOCH};
use tonic::Status;

pub mod btree;
pub mod log;
pub mod lsm_tree;
pub mod memory;
pub mod wal;

/// Get current Unix timestamp in seconds
pub fn current_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

#[allow(clippy::result_large_err)]
pub trait Engine: Send + Sync + 'static {
    fn set(&self, key: String, value: String) -> Result<(), Status>;
    fn get(&self, key: String) -> Result<String, Status>;
    fn delete(&self, key: String) -> Result<(), Status>;

    /// Set a key-value pair with TTL (time-to-live) in seconds.
    /// The key will expire after `ttl_secs` seconds from now.
    /// Default implementation calls set() (no TTL support).
    fn set_with_ttl(&self, key: String, value: String, ttl_secs: u64) -> Result<(), Status> {
        let _ = ttl_secs; // Default: ignore TTL
        self.set(key, value)
    }

    /// Batch set multiple key-value pairs.
    /// Default implementation calls set() for each item.
    /// Engines can override this for optimized batch processing.
    fn batch_set(&self, items: Vec<(String, String)>) -> Result<usize, Status> {
        let mut count = 0;
        for (key, value) in items {
            self.set(key, value)?;
            count += 1;
        }
        Ok(count)
    }

    /// Batch get multiple keys.
    /// Default implementation calls get() for each key.
    /// Engines can override this for optimized batch processing.
    fn batch_get(&self, keys: Vec<String>) -> Vec<(String, String)> {
        keys.into_iter()
            .filter_map(|key| self.get(key.clone()).ok().map(|value| (key, value)))
            .collect()
    }

    /// Batch delete multiple keys.
    /// Default implementation calls delete() for each key.
    /// Engines can override this for optimized batch processing.
    fn batch_delete(&self, keys: Vec<String>) -> Result<usize, Status> {
        let mut count = 0;
        for key in keys {
            self.delete(key)?;
            count += 1;
        }
        Ok(count)
    }
}
