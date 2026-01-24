use tonic::Status;

pub mod log;
pub mod lsm_tree;
pub mod memory;

#[allow(clippy::result_large_err)]
pub trait Engine: Send + Sync + 'static {
    fn set(&self, key: String, value: String) -> Result<(), Status>;
    fn get(&self, key: String) -> Result<String, Status>;
    fn delete(&self, key: String) -> Result<(), Status>;

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
