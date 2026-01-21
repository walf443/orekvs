use tonic::Status;

pub mod log;
pub mod lsm_tree;
pub mod memory;

#[allow(clippy::result_large_err)]
pub trait Engine: Send + Sync + 'static {
    fn set(&self, key: String, value: String) -> Result<(), Status>;
    fn get(&self, key: String) -> Result<String, Status>;
    fn delete(&self, key: String) -> Result<(), Status>;
}