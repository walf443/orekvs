use tonic::Status;

pub mod log;
pub mod lsm_tree;
pub mod memory;

pub trait Engine: Send + Sync + 'static {
    fn set(&self, key: String, value: String) -> Result<(), Status>;
    fn get(&self, key: String) -> Result<String, Status>;
    fn delete(&self, key: String) -> Result<(), Status>;
}