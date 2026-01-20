use super::Engine;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::sync::{Arc, Mutex};
use tonic::Status;

#[derive(Debug)]
pub struct LogEngine {
    // Stores Key -> (FileOffset, ValueLength)
    index: Arc<Mutex<HashMap<String, (u64, usize)>>>,
    // File writer for appending new entries
    writer: Arc<Mutex<File>>,
    // Path to the data file
    file_path: String,
}

impl LogEngine {
    pub fn new() -> Self {
        let file_path = "kv_store.data".to_string();
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&file_path)
            .expect("Failed to open data file");

        LogEngine {
            index: Arc::new(Mutex::new(HashMap::new())),
            writer: Arc::new(Mutex::new(file)),
            file_path,
        }
    }
}

impl Engine for LogEngine {
    fn set(&self, key: String, value: String) -> Result<(), Status> {
        let key_bytes = key.as_bytes();
        let val_bytes = value.as_bytes();
        
        let key_len = key_bytes.len() as u64;
        let val_len = val_bytes.len() as u64;

        let mut writer = self.writer.lock().unwrap();
        
        // Get current offset (end of file)
        let start_offset = writer.seek(SeekFrom::End(0)).map_err(|e| Status::internal(e.to_string()))?;

        // Write Format: [key_len(8)][val_len(8)][key][value]
        writer.write_all(&key_len.to_le_bytes()).map_err(|e| Status::internal(e.to_string()))?;
        writer.write_all(&val_len.to_le_bytes()).map_err(|e| Status::internal(e.to_string()))?;
        writer.write_all(key_bytes).map_err(|e| Status::internal(e.to_string()))?;
        writer.write_all(val_bytes).map_err(|e| Status::internal(e.to_string()))?;
        writer.flush().map_err(|e| Status::internal(e.to_string()))?;

        // Calculate where the value starts:
        // start_offset + key_len_size(8) + val_len_size(8) + key_len
        let value_offset = start_offset + 8 + 8 + key_len;

        // Update in-memory index
        let mut index = self.index.lock().unwrap();
        index.insert(key, (value_offset, val_len as usize));
        
        Ok(())
    }

    fn get(&self, key: String) -> Result<String, Status> {
        let (offset, length) = {
            let index = self.index.lock().unwrap();
            match index.get(&key) {
                Some(&info) => info,
                None => return Err(Status::not_found("Key not found")),
            }
        };

        let mut file = File::open(&self.file_path).map_err(|e| Status::internal(e.to_string()))?;
        
        file.seek(SeekFrom::Start(offset)).map_err(|e| Status::internal(e.to_string()))?;
        
        let mut buffer = vec![0u8; length];
        file.read_exact(&mut buffer).map_err(|e| Status::internal(e.to_string()))?;

        let value = String::from_utf8(buffer).map_err(|e| Status::internal(format!("Invalid UTF-8: {}", e)))?;
        Ok(value)
    }
}
