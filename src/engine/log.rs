use super::Engine;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};
use tonic::Status;

const MAGIC_BYTES: &[u8; 6] = b"ORELSM";
const DATA_VERSION: u32 = 1;
const HEADER_SIZE: u64 = 10; // 6 (magic) + 4 (version)

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
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&file_path)
            .expect("Failed to open data file");

        let file_size = file.metadata().expect("Failed to get metadata").len();
        
        if file_size == 0 {
            // New file: write header
            file.write_all(MAGIC_BYTES).expect("Failed to write magic bytes");
            file.write_all(&DATA_VERSION.to_le_bytes()).expect("Failed to write version");
            file.flush().expect("Failed to flush header");
        } else {
            // Existing file: verify header
            if file_size < HEADER_SIZE {
                panic!("Data file is too small to contain a valid header");
            }
            let mut magic = [0u8; 6];
            let mut version_bytes = [0u8; 4];
            
            file.seek(SeekFrom::Start(0)).expect("Failed to seek to start");
            file.read_exact(&mut magic).expect("Failed to read magic bytes");
            file.read_exact(&mut version_bytes).expect("Failed to read version bytes");

            if &magic != MAGIC_BYTES {
                panic!("Invalid magic bytes in data file: expected {:?}, found {:?}", MAGIC_BYTES, magic);
            }
            
            let version = u32::from_le_bytes(version_bytes);
            if version != DATA_VERSION {
                panic!("Incompatible data version: expected {}, found {}", DATA_VERSION, version);
            }
        }

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
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let mut writer = self.writer.lock().unwrap();
        
        // Get current offset (end of file)
        let start_offset = writer.seek(SeekFrom::End(0)).map_err(|e| Status::internal(e.to_string()))?;

        // Write Format: [timestamp(8)][key_len(8)][val_len(8)][key][value]
        writer.write_all(&timestamp.to_le_bytes()).map_err(|e| Status::internal(e.to_string()))?;
        writer.write_all(&key_len.to_le_bytes()).map_err(|e| Status::internal(e.to_string()))?;
        writer.write_all(&val_len.to_le_bytes()).map_err(|e| Status::internal(e.to_string()))?;
        writer.write_all(key_bytes).map_err(|e| Status::internal(e.to_string()))?;
        writer.write_all(val_bytes).map_err(|e| Status::internal(e.to_string()))?;
        writer.flush().map_err(|e| Status::internal(e.to_string()))?;

        // Calculate where the value starts:
        // start_offset + timestamp(8) + key_len_size(8) + val_len_size(8) + key_len
        let value_offset = start_offset + 8 + 8 + 8 + key_len;

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
