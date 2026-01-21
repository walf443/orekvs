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
    // Threshold for compaction in bytes
    compaction_threshold: u64,
}

impl LogEngine {
    pub fn new(file_path: String, compaction_threshold: u64) -> Self {
        let mut file = OpenOptions::new()
            .read(true)
            .create(true)
            .append(true)
            .open(&file_path)
            .expect("Failed to open data file");

        let file_size = file.metadata().expect("Failed to get metadata").len();

        if file_size == 0 {
            // New file: write header
            file.write_all(MAGIC_BYTES)
                .expect("Failed to write magic bytes");
            file.write_all(&DATA_VERSION.to_le_bytes())
                .expect("Failed to write version");
            file.flush().expect("Failed to flush header");
        } else {
            // Existing file: verify header
            if file_size < HEADER_SIZE {
                panic!("Data file is too small to contain a valid header");
            }
            let mut magic = [0u8; 6];
            let mut version_bytes = [0u8; 4];

            file.seek(SeekFrom::Start(0))
                .expect("Failed to seek to start");
            file.read_exact(&mut magic)
                .expect("Failed to read magic bytes");
            file.read_exact(&mut version_bytes)
                .expect("Failed to read version bytes");

            if &magic != MAGIC_BYTES {
                panic!(
                    "Invalid magic bytes in data file: expected {:?}, found {:?}",
                    MAGIC_BYTES, magic
                );
            }

            let version = u32::from_le_bytes(version_bytes);
            if version != DATA_VERSION {
                panic!(
                    "Incompatible data version: expected {}, found {}",
                    DATA_VERSION, version
                );
            }
        }

        // Rebuild index from file
        let mut index_map = HashMap::new();
        let current_len = file.metadata().unwrap().len();
        if current_len > HEADER_SIZE {
            file.seek(SeekFrom::Start(HEADER_SIZE))
                .expect("Failed to seek to data");

            loop {
                let mut ts_bytes = [0u8; 8];
                match file.read_exact(&mut ts_bytes) {
                    Ok(_) => {}
                    Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                    Err(e) => panic!("Error reading timestamp: {}", e),
                }

                let mut klen_bytes = [0u8; 8];
                file.read_exact(&mut klen_bytes)
                    .expect("Failed to read key length");
                let mut vlen_bytes = [0u8; 8];
                file.read_exact(&mut vlen_bytes)
                    .expect("Failed to read value length");

                let key_len = u64::from_le_bytes(klen_bytes);
                let val_len = u64::from_le_bytes(vlen_bytes);

                let mut key_buf = vec![0u8; key_len as usize];
                file.read_exact(&mut key_buf).expect("Failed to read key");
                let key = String::from_utf8(key_buf).expect("Invalid UTF-8 in key");

                // Current position is start of value
                let value_offset = file
                    .stream_position()
                    .expect("Failed to get stream position");

                // Skip value
                file.seek(SeekFrom::Current(val_len as i64))
                    .expect("Failed to skip value");

                index_map.insert(key, (value_offset, val_len as usize));
            }
        }

        LogEngine {
            index: Arc::new(Mutex::new(index_map)),
            writer: Arc::new(Mutex::new(file)),
            file_path,
            compaction_threshold,
        }
    }

    fn compact(&self) -> Result<(), Status> {
        // Acquire locks in the same order as set() to avoid deadlocks: Writer first, then Index
        let mut writer = self.writer.lock().unwrap();
        let mut index = self.index.lock().unwrap();

        // Check size again under lock to avoid unnecessary compaction or races
        let file_size = writer
            .metadata()
            .map_err(|e| Status::internal(e.to_string()))?
            .len();
        if file_size <= self.compaction_threshold {
            return Ok(());
        }

        println!("Compacting log file (size: {} bytes)...", file_size);

        let new_file_path = format!("{}.tmp", self.file_path);
        let mut new_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&new_file_path)
            .map_err(|e| Status::internal(e.to_string()))?;

        // Write header
        new_file
            .write_all(MAGIC_BYTES)
            .map_err(|e| Status::internal(e.to_string()))?;
        new_file
            .write_all(&DATA_VERSION.to_le_bytes())
            .map_err(|e| Status::internal(e.to_string()))?;

        let mut new_index = HashMap::new();

        for (key, &(offset, length)) in index.iter() {
            // Read old value using the existing writer file handle
            writer
                .seek(SeekFrom::Start(offset))
                .map_err(|e| Status::internal(e.to_string()))?;
            let mut value = vec![0u8; length];
            writer
                .read_exact(&mut value)
                .map_err(|e| Status::internal(e.to_string()))?;

            let key_bytes = key.as_bytes();
            let key_len = key_bytes.len() as u64;
            let val_len = length as u64;
            let timestamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs();

            new_file
                .write_all(&timestamp.to_le_bytes())
                .map_err(|e| Status::internal(e.to_string()))?;
            new_file
                .write_all(&key_len.to_le_bytes())
                .map_err(|e| Status::internal(e.to_string()))?;
            new_file
                .write_all(&val_len.to_le_bytes())
                .map_err(|e| Status::internal(e.to_string()))?;
            new_file
                .write_all(key_bytes)
                .map_err(|e| Status::internal(e.to_string()))?;
            new_file
                .write_all(&value)
                .map_err(|e| Status::internal(e.to_string()))?;

            let current_pos = new_file
                .stream_position()
                .map_err(|e| Status::internal(e.to_string()))?;
            let value_offset = current_pos - val_len;
            new_index.insert(key.clone(), (value_offset, length));
        }

        new_file
            .flush()
            .map_err(|e| Status::internal(e.to_string()))?;

        // Rename tmp file to actual file
        std::fs::rename(&new_file_path, &self.file_path)
            .map_err(|e| Status::internal(e.to_string()))?;

        // Update internal state
        *writer = new_file;
        *index = new_index;

        println!("Compaction finished.");
        Ok(())
    }
}

impl Engine for LogEngine {
    fn set(&self, key: String, value: String) -> Result<(), Status> {
        let key_bytes = key.as_bytes();
        let val_bytes = value.as_bytes();

        let key_len = key_bytes.len() as u64;
        let val_len = val_bytes.len() as u64;

        // Check if the data itself is larger than the threshold
        // Entry size = timestamp(8) + key_len_size(8) + val_len_size(8) + key + value
        let entry_size = 8 + 8 + 8 + key_len + val_len;
        if entry_size > self.compaction_threshold {
            return Err(Status::invalid_argument(format!(
                "Data size ({} bytes) exceeds the compaction threshold ({} bytes)",
                entry_size, self.compaction_threshold
            )));
        }

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        // Scope for locks
        let should_compact = {
            let mut writer = self.writer.lock().unwrap();

            // Get current offset (end of file)
            let start_offset = writer
                .seek(SeekFrom::End(0))
                .map_err(|e| Status::internal(e.to_string()))?;

            // Write Format: [timestamp(8)][key_len(8)][val_len(8)][key][value]
            writer
                .write_all(&timestamp.to_le_bytes())
                .map_err(|e| Status::internal(e.to_string()))?;
            writer
                .write_all(&key_len.to_le_bytes())
                .map_err(|e| Status::internal(e.to_string()))?;
            writer
                .write_all(&val_len.to_le_bytes())
                .map_err(|e| Status::internal(e.to_string()))?;
            writer
                .write_all(key_bytes)
                .map_err(|e| Status::internal(e.to_string()))?;
            writer
                .write_all(val_bytes)
                .map_err(|e| Status::internal(e.to_string()))?;
            writer
                .flush()
                .map_err(|e| Status::internal(e.to_string()))?;

            // Calculate where the value starts:
            // start_offset + timestamp(8) + key_len_size(8) + val_len_size(8) + key_len
            let value_offset = start_offset + 8 + 8 + 8 + key_len;

            // Update in-memory index
            let mut index = self.index.lock().unwrap();
            index.insert(key, (value_offset, val_len as usize));

            // Check if compaction is needed
            writer
                .metadata()
                .map_err(|e| Status::internal(e.to_string()))?
                .len()
                > self.compaction_threshold
        };

        #[allow(clippy::collapsible_if)]
        if should_compact {
            if let Err(e) = self.compact() {
                eprintln!("Compaction failed: {}", e);
            }
        }

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

        file.seek(SeekFrom::Start(offset))
            .map_err(|e| Status::internal(e.to_string()))?;

        let mut buffer = vec![0u8; length];
        file.read_exact(&mut buffer)
            .map_err(|e| Status::internal(e.to_string()))?;

        let value = String::from_utf8(buffer)
            .map_err(|e| Status::internal(format!("Invalid UTF-8: {}", e)))?;
        Ok(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn test_set_exceeding_threshold_returns_error() {
        let temp_file = NamedTempFile::new().unwrap();
        let file_path = temp_file.path().to_str().unwrap().to_string();

        // 閾値を30バイトに設定
        let engine = LogEngine::new(file_path, 30);

        // 30バイトを超えるデータを準備 (ヘッダー24バイト + キー + 値)
        let key = "mykey".to_string(); // 5 bytes
        let value = "this_is_too_long_value".to_string(); // 22 bytes
        // 合計: 24 + 5 + 22 = 51 bytes > 30 bytes

        let result = engine.set(key, value);

        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code(), tonic::Code::InvalidArgument);
    }

        #[test]
        fn test_set_within_threshold_succeeds() {
            let temp_file = NamedTempFile::new().unwrap();
            let file_path = temp_file.path().to_str().unwrap().to_string();
            
            // 閾値を100バイトに設定
            let engine = LogEngine::new(file_path, 100);
            
            let result = engine.set("key".to_string(), "value".to_string());
            assert!(result.is_ok());
            
            let value = engine.get("key".to_string()).unwrap();
            assert_eq!(value, "value");
        }
    
        #[test]
        #[should_panic(expected = "Invalid magic bytes")]
        fn test_new_with_invalid_magic_bytes_panics() {
            let temp_file = NamedTempFile::new().unwrap();
            let file_path = temp_file.path().to_str().unwrap().to_string();
            
            {
                let mut f = File::create(&file_path).unwrap();
                f.write_all(b"WRONG!").unwrap(); // 6 bytes wrong magic
                f.write_all(&1u32.to_le_bytes()).unwrap(); // version
            }
            
            LogEngine::new(file_path, 1024);
        }
    
        #[test]
        #[should_panic(expected = "Incompatible data version")]
        fn test_new_with_invalid_version_panics() {
            let temp_file = NamedTempFile::new().unwrap();
            let file_path = temp_file.path().to_str().unwrap().to_string();
            
            {
                let mut f = File::create(&file_path).unwrap();
                f.write_all(MAGIC_BYTES).unwrap();
                f.write_all(&999u32.to_le_bytes()).unwrap(); // Wrong version
            }
            
            LogEngine::new(file_path, 1024);
        }
    
        #[test]
        #[should_panic(expected = "Data file is too small")]
        fn test_new_with_too_small_file_panics() {
            let temp_file = NamedTempFile::new().unwrap();
            let file_path = temp_file.path().to_str().unwrap().to_string();
            
            {
                let mut f = File::create(&file_path).unwrap();
                f.write_all(b"TOO_SMALL").unwrap(); // 9 bytes (Header is 10)
            }
            
            LogEngine::new(file_path, 1024);
        }
    }
