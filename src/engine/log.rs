use super::Engine;
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};
use tonic::Status;

const MAGIC_BYTES: &[u8; 6] = b"ORELSM";
const DATA_VERSION: u32 = 1;
const HEADER_SIZE: u64 = 10; // 6 (magic) + 4 (version)

#[derive(Debug, Clone)]
pub struct LogEngine {
    // Stores Key -> (FileOffset, ValueLength)
    index: Arc<Mutex<HashMap<String, (u64, usize)>>>,
    // File writer for appending new entries
    writer: Arc<Mutex<File>>,
    // Path to the data file
    file_path: String,
    // Threshold for compaction in bytes
    compaction_threshold: u64,
    // Flag to check if compaction is running
    is_compacting: Arc<AtomicBool>,
}

impl LogEngine {
    pub fn new(data_dir: String, compaction_threshold: u64) -> Self {
        let dir_path = PathBuf::from(&data_dir);
        if !dir_path.exists() {
            fs::create_dir_all(&dir_path).expect("Failed to create data directory");
        }
        let file_path = dir_path
            .join("log_engine.data")
            .to_str()
            .unwrap()
            .to_string();

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

                if val_len == u64::MAX {
                    // Tombstone found, remove from index
                    index_map.remove(&key);
                } else {
                    // Skip value
                    file.seek(SeekFrom::Current(val_len as i64))
                        .expect("Failed to skip value");
                    index_map.insert(key, (value_offset, val_len as usize));
                }
            }
        }

        LogEngine {
            index: Arc::new(Mutex::new(index_map)),
            writer: Arc::new(Mutex::new(file)),
            file_path,
            compaction_threshold,
            is_compacting: Arc::new(AtomicBool::new(false)),
        }
    }

    #[allow(clippy::result_large_err)]
    fn compact(&self) -> Result<(), Status> {
        // Set compacting flag
        self.is_compacting.store(true, Ordering::SeqCst);

        // Acquire locks in the same order as set() to avoid deadlocks: Writer first, then Index
        let mut writer = match self.writer.lock() {
            Ok(w) => w,
            Err(e) => {
                self.is_compacting.store(false, Ordering::SeqCst);
                return Err(Status::internal(e.to_string()));
            }
        };
        let mut index = match self.index.lock() {
            Ok(i) => i,
            Err(e) => {
                self.is_compacting.store(false, Ordering::SeqCst);
                return Err(Status::internal(e.to_string()));
            }
        };

        // Check size again under lock to avoid unnecessary compaction or races
        let file_size = match writer.metadata() {
            Ok(m) => m.len(),
            Err(e) => {
                self.is_compacting.store(false, Ordering::SeqCst);
                return Err(Status::internal(e.to_string()));
            }
        };

        if file_size <= self.compaction_threshold {
            self.is_compacting.store(false, Ordering::SeqCst);
            return Ok(());
        }

        println!("Compacting log file (size: {} bytes)...", file_size);

        let new_file_path = format!("{}.tmp", self.file_path);
        let mut new_file = match OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&new_file_path)
        {
            Ok(f) => f,
            Err(e) => {
                self.is_compacting.store(false, Ordering::SeqCst);
                return Err(Status::internal(e.to_string()));
            }
        };

        if let Err(e) = new_file.write_all(MAGIC_BYTES) {
            self.is_compacting.store(false, Ordering::SeqCst);
            return Err(Status::internal(e.to_string()));
        }
        if let Err(e) = new_file.write_all(&DATA_VERSION.to_le_bytes()) {
            self.is_compacting.store(false, Ordering::SeqCst);
            return Err(Status::internal(e.to_string()));
        }

        let mut new_index = HashMap::new();

        for (key, &(offset, length)) in index.iter() {
            if let Err(e) = writer.seek(SeekFrom::Start(offset)) {
                self.is_compacting.store(false, Ordering::SeqCst);
                return Err(Status::internal(e.to_string()));
            }
            let mut value = vec![0u8; length];
            if let Err(e) = writer.read_exact(&mut value) {
                self.is_compacting.store(false, Ordering::SeqCst);
                return Err(Status::internal(e.to_string()));
            }

            let key_bytes = key.as_bytes();
            let key_len = key_bytes.len() as u64;
            let val_len = length as u64;
            let timestamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs();

            if let Err(e) = new_file.write_all(&timestamp.to_le_bytes()) {
                self.is_compacting.store(false, Ordering::SeqCst);
                return Err(Status::internal(e.to_string()));
            }
            if let Err(e) = new_file.write_all(&key_len.to_le_bytes()) {
                self.is_compacting.store(false, Ordering::SeqCst);
                return Err(Status::internal(e.to_string()));
            }
            if let Err(e) = new_file.write_all(&val_len.to_le_bytes()) {
                self.is_compacting.store(false, Ordering::SeqCst);
                return Err(Status::internal(e.to_string()));
            }
            if let Err(e) = new_file.write_all(key_bytes) {
                self.is_compacting.store(false, Ordering::SeqCst);
                return Err(Status::internal(e.to_string()));
            }
            if let Err(e) = new_file.write_all(&value) {
                self.is_compacting.store(false, Ordering::SeqCst);
                return Err(Status::internal(e.to_string()));
            }

            let current_pos = match new_file.stream_position() {
                Ok(pos) => pos,
                Err(e) => {
                    self.is_compacting.store(false, Ordering::SeqCst);
                    return Err(Status::internal(e.to_string()));
                }
            };
            let value_offset = current_pos - val_len;
            new_index.insert(key.clone(), (value_offset, length));
        }

        if let Err(e) = new_file.flush() {
            self.is_compacting.store(false, Ordering::SeqCst);
            return Err(Status::internal(e.to_string()));
        }

        if let Err(e) = std::fs::rename(&new_file_path, &self.file_path) {
            self.is_compacting.store(false, Ordering::SeqCst);
            return Err(Status::internal(e.to_string()));
        }

        *writer = new_file;
        *index = new_index;

        println!("Compaction finished.");
        self.is_compacting.store(false, Ordering::SeqCst);
        Ok(())
    }
}

impl Engine for LogEngine {
    fn set(&self, key: String, value: String) -> Result<(), Status> {
        let key_bytes = key.as_bytes();
        let val_bytes = value.as_bytes();

        let key_len = key_bytes.len() as u64;
        let val_len = val_bytes.len() as u64;

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
            if self
                .is_compacting
                .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                let engine_clone = self.clone();
                tokio::spawn(async move {
                    if let Err(e) = engine_clone.compact() {
                        eprintln!("Compaction failed: {}", e);
                    }
                });
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

    fn delete(&self, key: String) -> Result<(), Status> {
        let key_bytes = key.as_bytes();
        let key_len = key_bytes.len() as u64;
        let val_len = u64::MAX; // Tombstone marker

        let entry_size = 8 + 8 + 8 + key_len;
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

        let should_compact = {
            let mut writer = self.writer.lock().unwrap();
            writer
                .seek(SeekFrom::End(0))
                .map_err(|e| Status::internal(e.to_string()))?;

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
                .flush()
                .map_err(|e| Status::internal(e.to_string()))?;

            let mut index = self.index.lock().unwrap();
            index.remove(&key);

            writer
                .metadata()
                .map_err(|e| Status::internal(e.to_string()))?
                .len()
                > self.compaction_threshold
        };

        #[allow(clippy::collapsible_if)]
        if should_compact {
            if self
                .is_compacting
                .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                let engine_clone = self.clone();
                tokio::spawn(async move {
                    if let Err(e) = engine_clone.compact() {
                        eprintln!("Compaction failed: {}", e);
                    }
                });
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_set_exceeding_threshold_returns_error() {
        let dir = tempdir().unwrap();
        let data_dir = dir.path().to_str().unwrap().to_string();
        let engine = LogEngine::new(data_dir, 30);

        let key = "mykey".to_string(); // 5 bytes
        let value = "this_is_too_long_value".to_string(); // 22 bytes
        // Header(24) + key(5) + val(22) = 51 > 30

        let result = engine.set(key, value);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code(), tonic::Code::InvalidArgument);
    }

    #[test]
    fn test_set_within_threshold_succeeds() {
        let dir = tempdir().unwrap();
        let data_dir = dir.path().to_str().unwrap().to_string();
        let engine = LogEngine::new(data_dir, 100);

        let result = engine.set("key".to_string(), "value".to_string());
        assert!(result.is_ok());

        let value = engine.get("key".to_string()).unwrap();
        assert_eq!(value, "value");
    }

    #[test]
    #[should_panic(expected = "Invalid magic bytes")]
    fn test_new_with_invalid_magic_bytes_panics() {
        let dir = tempdir().unwrap();
        let data_dir = dir.path().to_str().unwrap().to_string();
        let file_path = dir.path().join("log_engine.data");

        {
            let mut f = File::create(&file_path).unwrap();
            f.write_all(b"WRONG!").unwrap();
            f.write_all(&1u32.to_le_bytes()).unwrap();
        }

        LogEngine::new(data_dir, 1024);
    }

    #[test]
    #[should_panic(expected = "Incompatible data version")]
    fn test_new_with_invalid_version_panics() {
        let dir = tempdir().unwrap();
        let data_dir = dir.path().to_str().unwrap().to_string();
        let file_path = dir.path().join("log_engine.data");

        {
            let mut f = File::create(&file_path).unwrap();
            f.write_all(MAGIC_BYTES).unwrap();
            f.write_all(&999u32.to_le_bytes()).unwrap();
        }

        LogEngine::new(data_dir, 1024);
    }

    #[test]
    #[should_panic(expected = "Data file is too small")]
    fn test_new_with_too_small_file_panics() {
        let dir = tempdir().unwrap();
        let data_dir = dir.path().to_str().unwrap().to_string();
        let file_path = dir.path().join("log_engine.data");

        {
            let mut f = File::create(&file_path).unwrap();
            f.write_all(b"TOO_SMALL").unwrap();
        }

        LogEngine::new(data_dir, 1024);
    }

    #[tokio::test]
    async fn test_compaction_runs_async() {
        let dir = tempdir().unwrap();
        let data_dir = dir.path().to_str().unwrap().to_string();
        let file_path = dir.path().join("log_engine.data");

        let engine = LogEngine::new(data_dir, 80);

        engine.set("key1".to_string(), "val1".to_string()).unwrap();
        engine.set("key2".to_string(), "val2".to_string()).unwrap();

        let size_before = std::fs::metadata(&file_path).unwrap().len();
        assert!(size_before < 80);

        engine
            .set("key1".to_string(), "new_val1".to_string())
            .unwrap();

        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

        let size_after = std::fs::metadata(&file_path).unwrap().len();
        assert!(size_after < 110);

        assert_eq!(engine.get("key1".to_string()).unwrap(), "new_val1");
        assert_eq!(engine.get("key2".to_string()).unwrap(), "val2");
    }

    #[test]
    fn test_delete() {
        let dir = tempdir().unwrap();
        let data_dir = dir.path().to_str().unwrap().to_string();

        {
            let engine = LogEngine::new(data_dir.clone(), 1024);
            engine
                .set("key1".to_string(), "value1".to_string())
                .unwrap();
            assert_eq!(engine.get("key1".to_string()).unwrap(), "value1");

            engine.delete("key1".to_string()).unwrap();
            let result = engine.get("key1".to_string());
            assert!(result.is_err());
            assert_eq!(result.unwrap_err().code(), tonic::Code::NotFound);
        }

        {
            let engine = LogEngine::new(data_dir, 1024);
            let result = engine.get("key1".to_string());
            assert!(result.is_err());
            assert_eq!(result.unwrap_err().code(), tonic::Code::NotFound);
        }
    }
}
