use super::{Engine, current_timestamp};
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};
use tonic::Status;

const MAGIC_BYTES: &[u8; 6] = b"OREKVS";
const DATA_VERSION: u32 = 2; // v2: added expire_at support
const HEADER_SIZE: u64 = 10; // 6 (magic) + 4 (version)

/// Index entry: (FileOffset, ValueLength, ExpireAt)
/// ExpireAt: 0 = no expiration, >0 = Unix timestamp when key expires
type IndexEntry = (u64, usize, u64);

#[derive(Debug, Clone)]
pub struct LogEngine {
    index: Arc<Mutex<HashMap<String, IndexEntry>>>,
    // File writer for appending new entries
    writer: Arc<Mutex<File>>,
    // Path to the data file
    file_path: String,
    // Max size for log file in bytes
    log_capacity_bytes: u64,
    // Flag to check if compaction is running
    is_compacting: Arc<AtomicBool>,
    // Handle to the compaction task for graceful shutdown
    compaction_handle: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
}

impl LogEngine {
    pub fn new(data_dir: String, log_capacity_bytes: u64) -> Self {
        let dir_path = PathBuf::from(&data_dir);
        if !dir_path.exists() {
            fs::create_dir_all(&dir_path).expect("Failed to create data directory");
        }

        // Clean up orphaned .tmp files from interrupted compaction
        if let Ok(entries) = fs::read_dir(&dir_path) {
            for entry in entries.flatten() {
                let p = entry.path();
                if let Some(filename) = p.file_name().and_then(|n| n.to_str())
                    && filename.ends_with(".tmp")
                {
                    if let Err(e) = fs::remove_file(&p) {
                        eprintln!("Warning: Failed to remove orphaned tmp file {:?}: {}", p, e);
                    } else {
                        println!("Cleaned up orphaned tmp file: {:?}", p);
                    }
                }
            }
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
            file.sync_all().expect("Failed to sync header");
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
            if version != DATA_VERSION && version != 1 {
                panic!(
                    "Incompatible data version: expected {} or 1, found {}",
                    DATA_VERSION, version
                );
            }
        }

        // Read version again for format detection
        file.seek(SeekFrom::Start(6))
            .expect("Failed to seek to version");
        let mut ver_bytes = [0u8; 4];
        file.read_exact(&mut ver_bytes)
            .expect("Failed to read version");
        let file_version = u32::from_le_bytes(ver_bytes);

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

                // Read expire_at (v2+) or default to 0 (v1)
                let expire_at = if file_version >= 2 {
                    let mut expire_bytes = [0u8; 8];
                    file.read_exact(&mut expire_bytes)
                        .expect("Failed to read expire_at");
                    u64::from_le_bytes(expire_bytes)
                } else {
                    0
                };

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
                    index_map.insert(key, (value_offset, val_len as usize, expire_at));
                }
            }
        }

        LogEngine {
            index: Arc::new(Mutex::new(index_map)),
            writer: Arc::new(Mutex::new(file)),
            file_path,
            log_capacity_bytes,
            is_compacting: Arc::new(AtomicBool::new(false)),
            compaction_handle: Arc::new(Mutex::new(None)),
        }
    }

    /// Gracefully shutdown the engine, waiting for any running compaction to complete
    pub async fn shutdown(&self) {
        println!("Shutting down LogEngine...");
        let handle = {
            let mut guard = self.compaction_handle.lock().unwrap();
            guard.take()
        };
        if let Some(handle) = handle
            && let Err(e) = handle.await
        {
            eprintln!("Error waiting for compaction to complete: {}", e);
        }
        println!("LogEngine shutdown complete.");
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

        if file_size <= self.log_capacity_bytes {
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
        let now = current_timestamp();

        for (key, &(offset, length, expire_at)) in index.iter() {
            // Skip expired entries during compaction
            if expire_at > 0 && now > expire_at {
                continue;
            }

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
            // Write expire_at (v2 format)
            if let Err(e) = new_file.write_all(&expire_at.to_le_bytes()) {
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
            new_index.insert(key.clone(), (value_offset, length, expire_at));
        }

        if let Err(e) = new_file.sync_data() {
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

impl LogEngine {
    /// Write entry to file and update index with already-acquired locks
    /// Returns the current file length for compaction check
    #[allow(clippy::result_large_err)]
    fn write_entry_with_locks(
        &self,
        writer: &mut File,
        index: &mut HashMap<String, IndexEntry>,
        key: String,
        value: String,
        expire_at: u64,
    ) -> Result<u64, Status> {
        let key_bytes = key.as_bytes();
        let val_bytes = value.as_bytes();
        let key_len = key_bytes.len() as u64;
        let val_len = val_bytes.len() as u64;

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        // Get current offset (end of file)
        let start_offset = writer
            .seek(SeekFrom::End(0))
            .map_err(|e| Status::internal(e.to_string()))?;

        // Write Format: [timestamp(8)][expire_at(8)][key_len(8)][val_len(8)][key][value]
        writer
            .write_all(&timestamp.to_le_bytes())
            .map_err(|e| Status::internal(e.to_string()))?;
        writer
            .write_all(&expire_at.to_le_bytes())
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
            .sync_data()
            .map_err(|e| Status::internal(e.to_string()))?;

        // Calculate where the value starts:
        // timestamp(8) + expire_at(8) + key_len(8) + val_len(8) + key
        let value_offset = start_offset + 8 + 8 + 8 + 8 + key_len;

        // Update in-memory index
        index.insert(key, (value_offset, val_len as usize, expire_at));

        // Return file length for compaction check
        writer
            .metadata()
            .map_err(|e| Status::internal(e.to_string()))
            .map(|m| m.len())
    }

    #[allow(clippy::result_large_err)]
    fn set_internal(&self, key: String, value: String, expire_at: u64) -> Result<(), Status> {
        let key_len = key.len() as u64;
        let val_len = value.len() as u64;

        // Entry size = timestamp(8) + expire_at(8) + key_len_size(8) + val_len_size(8) + key + value
        let entry_size = 8 + 8 + 8 + 8 + key_len + val_len;
        if entry_size > self.log_capacity_bytes {
            return Err(Status::invalid_argument(format!(
                "Data size ({} bytes) exceeds the log capacity ({} bytes)",
                entry_size, self.log_capacity_bytes
            )));
        }

        // Scope for locks
        let should_compact = {
            let mut writer = self.writer.lock().unwrap();
            let mut index = self.index.lock().unwrap();
            let file_len =
                self.write_entry_with_locks(&mut writer, &mut index, key, value, expire_at)?;
            file_len > self.log_capacity_bytes
        };

        #[allow(clippy::collapsible_if)]
        if should_compact {
            if self
                .is_compacting
                .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                let engine_clone = self.clone();
                let handle = tokio::spawn(async move {
                    if let Err(e) = engine_clone.compact() {
                        eprintln!("Compaction failed: {}", e);
                    }
                });
                *self.compaction_handle.lock().unwrap() = Some(handle);
            }
        }

        Ok(())
    }
}

impl Engine for LogEngine {
    fn set(&self, key: String, value: String) -> Result<(), Status> {
        self.set_internal(key, value, 0)
    }

    fn set_with_ttl(&self, key: String, value: String, ttl_secs: u64) -> Result<(), Status> {
        let expire_at = current_timestamp() + ttl_secs;
        self.set_internal(key, value, expire_at)
    }

    fn get_with_expire_at(&self, key: String) -> Result<(String, u64), Status> {
        let (offset, length, expire_at) = {
            let index = self.index.lock().unwrap();
            match index.get(&key) {
                Some(&info) => info,
                None => return Err(Status::not_found("Key not found")),
            }
        };

        // Check if expired
        if expire_at > 0 && current_timestamp() > expire_at {
            return Err(Status::not_found("Key not found"));
        }

        let mut file = File::open(&self.file_path).map_err(|e| Status::internal(e.to_string()))?;

        file.seek(SeekFrom::Start(offset))
            .map_err(|e| Status::internal(e.to_string()))?;

        let mut buffer = vec![0u8; length];
        file.read_exact(&mut buffer)
            .map_err(|e| Status::internal(e.to_string()))?;

        let value = String::from_utf8(buffer)
            .map_err(|e| Status::internal(format!("Invalid UTF-8: {}", e)))?;
        Ok((value, expire_at))
    }

    fn delete(&self, key: String) -> Result<(), Status> {
        let key_bytes = key.as_bytes();
        let key_len = key_bytes.len() as u64;
        let val_len = u64::MAX; // Tombstone marker
        let expire_at = 0u64; // No expiration for tombstones

        // Entry size = timestamp(8) + expire_at(8) + key_len_size(8) + val_len_size(8) + key
        let entry_size = 8 + 8 + 8 + 8 + key_len;
        if entry_size > self.log_capacity_bytes {
            return Err(Status::invalid_argument(format!(
                "Data size ({} bytes) exceeds the log capacity ({} bytes)",
                entry_size, self.log_capacity_bytes
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
                .write_all(&expire_at.to_le_bytes())
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
                .sync_data()
                .map_err(|e| Status::internal(e.to_string()))?;

            let mut index = self.index.lock().unwrap();
            index.remove(&key);

            writer
                .metadata()
                .map_err(|e| Status::internal(e.to_string()))?
                .len()
                > self.log_capacity_bytes
        };

        #[allow(clippy::collapsible_if)]
        if should_compact {
            if self
                .is_compacting
                .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                let engine_clone = self.clone();
                let handle = tokio::spawn(async move {
                    if let Err(e) = engine_clone.compact() {
                        eprintln!("Compaction failed: {}", e);
                    }
                });
                *self.compaction_handle.lock().unwrap() = Some(handle);
            }
        }

        Ok(())
    }

    fn compare_and_set(
        &self,
        key: String,
        expected_value: Option<String>,
        new_value: String,
        expire_at: u64,
    ) -> Result<(bool, Option<String>), Status> {
        let now = current_timestamp();

        // Acquire writer lock first for atomicity (same order as other methods)
        let mut writer = self.writer.lock().unwrap();
        let mut index = self.index.lock().unwrap();

        // Get current value if exists
        let current_entry = index.get(&key).copied();
        let current_value = if let Some((offset, length, entry_expire_at)) = current_entry {
            // Check if expired
            if entry_expire_at > 0 && now > entry_expire_at {
                None
            } else {
                // Read current value from file
                let mut file =
                    File::open(&self.file_path).map_err(|e| Status::internal(e.to_string()))?;
                file.seek(SeekFrom::Start(offset))
                    .map_err(|e| Status::internal(e.to_string()))?;
                let mut buffer = vec![0u8; length];
                file.read_exact(&mut buffer)
                    .map_err(|e| Status::internal(e.to_string()))?;
                Some(
                    String::from_utf8(buffer)
                        .map_err(|e| Status::internal(format!("Invalid UTF-8: {}", e)))?,
                )
            }
        } else {
            None
        };

        // Check if CAS condition is met
        let should_update = match (&expected_value, &current_value) {
            (None, None) => true,     // Key should not exist, and it doesn't
            (None, Some(_)) => false, // Key should not exist, but it does
            (Some(expected), Some(current)) if expected == current => true, // Values match
            (Some(_), _) => false,    // Values don't match or key doesn't exist
        };

        if !should_update {
            return Ok((false, current_value));
        }

        // Perform the update
        self.write_entry_with_locks(&mut writer, &mut index, key, new_value, expire_at)?;

        Ok((true, current_value))
    }

    fn count(&self, prefix: &str) -> Result<u64, Status> {
        let index = self.index.lock().unwrap();
        let now = current_timestamp();
        let count = index
            .iter()
            .filter(|(key, (_, _, expire_at))| {
                key.starts_with(prefix) && (*expire_at == 0 || now <= *expire_at)
            })
            .count() as u64;
        Ok(count)
    }
}

/// Wrapper to hold Log engine reference for graceful shutdown
pub struct LogEngineHolder {
    engine: Option<Arc<LogEngine>>,
}

impl LogEngineHolder {
    pub fn new() -> Self {
        LogEngineHolder { engine: None }
    }

    pub fn set(&mut self, engine: Arc<LogEngine>) {
        self.engine = Some(engine);
    }

    pub async fn shutdown(&self) {
        if let Some(ref engine) = self.engine {
            engine.shutdown().await;
        }
    }
}

impl Default for LogEngineHolder {
    fn default() -> Self {
        Self::new()
    }
}

/// Wrapper to implement Engine for Arc<LogEngine>
pub struct LogEngineWrapper(pub Arc<LogEngine>);

impl Engine for LogEngineWrapper {
    fn set(&self, key: String, value: String) -> Result<(), Status> {
        self.0.set(key, value)
    }

    fn delete(&self, key: String) -> Result<(), Status> {
        self.0.delete(key)
    }

    fn get_with_expire_at(&self, key: String) -> Result<(String, u64), Status> {
        self.0.get_with_expire_at(key)
    }

    fn compare_and_set(
        &self,
        key: String,
        expected_value: Option<String>,
        new_value: String,
        expire_at: u64,
    ) -> Result<(bool, Option<String>), Status> {
        self.0
            .compare_and_set(key, expected_value, new_value, expire_at)
    }

    fn count(&self, prefix: &str) -> Result<u64, Status> {
        self.0.count(prefix)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_set_exceeding_capacity_returns_error() {
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
    fn test_set_within_capacity_succeeds() {
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

        // Size increased due to expire_at field (8 bytes per entry)
        // Entry format: timestamp(8) + expire_at(8) + key_len(8) + val_len(8) + key + value
        // = 32 bytes + key + value per entry
        let engine = LogEngine::new(data_dir, 100);

        engine.set("key1".to_string(), "val1".to_string()).unwrap();
        engine.set("key2".to_string(), "val2".to_string()).unwrap();

        let size_before = std::fs::metadata(&file_path).unwrap().len();
        assert!(size_before < 100);

        engine
            .set("key1".to_string(), "new_val1".to_string())
            .unwrap();

        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

        let size_after = std::fs::metadata(&file_path).unwrap().len();
        assert!(size_after < 140);

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
