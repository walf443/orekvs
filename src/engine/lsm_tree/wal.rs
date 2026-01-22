use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};
use tonic::Status;

use super::memtable::MemTable;

const WAL_MAGIC_BYTES: &[u8; 9] = b"ORELSMWAL";
const WAL_VERSION: u32 = 1;

/// WAL (Write-Ahead Log) writer for crash recovery
#[derive(Debug)]
pub struct WalWriter {
    /// Current active WAL file handle
    writer: Arc<Mutex<File>>,
    /// Path to current WAL file
    path: Arc<Mutex<PathBuf>>,
    /// Current WAL ID
    current_id: Arc<AtomicU64>,
    /// Data directory
    data_dir: PathBuf,
}

impl Clone for WalWriter {
    fn clone(&self) -> Self {
        WalWriter {
            writer: Arc::clone(&self.writer),
            path: Arc::clone(&self.path),
            current_id: Arc::clone(&self.current_id),
            data_dir: self.data_dir.clone(),
        }
    }
}

impl WalWriter {
    /// Create a new WAL writer
    #[allow(clippy::result_large_err)]
    pub fn new(data_dir: &Path, wal_id: u64) -> Result<Self, Status> {
        let (file, path) = Self::create_wal_file(data_dir, wal_id)?;
        Ok(WalWriter {
            writer: Arc::new(Mutex::new(file)),
            path: Arc::new(Mutex::new(path)),
            current_id: Arc::new(AtomicU64::new(wal_id)),
            data_dir: data_dir.to_path_buf(),
        })
    }

    /// Get current WAL ID
    pub fn current_id(&self) -> u64 {
        self.current_id.load(Ordering::SeqCst)
    }

    /// Create a new WAL file and return the file handle
    #[allow(clippy::result_large_err)]
    fn create_wal_file(data_dir: &Path, wal_id: u64) -> Result<(File, PathBuf), Status> {
        let wal_path = data_dir.join(format!("wal_{:05}.log", wal_id));
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&wal_path)
            .map_err(|e| Status::internal(format!("Failed to create WAL: {}", e)))?;

        // Write header
        file.write_all(WAL_MAGIC_BYTES)
            .map_err(|e| Status::internal(e.to_string()))?;
        file.write_all(&WAL_VERSION.to_le_bytes())
            .map_err(|e| Status::internal(e.to_string()))?;
        file.sync_all()
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok((file, wal_path))
    }

    /// Append an entry to the WAL
    #[allow(clippy::result_large_err)]
    pub fn append(&self, key: &str, value: &Option<String>) -> Result<(), Status> {
        let mut wal = self.writer.lock().unwrap();

        let key_bytes = key.as_bytes();
        let key_len = key_bytes.len() as u64;
        let (val_len, val_bytes): (u64, &[u8]) = match value {
            Some(v) => (v.len() as u64, v.as_bytes()),
            None => (u64::MAX, &[]), // Tombstone
        };

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        // Write entry
        wal.write_all(&timestamp.to_le_bytes())
            .map_err(|e| Status::internal(e.to_string()))?;
        wal.write_all(&key_len.to_le_bytes())
            .map_err(|e| Status::internal(e.to_string()))?;
        wal.write_all(&val_len.to_le_bytes())
            .map_err(|e| Status::internal(e.to_string()))?;
        wal.write_all(key_bytes)
            .map_err(|e| Status::internal(e.to_string()))?;
        if val_len != u64::MAX {
            wal.write_all(val_bytes)
                .map_err(|e| Status::internal(e.to_string()))?;
        }

        // Sync to disk for durability
        wal.sync_all()
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(())
    }

    /// Write multiple entries to the WAL (used for recovery re-write)
    #[allow(clippy::result_large_err)]
    pub fn write_entries(&self, entries: &MemTable) -> Result<(), Status> {
        let mut wal = self.writer.lock().unwrap();

        for (key, value) in entries {
            let key_bytes = key.as_bytes();
            let key_len = key_bytes.len() as u64;
            let (val_len, val_bytes): (u64, &[u8]) = match value {
                Some(v) => (v.len() as u64, v.as_bytes()),
                None => (u64::MAX, &[]),
            };
            let timestamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs();

            wal.write_all(&timestamp.to_le_bytes())
                .map_err(|e| Status::internal(e.to_string()))?;
            wal.write_all(&key_len.to_le_bytes())
                .map_err(|e| Status::internal(e.to_string()))?;
            wal.write_all(&val_len.to_le_bytes())
                .map_err(|e| Status::internal(e.to_string()))?;
            wal.write_all(key_bytes)
                .map_err(|e| Status::internal(e.to_string()))?;
            if val_len != u64::MAX {
                wal.write_all(val_bytes)
                    .map_err(|e| Status::internal(e.to_string()))?;
            }
        }

        wal.sync_all()
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(())
    }

    /// Rotate WAL: create a new WAL file (old WAL is preserved for replication)
    #[allow(clippy::result_large_err)]
    pub fn rotate(&self) -> Result<u64, Status> {
        let new_wal_id = self.current_id.load(Ordering::SeqCst) + 1;
        let (new_file, new_path) = Self::create_wal_file(&self.data_dir, new_wal_id)?;

        // Swap WAL file
        {
            let mut wal_writer = self.writer.lock().unwrap();
            let mut wal_path = self.path.lock().unwrap();

            // Ensure old WAL is synced
            wal_writer
                .sync_all()
                .map_err(|e| Status::internal(e.to_string()))?;

            *wal_writer = new_file;
            *wal_path = new_path.clone();
        }

        self.current_id.store(new_wal_id, Ordering::SeqCst);

        println!("WAL rotated to {:?}", new_path);

        Ok(new_wal_id)
    }

    /// Read all entries from a WAL file
    #[allow(clippy::result_large_err)]
    pub fn read_entries(path: &Path) -> Result<MemTable, Status> {
        let mut file = match File::open(path) {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Ok(BTreeMap::new());
            }
            Err(e) => return Err(Status::internal(e.to_string())),
        };

        // Verify header
        let mut magic = [0u8; 9];
        if file.read_exact(&mut magic).is_err() {
            return Ok(BTreeMap::new()); // Empty file
        }
        if &magic != WAL_MAGIC_BYTES {
            return Err(Status::internal("Invalid WAL magic bytes"));
        }

        let mut version_bytes = [0u8; 4];
        file.read_exact(&mut version_bytes)
            .map_err(|e| Status::internal(e.to_string()))?;
        let version = u32::from_le_bytes(version_bytes);
        if version != WAL_VERSION {
            return Err(Status::internal(format!(
                "Unsupported WAL version: {}",
                version
            )));
        }

        // Read entries
        let mut memtable = BTreeMap::new();

        loop {
            let mut ts_bytes = [0u8; 8];
            if file.read_exact(&mut ts_bytes).is_err() {
                break; // End of file
            }

            let mut klen_bytes = [0u8; 8];
            let mut vlen_bytes = [0u8; 8];

            file.read_exact(&mut klen_bytes)
                .map_err(|e| Status::internal(e.to_string()))?;
            file.read_exact(&mut vlen_bytes)
                .map_err(|e| Status::internal(e.to_string()))?;

            let key_len = u64::from_le_bytes(klen_bytes);
            let val_len = u64::from_le_bytes(vlen_bytes);

            let mut key_buf = vec![0u8; key_len as usize];
            file.read_exact(&mut key_buf)
                .map_err(|e| Status::internal(e.to_string()))?;
            let key = String::from_utf8_lossy(&key_buf).to_string();

            let value = if val_len == u64::MAX {
                None // Tombstone
            } else {
                let mut val_buf = vec![0u8; val_len as usize];
                file.read_exact(&mut val_buf)
                    .map_err(|e| Status::internal(e.to_string()))?;
                Some(String::from_utf8_lossy(&val_buf).to_string())
            };

            memtable.insert(key, value);
        }

        Ok(memtable)
    }

    /// Parse WAL filename and return WAL ID
    pub fn parse_wal_filename(filename: &str) -> Option<u64> {
        if filename.starts_with("wal_") && filename.ends_with(".log") {
            filename[4..filename.len() - 4].parse::<u64>().ok()
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_wal_creation_and_header() {
        let dir = tempdir().unwrap();
        let wal_id = 123;
        let _wal = WalWriter::new(dir.path(), wal_id).unwrap();

        let wal_path = dir.path().join(format!("wal_{:05}.log", wal_id));
        assert!(wal_path.exists());

        let mut file = File::open(wal_path).unwrap();
        let mut magic = [0u8; 9];
        file.read_exact(&mut magic).unwrap();
        assert_eq!(&magic, WAL_MAGIC_BYTES);

        let mut version_bytes = [0u8; 4];
        file.read_exact(&mut version_bytes).unwrap();
        assert_eq!(u32::from_le_bytes(version_bytes), WAL_VERSION);
    }

    #[test]
    fn test_wal_append_and_read() {
        let dir = tempdir().unwrap();
        let wal = WalWriter::new(dir.path(), 1).unwrap();

        wal.append("k1", &Some("v1".to_string())).unwrap();
        wal.append("k2", &Some("v2".to_string())).unwrap();
        wal.append("k1", &None).unwrap(); // Tombstone

        let path = wal.path.lock().unwrap().clone();
        let entries = WalWriter::read_entries(&path).unwrap();

        assert_eq!(entries.len(), 2);
        assert_eq!(entries.get("k1"), Some(&None));
        assert_eq!(entries.get("k2"), Some(&Some("v2".to_string())));
    }

    #[test]
    fn test_wal_rotation() {
        let dir = tempdir().unwrap();
        let wal = WalWriter::new(dir.path(), 1).unwrap();

        wal.append("k1", &Some("v1".to_string())).unwrap();
        let old_path = wal.path.lock().unwrap().clone();

        let new_id = wal.rotate().unwrap();
        assert_eq!(new_id, 2);
        assert_eq!(wal.current_id(), 2);

        let new_path = wal.path.lock().unwrap().clone();
        assert_ne!(old_path, new_path);
        assert!(new_path.exists());

        wal.append("k2", &Some("v2".to_string())).unwrap();

        // Old WAL should still have k1
        let old_entries = WalWriter::read_entries(&old_path).unwrap();
        assert_eq!(old_entries.get("k1"), Some(&Some("v1".to_string())));
        assert!(!old_entries.contains_key("k2"));

        // New WAL should have k2
        let new_entries = WalWriter::read_entries(&new_path).unwrap();
        assert_eq!(new_entries.get("k2"), Some(&Some("v2".to_string())));
        assert!(!new_entries.contains_key("k1"));
    }

    #[test]
    fn test_parse_wal_filename() {
        assert_eq!(WalWriter::parse_wal_filename("wal_00001.log"), Some(1));
        assert_eq!(WalWriter::parse_wal_filename("wal_12345.log"), Some(12345));
        assert_eq!(WalWriter::parse_wal_filename("wal_00000.log"), Some(0));
        assert_eq!(WalWriter::parse_wal_filename("other.log"), None);
        assert_eq!(WalWriter::parse_wal_filename("wal_123.data"), None);
    }
}