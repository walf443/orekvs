use super::Engine;
use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};
use tonic::Status;

const MAGIC_BYTES: &[u8; 6] = b"ORELSM";
const DATA_VERSION: u32 = 1;

// Type alias to reduce complexity warnings
type MemTable = BTreeMap<String, Option<String>>;

#[derive(Debug, Clone)]
pub struct LsmTreeEngine {
    // Current in-memory write buffer
    active_memtable: Arc<Mutex<MemTable>>,
    // MemTables currently being flushed to disk
    immutable_memtables: Arc<Mutex<Vec<Arc<MemTable>>>>,
    // List of SSTable file paths on disk (newest first)
    sstables: Arc<Mutex<Vec<PathBuf>>>,
    // Base directory for SSTables
    data_dir: PathBuf,
    // Threshold for MemTable size in bytes
    memtable_threshold: u64,
    // Approximate size of current active MemTable
    current_mem_size: Arc<AtomicU64>,
    // Counter for next SSTable file ID
    next_sst_id: Arc<AtomicU64>,
}

impl LsmTreeEngine {
    pub fn new(data_dir_str: String, memtable_threshold: u64) -> Self {
        let data_dir = PathBuf::from(&data_dir_str);

        if !data_dir.exists() {
            fs::create_dir_all(&data_dir).expect("Failed to create data directory");
        }

        let mut sst_files = Vec::new();
        let mut max_id = 0;

        if let Ok(entries) = fs::read_dir(&data_dir) {
            for entry in entries.flatten() {
                let p = entry.path();
                #[allow(clippy::collapsible_if)]
                if let Some(filename) = p.file_name().and_then(|n| n.to_str()) {
                    if filename.starts_with("sst_") && filename.ends_with(".data") {
                        if let Ok(id) = filename[4..filename.len() - 5].parse::<u64>() {
                            if id >= max_id {
                                max_id = id + 1;
                            }
                            sst_files.push(p);
                        }
                    }
                }
            }
        }

        sst_files.sort_by(|a, b| b.cmp(a));

        LsmTreeEngine {
            active_memtable: Arc::new(Mutex::new(BTreeMap::new())),
            immutable_memtables: Arc::new(Mutex::new(Vec::new())),
            sstables: Arc::new(Mutex::new(sst_files)),
            data_dir,
            memtable_threshold,
            current_mem_size: Arc::new(AtomicU64::new(0)),
            next_sst_id: Arc::new(AtomicU64::new(max_id)),
        }
    }

    fn estimate_entry_size(key: &str, value: &Option<String>) -> u64 {
        let vlen = value.as_ref().map_or(0, |v| v.len() as u64);
        8 + 8 + 8 + key.len() as u64 + vlen
    }

    fn check_flush(&self) {
        let size = self.current_mem_size.load(Ordering::SeqCst);
        if size >= self.memtable_threshold {
            let mut active = self.active_memtable.lock().unwrap();
            if self.current_mem_size.load(Ordering::SeqCst) >= self.memtable_threshold {
                let immutable = Arc::new(std::mem::take(&mut *active));
                self.current_mem_size.store(0, Ordering::SeqCst);

                let mut immutables = self.immutable_memtables.lock().unwrap();
                immutables.push(immutable.clone());

                let engine_clone = self.clone();
                tokio::spawn(async move {
                    if let Err(e) = engine_clone.flush_memtable(immutable).await {
                        eprintln!("Failed to flush memtable: {}", e);
                    }
                });
            }
        }
    }

    async fn flush_memtable(&self, memtable: Arc<MemTable>) -> Result<(), Status> {
        let sst_id = self.next_sst_id.fetch_add(1, Ordering::SeqCst);
        let sst_path = self.data_dir.join(format!("sst_{:05}.data", sst_id));

        println!("Flushing MemTable to {:?}...", sst_path);

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&sst_path)
            .map_err(|e| Status::internal(e.to_string()))?;

        file.write_all(MAGIC_BYTES)
            .map_err(|e| Status::internal(e.to_string()))?;
        file.write_all(&DATA_VERSION.to_le_bytes())
            .map_err(|e| Status::internal(e.to_string()))?;

        for (key, value_opt) in memtable.iter() {
            let key_bytes = key.as_bytes();
            let key_len = key_bytes.len() as u64;
            let (val_len, val_bytes) = match value_opt {
                Some(v) => (v.len() as u64, v.as_bytes()),
                None => (u64::MAX, &[] as &[u8]),
            };

            let timestamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs();
            file.write_all(&timestamp.to_le_bytes())
                .map_err(|e| Status::internal(e.to_string()))?;
            file.write_all(&key_len.to_le_bytes())
                .map_err(|e| Status::internal(e.to_string()))?;
            file.write_all(&val_len.to_le_bytes())
                .map_err(|e| Status::internal(e.to_string()))?;
            file.write_all(key_bytes)
                .map_err(|e| Status::internal(e.to_string()))?;
            if val_len != u64::MAX {
                file.write_all(val_bytes)
                    .map_err(|e| Status::internal(e.to_string()))?;
            }
        }
        file.flush().map_err(|e| Status::internal(e.to_string()))?;

        {
            let mut sstables = self.sstables.lock().unwrap();
            sstables.insert(0, sst_path);
        }

        {
            let mut immutables = self.immutable_memtables.lock().unwrap();
            immutables.retain(|m| !Arc::ptr_eq(m, &memtable));
        }

        println!("Flush finished.");
        Ok(())
    }

    #[allow(clippy::result_large_err)]
    fn search_in_sstable(&self, path: &Path, key: &str) -> Result<Option<String>, Status> {
        let mut file = File::open(path).map_err(|e| Status::internal(e.to_string()))?;
        file.seek(SeekFrom::Start(10))
            .map_err(|e| Status::internal(e.to_string()))?;

        loop {
            let mut ts_bytes = [0u8; 8];
            if file.read_exact(&mut ts_bytes).is_err() {
                break;
            }

            let mut klen_bytes = [0u8; 8];
            file.read_exact(&mut klen_bytes)
                .map_err(|e| Status::internal(e.to_string()))?;
            let mut vlen_bytes = [0u8; 8];
            file.read_exact(&mut vlen_bytes)
                .map_err(|e| Status::internal(e.to_string()))?;

            let key_len = u64::from_le_bytes(klen_bytes);
            let val_len = u64::from_le_bytes(vlen_bytes);

            let mut key_buf = vec![0u8; key_len as usize];
            file.read_exact(&mut key_buf)
                .map_err(|e| Status::internal(e.to_string()))?;
            let current_key = String::from_utf8_lossy(&key_buf);

            if current_key == key {
                if val_len == u64::MAX {
                    return Ok(None);
                }
                let mut val_buf = vec![0u8; val_len as usize];
                file.read_exact(&mut val_buf)
                    .map_err(|e| Status::internal(e.to_string()))?;
                return Ok(Some(String::from_utf8_lossy(&val_buf).to_string()));
            }

            if val_len != u64::MAX {
                file.seek(SeekFrom::Current(val_len as i64))
                    .map_err(|e| Status::internal(e.to_string()))?;
            }
        }
        Err(Status::not_found("Key not found in SSTable"))
    }
}

impl Engine for LsmTreeEngine {
    fn set(&self, key: String, value: String) -> Result<(), Status> {
        let new_val_opt = Some(value);
        let entry_size = Self::estimate_entry_size(&key, &new_val_opt);
        {
            let mut memtable = self.active_memtable.lock().unwrap();
            if let Some(old_val_opt) = memtable.get(&key) {
                let old_size = Self::estimate_entry_size(&key, old_val_opt);
                if entry_size > old_size {
                    self.current_mem_size.fetch_add(entry_size - old_size, Ordering::SeqCst);
                } else {
                    self.current_mem_size.fetch_sub(old_size - entry_size, Ordering::SeqCst);
                }
            } else {
                self.current_mem_size.fetch_add(entry_size, Ordering::SeqCst);
            }
            memtable.insert(key, new_val_opt);
        }
        self.check_flush();
        Ok(())
    }

    fn get(&self, key: String) -> Result<String, Status> {
        {
            let memtable = self.active_memtable.lock().unwrap();
            if let Some(val_opt) = memtable.get(&key) {
                return val_opt
                    .as_ref()
                    .cloned()
                    .ok_or_else(|| Status::not_found("Key deleted"));
            }
        }
        {
            let immutables = self.immutable_memtables.lock().unwrap();
            for mem in immutables.iter().rev() {
                if let Some(val_opt) = mem.get(&key) {
                    return val_opt
                        .as_ref()
                        .cloned()
                        .ok_or_else(|| Status::not_found("Key deleted"));
                }
            }
        }
        let sst_paths = {
            let sstables = self.sstables.lock().unwrap();
            sstables.clone()
        };
        for path in sst_paths {
            match self.search_in_sstable(&path, &key) {
                Ok(Some(v)) => return Ok(v),
                Ok(None) => return Err(Status::not_found("Key deleted")),
                Err(e) if e.code() == tonic::Code::NotFound => continue,
                Err(e) => return Err(e),
            }
        }
        Err(Status::not_found("Key not found"))
    }

    fn delete(&self, key: String) -> Result<(), Status> {
        let entry_size = Self::estimate_entry_size(&key, &None);
        {
            let mut memtable = self.active_memtable.lock().unwrap();
            if let Some(old_val_opt) = memtable.get(&key) {
                let old_size = Self::estimate_entry_size(&key, old_val_opt);
                if entry_size > old_size {
                    self.current_mem_size.fetch_add(entry_size - old_size, Ordering::SeqCst);
                } else {
                    self.current_mem_size.fetch_sub(old_size - entry_size, Ordering::SeqCst);
                }
            } else {
                self.current_mem_size.fetch_add(entry_size, Ordering::SeqCst);
            }
            memtable.insert(key, None);
        }
        self.check_flush();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_lsm_flush_and_get() {
        let dir = tempdir().unwrap();
        let data_dir = dir.path().to_str().unwrap().to_string();
        let engine = LsmTreeEngine::new(data_dir, 50);

        engine.set("k1".to_string(), "v1".to_string()).unwrap();
        engine.set("k2".to_string(), "v2".to_string()).unwrap();
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

        assert_eq!(engine.get("k1".to_string()).unwrap(), "v1");
        assert_eq!(engine.get("k2".to_string()).unwrap(), "v2");
    }

    #[tokio::test]
    async fn test_lsm_recovery() {
        let dir = tempdir().unwrap();
        let data_dir = dir.path().to_str().unwrap().to_string();

        {
            let engine = LsmTreeEngine::new(data_dir.clone(), 50);
            engine.set("k1".to_string(), "v1".to_string()).unwrap();
            engine.set("k2".to_string(), "v2".to_string()).unwrap();
            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
        }

        {
            let engine = LsmTreeEngine::new(data_dir.clone(), 50);
            assert_eq!(engine.get("k1".to_string()).unwrap(), "v1");
            assert_eq!(engine.get("k2".to_string()).unwrap(), "v2");

            engine.set("k3".to_string(), "v3".to_string()).unwrap();
            engine.set("k4".to_string(), "v4".to_string()).unwrap();
            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

            assert_eq!(engine.get("k1".to_string()).unwrap(), "v1");
            assert_eq!(engine.get("k3".to_string()).unwrap(), "v3");
        }
    }

    #[tokio::test]
    async fn test_lsm_overwrite_size_tracking() {
        let dir = tempdir().unwrap();
        let data_dir = dir.path().to_str().unwrap().to_string();
        // 閾値を100バイトに設定
        let engine = LsmTreeEngine::new(data_dir, 100);

        // 同じキーを何度も更新しても、メモリ上のサイズは増えないはず
        for _ in 0..100 {
            engine.set("key".to_string(), "value".to_string()).unwrap();
        }
        
        // 少し待ってもSSTableが大量に生成されていないことを確認
        // (バグがあればここで100個近いSSTableが生成される)
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
        
        let sst_count = engine.sstables.lock().unwrap().len();
        assert!(sst_count <= 1, "Should not flush multiple times for the same key. Count: {}", sst_count);
    }
}
