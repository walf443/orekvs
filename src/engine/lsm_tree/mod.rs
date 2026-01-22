mod memtable;
mod sstable;
mod wal;

use super::Engine;
use memtable::{MemTable, MemTableState};
use sstable::TimestampedEntry;
use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tonic::Status;
use wal::WalWriter;

#[derive(Debug)]
pub struct LsmTreeEngine {
    // MemTable management state
    mem_state: MemTableState,
    // List of SSTable file paths on disk (newest first)
    sstables: Arc<Mutex<Vec<PathBuf>>>,
    // Base directory for SSTables
    data_dir: PathBuf,
    // Counter for next SSTable file ID
    next_sst_id: Arc<AtomicU64>,
    // Trigger count for compaction (number of SSTables)
    compaction_trigger_file_count: usize,
    // Flag to prevent concurrent compaction
    compaction_in_progress: Arc<Mutex<bool>>,
    // WAL writer
    wal: WalWriter,
}

impl Clone for LsmTreeEngine {
    fn clone(&self) -> Self {
        LsmTreeEngine {
            mem_state: self.mem_state.clone(),
            sstables: Arc::clone(&self.sstables),
            data_dir: self.data_dir.clone(),
            next_sst_id: Arc::clone(&self.next_sst_id),
            compaction_trigger_file_count: self.compaction_trigger_file_count,
            compaction_in_progress: Arc::clone(&self.compaction_in_progress),
            wal: self.wal.clone(),
        }
    }
}

impl LsmTreeEngine {
    pub fn new(
        data_dir_str: String,
        memtable_capacity_bytes: u64,
        compaction_trigger_file_count: usize,
    ) -> Self {
        let data_dir = PathBuf::from(&data_dir_str);

        if !data_dir.exists() {
            fs::create_dir_all(&data_dir).expect("Failed to create data directory");
        }

        let mut sst_files = Vec::new();
        let mut max_sst_id = 0u64;
        let mut max_wal_id_in_sst = 0u64;
        let mut wal_files: Vec<(u64, PathBuf)> = Vec::new();
        let mut max_wal_id = 0u64;

        // Scan for SSTable and WAL files
        if let Ok(entries) = fs::read_dir(&data_dir) {
            for entry in entries.flatten() {
                let p = entry.path();
                if let Some(filename) = p.file_name().and_then(|n| n.to_str()) {
                    // SSTable files (old format: sst_{id}.data, new format: sst_{sst_id}_{wal_id}.data)
                    if let Some((sst_id, wal_id_opt)) = sstable::parse_filename(filename) {
                        if sst_id >= max_sst_id {
                            max_sst_id = sst_id + 1;
                        }
                        if let Some(wal_id) = wal_id_opt
                            && wal_id > max_wal_id_in_sst
                        {
                            max_wal_id_in_sst = wal_id;
                        }
                        sst_files.push(p);
                    } else if let Some(id) = WalWriter::parse_wal_filename(filename) {
                        // WAL files: wal_{id}.log
                        if id >= max_wal_id {
                            max_wal_id = id + 1;
                        }
                        wal_files.push((id, p));
                    }
                }
            }
        }

        sst_files.sort_by(|a, b| b.cmp(a));
        wal_files.sort_by_key(|(id, _)| *id);

        // Recover MemTable from WAL files that are newer than the latest SSTable
        let mut recovered_memtable: MemTable = BTreeMap::new();
        let mut recovered_size: u64 = 0;

        // Determine if we have any SSTables with WAL ID info
        let has_sstables_with_wal_id = sst_files
            .iter()
            .any(|p| sstable::extract_wal_id_from_sstable(p).is_some());

        for (wal_id, wal_path) in &wal_files {
            // Recover WAL files that are newer than the latest flushed WAL ID
            // If no SSTables have WAL ID info, recover all WAL files
            let should_recover = !has_sstables_with_wal_id || *wal_id > max_wal_id_in_sst;
            if should_recover {
                match WalWriter::read_entries(wal_path) {
                    Ok(entries) => {
                        for (key, value) in entries {
                            let entry_size = memtable::estimate_entry_size(&key, &value);
                            if let Some(old_value) = recovered_memtable.get(&key) {
                                let old_size = memtable::estimate_entry_size(&key, old_value);
                                if entry_size > old_size {
                                    recovered_size += entry_size - old_size;
                                } else {
                                    recovered_size -= old_size - entry_size;
                                }
                            } else {
                                recovered_size += entry_size;
                            }
                            recovered_memtable.insert(key, value);
                        }
                        println!(
                            "Recovered {} entries from WAL: {:?}",
                            recovered_memtable.len(),
                            wal_path
                        );
                    }
                    Err(e) => {
                        eprintln!("Warning: Failed to read WAL {:?}: {}", wal_path, e);
                    }
                }
            }
        }

        // Determine next WAL ID
        let next_wal_id = if max_wal_id > 0 { max_wal_id } else { 0 };

        // Create new WAL file
        let wal = WalWriter::new(&data_dir, next_wal_id).expect("Failed to create initial WAL");

        // Re-write recovered entries to new WAL for safety
        if !recovered_memtable.is_empty() {
            wal.write_entries(&recovered_memtable)
                .expect("Failed to write recovered entries to WAL");
        }

        let mem_state =
            MemTableState::new(memtable_capacity_bytes, recovered_memtable, recovered_size);

        LsmTreeEngine {
            mem_state,
            sstables: Arc::new(Mutex::new(sst_files)),
            data_dir,
            next_sst_id: Arc::new(AtomicU64::new(max_sst_id)),
            compaction_trigger_file_count,
            compaction_in_progress: Arc::new(Mutex::new(false)),
            wal,
        }
    }

    // Check if MemTable needs to be flushed and trigger if necessary
    fn trigger_flush_if_needed(&self) {
        if let Some(immutable) = self.mem_state.needs_flush() {
            let engine_clone = self.clone();
            tokio::spawn(async move {
                if let Err(e) = engine_clone.flush_memtable(immutable).await {
                    eprintln!("Failed to flush memtable: {}", e);
                }
            });
        }
    }

    async fn flush_memtable(&self, memtable: Arc<MemTable>) -> Result<(), Status> {
        let sst_id = self.next_sst_id.fetch_add(1, Ordering::SeqCst);
        // Get current WAL ID to include in SSTable filename
        let wal_id = self.wal.current_id();
        let sst_path = sstable::generate_path(&self.data_dir, sst_id, wal_id);

        println!("Flushing MemTable to {:?}...", sst_path);

        sstable::create_from_memtable(&sst_path, &memtable)?;

        {
            let mut sstables = self.sstables.lock().unwrap();
            sstables.insert(0, sst_path);
        }

        self.mem_state.remove_immutable(&memtable);

        // Rotate WAL: create a new WAL file (old WAL preserved for replication)
        if let Err(e) = self.wal.rotate() {
            eprintln!("Warning: Failed to rotate WAL: {}", e);
        }

        println!("Flush finished.");

        // Check if compaction is needed
        self.check_compaction();

        Ok(())
    }

    /// Check if compaction should be triggered and run it if needed
    fn check_compaction(&self) {
        let sst_count = {
            let sstables = self.sstables.lock().unwrap();
            sstables.len()
        };

        if sst_count >= self.compaction_trigger_file_count {
            let mut in_progress = self.compaction_in_progress.lock().unwrap();
            if !*in_progress {
                *in_progress = true;
                drop(in_progress);

                let engine_clone = self.clone();
                tokio::spawn(async move {
                    if let Err(e) = engine_clone.run_compaction().await {
                        eprintln!("Compaction failed: {}", e);
                    }
                    *engine_clone.compaction_in_progress.lock().unwrap() = false;
                });
            }
        }
    }

    /// Run compaction: merge all SSTables into a single new SSTable and delete old ones
    async fn run_compaction(&self) -> Result<(), Status> {
        println!("Starting compaction...");

        // Get current SSTables to compact
        let sst_paths: Vec<PathBuf> = {
            let sstables = self.sstables.lock().unwrap();
            sstables.clone()
        };

        if sst_paths.len() < 2 {
            println!("Not enough SSTables to compact");
            return Ok(());
        }

        // Merge all SSTables, keeping only the latest value for each key
        let mut merged: BTreeMap<String, TimestampedEntry> = BTreeMap::new();

        for path in &sst_paths {
            let entries = sstable::read_entries(path)?;
            for (key, (timestamp, value)) in entries {
                match merged.get(&key) {
                    Some((existing_ts, _)) if *existing_ts >= timestamp => {
                        // Keep existing entry (newer timestamp)
                    }
                    _ => {
                        merged.insert(key, (timestamp, value));
                    }
                }
            }
        }

        // Remove tombstones (deleted entries) during compaction
        merged.retain(|_, (_, value)| value.is_some());

        // Write merged data to a new SSTable
        let new_sst_id = self.next_sst_id.fetch_add(1, Ordering::SeqCst);
        // Use the max WAL ID from compacted SSTables
        let max_wal_id = sst_paths
            .iter()
            .filter_map(|p| sstable::extract_wal_id_from_sstable(p))
            .max()
            .unwrap_or(0);
        let new_sst_path = sstable::generate_path(&self.data_dir, new_sst_id, max_wal_id);

        println!("Writing compacted SSTable to {:?}...", new_sst_path);

        sstable::write_timestamped_entries(&new_sst_path, &merged)?;

        // Update sstables list: remove compacted SSTables and add new one
        // Keep any SSTables that were added during compaction (by flush)
        let compacted_set: std::collections::HashSet<_> = sst_paths.iter().collect();
        {
            let mut sstables = self.sstables.lock().unwrap();
            // Keep SSTables that were not part of this compaction
            let mut new_list: Vec<PathBuf> = sstables
                .iter()
                .filter(|p| !compacted_set.contains(p))
                .cloned()
                .collect();
            // Add the new compacted SSTable
            new_list.push(new_sst_path);
            // Sort by path (descending) to maintain newest-first order
            new_list.sort_by(|a, b| b.cmp(a));
            *sstables = new_list;
        }

        // Delete old SSTable files after updating the list
        for path in &sst_paths {
            if let Err(e) = fs::remove_file(path) {
                eprintln!("Failed to delete old SSTable {:?}: {}", path, e);
            } else {
                println!("Deleted old SSTable: {:?}", path);
            }
        }

        println!(
            "Compaction finished. Merged {} SSTables into 1.",
            sst_paths.len()
        );
        Ok(())
    }
}

impl Engine for LsmTreeEngine {
    fn set(&self, key: String, value: String) -> Result<(), Status> {
        let new_val_opt = Some(value);

        // 1. Write to WAL FIRST (before MemTable) for durability
        self.wal.append(&key, &new_val_opt)?;

        // 2. Then update MemTable
        self.mem_state.insert(key, new_val_opt);
        self.trigger_flush_if_needed();
        Ok(())
    }

    fn get(&self, key: String) -> Result<String, Status> {
        if let Some(val_opt) = self.mem_state.get(&key) {
            return val_opt
                .as_ref()
                .cloned()
                .ok_or_else(|| Status::not_found("Key deleted"));
        }

        let sst_paths = {
            let sstables = self.sstables.lock().unwrap();
            sstables.clone()
        };
        for path in sst_paths {
            match sstable::search_key(&path, &key) {
                Ok(Some(v)) => return Ok(v),
                Ok(None) => return Err(Status::not_found("Key deleted")),
                Err(e) if e.code() == tonic::Code::NotFound => continue,
                Err(e) => return Err(e),
            }
        }
        Err(Status::not_found("Key not found"))
    }

    fn delete(&self, key: String) -> Result<(), Status> {
        // 1. Write tombstone to WAL FIRST
        self.wal.append(&key, &None)?;

        // 2. Then update MemTable
        self.mem_state.insert(key, None);
        self.trigger_flush_if_needed();
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
        let engine = LsmTreeEngine::new(data_dir, 50, 4);

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
            let engine = LsmTreeEngine::new(data_dir.clone(), 50, 4);
            engine.set("k1".to_string(), "v1".to_string()).unwrap();
            engine.set("k2".to_string(), "v2".to_string()).unwrap();
            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
        }

        {
            let engine = LsmTreeEngine::new(data_dir.clone(), 50, 4);
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
        // Set capacity to 100 bytes
        let engine = LsmTreeEngine::new(data_dir, 100, 4);

        // 同じキーを何度も更新しても、メモリ上のサイズは増えないはず
        for _ in 0..100 {
            engine.set("key".to_string(), "value".to_string()).unwrap();
        }

        // 少し待ってもSSTableが大量に生成されていないことを確認
        // (バグがあればここで100個近いSSTableが生成される)
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

        let sst_count = engine.sstables.lock().unwrap().len();
        assert!(
            sst_count <= 1,
            "Should not flush multiple times for the same key. Count: {}",
            sst_count
        );
    }

    #[tokio::test]
    async fn test_compaction_merges_sstables() {
        let dir = tempdir().unwrap();
        let data_dir = dir.path().to_str().unwrap().to_string();
        // Low capacity to trigger multiple flushes
        let engine = LsmTreeEngine::new(data_dir.clone(), 30, 2);

        // Write data in batches to create multiple SSTables
        for i in 0..10 {
            engine
                .set(format!("key{}", i), format!("value{}", i))
                .unwrap();
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        }

        // Wait for flushes and potential compaction
        tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;

        // Verify data is still accessible after compaction
        for i in 0..10 {
            let result = engine.get(format!("key{}", i));
            assert!(result.is_ok(), "key{} should be accessible", i);
            assert_eq!(result.unwrap(), format!("value{}", i));
        }
    }

    #[tokio::test]
    async fn test_compaction_removes_deleted_keys() {
        let dir = tempdir().unwrap();
        let data_dir = dir.path().to_str().unwrap().to_string();
        let engine = LsmTreeEngine::new(data_dir.clone(), 30, 2);

        // Write some data
        engine
            .set("keep".to_string(), "keep_value".to_string())
            .unwrap();
        engine
            .set("delete_me".to_string(), "will_be_deleted".to_string())
            .unwrap();
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

        // Delete one key
        engine.delete("delete_me".to_string()).unwrap();
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

        // Add more data to trigger compaction
        for i in 0..5 {
            engine
                .set(format!("extra{}", i), format!("extra_value{}", i))
                .unwrap();
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        }

        // Wait for compaction
        tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;

        // Verify kept key is accessible
        assert_eq!(engine.get("keep".to_string()).unwrap(), "keep_value");

        // Verify deleted key is not found
        let result = engine.get("delete_me".to_string());
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_compaction_deletes_old_files() {
        let dir = tempdir().unwrap();
        let data_dir = dir.path().to_str().unwrap().to_string();
        let engine = LsmTreeEngine::new(data_dir.clone(), 30, 2);

        // Create multiple SSTables
        for i in 0..8 {
            engine.set(format!("k{}", i), format!("v{}", i)).unwrap();
            tokio::time::sleep(tokio::time::Duration::from_millis(150)).await;
        }

        // Wait for compaction to complete
        tokio::time::sleep(tokio::time::Duration::from_millis(1500)).await;

        // Count SSTable files in directory
        let sst_files: Vec<_> = fs::read_dir(&data_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.path()
                    .file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|name| name.starts_with("sst_") && name.ends_with(".data"))
            })
            .collect();

        // After compaction, should have fewer SSTables
        assert!(
            sst_files.len() <= 2,
            "Expected at most 2 SSTables after compaction, got {}",
            sst_files.len()
        );

        // Data should still be accessible
        for i in 0..8 {
            assert_eq!(engine.get(format!("k{}", i)).unwrap(), format!("v{}", i));
        }
    }

    #[tokio::test]
    async fn test_wal_recovery_basic() {
        let dir = tempdir().unwrap();
        let data_dir = dir.path().to_str().unwrap().to_string();

        // Write some data without flushing (high capacity)
        {
            let engine = LsmTreeEngine::new(data_dir.clone(), 1024 * 1024, 4);
            engine.set("k1".to_string(), "v1".to_string()).unwrap();
            engine.set("k2".to_string(), "v2".to_string()).unwrap();
            // Engine dropped without flush - data should be in WAL
        }

        // Recover and verify data
        {
            let engine = LsmTreeEngine::new(data_dir.clone(), 1024 * 1024, 4);
            assert_eq!(engine.get("k1".to_string()).unwrap(), "v1");
            assert_eq!(engine.get("k2".to_string()).unwrap(), "v2");
        }
    }

    #[tokio::test]
    async fn test_wal_recovery_with_delete() {
        let dir = tempdir().unwrap();
        let data_dir = dir.path().to_str().unwrap().to_string();

        // Write and delete data without flushing
        {
            let engine = LsmTreeEngine::new(data_dir.clone(), 1024 * 1024, 4);
            engine.set("k1".to_string(), "v1".to_string()).unwrap();
            engine.set("k2".to_string(), "v2".to_string()).unwrap();
            engine.delete("k1".to_string()).unwrap();
        }

        // Recover and verify
        {
            let engine = LsmTreeEngine::new(data_dir.clone(), 1024 * 1024, 4);
            // k1 should be deleted (tombstone recovered)
            assert!(engine.get("k1".to_string()).is_err());
            // k2 should still exist
            assert_eq!(engine.get("k2".to_string()).unwrap(), "v2");
        }
    }

    #[tokio::test]
    async fn test_wal_preserved_after_flush() {
        let dir = tempdir().unwrap();
        let data_dir = dir.path().to_str().unwrap().to_string();

        // Write data and trigger flush (low capacity)
        {
            let engine = LsmTreeEngine::new(data_dir.clone(), 30, 4);
            engine.set("k1".to_string(), "v1".to_string()).unwrap();
            engine.set("k2".to_string(), "v2".to_string()).unwrap();
            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
        }

        // Check that WAL files are preserved (for replication)
        let wal_count = fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.path()
                    .file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|name| name.starts_with("wal_") && name.ends_with(".log"))
            })
            .count();

        // Should have at least 1 WAL file (current one)
        assert!(
            wal_count >= 1,
            "Expected at least 1 WAL file, got {}",
            wal_count
        );

        // Data should still be accessible
        {
            let engine = LsmTreeEngine::new(data_dir.clone(), 1024 * 1024, 4);
            assert_eq!(engine.get("k1".to_string()).unwrap(), "v1");
            assert_eq!(engine.get("k2".to_string()).unwrap(), "v2");
        }
    }

    #[tokio::test]
    async fn test_lsm_versioning_across_sstables() {
        let dir = tempdir().unwrap();
        let data_dir = dir.path().to_str().unwrap().to_string();
        // Low capacity to force flush every write
        let engine = LsmTreeEngine::new(data_dir.clone(), 10, 100);

        // v1 in SSTable 1
        engine.set("key".to_string(), "v1".to_string()).unwrap();
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

        // v2 in SSTable 2
        engine.set("key".to_string(), "v2".to_string()).unwrap();
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

        // v3 in MemTable
        engine.set("key".to_string(), "v3".to_string()).unwrap();

        assert_eq!(engine.get("key".to_string()).unwrap(), "v3");

        // Force flush v3 to SSTable 3
        engine.trigger_flush_if_needed(); // This won't work easily because of capacity check, but another set will
        engine.set("other".to_string(), "large_value_to_force_flush".to_string()).unwrap();
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

        assert_eq!(engine.get("key".to_string()).unwrap(), "v3");
    }

    #[tokio::test]
    async fn test_lsm_recovery_merges_wal_and_sst() {
        let dir = tempdir().unwrap();
        let data_dir = dir.path().to_str().unwrap().to_string();

        {
            let engine = LsmTreeEngine::new(data_dir.clone(), 30, 100);
            // k1 goes to SSTable
            engine.set("k1".to_string(), "v1".to_string()).unwrap();
            tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
            
            // k2 stays in WAL/MemTable
            engine.set("k2".to_string(), "v2".to_string()).unwrap();
            
            // k1 updated in WAL/MemTable
            engine.set("k1".to_string(), "v1_new".to_string()).unwrap();
        }

        {
            let engine = LsmTreeEngine::new(data_dir.clone(), 1024, 100);
            assert_eq!(engine.get("k1".to_string()).unwrap(), "v1_new");
            assert_eq!(engine.get("k2".to_string()).unwrap(), "v2");
        }
    }

    #[tokio::test]
    async fn test_lsm_delete_after_flush() {
        let dir = tempdir().unwrap();
        let data_dir = dir.path().to_str().unwrap().to_string();
        let engine = LsmTreeEngine::new(data_dir.clone(), 30, 100);

        // Flush k1 to SSTable
        engine.set("k1".to_string(), "v1".to_string()).unwrap();
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
        
        assert_eq!(engine.get("k1".to_string()).unwrap(), "v1");

        // Delete k1 (tombstone in MemTable)
        engine.delete("k1".to_string()).unwrap();
        assert!(engine.get("k1".to_string()).is_err());

        // Flush tombstone
        engine.set("k2".to_string(), "trigger_flush".repeat(10)).unwrap();
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

        // Still should not find k1 (SSTable has v1, but newer SSTable has tombstone)
        assert!(engine.get("k1".to_string()).is_err());
    }
}
