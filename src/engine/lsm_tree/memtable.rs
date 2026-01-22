use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

// Type alias for MemTable entries
pub type MemTable = BTreeMap<String, Option<String>>;

/// Estimate the size of a MemTable entry in bytes
pub fn estimate_entry_size(key: &str, value: &Option<String>) -> u64 {
    let vlen = value.as_ref().map_or(0, |v| v.len() as u64);
    8 + 8 + 8 + key.len() as u64 + vlen
}

#[derive(Debug)]
pub struct MemTableState {
    // Current in-memory write buffer
    pub active_memtable: Arc<Mutex<MemTable>>,
    // MemTables currently being flushed to disk
    pub immutable_memtables: Arc<Mutex<Vec<Arc<MemTable>>>>,
    // Max size for MemTable in bytes
    pub capacity_bytes: u64,
    // Approximate size of current active MemTable
    pub current_size: Arc<AtomicU64>,
}

impl Clone for MemTableState {
    fn clone(&self) -> Self {
        MemTableState {
            active_memtable: Arc::clone(&self.active_memtable),
            immutable_memtables: Arc::clone(&self.immutable_memtables),
            capacity_bytes: self.capacity_bytes,
            current_size: Arc::clone(&self.current_size),
        }
    }
}

impl MemTableState {
    pub fn new(capacity_bytes: u64, recovered_memtable: MemTable, recovered_size: u64) -> Self {
        MemTableState {
            active_memtable: Arc::new(Mutex::new(recovered_memtable)),
            immutable_memtables: Arc::new(Mutex::new(Vec::new())),
            capacity_bytes,
            current_size: Arc::new(AtomicU64::new(recovered_size)),
        }
    }

    pub fn insert(&self, key: String, value: Option<String>) {
        let entry_size = estimate_entry_size(&key, &value);
        let mut memtable = self.active_memtable.lock().unwrap();

        if let Some(old_val_opt) = memtable.get(&key) {
            let old_size = estimate_entry_size(&key, old_val_opt);
            if entry_size > old_size {
                self.current_size
                    .fetch_add(entry_size - old_size, Ordering::SeqCst);
            } else {
                self.current_size
                    .fetch_sub(old_size - entry_size, Ordering::SeqCst);
            }
        } else {
            self.current_size.fetch_add(entry_size, Ordering::SeqCst);
        }
        memtable.insert(key, value);
    }

    pub fn needs_flush(&self) -> Option<Arc<MemTable>> {
        let size = self.current_size.load(Ordering::SeqCst);
        if size >= self.capacity_bytes {
            let mut active = self.active_memtable.lock().unwrap();
            // Double check inside lock
            if self.current_size.load(Ordering::SeqCst) >= self.capacity_bytes {
                let immutable = Arc::new(std::mem::take(&mut *active));
                self.current_size.store(0, Ordering::SeqCst);

                let mut immutables = self.immutable_memtables.lock().unwrap();
                immutables.push(immutable.clone());

                return Some(immutable);
            }
        }
        None
    }

    pub fn remove_immutable(&self, memtable: &Arc<MemTable>) {
        let mut immutables = self.immutable_memtables.lock().unwrap();
        immutables.retain(|m| !Arc::ptr_eq(m, memtable));
    }

    pub fn get(&self, key: &str) -> Option<Option<String>> {
        // Check active memtable
        {
            let memtable = self.active_memtable.lock().unwrap();
            if let Some(val_opt) = memtable.get(key) {
                return Some(val_opt.clone());
            }
        }

        // Check immutable memtables
        {
            let immutables = self.immutable_memtables.lock().unwrap();
            for mem in immutables.iter().rev() {
                if let Some(val_opt) = mem.get(key) {
                    return Some(val_opt.clone());
                }
            }
        }

        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_estimate_entry_size() {
        let key = "key1";
        let value = Some("value1".to_string());
        let size = estimate_entry_size(key, &value);
        // 8 (size_of overhead) + 8 (key len) + 8 (val len) + 4 (key) + 6 (val) = 34
        assert_eq!(size, 8 + 8 + 8 + 4 + 6);

        let value_none = None;
        let size_none = estimate_entry_size(key, &value_none);
        // 8 + 8 + 8 + 4 + 0 = 28
        assert_eq!(size_none, 8 + 8 + 8 + 4 + 0);
    }

    #[test]
    fn test_memtable_basic_ops() {
        let state = MemTableState::new(1000, BTreeMap::new(), 0);

        state.insert("k1".to_string(), Some("v1".to_string()));
        state.insert("k2".to_string(), Some("v2".to_string()));

        assert_eq!(state.get("k1"), Some(Some("v1".to_string())));
        assert_eq!(state.get("k2"), Some(Some("v2".to_string())));
        assert_eq!(state.get("k3"), None);

        // Delete
        state.insert("k1".to_string(), None);
        assert_eq!(state.get("k1"), Some(None));
    }

    #[test]
    fn test_memtable_size_tracking() {
        let state = MemTableState::new(1000, BTreeMap::new(), 0);

        let k1 = "k1".to_string();
        let v1 = "v1".to_string();
        let size1 = estimate_entry_size(&k1, &Some(v1.clone()));

        state.insert(k1.clone(), Some(v1.clone()));
        assert_eq!(state.current_size.load(Ordering::SeqCst), size1);

        // Update with larger value
        let v1_large = "v1_large".to_string();
        let size1_large = estimate_entry_size(&k1, &Some(v1_large.clone()));
        state.insert(k1.clone(), Some(v1_large));
        assert_eq!(state.current_size.load(Ordering::SeqCst), size1_large);

        // Update with smaller value
        let v1_small = "s".to_string();
        let size1_small = estimate_entry_size(&k1, &Some(v1_small.clone()));
        state.insert(k1, Some(v1_small));
        assert_eq!(state.current_size.load(Ordering::SeqCst), size1_small);

        // Delete
        let k1 = "k1".to_string();
        let size_delete = estimate_entry_size(&k1, &None);
        state.insert(k1, None);
        assert_eq!(state.current_size.load(Ordering::SeqCst), size_delete);
    }

    #[test]
    fn test_memtable_flush_trigger() {
        let capacity = 50;
        let state = MemTableState::new(capacity, BTreeMap::new(), 0);

        // This should not trigger flush
        state.insert("k1".to_string(), Some("v1".to_string()));
        assert!(state.needs_flush().is_none());

        // This should trigger flush
        state.insert(
            "k2".to_string(),
            Some("very_long_value_to_trigger_flush".to_string()),
        );
        let immutable = state.needs_flush();
        assert!(immutable.is_some());

        // Active memtable should be empty now
        assert!(state.active_memtable.lock().unwrap().is_empty());
        assert_eq!(state.current_size.load(Ordering::SeqCst), 0);

        // Immutable memtables should have one entry
        assert_eq!(state.immutable_memtables.lock().unwrap().len(), 1);

        // Retrieval should still work from immutable
        assert_eq!(state.get("k1"), Some(Some("v1".to_string())));
    }

    #[test]
    fn test_memtable_lookup_order() {
        let state = MemTableState::new(1000, BTreeMap::new(), 0);

        // 1. Insert k1=v1
        state.insert("k1".to_string(), Some("v1".to_string()));

        // 2. Force flush to immutable
        {
            let mut active = state.active_memtable.lock().unwrap();
            let immutable1 = Arc::new(std::mem::replace(&mut *active, BTreeMap::new()));
            state.current_size.store(0, Ordering::SeqCst);
            state.immutable_memtables.lock().unwrap().push(immutable1);
        }

        // 3. Insert k1=v2 in active
        state.insert("k1".to_string(), Some("v2".to_string()));

        // Should get v2 from active
        assert_eq!(state.get("k1"), Some(Some("v2".to_string())));

        // 4. Force another flush
        {
            let mut active = state.active_memtable.lock().unwrap();
            let immutable2 = Arc::new(std::mem::replace(&mut *active, BTreeMap::new()));
            state.current_size.store(0, Ordering::SeqCst);
            state.immutable_memtables.lock().unwrap().push(immutable2);
        }

        // 5. Insert k1=v3 in active
        state.insert("k1".to_string(), Some("v3".to_string()));

        // Should get v3 from active
        assert_eq!(state.get("k1"), Some(Some("v3".to_string())));

        // 6. Clear active, should get v2 from newest immutable (immutable2)
        state.active_memtable.lock().unwrap().clear();
        assert_eq!(state.get("k1"), Some(Some("v2".to_string())));
    }

    #[test]
    fn test_remove_immutable() {
        let state = MemTableState::new(1000, BTreeMap::new(), 0);
        state.insert("k1".to_string(), Some("v1".to_string()));

        let imm = state.needs_flush_manual();
        assert_eq!(state.immutable_memtables.lock().unwrap().len(), 1);

        state.remove_immutable(&imm);
        assert_eq!(state.immutable_memtables.lock().unwrap().len(), 0);
    }
}

impl MemTableState {
    // Helper for testing to force a flush regardless of size
    #[cfg(test)]
    fn needs_flush_manual(&self) -> Arc<MemTable> {
        let mut active = self.active_memtable.lock().unwrap();
        let immutable = Arc::new(std::mem::take(&mut *active));
        self.current_size.store(0, Ordering::SeqCst);
        let mut immutables = self.immutable_memtables.lock().unwrap();
        immutables.push(immutable.clone());
        immutable
    }
}
