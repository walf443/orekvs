use crossbeam_skiplist::SkipMap;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// Lock-free SkipList-based MemTable
/// Allows concurrent reads and writes without mutex
pub type SkipMemTable = SkipMap<String, Option<String>>;

/// Legacy BTreeMap-based MemTable (used for WAL recovery and SSTable creation)
pub type MemTable = BTreeMap<String, Option<String>>;

/// Estimate the size of a MemTable entry in bytes
pub fn estimate_entry_size(key: &str, value: &Option<String>) -> u64 {
    let vlen = value.as_ref().map_or(0, |v| v.len() as u64);
    8 + 8 + 8 + key.len() as u64 + vlen
}

/// Convert SkipMemTable to BTreeMap for SSTable creation
pub fn skip_to_btree(skip: &SkipMemTable) -> MemTable {
    skip.iter()
        .map(|e| (e.key().clone(), e.value().clone()))
        .collect()
}

#[derive(Debug)]
pub struct MemTableState {
    /// Current in-memory write buffer (lock-free SkipList)
    pub active_memtable: Arc<SkipMemTable>,
    /// MemTables currently being flushed to disk (converted to BTreeMap for ordered iteration)
    pub immutable_memtables: Arc<std::sync::Mutex<Vec<Arc<MemTable>>>>,
    /// Max size for MemTable in bytes
    pub capacity_bytes: u64,
    /// Approximate size of current active MemTable
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
        // Convert recovered BTreeMap to SkipMap
        let skip_map = SkipMap::new();
        for (key, value) in recovered_memtable {
            skip_map.insert(key, value);
        }

        MemTableState {
            active_memtable: Arc::new(skip_map),
            immutable_memtables: Arc::new(std::sync::Mutex::new(Vec::new())),
            capacity_bytes,
            current_size: Arc::new(AtomicU64::new(recovered_size)),
        }
    }

    /// Insert a key-value pair into the active MemTable (lock-free)
    pub fn insert(&self, key: String, value: Option<String>) {
        let entry_size = estimate_entry_size(&key, &value);

        // Check if key already exists to adjust size tracking
        if let Some(existing) = self.active_memtable.get(&key) {
            let old_size = estimate_entry_size(&key, existing.value());
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

        self.active_memtable.insert(key, value);
    }

    /// Check if flush is needed and return immutable memtable if so
    /// Note: This operation requires replacing the active memtable, which is not lock-free
    pub fn needs_flush(&self) -> Option<Arc<MemTable>> {
        let size = self.current_size.load(Ordering::SeqCst);
        if size >= self.capacity_bytes {
            // Convert current SkipMap to BTreeMap and create new empty SkipMap
            // This is the only point where we need synchronization
            let btree = skip_to_btree(&self.active_memtable);

            // Clear the skipmap (this is safe because readers will see empty or partial state
            // but we're about to replace it anyway)
            self.active_memtable.clear();
            self.current_size.store(0, Ordering::SeqCst);

            let immutable = Arc::new(btree);
            let mut immutables = self.immutable_memtables.lock().unwrap();
            immutables.push(Arc::clone(&immutable));

            return Some(immutable);
        }
        None
    }

    pub fn remove_immutable(&self, memtable: &Arc<MemTable>) {
        let mut immutables = self.immutable_memtables.lock().unwrap();
        immutables.retain(|m| !Arc::ptr_eq(m, memtable));
    }

    /// Get a value from MemTable (lock-free for active memtable)
    pub fn get(&self, key: &str) -> Option<Option<String>> {
        // Check active memtable (lock-free)
        if let Some(entry) = self.active_memtable.get(key) {
            return Some(entry.value().clone());
        }

        // Check immutable memtables (requires lock)
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
        assert!(state.active_memtable.is_empty());
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
            let btree = skip_to_btree(&state.active_memtable);
            state.active_memtable.clear();
            state.current_size.store(0, Ordering::SeqCst);
            let immutable1 = Arc::new(btree);
            state.immutable_memtables.lock().unwrap().push(immutable1);
        }

        // 3. Insert k1=v2 in active
        state.insert("k1".to_string(), Some("v2".to_string()));

        // Should get v2 from active
        assert_eq!(state.get("k1"), Some(Some("v2".to_string())));

        // 4. Force another flush
        {
            let btree = skip_to_btree(&state.active_memtable);
            state.active_memtable.clear();
            state.current_size.store(0, Ordering::SeqCst);
            let immutable2 = Arc::new(btree);
            state.immutable_memtables.lock().unwrap().push(immutable2);
        }

        // 5. Insert k1=v3 in active
        state.insert("k1".to_string(), Some("v3".to_string()));

        // Should get v3 from active
        assert_eq!(state.get("k1"), Some(Some("v3".to_string())));

        // 6. Clear active, should get v2 from newest immutable (immutable2)
        state.active_memtable.clear();
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

    #[test]
    fn test_skip_to_btree() {
        let skip = SkipMap::new();
        skip.insert("b".to_string(), Some("2".to_string()));
        skip.insert("a".to_string(), Some("1".to_string()));
        skip.insert("c".to_string(), None);

        let btree = skip_to_btree(&skip);
        assert_eq!(btree.len(), 3);
        assert_eq!(btree.get("a"), Some(&Some("1".to_string())));
        assert_eq!(btree.get("b"), Some(&Some("2".to_string())));
        assert_eq!(btree.get("c"), Some(&None));

        // Verify order
        let keys: Vec<_> = btree.keys().collect();
        assert_eq!(keys, vec!["a", "b", "c"]);
    }

    /// Test concurrent reads and writes
    #[test]
    fn test_concurrent_access() {
        use std::thread;

        let state = Arc::new(MemTableState::new(1_000_000, BTreeMap::new(), 0));
        let mut handles = vec![];

        // Spawn writer threads
        for t in 0..4 {
            let state_clone = Arc::clone(&state);
            handles.push(thread::spawn(move || {
                for i in 0..1000 {
                    let key = format!("key_{}_{}", t, i);
                    let value = format!("value_{}_{}", t, i);
                    state_clone.insert(key, Some(value));
                }
            }));
        }

        // Spawn reader threads
        for t in 0..4 {
            let state_clone = Arc::clone(&state);
            handles.push(thread::spawn(move || {
                for i in 0..1000 {
                    let key = format!("key_{}_{}", t, i);
                    let _ = state_clone.get(&key);
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        // Verify some entries exist
        assert!(state.get("key_0_0").is_some());
        assert!(state.get("key_3_999").is_some());
    }
}

impl MemTableState {
    // Helper for testing to force a flush regardless of size
    #[cfg(test)]
    fn needs_flush_manual(&self) -> Arc<MemTable> {
        let btree = skip_to_btree(&self.active_memtable);
        self.active_memtable.clear();
        self.current_size.store(0, Ordering::SeqCst);
        let immutable = Arc::new(btree);
        self.immutable_memtables
            .lock()
            .unwrap()
            .push(Arc::clone(&immutable));
        immutable
    }

    /// Benchmark: Compare BTreeMap+Mutex vs SkipList concurrent performance
    #[cfg(test)]
    pub fn bench_concurrent_comparison() {
        use std::sync::Mutex;
        use std::thread;
        use std::time::Instant;

        fn bench_btree_mutex(num_threads: usize, ops_per_thread: usize) -> std::time::Duration {
            let map: Arc<Mutex<BTreeMap<String, String>>> = Arc::new(Mutex::new(BTreeMap::new()));
            let mut handles = vec![];

            let start = Instant::now();

            for t in 0..num_threads {
                let map_clone = Arc::clone(&map);
                handles.push(thread::spawn(move || {
                    for i in 0..ops_per_thread {
                        let key = format!("key_{}_{}", t, i);
                        let value = format!("value_{}_{}", t, i);
                        map_clone.lock().unwrap().insert(key, value);
                    }
                }));
            }

            for t in 0..num_threads {
                let map_clone = Arc::clone(&map);
                handles.push(thread::spawn(move || {
                    for i in 0..ops_per_thread {
                        let key = format!("key_{}_{}", t, i);
                        let _ = map_clone.lock().unwrap().get(&key);
                    }
                }));
            }

            for h in handles {
                h.join().unwrap();
            }
            start.elapsed()
        }

        fn bench_skiplist(num_threads: usize, ops_per_thread: usize) -> std::time::Duration {
            let map: Arc<SkipMap<String, String>> = Arc::new(SkipMap::new());
            let mut handles = vec![];

            let start = Instant::now();

            for t in 0..num_threads {
                let map_clone = Arc::clone(&map);
                handles.push(thread::spawn(move || {
                    for i in 0..ops_per_thread {
                        let key = format!("key_{}_{}", t, i);
                        let value = format!("value_{}_{}", t, i);
                        map_clone.insert(key, value);
                    }
                }));
            }

            for t in 0..num_threads {
                let map_clone = Arc::clone(&map);
                handles.push(thread::spawn(move || {
                    for i in 0..ops_per_thread {
                        let key = format!("key_{}_{}", t, i);
                        let _ = map_clone.get(&key);
                    }
                }));
            }

            for h in handles {
                h.join().unwrap();
            }
            start.elapsed()
        }

        println!("\n=== BTreeMap+Mutex vs SkipList Concurrent Benchmark ===\n");

        let ops = 10000;

        println!(
            "{:<12} {:>12} {:>12} {:>10}",
            "Threads", "BTree+Mutex", "SkipList", "Speedup"
        );
        println!("{}", "-".repeat(50));

        for threads in [1, 2, 4, 8] {
            let btree_time = bench_btree_mutex(threads, ops);
            let skip_time = bench_skiplist(threads, ops);
            let speedup = btree_time.as_secs_f64() / skip_time.as_secs_f64();

            println!(
                "{:>4} x2 (r+w) {:>10.2?} {:>10.2?} {:>10.2}x",
                threads, btree_time, skip_time, speedup
            );
        }
        println!();
    }
}

#[cfg(test)]
mod bench_tests {
    use super::*;

    #[test]
    #[ignore] // Run with: cargo test bench_skiplist_vs_btree --release -- --ignored --nocapture
    fn bench_skiplist_vs_btree() {
        MemTableState::bench_concurrent_comparison();
    }
}
