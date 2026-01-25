use arc_swap::ArcSwap;
use crossbeam_skiplist::SkipMap;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, RwLock};

/// Lock-free SkipList-based MemTable
/// Allows concurrent reads and writes without mutex
pub type SkipMemTable = SkipMap<String, Option<String>>;

/// Legacy BTreeMap-based MemTable (used for WAL recovery and SSTable creation)
pub type MemTable = BTreeMap<String, Option<String>>;

/// Default maximum number of immutable memtables before write stall
pub const DEFAULT_MAX_IMMUTABLE_MEMTABLES: usize = 4;

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

/// Write stall state for coordinating backpressure
struct WriteStallState {
    /// Condition variable for waiting on flush completion
    condvar: Condvar,
    /// Mutex for the condition variable (holds stall count)
    mutex: Mutex<usize>,
}

impl WriteStallState {
    fn new() -> Self {
        Self {
            condvar: Condvar::new(),
            mutex: Mutex::new(0),
        }
    }
}

impl std::fmt::Debug for WriteStallState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WriteStallState")
            .field("stall_count", &*self.mutex.lock().unwrap())
            .finish()
    }
}

pub struct MemTableState {
    /// Lock to coordinate inserts and flush:
    /// - insert() takes read lock (concurrent inserts allowed)
    /// - needs_flush() takes write lock (exclusive, blocks inserts during swap+conversion)
    /// This prevents data loss when skip_to_btree runs during concurrent inserts.
    flush_lock: Arc<RwLock<()>>,
    /// Current in-memory write buffer (lock-free SkipList with atomic swap)
    /// Uses ArcSwap for lock-free atomic replacement during flush
    pub active_memtable: ArcSwap<SkipMemTable>,
    /// MemTables currently being flushed to disk (converted to BTreeMap for ordered iteration)
    pub immutable_memtables: Arc<Mutex<Vec<Arc<MemTable>>>>,
    /// Max size for MemTable in bytes
    pub capacity_bytes: u64,
    /// Approximate size of current active MemTable
    pub current_size: Arc<AtomicU64>,
    /// Maximum number of immutable memtables before write stall
    max_immutable_memtables: usize,
    /// Write stall coordination
    write_stall: Arc<WriteStallState>,
    /// Count of stalled writes (for metrics)
    stall_count: Arc<AtomicUsize>,
}

impl std::fmt::Debug for MemTableState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemTableState")
            .field("capacity_bytes", &self.capacity_bytes)
            .field("current_size", &self.current_size.load(Ordering::Relaxed))
            .field("max_immutable_memtables", &self.max_immutable_memtables)
            .finish()
    }
}

impl Clone for MemTableState {
    fn clone(&self) -> Self {
        MemTableState {
            flush_lock: Arc::clone(&self.flush_lock),
            active_memtable: ArcSwap::new(self.active_memtable.load_full()),
            immutable_memtables: Arc::clone(&self.immutable_memtables),
            capacity_bytes: self.capacity_bytes,
            current_size: Arc::clone(&self.current_size),
            max_immutable_memtables: self.max_immutable_memtables,
            write_stall: Arc::clone(&self.write_stall),
            stall_count: Arc::clone(&self.stall_count),
        }
    }
}

impl MemTableState {
    pub fn new(capacity_bytes: u64, recovered_memtable: MemTable, recovered_size: u64) -> Self {
        Self::new_with_config(
            capacity_bytes,
            recovered_memtable,
            recovered_size,
            DEFAULT_MAX_IMMUTABLE_MEMTABLES,
        )
    }

    pub fn new_with_config(
        capacity_bytes: u64,
        recovered_memtable: MemTable,
        recovered_size: u64,
        max_immutable_memtables: usize,
    ) -> Self {
        // Convert recovered BTreeMap to SkipMap
        let skip_map = SkipMap::new();
        for (key, value) in recovered_memtable {
            skip_map.insert(key, value);
        }

        MemTableState {
            flush_lock: Arc::new(RwLock::new(())),
            active_memtable: ArcSwap::new(Arc::new(skip_map)),
            immutable_memtables: Arc::new(Mutex::new(Vec::new())),
            capacity_bytes,
            current_size: Arc::new(AtomicU64::new(recovered_size)),
            max_immutable_memtables,
            write_stall: Arc::new(WriteStallState::new()),
            stall_count: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Wait if too many immutable memtables are pending flush.
    /// Returns true if we had to wait (write was stalled).
    pub fn wait_if_stalled(&self) -> bool {
        let immutables = self.immutable_memtables.lock().unwrap();
        if immutables.len() < self.max_immutable_memtables {
            return false;
        }
        drop(immutables); // Release lock before waiting

        // Increment stall count for metrics
        self.stall_count.fetch_add(1, Ordering::Relaxed);

        // Wait for a flush to complete
        let mut guard = self.write_stall.mutex.lock().unwrap();
        *guard += 1; // Track number of waiters

        loop {
            let immutables = self.immutable_memtables.lock().unwrap();
            if immutables.len() < self.max_immutable_memtables {
                drop(immutables);
                *guard -= 1;
                return true;
            }
            drop(immutables);

            // Wait with timeout to avoid deadlock on spurious wakeups
            guard = self
                .write_stall
                .condvar
                .wait_timeout(guard, std::time::Duration::from_millis(10))
                .unwrap()
                .0;
        }
    }

    /// Get the number of times writes were stalled
    #[cfg(test)]
    pub fn stall_count(&self) -> usize {
        self.stall_count.load(Ordering::Relaxed)
    }

    /// Get the current number of immutable memtables
    #[cfg(test)]
    pub fn immutable_count(&self) -> usize {
        self.immutable_memtables.lock().unwrap().len()
    }

    /// Insert a key-value pair into the active MemTable
    /// Takes a read lock to prevent data loss during flush (multiple inserts can run concurrently)
    pub fn insert(&self, key: String, value: Option<String>) {
        // Take read lock - allows concurrent inserts but blocks during flush
        let _guard = self.flush_lock.read().unwrap();

        let entry_size = estimate_entry_size(&key, &value);

        // Load current active memtable
        let active = self.active_memtable.load();

        // Check if key already exists to adjust size tracking
        if let Some(existing) = active.get(&key) {
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

        active.insert(key, value);
    }

    /// Check if flush is needed and return immutable memtable if so.
    /// Takes write lock to ensure no concurrent inserts during swap+conversion.
    pub fn needs_flush(&self) -> Option<Arc<MemTable>> {
        let size = self.current_size.load(Ordering::SeqCst);
        if size >= self.capacity_bytes {
            // Take write lock - blocks all inserts during swap and conversion
            // This prevents data loss from concurrent inserts during skip_to_btree
            let _guard = self.flush_lock.write().unwrap();

            // Re-check size under lock (another thread might have triggered flush)
            let size = self.current_size.load(Ordering::SeqCst);
            if size < self.capacity_bytes {
                return None;
            }

            // Create a new empty SkipMap
            let new_memtable = Arc::new(SkipMap::new());

            // Atomically swap the old memtable with the new one
            let old_memtable = self.active_memtable.swap(new_memtable);

            // Reset size counter (new memtable is empty)
            self.current_size.store(0, Ordering::SeqCst);

            // Convert the old SkipMap to BTreeMap for SSTable creation
            // Safe because write lock ensures no concurrent inserts
            let btree = skip_to_btree(&old_memtable);

            let immutable = Arc::new(btree);
            let mut immutables = self.immutable_memtables.lock().unwrap();
            immutables.push(Arc::clone(&immutable));

            return Some(immutable);
        }
        None
    }

    pub fn remove_immutable(&self, memtable: &Arc<MemTable>) {
        {
            let mut immutables = self.immutable_memtables.lock().unwrap();
            immutables.retain(|m| !Arc::ptr_eq(m, memtable));
        }
        // Notify any waiting writers that a flush completed
        self.write_stall.condvar.notify_all();
    }

    /// Get a value from MemTable (lock-free for active memtable)
    pub fn get(&self, key: &str) -> Option<Option<String>> {
        // Check active memtable (lock-free)
        let active = self.active_memtable.load();
        if let Some(entry) = active.get(key) {
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
        assert!(state.active_memtable.load().is_empty());
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

        // 2. Force flush to immutable using atomic swap
        let old = state.active_memtable.swap(Arc::new(SkipMap::new()));
        state.current_size.store(0, Ordering::SeqCst);
        let btree = skip_to_btree(&old);
        let immutable1 = Arc::new(btree);
        state.immutable_memtables.lock().unwrap().push(immutable1);

        // 3. Insert k1=v2 in active
        state.insert("k1".to_string(), Some("v2".to_string()));

        // Should get v2 from active
        assert_eq!(state.get("k1"), Some(Some("v2".to_string())));

        // 4. Force another flush
        let old = state.active_memtable.swap(Arc::new(SkipMap::new()));
        state.current_size.store(0, Ordering::SeqCst);
        let btree = skip_to_btree(&old);
        let immutable2 = Arc::new(btree);
        state.immutable_memtables.lock().unwrap().push(immutable2);

        // 5. Insert k1=v3 in active
        state.insert("k1".to_string(), Some("v3".to_string()));

        // Should get v3 from active
        assert_eq!(state.get("k1"), Some(Some("v3".to_string())));

        // 6. Clear active, should get v2 from newest immutable (immutable2)
        state.active_memtable.swap(Arc::new(SkipMap::new()));
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

    /// Test atomic swap doesn't lose writes during flush
    #[test]
    fn test_atomic_swap_no_data_loss() {
        use std::thread;

        let state = Arc::new(MemTableState::new(u64::MAX, BTreeMap::new(), 0)); // Large capacity to prevent auto-flush

        // Writer thread that continuously writes
        let state_writer = Arc::clone(&state);
        let writer = thread::spawn(move || {
            for i in 0..10000 {
                let key = format!("key_{}", i);
                let value = format!("value_{}", i);
                state_writer.insert(key, Some(value));
            }
        });

        // Flusher thread that does multiple swaps (using proper locking)
        let state_flusher = Arc::clone(&state);
        let flusher = thread::spawn(move || {
            let mut all_immutables = Vec::new();
            for _ in 0..10 {
                thread::sleep(std::time::Duration::from_micros(100));
                // Take write lock to prevent concurrent inserts during conversion
                let _guard = state_flusher.flush_lock.write().unwrap();
                let old = state_flusher.active_memtable.swap(Arc::new(SkipMap::new()));
                let btree = skip_to_btree(&old);
                all_immutables.push(btree);
            }
            all_immutables
        });

        writer.join().unwrap();
        let immutables = flusher.join().unwrap();

        // Collect all keys from active and immutables
        let active = state.active_memtable.load();
        let mut all_keys: std::collections::HashSet<String> =
            active.iter().map(|e| e.key().clone()).collect();

        for imm in &immutables {
            for key in imm.keys() {
                all_keys.insert(key.clone());
            }
        }

        // All 10000 keys should be present (no data loss)
        assert_eq!(
            all_keys.len(),
            10000,
            "Some keys were lost during atomic swap"
        );
    }

    /// Test write stall prevention
    #[test]
    fn test_write_stall_prevention() {
        use std::thread;
        use std::time::{Duration, Instant};

        // Create state with max 2 immutable memtables
        let state = Arc::new(MemTableState::new_with_config(
            u64::MAX, // Large capacity
            BTreeMap::new(),
            0,
            2, // Max 2 immutable memtables
        ));

        // Create 2 immutable memtables (at the limit)
        state.insert("k1".to_string(), Some("v1".to_string()));
        let imm1 = state.needs_flush_manual();
        state.insert("k2".to_string(), Some("v2".to_string()));
        let imm2 = state.needs_flush_manual();

        assert_eq!(state.immutable_count(), 2);

        // Start a writer thread that should stall
        let state_writer = Arc::clone(&state);
        let writer = thread::spawn(move || {
            let start = Instant::now();
            state_writer.wait_if_stalled();
            start.elapsed()
        });

        // Give the writer time to start waiting
        thread::sleep(Duration::from_millis(50));

        // Verify no stalls have completed yet
        assert_eq!(state.stall_count(), 1);

        // Remove one immutable to allow writer to proceed
        state.remove_immutable(&imm1);

        // Writer should now complete
        let wait_time = writer.join().unwrap();
        assert!(
            wait_time >= Duration::from_millis(40),
            "Writer should have waited at least 40ms, waited {:?}",
            wait_time
        );

        // Verify stall was recorded
        assert_eq!(state.stall_count(), 1);

        // Clean up
        state.remove_immutable(&imm2);
        assert_eq!(state.immutable_count(), 0);
    }

    /// Test that writes don't stall when under the limit
    #[test]
    fn test_no_stall_under_limit() {
        let state = MemTableState::new_with_config(
            u64::MAX,
            BTreeMap::new(),
            0,
            4, // Max 4 immutable memtables
        );

        // Create 3 immutable memtables (under the limit)
        state.insert("k1".to_string(), Some("v1".to_string()));
        let _imm1 = state.needs_flush_manual();
        state.insert("k2".to_string(), Some("v2".to_string()));
        let _imm2 = state.needs_flush_manual();
        state.insert("k3".to_string(), Some("v3".to_string()));
        let _imm3 = state.needs_flush_manual();

        assert_eq!(state.immutable_count(), 3);

        // This should NOT stall
        let stalled = state.wait_if_stalled();
        assert!(!stalled, "Should not stall when under limit");
        assert_eq!(state.stall_count(), 0);
    }
}

impl MemTableState {
    // Helper for testing to force a flush regardless of size
    #[cfg(test)]
    fn needs_flush_manual(&self) -> Arc<MemTable> {
        // Take write lock to prevent concurrent inserts during conversion
        let _guard = self.flush_lock.write().unwrap();

        let old = self.active_memtable.swap(Arc::new(SkipMap::new()));
        self.current_size.store(0, Ordering::SeqCst);
        let btree = skip_to_btree(&old);
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
