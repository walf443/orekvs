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
    // Threshold for MemTable size in bytes
    pub threshold: u64,
    // Approximate size of current active MemTable
    pub current_size: Arc<AtomicU64>,
}

impl Clone for MemTableState {
    fn clone(&self) -> Self {
        MemTableState {
            active_memtable: Arc::clone(&self.active_memtable),
            immutable_memtables: Arc::clone(&self.immutable_memtables),
            threshold: self.threshold,
            current_size: Arc::clone(&self.current_size),
        }
    }
}

impl MemTableState {
    pub fn new(threshold: u64, recovered_memtable: MemTable, recovered_size: u64) -> Self {
        MemTableState {
            active_memtable: Arc::new(Mutex::new(recovered_memtable)),
            immutable_memtables: Arc::new(Mutex::new(Vec::new())),
            threshold,
            current_size: Arc::new(AtomicU64::new(recovered_size)),
        }
    }

    pub fn insert(&self, key: String, value: Option<String>) {
        let entry_size = estimate_entry_size(&key, &value);
        let mut memtable = self.active_memtable.lock().unwrap();
        
        if let Some(old_val_opt) = memtable.get(&key) {
            let old_size = estimate_entry_size(&key, old_val_opt);
            if entry_size > old_size {
                self.current_size.fetch_add(entry_size - old_size, Ordering::SeqCst);
            } else {
                self.current_size.fetch_sub(old_size - entry_size, Ordering::SeqCst);
            }
        } else {
            self.current_size.fetch_add(entry_size, Ordering::SeqCst);
        }
        memtable.insert(key, value);
    }

    pub fn check_flush_threshold(&self) -> Option<Arc<MemTable>> {
        let size = self.current_size.load(Ordering::SeqCst);
        if size >= self.threshold {
            let mut active = self.active_memtable.lock().unwrap();
            // Double check inside lock
            if self.current_size.load(Ordering::SeqCst) >= self.threshold {
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
