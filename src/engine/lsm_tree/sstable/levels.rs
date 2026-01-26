//! SSTable level management with separate L0 and L1+ implementations.
//!
//! - L0: Recently flushed SSTables with potentially overlapping key ranges
//! - L1+: Non-overlapping key ranges within each level, sorted by min_key

use std::sync::Arc;

use crate::engine::lsm_tree::SstableHandle;

/// Common interface for SSTable levels
#[allow(dead_code)]
pub trait SstableLevel {
    /// Add an SSTable to this level
    fn add(&mut self, handle: Arc<SstableHandle>);

    /// Remove an SSTable from this level
    fn remove(&mut self, handle: &Arc<SstableHandle>) -> bool;

    /// Get all SSTables in this level
    fn sstables(&self) -> &[Arc<SstableHandle>];

    /// Get the number of SSTables
    fn len(&self) -> usize {
        self.sstables().len()
    }

    /// Check if this level is empty
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Get the total size of this level in bytes
    fn size_bytes(&self) -> u64 {
        self.sstables()
            .iter()
            .filter_map(|h| std::fs::metadata(h.mmap.path()).ok())
            .map(|m: std::fs::Metadata| m.len())
            .sum()
    }

    /// Returns candidate SSTables that might contain the given key.
    ///
    /// - For L0: returns all SSTables (newest first, since they may overlap)
    /// - For L1+: returns at most 1 SSTable (via binary search)
    fn candidates_for_key(&self, key: &[u8]) -> Vec<Arc<SstableHandle>>;

    /// Clear all SSTables from this level
    fn clear(&mut self);
}

/// Level 0: Recently flushed SSTables with overlapping key ranges.
///
/// SSTables are stored newest-first, and all must be scanned during lookups
/// since their key ranges may overlap.
#[derive(Default)]
pub struct Level0 {
    /// SSTables sorted by creation time (newest first)
    sstables: Vec<Arc<SstableHandle>>,
}

impl Level0 {
    /// Create a new empty Level0
    pub fn new() -> Self {
        Self {
            sstables: Vec::new(),
        }
    }
}

impl SstableLevel for Level0 {
    fn add(&mut self, handle: Arc<SstableHandle>) {
        // Insert at the beginning (newest first)
        self.sstables.insert(0, handle);
    }

    fn remove(&mut self, handle: &Arc<SstableHandle>) -> bool {
        if let Some(pos) = self.sstables.iter().position(|h| Arc::ptr_eq(h, handle)) {
            self.sstables.remove(pos);
            true
        } else {
            false
        }
    }

    fn sstables(&self) -> &[Arc<SstableHandle>] {
        &self.sstables
    }

    fn candidates_for_key(&self, _key: &[u8]) -> Vec<Arc<SstableHandle>> {
        // Return all SSTables since they may have overlapping keys
        self.sstables.clone()
    }

    fn clear(&mut self) {
        self.sstables.clear();
    }
}

/// Sorted level (L1+): Non-overlapping SSTables sorted by min_key.
///
/// Supports efficient binary search for key lookups.
#[derive(Default)]
pub struct SortedLevel {
    /// SSTables sorted by min_key (non-overlapping)
    sstables: Vec<Arc<SstableHandle>>,
}

impl SortedLevel {
    /// Create a new empty SortedLevel
    #[allow(unused_variables)]
    pub fn new(level: usize) -> Self {
        Self {
            sstables: Vec::new(),
        }
    }

    /// Binary search for the SSTable that might contain the key.
    ///
    /// Returns the SSTable where min_key <= key <= max_key, or None.
    fn binary_search(&self, key: &[u8]) -> Option<Arc<SstableHandle>> {
        if self.sstables.is_empty() {
            return None;
        }

        // Binary search by min_key to find the rightmost SSTable where min_key <= key
        let pos = self
            .sstables
            .binary_search_by(|h| h.mmap.min_key().cmp(key))
            .unwrap_or_else(|p: usize| p.saturating_sub(1));

        if pos < self.sstables.len() {
            let handle = &self.sstables[pos];
            // Verify key is within this SSTable's range
            if handle.mmap.key_in_range(key) {
                return Some(Arc::clone(handle));
            }
        }

        None
    }

    /// Get SSTables that overlap with the given key range.
    ///
    /// Used for compaction to find files that need to be merged.
    #[cfg(test)]
    pub fn get_overlapping(&self, min_key: &[u8], max_key: &[u8]) -> Vec<Arc<SstableHandle>> {
        let mut result = Vec::new();

        for handle in &self.sstables {
            let sst_min = handle.mmap.min_key();
            let sst_max = handle.mmap.max_key();

            // Check if ranges overlap: [sst_min, sst_max] overlaps [min_key, max_key]
            if sst_min <= max_key && sst_max >= min_key {
                result.push(Arc::clone(handle));
            }
        }

        result
    }
}

impl SstableLevel for SortedLevel {
    fn add(&mut self, handle: Arc<SstableHandle>) {
        // Insert in sorted order by min_key
        let min_key = handle.mmap.min_key();
        let pos = self
            .sstables
            .binary_search_by(|h| h.mmap.min_key().cmp(min_key))
            .unwrap_or_else(|p| p);
        self.sstables.insert(pos, handle);
    }

    fn remove(&mut self, handle: &Arc<SstableHandle>) -> bool {
        if let Some(pos) = self.sstables.iter().position(|h| Arc::ptr_eq(h, handle)) {
            self.sstables.remove(pos);
            true
        } else {
            false
        }
    }

    fn sstables(&self) -> &[Arc<SstableHandle>] {
        &self.sstables
    }

    fn candidates_for_key(&self, key: &[u8]) -> Vec<Arc<SstableHandle>> {
        // Binary search returns at most 1 candidate
        self.binary_search(key).into_iter().collect()
    }

    fn clear(&mut self) {
        self.sstables.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::lsm_tree::sstable::{MappedSSTable, write_timestamped_entries};
    use std::collections::BTreeMap;
    use tempfile::tempdir;

    fn create_test_sstable(
        dir: &std::path::Path,
        id: u64,
        entries: Vec<(&str, Option<&str>)>,
    ) -> Arc<SstableHandle> {
        let path = dir.join(format!("test_{}.sst", id));

        // Build a BTreeMap with timestamped entries
        let mut btree: BTreeMap<String, (u64, Option<String>)> = BTreeMap::new();
        for (key, value) in &entries {
            btree.insert(key.to_string(), (1000 + id, value.map(|v| v.to_string())));
        }

        // Write using public API
        let bloom = write_timestamped_entries(&path, &btree).unwrap();

        // Open using MappedSSTable
        let mmap = MappedSSTable::open(&path).unwrap();
        Arc::new(SstableHandle { mmap, bloom })
    }

    #[test]
    fn test_level0_add_newest_first() {
        let dir = tempdir().unwrap();
        let mut level0 = Level0::new();

        let sst1 = create_test_sstable(dir.path(), 1, vec![("a", Some("1"))]);
        let sst2 = create_test_sstable(dir.path(), 2, vec![("b", Some("2"))]);
        let sst3 = create_test_sstable(dir.path(), 3, vec![("c", Some("3"))]);

        level0.add(sst1.clone());
        level0.add(sst2.clone());
        level0.add(sst3.clone());

        // Newest (sst3) should be first
        assert_eq!(level0.len(), 3);
        assert!(Arc::ptr_eq(&level0.sstables()[0], &sst3));
        assert!(Arc::ptr_eq(&level0.sstables()[1], &sst2));
        assert!(Arc::ptr_eq(&level0.sstables()[2], &sst1));
    }

    #[test]
    fn test_level0_candidates_returns_all() {
        let dir = tempdir().unwrap();
        let mut level0 = Level0::new();

        let sst1 = create_test_sstable(dir.path(), 1, vec![("a", Some("1")), ("z", Some("26"))]);
        let sst2 = create_test_sstable(dir.path(), 2, vec![("b", Some("2")), ("y", Some("25"))]);

        level0.add(sst1);
        level0.add(sst2);

        // Any key should return all SSTables (they overlap)
        let candidates = level0.candidates_for_key(b"m");
        assert_eq!(candidates.len(), 2);
    }

    #[test]
    fn test_sorted_level_add_sorted() {
        let dir = tempdir().unwrap();
        let mut level = SortedLevel::new(1);

        // Add in non-sorted order
        let sst_m = create_test_sstable(dir.path(), 1, vec![("m", Some("1"))]);
        let sst_a = create_test_sstable(dir.path(), 2, vec![("a", Some("2"))]);
        let sst_z = create_test_sstable(dir.path(), 3, vec![("z", Some("3"))]);

        level.add(sst_m.clone());
        level.add(sst_a.clone());
        level.add(sst_z.clone());

        // Should be sorted by min_key
        assert_eq!(level.len(), 3);
        assert!(Arc::ptr_eq(&level.sstables()[0], &sst_a)); // "a"
        assert!(Arc::ptr_eq(&level.sstables()[1], &sst_m)); // "m"
        assert!(Arc::ptr_eq(&level.sstables()[2], &sst_z)); // "z"
    }

    #[test]
    fn test_sorted_level_binary_search() {
        let dir = tempdir().unwrap();
        let mut level = SortedLevel::new(1);

        let sst1 = create_test_sstable(dir.path(), 1, vec![("a", Some("1")), ("c", Some("3"))]);
        let sst2 = create_test_sstable(dir.path(), 2, vec![("d", Some("4")), ("f", Some("6"))]);
        let sst3 = create_test_sstable(dir.path(), 3, vec![("g", Some("7")), ("i", Some("9"))]);

        level.add(sst1.clone());
        level.add(sst2.clone());
        level.add(sst3.clone());

        // Key in first SSTable
        let candidates = level.candidates_for_key(b"b");
        assert_eq!(candidates.len(), 1);
        assert!(Arc::ptr_eq(&candidates[0], &sst1));

        // Key in second SSTable
        let candidates = level.candidates_for_key(b"e");
        assert_eq!(candidates.len(), 1);
        assert!(Arc::ptr_eq(&candidates[0], &sst2));

        // Key not in any SSTable
        let candidates = level.candidates_for_key(b"z");
        assert_eq!(candidates.len(), 0);
    }

    #[test]
    fn test_sorted_level_get_overlapping() {
        let dir = tempdir().unwrap();
        let mut level = SortedLevel::new(1);

        let sst1 = create_test_sstable(dir.path(), 1, vec![("a", Some("1")), ("c", Some("3"))]);
        let sst2 = create_test_sstable(dir.path(), 2, vec![("d", Some("4")), ("f", Some("6"))]);
        let sst3 = create_test_sstable(dir.path(), 3, vec![("g", Some("7")), ("i", Some("9"))]);

        level.add(sst1.clone());
        level.add(sst2.clone());
        level.add(sst3.clone());

        // Range overlapping sst1 and sst2
        let overlapping = level.get_overlapping(b"b", b"e");
        assert_eq!(overlapping.len(), 2);

        // Range overlapping only sst3
        let overlapping = level.get_overlapping(b"h", b"z");
        assert_eq!(overlapping.len(), 1);
        assert!(Arc::ptr_eq(&overlapping[0], &sst3));
    }

    #[test]
    fn test_remove() {
        let dir = tempdir().unwrap();
        let mut level0 = Level0::new();

        let sst1 = create_test_sstable(dir.path(), 1, vec![("a", Some("1"))]);
        let sst2 = create_test_sstable(dir.path(), 2, vec![("b", Some("2"))]);

        level0.add(sst1.clone());
        level0.add(sst2.clone());

        assert_eq!(level0.len(), 2);
        assert!(level0.remove(&sst1));
        assert_eq!(level0.len(), 1);
        assert!(!level0.remove(&sst1)); // Already removed
    }
}
