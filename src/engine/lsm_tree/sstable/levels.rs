//! SSTable level management with separate L0 and L1+ implementations.
//!
//! - L0: Recently flushed SSTables with potentially overlapping key ranges
//! - L1+: Non-overlapping key ranges within each level, sorted by min_key

use std::sync::Arc;

use crate::engine::lsm_tree::SstableHandle;

/// Default maximum level for leveled compaction
pub const MAX_LEVELS: usize = 7;

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

/// Manages SSTables organized by levels for leveled compaction.
///
/// Uses separate implementations for L0 (overlapping, time-ordered) and
/// L1+ (non-overlapping, key-sorted) levels.
pub struct LeveledSstables {
    /// Level 0: overlapping SSTables sorted by creation time (newest first)
    l0: Level0,
    /// Levels 1+: non-overlapping SSTables sorted by min_key
    sorted_levels: Vec<SortedLevel>,
}

impl LeveledSstables {
    /// Create a new empty LeveledSstables structure
    pub fn new() -> Self {
        let sorted_levels = (1..MAX_LEVELS).map(SortedLevel::new).collect();
        Self {
            l0: Level0::new(),
            sorted_levels,
        }
    }

    /// Add an SSTable to a specific level
    pub fn add_to_level(&mut self, level: usize, handle: Arc<SstableHandle>) {
        if level == 0 {
            self.l0.add(handle);
        } else {
            // Ensure we have enough levels
            while self.sorted_levels.len() < level {
                self.sorted_levels
                    .push(SortedLevel::new(self.sorted_levels.len() + 1));
            }
            self.sorted_levels[level - 1].add(handle);
        }
    }

    /// Remove an SSTable from its level
    #[cfg(test)]
    pub fn remove(&mut self, handle: &Arc<SstableHandle>) {
        if self.l0.remove(handle) {
            return;
        }
        for level in &mut self.sorted_levels {
            if level.remove(handle) {
                return;
            }
        }
    }

    /// Get the level count (including L0)
    pub fn level_count(&self) -> usize {
        1 + self.sorted_levels.len()
    }

    /// Get SSTables at a specific level
    pub fn get_level(&self, level: usize) -> &[Arc<SstableHandle>] {
        if level == 0 {
            self.l0.sstables()
        } else if level <= self.sorted_levels.len() {
            self.sorted_levels[level - 1].sstables()
        } else {
            &[]
        }
    }

    /// Get all L0 SSTables (for scanning during reads)
    pub fn l0_sstables(&self) -> &[Arc<SstableHandle>] {
        self.l0.sstables()
    }

    /// Get SSTables in L1+ that overlap with the given key range
    #[cfg(test)]
    pub fn get_overlapping(
        &self,
        level: usize,
        min_key: &[u8],
        max_key: &[u8],
    ) -> Vec<Arc<SstableHandle>> {
        if level == 0 || level > self.sorted_levels.len() {
            return Vec::new();
        }
        self.sorted_levels[level - 1].get_overlapping(min_key, max_key)
    }

    /// Get candidates for a key lookup at a specific level
    pub fn candidates_for_key(&self, level: usize, key: &[u8]) -> Vec<Arc<SstableHandle>> {
        if level == 0 {
            self.l0.candidates_for_key(key)
        } else if level <= self.sorted_levels.len() {
            self.sorted_levels[level - 1].candidates_for_key(key)
        } else {
            Vec::new()
        }
    }

    /// Remove an SSTable by filename from any level
    pub fn remove_by_filename(&mut self, filename: &str) -> bool {
        // Check L0
        if let Some(pos) = self.l0.sstables().iter().position(|h| {
            h.mmap
                .path()
                .file_name()
                .and_then(|n: &std::ffi::OsStr| n.to_str())
                .unwrap_or("")
                == filename
        }) {
            let handle = self.l0.sstables()[pos].clone();
            return self.l0.remove(&handle);
        }

        // Check sorted levels
        for level in &mut self.sorted_levels {
            if let Some(pos) = level.sstables().iter().position(|h| {
                h.mmap
                    .path()
                    .file_name()
                    .and_then(|n: &std::ffi::OsStr| n.to_str())
                    .unwrap_or("")
                    == filename
            }) {
                let handle = level.sstables()[pos].clone();
                return level.remove(&handle);
            }
        }

        false
    }

    /// Get total number of SSTables across all levels
    #[cfg(test)]
    pub fn total_sstable_count(&self) -> usize {
        self.l0.len() + self.sorted_levels.iter().map(|l| l.len()).sum::<usize>()
    }

    /// Get the total size of a level in bytes
    pub fn level_size(&self, level: usize) -> u64 {
        if level == 0 {
            self.l0.size_bytes()
        } else if level <= self.sorted_levels.len() {
            self.sorted_levels[level - 1].size_bytes()
        } else {
            0
        }
    }

    /// Convert from flat SSTable list (for migration)
    /// Places all SSTables in L0 since we don't have level info
    #[cfg(test)]
    pub fn from_flat_list(sstables: Vec<Arc<SstableHandle>>) -> Self {
        let mut leveled = Self::new();
        for handle in sstables {
            leveled.l0.add(handle);
        }
        leveled
    }

    /// Convert to flat SSTable list (for backward compatibility)
    /// Returns all SSTables from all levels, L0 first (newest first),
    /// then L1, L2, etc.
    pub fn to_flat_list(&self) -> Vec<Arc<SstableHandle>> {
        let mut result: Vec<Arc<SstableHandle>> = self.l0.sstables().to_vec();
        for level in &self.sorted_levels {
            result.extend(level.sstables().iter().cloned());
        }
        result
    }
}

impl Default for LeveledSstables {
    fn default() -> Self {
        Self::new()
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

        // Build a BTreeMap with timestamped entries (timestamp, expire_at, value)
        let mut btree: BTreeMap<String, (u64, u64, Option<String>)> = BTreeMap::new();
        for (key, value) in &entries {
            btree.insert(
                key.to_string(),
                (1000 + id, 0, value.map(|v| v.to_string())),
            );
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
