//! Leveled Compaction implementation.
//!
//! This module implements leveled compaction strategy:
//! - L0: Overlapping SSTables from memtable flushes
//! - L1+: Non-overlapping key ranges, each level ~10x larger than previous
//!
//! Compaction triggers:
//! - L0 → L1: When L0 file count exceeds threshold
//! - Ln → Ln+1: When level size exceeds target

use super::manifest::ManifestEntry;
use super::sstable::{self, MappedSSTable, TimestampedEntry};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use tonic::Status;

/// Configuration for leveled compaction
#[derive(Debug, Clone)]
pub struct CompactionConfig {
    /// Number of L0 files that triggers compaction
    pub l0_compaction_threshold: usize,
    /// Size multiplier between levels (L(n+1) = multiplier * Ln)
    pub level_size_multiplier: u64,
    /// Target size for L1 in bytes
    pub l1_target_size_bytes: u64,
    /// Maximum number of levels (reserved for future use)
    #[allow(dead_code)]
    pub max_levels: usize,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            l0_compaction_threshold: 4,
            level_size_multiplier: 10,
            l1_target_size_bytes: 64 * 1024 * 1024, // 64MB
            max_levels: 7,
        }
    }
}

impl CompactionConfig {
    /// Calculate target size for a given level
    pub fn target_size_for_level(&self, level: usize) -> u64 {
        if level == 0 {
            return 0; // L0 is controlled by file count
        }
        self.l1_target_size_bytes * self.level_size_multiplier.pow((level - 1) as u32)
    }
}

/// Represents a planned compaction task
#[derive(Debug)]
pub struct CompactionTask {
    /// Source level (files to compact from)
    #[allow(dead_code)] // Read in tests
    pub source_level: usize,
    /// Target level (files to write to)
    pub target_level: usize,
    /// SSTable files to compact from source level
    pub source_files: Vec<PathBuf>,
    /// SSTable files from target level that overlap with source
    pub target_files: Vec<PathBuf>,
    /// Key range of the compaction [min_key, max_key]
    #[allow(dead_code)] // Read in tests
    pub key_range: (Vec<u8>, Vec<u8>),
}

/// Result of a compaction operation
#[derive(Debug)]
pub struct CompactionResult {
    /// New SSTable files created
    pub new_files: Vec<CompactionOutputFile>,
    /// Files that were compacted and can be deleted
    pub files_to_delete: Vec<PathBuf>,
    /// Bytes read during compaction
    pub bytes_read: u64,
    /// Bytes written during compaction
    pub bytes_written: u64,
}

/// Information about a newly created SSTable from compaction
#[derive(Debug, Clone)]
pub struct CompactionOutputFile {
    /// Path to the new SSTable
    pub path: PathBuf,
    /// Minimum key in this SSTable
    pub min_key: Vec<u8>,
    /// Maximum key in this SSTable
    pub max_key: Vec<u8>,
    /// File size in bytes
    pub size_bytes: u64,
    /// Target level for this file
    pub level: usize,
}

/// Leveled compaction planner and executor
pub struct LeveledCompaction {
    config: CompactionConfig,
    data_dir: PathBuf,
}

impl LeveledCompaction {
    /// Create a new leveled compaction handler
    pub fn new(data_dir: PathBuf, config: CompactionConfig) -> Self {
        Self { config, data_dir }
    }

    /// Check if L0 needs compaction
    pub fn l0_needs_compaction(&self, l0_file_count: usize) -> bool {
        l0_file_count >= self.config.l0_compaction_threshold
    }

    /// Check if a level needs compaction based on size
    pub fn level_needs_compaction(&self, level: usize, level_size: u64) -> bool {
        if level == 0 {
            return false; // L0 is handled by file count
        }
        level_size > self.config.target_size_for_level(level)
    }

    /// Plan L0 → L1 compaction
    ///
    /// Takes all L0 files and finds overlapping L1 files to merge with.
    #[allow(clippy::result_large_err)]
    pub fn plan_l0_compaction(
        &self,
        l0_files: &[PathBuf],
        l1_files: &[PathBuf],
    ) -> Result<Option<CompactionTask>, Status> {
        if l0_files.is_empty() {
            return Ok(None);
        }

        // Get key range of all L0 files
        let (min_key, max_key) = self.get_key_range_for_files(l0_files)?;

        // Find overlapping L1 files
        let overlapping_l1 = self.find_overlapping_files(l1_files, &min_key, &max_key)?;

        Ok(Some(CompactionTask {
            source_level: 0,
            target_level: 1,
            source_files: l0_files.to_vec(),
            target_files: overlapping_l1,
            key_range: (min_key, max_key),
        }))
    }

    /// Plan Ln → Ln+1 compaction
    ///
    /// Picks one file from the source level and finds overlapping files in target level.
    #[allow(clippy::result_large_err)]
    pub fn plan_level_compaction(
        &self,
        source_level: usize,
        source_files: &[PathBuf],
        target_files: &[PathBuf],
    ) -> Result<Option<CompactionTask>, Status> {
        if source_files.is_empty() {
            return Ok(None);
        }

        // Pick the first file (could be improved with better selection strategy)
        let source_file = &source_files[0];

        // Get key range of the source file
        let (min_key, max_key) = self.get_key_range_for_file(source_file)?;

        // Find overlapping files in target level
        let overlapping_target = self.find_overlapping_files(target_files, &min_key, &max_key)?;

        // Expand key range to include all overlapping target files
        let (final_min, final_max) = if overlapping_target.is_empty() {
            (min_key, max_key)
        } else {
            let mut all_files = vec![source_file.clone()];
            all_files.extend(overlapping_target.iter().cloned());
            self.get_key_range_for_files(&all_files)?
        };

        Ok(Some(CompactionTask {
            source_level,
            target_level: source_level + 1,
            source_files: vec![source_file.clone()],
            target_files: overlapping_target,
            key_range: (final_min, final_max),
        }))
    }

    /// Execute a compaction task
    #[allow(clippy::result_large_err)]
    pub fn execute(
        &self,
        task: &CompactionTask,
        next_sst_id: &mut u64,
        wal_id: u64,
    ) -> Result<CompactionResult, Status> {
        // Collect all files to merge
        let mut all_files = task.source_files.clone();
        all_files.extend(task.target_files.iter().cloned());

        // Calculate bytes read
        let bytes_read: u64 = all_files
            .iter()
            .filter_map(|p| std::fs::metadata(p).ok())
            .map(|m| m.len())
            .sum();

        // Merge all entries
        let merged = self.merge_sstables(&all_files)?;

        // Split into output files for target level
        let output_files =
            self.write_output_files(&merged, task.target_level, next_sst_id, wal_id)?;

        // Calculate bytes written
        let bytes_written: u64 = output_files.iter().map(|f| f.size_bytes).sum();

        // Remove duplicate paths from files_to_delete (can occur in edge cases)
        // Use a seen set to preserve order while removing duplicates
        let mut seen = std::collections::HashSet::new();
        let files_to_delete: Vec<PathBuf> = all_files
            .into_iter()
            .filter(|p| seen.insert(p.clone()))
            .collect();

        Ok(CompactionResult {
            new_files: output_files,
            files_to_delete,
            bytes_read,
            bytes_written,
        })
    }

    /// Merge multiple SSTables into a single sorted map
    #[allow(clippy::result_large_err)]
    fn merge_sstables(
        &self,
        files: &[PathBuf],
    ) -> Result<BTreeMap<String, TimestampedEntry>, Status> {
        let mut merged: BTreeMap<String, TimestampedEntry> = BTreeMap::new();

        for path in files {
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

        // Remove tombstones during compaction
        merged.retain(|_, (_, value)| value.is_some());

        Ok(merged)
    }

    /// Write merged entries to output files
    ///
    /// For L1+, we write a single file since the merged entries are non-overlapping.
    /// In the future, this could be enhanced to split into multiple files based on size.
    #[allow(clippy::result_large_err)]
    fn write_output_files(
        &self,
        entries: &BTreeMap<String, TimestampedEntry>,
        target_level: usize,
        next_sst_id: &mut u64,
        wal_id: u64,
    ) -> Result<Vec<CompactionOutputFile>, Status> {
        if entries.is_empty() {
            return Ok(Vec::new());
        }

        let sst_id = *next_sst_id;
        *next_sst_id += 1;

        let path = sstable::generate_path(&self.data_dir, sst_id, wal_id);

        // Write the SSTable
        let _bloom = sstable::write_timestamped_entries(&path, entries)?;

        // Get file size
        let size_bytes = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);

        // Get min/max keys
        let min_key = entries
            .keys()
            .next()
            .map(|k| k.as_bytes().to_vec())
            .unwrap_or_default();
        let max_key = entries
            .keys()
            .next_back()
            .map(|k| k.as_bytes().to_vec())
            .unwrap_or_default();

        Ok(vec![CompactionOutputFile {
            path,
            min_key,
            max_key,
            size_bytes,
            level: target_level,
        }])
    }

    /// Get the combined key range for a set of files
    #[allow(clippy::result_large_err)]
    fn get_key_range_for_files(&self, files: &[PathBuf]) -> Result<(Vec<u8>, Vec<u8>), Status> {
        let mut min_key: Option<Vec<u8>> = None;
        let mut max_key: Option<Vec<u8>> = None;

        for path in files {
            let (file_min, file_max) = self.get_key_range_for_file(path)?;

            min_key = Some(match min_key {
                None => file_min.clone(),
                Some(ref current) if file_min < *current => file_min.clone(),
                Some(current) => current,
            });

            max_key = Some(match max_key {
                None => file_max.clone(),
                Some(ref current) if file_max > *current => file_max.clone(),
                Some(current) => current,
            });
        }

        Ok((min_key.unwrap_or_default(), max_key.unwrap_or_default()))
    }

    /// Get the key range for a single file
    #[allow(clippy::result_large_err)]
    fn get_key_range_for_file(&self, path: &Path) -> Result<(Vec<u8>, Vec<u8>), Status> {
        let mmap = MappedSSTable::open(path)?;

        let min_key = mmap.min_key().map(|k| k.to_vec()).unwrap_or_default();
        let max_key = mmap.max_key().map(|k| k.to_vec()).unwrap_or_default();

        Ok((min_key, max_key))
    }

    /// Find files that overlap with the given key range
    #[allow(clippy::result_large_err)]
    fn find_overlapping_files(
        &self,
        files: &[PathBuf],
        min_key: &[u8],
        max_key: &[u8],
    ) -> Result<Vec<PathBuf>, Status> {
        let mut overlapping = Vec::new();

        for path in files {
            let (file_min, file_max) = self.get_key_range_for_file(path)?;

            // Check if ranges overlap: file_min <= max_key AND file_max >= min_key
            if file_min.as_slice() <= max_key && file_max.as_slice() >= min_key {
                overlapping.push(path.clone());
            }
        }

        Ok(overlapping)
    }
}

/// Helper to create ManifestEntry from CompactionOutputFile
impl CompactionOutputFile {
    /// Convert to ManifestEntry
    pub fn to_manifest_entry(&self) -> ManifestEntry {
        ManifestEntry::new(
            self.path
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("")
                .to_string(),
            self.level,
            &self.min_key,
            &self.max_key,
            self.size_bytes,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use tempfile::tempdir;

    fn create_test_sstable(dir: &Path, filename: &str, entries: &[(&str, &str)]) -> PathBuf {
        let path = dir.join(filename);
        let mut memtable: BTreeMap<String, Option<String>> = BTreeMap::new();
        for (k, v) in entries {
            memtable.insert(k.to_string(), Some(v.to_string()));
        }
        sstable::create_from_memtable(&path, &memtable).unwrap();
        path
    }

    #[test]
    fn test_compaction_config_defaults() {
        let config = CompactionConfig::default();
        assert_eq!(config.l0_compaction_threshold, 4);
        assert_eq!(config.level_size_multiplier, 10);
        assert_eq!(config.l1_target_size_bytes, 64 * 1024 * 1024);
        assert_eq!(config.max_levels, 7);
    }

    #[test]
    fn test_target_size_calculation() {
        let config = CompactionConfig::default();

        assert_eq!(config.target_size_for_level(0), 0);
        assert_eq!(config.target_size_for_level(1), 64 * 1024 * 1024);
        assert_eq!(config.target_size_for_level(2), 640 * 1024 * 1024);
        assert_eq!(config.target_size_for_level(3), 6400 * 1024 * 1024);
    }

    #[test]
    fn test_l0_needs_compaction() {
        let dir = tempdir().unwrap();
        let compaction =
            LeveledCompaction::new(dir.path().to_path_buf(), CompactionConfig::default());

        assert!(!compaction.l0_needs_compaction(0));
        assert!(!compaction.l0_needs_compaction(3));
        assert!(compaction.l0_needs_compaction(4));
        assert!(compaction.l0_needs_compaction(10));
    }

    #[test]
    fn test_plan_l0_compaction() {
        let dir = tempdir().unwrap();
        let data_dir = dir.path();

        // Create L0 files
        let l0_file1 = create_test_sstable(data_dir, "sst_1_0.data", &[("a", "v1"), ("b", "v2")]);
        let l0_file2 = create_test_sstable(data_dir, "sst_2_0.data", &[("c", "v3"), ("d", "v4")]);

        // Create L1 files
        let l1_file1 =
            create_test_sstable(data_dir, "sst_3_0.data", &[("b", "old_b"), ("c", "old_c")]);

        let compaction =
            LeveledCompaction::new(data_dir.to_path_buf(), CompactionConfig::default());

        let task = compaction
            .plan_l0_compaction(&[l0_file1.clone(), l0_file2.clone()], &[l1_file1.clone()])
            .unwrap()
            .unwrap();

        assert_eq!(task.source_level, 0);
        assert_eq!(task.target_level, 1);
        assert_eq!(task.source_files.len(), 2);
        assert_eq!(task.target_files.len(), 1); // l1_file1 overlaps
        assert_eq!(task.key_range.0, b"a".to_vec());
        assert_eq!(task.key_range.1, b"d".to_vec());
    }

    #[test]
    fn test_execute_compaction() {
        let dir = tempdir().unwrap();
        let data_dir = dir.path();

        // Create L0 files with overlapping keys
        let l0_file1 =
            create_test_sstable(data_dir, "sst_1_0.data", &[("a", "new_a"), ("b", "new_b")]);
        let l0_file2 =
            create_test_sstable(data_dir, "sst_2_0.data", &[("c", "new_c"), ("d", "new_d")]);

        // Create L1 file with some overlapping keys
        let l1_file1 = create_test_sstable(
            data_dir,
            "sst_3_0.data",
            &[("b", "old_b"), ("c", "old_c"), ("e", "old_e")],
        );

        let compaction =
            LeveledCompaction::new(data_dir.to_path_buf(), CompactionConfig::default());

        // Plan compaction
        let task = compaction
            .plan_l0_compaction(&[l0_file1, l0_file2], &[l1_file1])
            .unwrap()
            .unwrap();

        // Execute compaction
        let mut next_sst_id = 10;
        let result = compaction.execute(&task, &mut next_sst_id, 0).unwrap();

        assert_eq!(result.new_files.len(), 1);
        assert_eq!(result.files_to_delete.len(), 3);
        assert!(result.bytes_read > 0);
        assert!(result.bytes_written > 0);

        // Verify the new file contains merged data
        let new_file = &result.new_files[0];
        assert_eq!(new_file.level, 1);
        assert_eq!(new_file.min_key, b"a".to_vec());
        assert_eq!(new_file.max_key, b"e".to_vec());

        // Read the new file and verify contents
        let entries = sstable::read_entries(&new_file.path).unwrap();
        assert_eq!(entries.len(), 5); // a, b, c, d, e

        // Newer values should win
        assert_eq!(entries.get("b").unwrap().1, Some("new_b".to_string()));
        assert_eq!(entries.get("c").unwrap().1, Some("new_c".to_string()));
        assert_eq!(entries.get("e").unwrap().1, Some("old_e".to_string()));
    }

    #[test]
    fn test_compaction_removes_tombstones() {
        let dir = tempdir().unwrap();
        let data_dir = dir.path();

        // Create SSTable with some entries
        let sst1 = create_test_sstable(
            data_dir,
            "sst_1_0.data",
            &[("a", "v1"), ("b", "v2"), ("c", "v3")],
        );

        // Create SSTable with a tombstone for "b"
        let sst2_path = data_dir.join("sst_2_0.data");
        let mut entries: BTreeMap<String, TimestampedEntry> = BTreeMap::new();
        entries.insert("b".to_string(), (u64::MAX, None)); // Tombstone with high timestamp
        sstable::write_timestamped_entries(&sst2_path, &entries).unwrap();

        let compaction =
            LeveledCompaction::new(data_dir.to_path_buf(), CompactionConfig::default());

        let task = CompactionTask {
            source_level: 0,
            target_level: 1,
            source_files: vec![sst1, sst2_path],
            target_files: vec![],
            key_range: (b"a".to_vec(), b"c".to_vec()),
        };

        let mut next_sst_id = 10;
        let result = compaction.execute(&task, &mut next_sst_id, 0).unwrap();

        // Verify tombstone was removed
        let new_entries = sstable::read_entries(&result.new_files[0].path).unwrap();
        assert_eq!(new_entries.len(), 2); // Only "a" and "c", "b" was deleted
        assert!(new_entries.contains_key("a"));
        assert!(!new_entries.contains_key("b")); // Tombstone removed
        assert!(new_entries.contains_key("c"));
    }

    #[test]
    fn test_find_overlapping_files() {
        let dir = tempdir().unwrap();
        let data_dir = dir.path();

        let file1 = create_test_sstable(data_dir, "sst_1.data", &[("a", "1"), ("c", "3")]);
        let file2 = create_test_sstable(data_dir, "sst_2.data", &[("d", "4"), ("f", "6")]);
        let file3 = create_test_sstable(data_dir, "sst_3.data", &[("g", "7"), ("i", "9")]);

        let compaction =
            LeveledCompaction::new(data_dir.to_path_buf(), CompactionConfig::default());

        // Query [b, e] should overlap with file1 and file2
        let overlapping = compaction
            .find_overlapping_files(&[file1.clone(), file2.clone(), file3.clone()], b"b", b"e")
            .unwrap();
        assert_eq!(overlapping.len(), 2);

        // Query [h, j] should only overlap with file3
        let overlapping = compaction
            .find_overlapping_files(&[file1.clone(), file2.clone(), file3.clone()], b"h", b"j")
            .unwrap();
        assert_eq!(overlapping.len(), 1);

        // Query [x, z] should not overlap with any
        let overlapping = compaction
            .find_overlapping_files(&[file1, file2, file3], b"x", b"z")
            .unwrap();
        assert_eq!(overlapping.len(), 0);
    }

    #[test]
    fn test_output_file_to_manifest_entry() {
        let output = CompactionOutputFile {
            path: PathBuf::from("/data/sst_10_5.data"),
            min_key: b"apple".to_vec(),
            max_key: b"zebra".to_vec(),
            size_bytes: 1024,
            level: 2,
        };

        let entry = output.to_manifest_entry();
        assert_eq!(entry.filename, "sst_10_5.data");
        assert_eq!(entry.level, 2);
        assert_eq!(entry.min_key(), b"apple".to_vec());
        assert_eq!(entry.max_key(), b"zebra".to_vec());
        assert_eq!(entry.size_bytes, 1024);
    }

    #[test]
    fn test_level_needs_compaction() {
        let dir = tempdir().unwrap();
        let compaction =
            LeveledCompaction::new(dir.path().to_path_buf(), CompactionConfig::default());

        // L0 is never triggered by size (handled by file count)
        assert!(!compaction.level_needs_compaction(0, 1000 * 1024 * 1024));

        // L1 target is 64MB
        assert!(!compaction.level_needs_compaction(1, 60 * 1024 * 1024)); // under limit
        assert!(compaction.level_needs_compaction(1, 65 * 1024 * 1024)); // over limit

        // L2 target is 640MB
        assert!(!compaction.level_needs_compaction(2, 600 * 1024 * 1024)); // under limit
        assert!(compaction.level_needs_compaction(2, 700 * 1024 * 1024)); // over limit
    }

    #[test]
    fn test_plan_level_compaction() {
        let dir = tempdir().unwrap();
        let data_dir = dir.path();

        // Create L1 files (non-overlapping ranges)
        let l1_file1 = create_test_sstable(data_dir, "sst_1_0.data", &[("a", "v1"), ("c", "v3")]);
        let l1_file2 = create_test_sstable(data_dir, "sst_2_0.data", &[("d", "v4"), ("f", "v6")]);

        // Create L2 files (non-overlapping ranges)
        let l2_file1 =
            create_test_sstable(data_dir, "sst_3_0.data", &[("a", "old_a"), ("b", "old_b")]);
        let l2_file2 =
            create_test_sstable(data_dir, "sst_4_0.data", &[("g", "old_g"), ("h", "old_h")]);

        let compaction =
            LeveledCompaction::new(data_dir.to_path_buf(), CompactionConfig::default());

        // Plan L1 → L2 compaction (should pick l1_file1 and find overlapping l2_file1)
        let task = compaction
            .plan_level_compaction(
                1,
                &[l1_file1.clone(), l1_file2.clone()],
                &[l2_file1, l2_file2],
            )
            .unwrap()
            .unwrap();

        assert_eq!(task.source_level, 1);
        assert_eq!(task.target_level, 2);
        assert_eq!(task.source_files.len(), 1); // Only picks one file from L1
        assert_eq!(task.target_files.len(), 1); // l2_file1 overlaps with l1_file1
    }

    #[test]
    fn test_execute_level_compaction() {
        let dir = tempdir().unwrap();
        let data_dir = dir.path();

        // Create L1 file
        let l1_file =
            create_test_sstable(data_dir, "sst_1_0.data", &[("b", "new_b"), ("c", "new_c")]);

        // Create L2 file with some overlapping keys
        let l2_file = create_test_sstable(
            data_dir,
            "sst_2_0.data",
            &[("a", "old_a"), ("b", "old_b"), ("d", "old_d")],
        );

        let compaction =
            LeveledCompaction::new(data_dir.to_path_buf(), CompactionConfig::default());

        // Plan L1 → L2 compaction
        let task = compaction
            .plan_level_compaction(1, &[l1_file], &[l2_file])
            .unwrap()
            .unwrap();

        // Execute compaction
        let mut next_sst_id = 10;
        let result = compaction.execute(&task, &mut next_sst_id, 0).unwrap();

        assert_eq!(result.new_files.len(), 1);
        assert_eq!(result.files_to_delete.len(), 2); // L1 file + L2 file

        // Verify the new file is at L2
        let new_file = &result.new_files[0];
        assert_eq!(new_file.level, 2);

        // Verify newer values win
        let entries = sstable::read_entries(&new_file.path).unwrap();
        assert_eq!(entries.len(), 4); // a, b, c, d
        assert_eq!(entries.get("b").unwrap().1, Some("new_b".to_string())); // new wins
        assert_eq!(entries.get("c").unwrap().1, Some("new_c".to_string())); // new
        assert_eq!(entries.get("a").unwrap().1, Some("old_a".to_string())); // old (no conflict)
        assert_eq!(entries.get("d").unwrap().1, Some("old_d".to_string())); // old (no conflict)
    }
}
