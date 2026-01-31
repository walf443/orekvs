//! Recovery module for LSM-Tree engine.
//!
//! This module handles recovery of engine state from disk:
//! - Scanning for SSTable and WAL files
//! - Loading manifest
//! - Rebuilding SSTable handles with mmap and Bloom filters
//! - Replaying WAL to recover MemTable state

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::manifest::Manifest;
use super::memtable::{self, MemTable, MemValue};
use super::sstable::{self, MappedSSTable};
use super::wal::GroupCommitWalWriter;
use super::{LeveledSstables, SstableHandle};
use crate::engine::current_timestamp;

/// Result of recovery process
pub struct RecoveryResult {
    /// Recovered SSTable handles organized by level
    pub leveled_sstables: LeveledSstables,
    /// Recovered manifest
    pub manifest: Manifest,
    /// Recovered MemTable from WAL replay
    pub recovered_memtable: MemTable,
    /// Size of recovered MemTable in bytes
    pub recovered_size: u64,
    /// Next SSTable ID to use
    pub next_sst_id: u64,
    /// Next WAL ID to use
    pub next_wal_id: u64,
    /// Maximum WAL sequence number recovered
    pub max_wal_seq: u64,
    /// WAL file max sequences: (wal_id, max_seq) for each WAL file
    /// Used to register WAL files in manifest for proper cleanup
    pub wal_file_max_seqs: Vec<(u64, u64)>,
}

/// Scanned file information from data directory
struct ScannedFiles {
    /// SSTable file paths
    sst_files: Vec<PathBuf>,
    /// WAL files with their IDs
    wal_files: Vec<(u64, PathBuf)>,
    /// Maximum SSTable ID found
    max_sst_id: u64,
    /// Maximum WAL ID embedded in SSTable filenames
    max_wal_id_in_sst: u64,
    /// Maximum WAL file ID found
    max_wal_id: u64,
}

/// Scan data directory for SSTable and WAL files
fn scan_files(data_dir: &Path) -> ScannedFiles {
    let mut sst_files = Vec::new();
    let mut wal_files: Vec<(u64, PathBuf)> = Vec::new();
    let mut max_sst_id = 0u64;
    let mut max_wal_id_in_sst = 0u64;
    let mut max_wal_id = 0u64;

    if let Ok(entries) = fs::read_dir(data_dir) {
        for entry in entries.flatten() {
            let p = entry.path();
            if let Some(filename) = p.file_name().and_then(|n| n.to_str()) {
                // Clean up orphaned .tmp files from interrupted SSTable writes
                if filename.ends_with(".tmp") {
                    if let Err(e) = fs::remove_file(&p) {
                        eprintln!("Warning: Failed to remove orphaned tmp file {:?}: {}", p, e);
                    } else {
                        println!("Cleaned up orphaned tmp file: {:?}", p);
                    }
                    continue;
                }

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
                } else if let Some(id) = GroupCommitWalWriter::parse_wal_filename(filename) {
                    // WAL files: wal_{id}.log
                    if id >= max_wal_id {
                        max_wal_id = id + 1;
                    }
                    wal_files.push((id, p));
                }
            }
        }
    }

    // Sort SSTable files in reverse order (newest first for searches)
    sst_files.sort_by(|a, b| b.cmp(a));
    // Sort WAL files by ID
    wal_files.sort_by_key(|(id, _)| *id);

    ScannedFiles {
        sst_files,
        wal_files,
        max_sst_id,
        max_wal_id_in_sst,
        max_wal_id,
    }
}

/// Load manifest from data directory
fn load_manifest(data_dir: &Path) -> Manifest {
    match Manifest::load(data_dir) {
        Ok(Some(m)) => {
            println!("Loaded manifest with {} entries", m.entries.len());
            m
        }
        Ok(None) => {
            println!("No manifest found, creating new one");
            Manifest::new()
        }
        Err(e) => {
            eprintln!("Warning: Failed to load manifest: {}, creating new one", e);
            Manifest::new()
        }
    }
}

/// Build SSTable handles from scanned files
/// Only loads SSTables that are tracked in the manifest.
/// Orphaned SSTables (files not in manifest) are deleted.
fn build_sstable_handles(sst_files: &[PathBuf], manifest: &Manifest) -> LeveledSstables {
    let mut leveled_sstables = LeveledSstables::new();

    for p in sst_files {
        let filename = p.file_name().and_then(|n| n.to_str()).unwrap_or("");

        // Check if this SSTable is tracked in manifest
        let manifest_entry = manifest.get_entry(filename);
        if manifest_entry.is_none() {
            // Orphaned SSTable - delete it
            println!("Deleting orphaned SSTable not in manifest: {:?}", p);
            if let Err(e) = fs::remove_file(p) {
                eprintln!("Warning: Failed to delete orphaned SSTable {:?}: {}", p, e);
            }
            continue;
        }

        // Open mmap for the SSTable
        let mmap = match MappedSSTable::open(p) {
            Ok(m) => m,
            Err(e) => {
                eprintln!("Warning: Failed to mmap SSTable {:?}: {}", p, e);
                continue;
            }
        };

        // Read embedded Bloom filter
        let bloom = mmap
            .read_bloom_filter()
            .expect("Failed to read Bloom filter from SSTable");
        let handle = Arc::new(SstableHandle { mmap, bloom });

        let level = manifest_entry.unwrap().level;
        leveled_sstables.add_to_level(level, handle);
    }

    leveled_sstables
}

/// Check if an entry is expired
fn is_expired(expire_at: u64, now: u64) -> bool {
    expire_at > 0 && expire_at <= now
}

/// Result of memtable recovery
struct MemtableRecoveryResult {
    memtable: MemTable,
    size: u64,
    max_wal_seq: u64,
    /// WAL file max sequences: (wal_id, max_seq)
    wal_file_max_seqs: Vec<(u64, u64)>,
}

/// Recover MemTable from WAL files
fn recover_memtable(
    wal_files: &[(u64, PathBuf)],
    max_wal_id_in_sst: u64,
    has_sstables_with_wal_id: bool,
    last_flushed_wal_seq: u64,
) -> MemtableRecoveryResult {
    let mut recovered_memtable: MemTable = BTreeMap::new();
    let mut recovered_size: u64 = 0;
    let mut max_wal_seq = last_flushed_wal_seq;
    let mut wal_file_max_seqs: Vec<(u64, u64)> = Vec::new();
    let now = current_timestamp();
    let mut skipped_expired = 0u64;

    for (wal_id, wal_path) in wal_files {
        // Get max_seq for this WAL file (for manifest registration)
        let wal_max_seq = GroupCommitWalWriter::get_max_seq(wal_path);
        if wal_max_seq > 0 {
            wal_file_max_seqs.push((*wal_id, wal_max_seq));
        }

        // Recover WAL files that are newer than the latest flushed WAL ID
        // If no SSTables have WAL ID info, recover all WAL files
        let should_recover = !has_sstables_with_wal_id || *wal_id > max_wal_id_in_sst;
        if should_recover {
            // Try LSN-based recovery first (v4 WAL format)
            // If WAL is older version, fall back to full recovery
            match GroupCommitWalWriter::read_entries_with_seq(wal_path, last_flushed_wal_seq) {
                Ok(entries) => {
                    for entry in entries {
                        max_wal_seq = max_wal_seq.max(entry.seq);
                        // Skip expired entries during recovery
                        if is_expired(entry.expire_at, now) {
                            skipped_expired += 1;
                            continue;
                        }
                        let mem_value = MemValue::new_with_ttl(entry.value, entry.expire_at);
                        let entry_size = memtable::estimate_entry_size(&entry.key, &mem_value);
                        if let Some(old_value) = recovered_memtable.get(&entry.key) {
                            let old_size = memtable::estimate_entry_size(&entry.key, old_value);
                            if entry_size > old_size {
                                recovered_size += entry_size - old_size;
                            } else {
                                recovered_size -= old_size - entry_size;
                            }
                        } else {
                            recovered_size += entry_size;
                        }
                        recovered_memtable.insert(entry.key, mem_value);
                    }
                    println!(
                        "Recovered {} entries from WAL (LSN > {}): {:?}",
                        recovered_memtable.len(),
                        last_flushed_wal_seq,
                        wal_path
                    );
                }
                Err(_) => {
                    // Fall back to reading all entries for older WAL versions
                    match GroupCommitWalWriter::read_entries(wal_path) {
                        Ok(entries) => {
                            for (key, mem_value) in entries {
                                // Skip expired entries during recovery
                                if is_expired(mem_value.expire_at, now) {
                                    skipped_expired += 1;
                                    continue;
                                }
                                let entry_size = memtable::estimate_entry_size(&key, &mem_value);
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
                                recovered_memtable.insert(key, mem_value);
                            }
                            println!(
                                "Recovered {} entries from WAL (full replay): {:?}",
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
        }
    }

    if skipped_expired > 0 {
        println!(
            "Skipped {} expired entries during WAL recovery",
            skipped_expired
        );
    }

    MemtableRecoveryResult {
        memtable: recovered_memtable,
        size: recovered_size,
        max_wal_seq,
        wal_file_max_seqs,
    }
}

/// Perform full recovery of engine state from disk
pub fn recover(data_dir: &Path) -> RecoveryResult {
    // Scan for files
    let scanned = scan_files(data_dir);

    // Load manifest
    let manifest = load_manifest(data_dir);

    // Build SSTable handles
    let leveled_sstables = build_sstable_handles(&scanned.sst_files, &manifest);

    // Check if we have any SSTables with WAL ID info
    let has_sstables_with_wal_id = scanned
        .sst_files
        .iter()
        .any(|p| sstable::extract_wal_id_from_sstable(p).is_some());

    // Recover MemTable from WAL
    let memtable_result = recover_memtable(
        &scanned.wal_files,
        scanned.max_wal_id_in_sst,
        has_sstables_with_wal_id,
        manifest.last_flushed_wal_seq,
    );

    // Determine next WAL ID
    let next_wal_id = if scanned.max_wal_id > 0 {
        scanned.max_wal_id
    } else {
        0
    };

    RecoveryResult {
        leveled_sstables,
        manifest,
        recovered_memtable: memtable_result.memtable,
        recovered_size: memtable_result.size,
        next_sst_id: scanned.max_sst_id,
        next_wal_id,
        max_wal_seq: memtable_result.max_wal_seq,
        wal_file_max_seqs: memtable_result.wal_file_max_seqs,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::lsm_tree::manifest::ManifestEntry;
    use tempfile::TempDir;

    // ==================== is_expired Tests ====================

    #[test]
    fn test_is_expired_no_ttl() {
        // expire_at = 0 means no TTL, should never be expired
        let now = current_timestamp();
        assert!(!is_expired(0, now));
        assert!(!is_expired(0, now + 1000));
    }

    #[test]
    fn test_is_expired_future() {
        let now = current_timestamp();
        // Expire time is in the future
        assert!(!is_expired(now + 3600, now));
    }

    #[test]
    fn test_is_expired_past() {
        let now = current_timestamp();
        // Expire time is in the past
        assert!(is_expired(now - 1, now));
        assert!(is_expired(now - 3600, now));
    }

    #[test]
    fn test_is_expired_exactly_now() {
        let now = current_timestamp();
        // Expire time equals now - should be expired (expire_at <= now)
        assert!(is_expired(now, now));
    }

    // ==================== scan_files Tests ====================

    #[test]
    fn test_scan_files_empty_dir() {
        let temp_dir = TempDir::new().unwrap();
        let scanned = scan_files(temp_dir.path());

        assert!(scanned.sst_files.is_empty());
        assert!(scanned.wal_files.is_empty());
        assert_eq!(scanned.max_sst_id, 0);
        assert_eq!(scanned.max_wal_id, 0);
        assert_eq!(scanned.max_wal_id_in_sst, 0);
    }

    #[test]
    fn test_scan_files_with_wal_files() {
        let temp_dir = TempDir::new().unwrap();

        // Create WAL files
        fs::write(temp_dir.path().join("wal_00001.log"), b"data1").unwrap();
        fs::write(temp_dir.path().join("wal_00003.log"), b"data3").unwrap();
        fs::write(temp_dir.path().join("wal_00002.log"), b"data2").unwrap();

        let scanned = scan_files(temp_dir.path());

        assert!(scanned.sst_files.is_empty());
        assert_eq!(scanned.wal_files.len(), 3);
        // WAL files should be sorted by ID
        assert_eq!(scanned.wal_files[0].0, 1);
        assert_eq!(scanned.wal_files[1].0, 2);
        assert_eq!(scanned.wal_files[2].0, 3);
        assert_eq!(scanned.max_wal_id, 4); // max_wal_id = 3 + 1
    }

    #[test]
    fn test_scan_files_with_sst_files_old_format() {
        let temp_dir = TempDir::new().unwrap();

        // Create old format SSTable files
        fs::write(temp_dir.path().join("sst_00001.data"), b"data1").unwrap();
        fs::write(temp_dir.path().join("sst_00005.data"), b"data5").unwrap();

        let scanned = scan_files(temp_dir.path());

        assert_eq!(scanned.sst_files.len(), 2);
        assert!(scanned.wal_files.is_empty());
        assert_eq!(scanned.max_sst_id, 6); // max_sst_id = 5 + 1
        assert_eq!(scanned.max_wal_id_in_sst, 0); // Old format has no WAL ID
    }

    #[test]
    fn test_scan_files_with_sst_files_new_format() {
        let temp_dir = TempDir::new().unwrap();

        // Create new format SSTable files (sst_{sst_id}_{wal_id}.data)
        fs::write(temp_dir.path().join("sst_00001_00010.data"), b"data1").unwrap();
        fs::write(temp_dir.path().join("sst_00002_00015.data"), b"data2").unwrap();

        let scanned = scan_files(temp_dir.path());

        assert_eq!(scanned.sst_files.len(), 2);
        assert_eq!(scanned.max_sst_id, 3); // max_sst_id = 2 + 1
        assert_eq!(scanned.max_wal_id_in_sst, 15); // max WAL ID from SSTable filenames
    }

    #[test]
    fn test_scan_files_removes_tmp_files() {
        let temp_dir = TempDir::new().unwrap();

        // Create a .tmp file (should be removed)
        let tmp_path = temp_dir.path().join("sst_00001.data.tmp");
        fs::write(&tmp_path, b"orphaned").unwrap();
        assert!(tmp_path.exists());

        let scanned = scan_files(temp_dir.path());

        // .tmp file should be removed
        assert!(!tmp_path.exists());
        assert!(scanned.sst_files.is_empty());
    }

    #[test]
    fn test_scan_files_ignores_other_files() {
        let temp_dir = TempDir::new().unwrap();

        // Create various files that should be ignored
        fs::write(temp_dir.path().join("manifest.json"), b"{}").unwrap();
        fs::write(temp_dir.path().join("other.txt"), b"text").unwrap();
        fs::write(temp_dir.path().join("readme.md"), b"readme").unwrap();

        let scanned = scan_files(temp_dir.path());

        assert!(scanned.sst_files.is_empty());
        assert!(scanned.wal_files.is_empty());
    }

    #[test]
    fn test_scan_files_mixed_files() {
        let temp_dir = TempDir::new().unwrap();

        // Create a mix of files
        fs::write(temp_dir.path().join("wal_00001.log"), b"wal").unwrap();
        fs::write(temp_dir.path().join("sst_00001.data"), b"sst").unwrap();
        fs::write(temp_dir.path().join("manifest.json"), b"{}").unwrap();

        let scanned = scan_files(temp_dir.path());

        assert_eq!(scanned.sst_files.len(), 1);
        assert_eq!(scanned.wal_files.len(), 1);
    }

    #[test]
    fn test_scan_files_sst_sorted_reverse() {
        let temp_dir = TempDir::new().unwrap();

        // Create SSTable files
        fs::write(temp_dir.path().join("sst_00001.data"), b"data1").unwrap();
        fs::write(temp_dir.path().join("sst_00003.data"), b"data3").unwrap();
        fs::write(temp_dir.path().join("sst_00002.data"), b"data2").unwrap();

        let scanned = scan_files(temp_dir.path());

        // SSTable files should be sorted in reverse order (newest first)
        assert_eq!(scanned.sst_files.len(), 3);
        assert!(
            scanned.sst_files[0]
                .file_name()
                .unwrap()
                .to_str()
                .unwrap()
                .contains("00003")
        );
        assert!(
            scanned.sst_files[2]
                .file_name()
                .unwrap()
                .to_str()
                .unwrap()
                .contains("00001")
        );
    }

    // ==================== load_manifest Tests ====================

    #[test]
    fn test_load_manifest_no_file() {
        let temp_dir = TempDir::new().unwrap();
        let manifest = load_manifest(temp_dir.path());

        // Should return empty manifest
        assert!(manifest.entries.is_empty());
    }

    #[test]
    fn test_load_manifest_with_file() {
        let temp_dir = TempDir::new().unwrap();

        // Create and save a manifest
        let mut manifest = Manifest::new();
        let entry = ManifestEntry {
            filename: "sst_00001.data".to_string(),
            level: 0,
            min_key_hex: "00".to_string(),
            max_key_hex: "ff".to_string(),
            size_bytes: 100,
            entry_count: None,
        };
        manifest.add_entry(entry);
        manifest.save(temp_dir.path()).unwrap();

        // Load it back
        let loaded = load_manifest(temp_dir.path());
        assert_eq!(loaded.entries.len(), 1);
        assert!(loaded.get_entry("sst_00001.data").is_some());
    }

    // ==================== build_sstable_handles Tests ====================

    #[test]
    fn test_build_sstable_handles_empty() {
        let manifest = Manifest::new();
        let sst_files: Vec<PathBuf> = vec![];

        let handles = build_sstable_handles(&sst_files, &manifest);

        // New LeveledSstables should have no files at any level
        assert_eq!(handles.total_sstable_count(), 0);
    }

    #[test]
    fn test_build_sstable_handles_orphan_deleted() {
        let temp_dir = TempDir::new().unwrap();

        // Create an SSTable file that is NOT in manifest
        let orphan_path = temp_dir.path().join("sst_00001.data");
        fs::write(&orphan_path, b"orphan data").unwrap();
        assert!(orphan_path.exists());

        // Empty manifest (SSTable is not tracked)
        let manifest = Manifest::new();
        let sst_files = vec![orphan_path.clone()];

        let _handles = build_sstable_handles(&sst_files, &manifest);

        // Orphan SSTable should be deleted
        assert!(!orphan_path.exists());
    }

    // ==================== recover Tests ====================

    #[test]
    fn test_recover_empty_dir() {
        let temp_dir = TempDir::new().unwrap();
        let result = recover(temp_dir.path());

        assert!(result.recovered_memtable.is_empty());
        assert_eq!(result.recovered_size, 0);
        assert_eq!(result.next_sst_id, 0);
        assert_eq!(result.next_wal_id, 0);
    }

    #[test]
    fn test_recover_updates_next_wal_id() {
        let temp_dir = TempDir::new().unwrap();

        // Create WAL files with specific IDs
        fs::write(temp_dir.path().join("wal_00010.log"), b"").unwrap();
        fs::write(temp_dir.path().join("wal_00005.log"), b"").unwrap();

        let result = recover(temp_dir.path());

        // next_wal_id should be max + 1
        assert_eq!(result.next_wal_id, 11);
    }

    #[test]
    fn test_recover_updates_next_sst_id() {
        let temp_dir = TempDir::new().unwrap();

        // Create SSTable files with specific IDs (will be orphaned but ID tracking still works)
        fs::write(temp_dir.path().join("sst_00007.data"), b"data").unwrap();
        fs::write(temp_dir.path().join("sst_00003.data"), b"data").unwrap();

        let result = recover(temp_dir.path());

        // next_sst_id should be max + 1
        assert_eq!(result.next_sst_id, 8);
    }

    #[test]
    fn test_recover_empty_manifest() {
        let temp_dir = TempDir::new().unwrap();

        let result = recover(temp_dir.path());

        assert!(result.manifest.entries.is_empty());
    }

    #[test]
    fn test_recover_with_manifest() {
        let temp_dir = TempDir::new().unwrap();

        // Create and save a manifest
        let mut manifest = Manifest::new();
        let entry = ManifestEntry {
            filename: "sst_00001.data".to_string(),
            level: 0,
            min_key_hex: "00".to_string(),
            max_key_hex: "ff".to_string(),
            size_bytes: 100,
            entry_count: None,
        };
        manifest.add_entry(entry);
        manifest.save(temp_dir.path()).unwrap();

        // Recover
        let result = recover(temp_dir.path());

        // Manifest should be loaded
        assert_eq!(result.manifest.entries.len(), 1);
    }

    #[test]
    fn test_recover_deletes_orphan_sstables() {
        let temp_dir = TempDir::new().unwrap();

        // Create an SSTable file that is NOT in manifest
        let orphan_path = temp_dir.path().join("sst_00001.data");
        fs::write(&orphan_path, b"orphan data").unwrap();
        assert!(orphan_path.exists());

        // Recover with empty manifest
        let _result = recover(temp_dir.path());

        // Orphan SSTable should be deleted
        assert!(!orphan_path.exists());
    }

    #[test]
    fn test_recover_cleans_tmp_files() {
        let temp_dir = TempDir::new().unwrap();

        // Create a .tmp file
        let tmp_path = temp_dir.path().join("sst_00001.data.tmp");
        fs::write(&tmp_path, b"incomplete").unwrap();
        assert!(tmp_path.exists());

        // Recover
        let _result = recover(temp_dir.path());

        // .tmp file should be deleted
        assert!(!tmp_path.exists());
    }

    // ==================== RecoveryResult Tests ====================

    #[test]
    fn test_recovery_result_fields() {
        let temp_dir = TempDir::new().unwrap();
        let result = recover(temp_dir.path());

        // Verify all fields are accessible and have expected defaults
        assert_eq!(result.leveled_sstables.total_sstable_count(), 0);
        assert!(result.manifest.entries.is_empty());
        assert!(result.recovered_memtable.is_empty());
        assert_eq!(result.recovered_size, 0);
        assert_eq!(result.next_sst_id, 0);
        assert_eq!(result.next_wal_id, 0);
        assert_eq!(result.max_wal_seq, 0);
        assert!(result.wal_file_max_seqs.is_empty());
    }

    // ==================== recover_memtable Tests ====================

    #[test]
    fn test_recover_memtable_empty() {
        // Test with no WAL files
        let wal_files: Vec<(u64, PathBuf)> = vec![];
        let result = recover_memtable(&wal_files, 0, false, 0);

        assert!(result.memtable.is_empty());
        assert_eq!(result.size, 0);
        assert_eq!(result.max_wal_seq, 0);
        assert!(result.wal_file_max_seqs.is_empty());
    }

    // ==================== ScannedFiles struct Tests ====================

    #[test]
    fn test_scanned_files_nonexistent_dir() {
        // Scanning a non-existent directory should return empty results
        let scanned = scan_files(Path::new("/nonexistent/path/that/does/not/exist"));

        assert!(scanned.sst_files.is_empty());
        assert!(scanned.wal_files.is_empty());
        assert_eq!(scanned.max_sst_id, 0);
        assert_eq!(scanned.max_wal_id, 0);
    }
}
