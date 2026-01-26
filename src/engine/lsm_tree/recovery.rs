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
use super::memtable::{self, MemTable};
use super::sstable::{self, MappedSSTable};
use super::wal::GroupCommitWalWriter;
use super::{LeveledSstables, SstableHandle};

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
fn build_sstable_handles(sst_files: &[PathBuf], manifest: &Manifest) -> LeveledSstables {
    let mut leveled_sstables = LeveledSstables::new();

    for p in sst_files {
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

        // Get level from manifest, default to L0 if not found
        let filename = p.file_name().and_then(|n| n.to_str()).unwrap_or("");
        let level = manifest.get_entry(filename).map(|e| e.level).unwrap_or(0);
        leveled_sstables.add_to_level(level, handle);
    }

    leveled_sstables
}

/// Recover MemTable from WAL files
fn recover_memtable(
    wal_files: &[(u64, PathBuf)],
    max_wal_id_in_sst: u64,
    has_sstables_with_wal_id: bool,
    last_flushed_wal_seq: u64,
) -> (MemTable, u64, u64) {
    let mut recovered_memtable: MemTable = BTreeMap::new();
    let mut recovered_size: u64 = 0;
    let mut max_wal_seq = last_flushed_wal_seq;

    for (wal_id, wal_path) in wal_files {
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
                        let entry_size = memtable::estimate_entry_size(&entry.key, &entry.value);
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
                        recovered_memtable.insert(entry.key, entry.value);
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

    (recovered_memtable, recovered_size, max_wal_seq)
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
    let (recovered_memtable, recovered_size, max_wal_seq) = recover_memtable(
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
        recovered_memtable,
        recovered_size,
        next_sst_id: scanned.max_sst_id,
        next_wal_id,
        max_wal_seq,
    }
}
