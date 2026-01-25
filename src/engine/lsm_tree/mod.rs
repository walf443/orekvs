mod block_cache;
mod bloom;
mod buffer_pool;
mod compaction;
mod manifest;
mod memtable;
mod metrics;
mod mmap;
mod sstable;
mod wal;

use super::Engine;
use block_cache::{BlockCache, DEFAULT_BLOCK_CACHE_SIZE_BYTES};
use bloom::BloomFilter;
use compaction::{CompactionConfig, LeveledCompaction};
use manifest::{Manifest, ManifestEntry};
use memtable::{MemTable, MemTableState};
pub use metrics::{EngineMetrics, MetricsSnapshot};
use rayon::prelude::*;
use sstable::MappedSSTable;
use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use tonic::Status;
use wal::GroupCommitWalWriter;

/// Lock to prevent SSTable deletion during snapshot transfer.
/// Snapshot transfer holds a read lock; compaction deletion requires a write lock.
pub type SnapshotLock = Arc<RwLock<()>>;

/// Handle to an SSTable with mmap reader and its Bloom filter
struct SstableHandle {
    /// Memory-mapped SSTable reader for zero-copy access
    mmap: MappedSSTable,
    /// Bloom filter for fast negative lookups
    bloom: BloomFilter,
}

/// Default maximum level for leveled compaction
const MAX_LEVELS: usize = 7;

/// Manages SSTables organized by levels for leveled compaction.
///
/// - L0: Recently flushed SSTables with potentially overlapping key ranges
/// - L1+: Non-overlapping key ranges within each level, sorted by min_key
struct LeveledSstables {
    /// SSTables organized by level. levels[0] = L0, levels[1] = L1, etc.
    /// L0: sorted by creation time (newest first)
    /// L1+: sorted by min_key (for binary search)
    levels: Vec<Vec<Arc<SstableHandle>>>,
}

impl LeveledSstables {
    /// Create a new empty LeveledSstables structure
    fn new() -> Self {
        Self {
            levels: vec![Vec::new(); MAX_LEVELS],
        }
    }

    /// Add an SSTable to a specific level
    fn add_to_level(&mut self, level: usize, handle: Arc<SstableHandle>) {
        if level >= self.levels.len() {
            self.levels.resize(level + 1, Vec::new());
        }

        if level == 0 {
            // L0: insert at the beginning (newest first)
            self.levels[0].insert(0, handle);
        } else {
            // L1+: insert in sorted order by min_key
            let min_key = handle.mmap.min_key().unwrap_or(&[]);
            let pos = self.levels[level]
                .binary_search_by(|h| h.mmap.min_key().unwrap_or(&[]).cmp(min_key))
                .unwrap_or_else(|p| p);
            self.levels[level].insert(pos, handle);
        }
    }

    /// Remove an SSTable from its level
    #[cfg(test)]
    fn remove(&mut self, handle: &Arc<SstableHandle>) {
        for level in &mut self.levels {
            if let Some(pos) = level.iter().position(|h| Arc::ptr_eq(h, handle)) {
                level.remove(pos);
                return;
            }
        }
    }

    /// Get the level count
    fn level_count(&self) -> usize {
        self.levels.len()
    }

    /// Get SSTables at a specific level
    fn get_level(&self, level: usize) -> &[Arc<SstableHandle>] {
        if level < self.levels.len() {
            &self.levels[level]
        } else {
            &[]
        }
    }

    /// Get all L0 SSTables (for scanning during reads)
    fn l0_sstables(&self) -> &[Arc<SstableHandle>] {
        self.get_level(0)
    }

    /// Get SSTables in L1+ that overlap with the given key range
    #[cfg(test)]
    fn get_overlapping(
        &self,
        level: usize,
        min_key: &[u8],
        max_key: &[u8],
    ) -> Vec<Arc<SstableHandle>> {
        if level == 0 || level >= self.levels.len() {
            return Vec::new();
        }

        let level_sstables = &self.levels[level];
        let mut result = Vec::new();

        for handle in level_sstables {
            let sst_min = handle.mmap.min_key().unwrap_or(&[]);
            let sst_max = handle.mmap.max_key().unwrap_or(&[]);

            // Check if ranges overlap: [sst_min, sst_max] overlaps [min_key, max_key]
            // Overlap occurs when: sst_min <= max_key AND sst_max >= min_key
            if sst_min <= max_key && sst_max >= min_key {
                result.push(Arc::clone(handle));
            }
        }

        result
    }

    /// Binary search for the SSTable that might contain the key in L1+
    /// Returns None if no SSTable could contain the key
    fn binary_search_level(&self, level: usize, key: &[u8]) -> Option<Arc<SstableHandle>> {
        if level == 0 || level >= self.levels.len() {
            return None;
        }

        let level_sstables = &self.levels[level];
        if level_sstables.is_empty() {
            return None;
        }

        // Binary search by min_key to find the rightmost SSTable where min_key <= key
        let pos = level_sstables
            .binary_search_by(|h| {
                let min = h.mmap.min_key().unwrap_or(&[]);
                min.cmp(key)
            })
            .unwrap_or_else(|p| p.saturating_sub(1));

        if pos < level_sstables.len() {
            let handle = &level_sstables[pos];
            // Verify key is within this SSTable's range
            if handle.mmap.key_in_range(key) {
                return Some(Arc::clone(handle));
            }
        }

        None
    }

    /// Get total number of SSTables across all levels
    #[cfg(test)]
    fn total_sstable_count(&self) -> usize {
        self.levels.iter().map(|l| l.len()).sum()
    }

    /// Get the total size of a level in bytes
    fn level_size(&self, level: usize) -> u64 {
        if level >= self.levels.len() {
            return 0;
        }

        self.levels[level]
            .iter()
            .filter_map(|h| std::fs::metadata(h.mmap.path()).ok())
            .map(|m| m.len())
            .sum()
    }

    /// Convert from flat SSTable list (for migration)
    /// Places all SSTables in L0 since we don't have level info
    #[cfg(test)]
    fn from_flat_list(sstables: Vec<Arc<SstableHandle>>) -> Self {
        let mut leveled = Self::new();
        for handle in sstables {
            leveled.levels[0].push(handle);
        }
        leveled
    }

    /// Convert to flat SSTable list (for backward compatibility)
    /// Returns all SSTables from all levels, L0 first (newest first),
    /// then L1, L2, etc.
    fn to_flat_list(&self) -> Vec<Arc<SstableHandle>> {
        let mut result = Vec::new();
        for level in &self.levels {
            result.extend(level.iter().cloned());
        }
        result
    }
}

impl Default for LeveledSstables {
    fn default() -> Self {
        Self::new()
    }
}

/// Configuration for WAL archiving
#[derive(Clone)]
pub struct WalArchiveConfig {
    /// Retention period in seconds (None = keep forever)
    pub retention_secs: Option<u64>,
    /// Maximum total WAL size in bytes (None = no limit)
    pub max_size_bytes: Option<u64>,
}

/// Default WAL retention period: 7 days
pub const DEFAULT_WAL_RETENTION_SECS: u64 = 7 * 24 * 60 * 60;
/// Default maximum WAL size: 1GB
pub const DEFAULT_WAL_MAX_SIZE_BYTES: u64 = 1024 * 1024 * 1024;

impl Default for WalArchiveConfig {
    fn default() -> Self {
        Self {
            retention_secs: Some(DEFAULT_WAL_RETENTION_SECS),
            max_size_bytes: Some(DEFAULT_WAL_MAX_SIZE_BYTES),
        }
    }
}

impl WalArchiveConfig {
    /// Create a config that disables WAL archiving (keep all WAL files)
    pub fn disabled() -> Self {
        Self {
            retention_secs: None,
            max_size_bytes: None,
        }
    }
}

pub struct LsmTreeEngine {
    // MemTable management state
    mem_state: MemTableState,
    // SSTables organized by level for leveled compaction
    leveled_sstables: Arc<Mutex<LeveledSstables>>,
    // Base directory for SSTables
    data_dir: PathBuf,
    // Counter for next SSTable file ID
    next_sst_id: Arc<AtomicU64>,
    // Trigger count for compaction (number of SSTables)
    compaction_trigger_file_count: usize,
    // Flag to prevent concurrent compaction
    compaction_in_progress: Arc<Mutex<bool>>,
    // WAL writer with group commit support
    wal: GroupCommitWalWriter,
    // Block cache for SSTable reads
    block_cache: Arc<BlockCache>,
    // Centralized metrics collection
    metrics: Arc<EngineMetrics>,
    // WAL archive configuration
    wal_archive_config: WalArchiveConfig,
    // Lock to prevent SSTable deletion during snapshot transfer
    snapshot_lock: SnapshotLock,
    // Manifest for persisting SSTable level assignments
    manifest: Arc<Mutex<Manifest>>,
    // Leveled compaction handler
    compaction_handler: LeveledCompaction,
}

impl Clone for LsmTreeEngine {
    fn clone(&self) -> Self {
        LsmTreeEngine {
            mem_state: self.mem_state.clone(),
            leveled_sstables: Arc::clone(&self.leveled_sstables),
            data_dir: self.data_dir.clone(),
            next_sst_id: Arc::clone(&self.next_sst_id),
            compaction_trigger_file_count: self.compaction_trigger_file_count,
            compaction_in_progress: Arc::clone(&self.compaction_in_progress),
            wal: self.wal.clone(),
            block_cache: Arc::clone(&self.block_cache),
            metrics: Arc::clone(&self.metrics),
            wal_archive_config: self.wal_archive_config.clone(),
            snapshot_lock: Arc::clone(&self.snapshot_lock),
            manifest: Arc::clone(&self.manifest),
            compaction_handler: LeveledCompaction::new(
                self.data_dir.clone(),
                CompactionConfig {
                    l0_compaction_threshold: self.compaction_trigger_file_count,
                    ..CompactionConfig::default()
                },
            ),
        }
    }
}

/// Default batch interval for WAL group commit (100 microseconds)
pub const DEFAULT_WAL_BATCH_INTERVAL_MICROS: u64 = 100;

impl LsmTreeEngine {
    pub fn new(
        data_dir_str: String,
        memtable_capacity_bytes: u64,
        compaction_trigger_file_count: usize,
    ) -> Self {
        Self::new_with_config(
            data_dir_str,
            memtable_capacity_bytes,
            compaction_trigger_file_count,
            DEFAULT_WAL_BATCH_INTERVAL_MICROS,
            WalArchiveConfig::default(),
        )
    }

    pub fn new_with_wal_config(
        data_dir_str: String,
        memtable_capacity_bytes: u64,
        compaction_trigger_file_count: usize,
        wal_batch_interval_micros: u64,
    ) -> Self {
        Self::new_with_config(
            data_dir_str,
            memtable_capacity_bytes,
            compaction_trigger_file_count,
            wal_batch_interval_micros,
            WalArchiveConfig::default(),
        )
    }

    pub fn new_with_config(
        data_dir_str: String,
        memtable_capacity_bytes: u64,
        compaction_trigger_file_count: usize,
        wal_batch_interval_micros: u64,
        wal_archive_config: WalArchiveConfig,
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

        // Scan for SSTable and WAL files, and clean up any orphaned .tmp files
        if let Ok(entries) = fs::read_dir(&data_dir) {
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

        sst_files.sort_by(|a, b| b.cmp(a));
        wal_files.sort_by_key(|(id, _)| *id);

        // Load manifest to get level assignments
        let manifest = match Manifest::load(&data_dir) {
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
        };

        // Build SstableHandles with mmap readers and Bloom filters for existing SSTables
        // Organize by level according to manifest
        let mut leveled_sstables = LeveledSstables::new();
        for p in &sst_files {
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

        // Create new WAL file with group commit
        let wal = GroupCommitWalWriter::new(&data_dir, next_wal_id, wal_batch_interval_micros)
            .expect("Failed to create initial WAL");

        // Re-write recovered entries to new WAL for safety
        if !recovered_memtable.is_empty() {
            wal.write_entries(&recovered_memtable)
                .expect("Failed to write recovered entries to WAL");
        }

        let mem_state =
            MemTableState::new(memtable_capacity_bytes, recovered_memtable, recovered_size);

        let compaction_config = CompactionConfig {
            l0_compaction_threshold: compaction_trigger_file_count,
            ..CompactionConfig::default()
        };
        let compaction_handler = LeveledCompaction::new(data_dir.clone(), compaction_config);

        LsmTreeEngine {
            mem_state,
            leveled_sstables: Arc::new(Mutex::new(leveled_sstables)),
            data_dir,
            next_sst_id: Arc::new(AtomicU64::new(max_sst_id)),
            compaction_trigger_file_count,
            compaction_in_progress: Arc::new(Mutex::new(false)),
            wal,
            block_cache: Arc::new(BlockCache::new(DEFAULT_BLOCK_CACHE_SIZE_BYTES)),
            metrics: Arc::new(EngineMetrics::new()),
            wal_archive_config,
            snapshot_lock: Arc::new(RwLock::new(())),
            manifest: Arc::new(Mutex::new(manifest)),
            compaction_handler,
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

        // create_from_memtable returns the embedded Bloom filter
        let bloom = sstable::create_from_memtable(&sst_path, &memtable)?;

        // Record flush metrics (estimate bytes from SSTable file size)
        if let Ok(metadata) = std::fs::metadata(&sst_path) {
            self.metrics.record_flush(metadata.len());
        }

        // Open mmap for the newly created SSTable
        let mmap = MappedSSTable::open(&sst_path)?;

        // Get min/max keys and entry count for manifest
        let min_key = mmap.min_key().map(|k| k.to_vec()).unwrap_or_default();
        let max_key = mmap.max_key().map(|k| k.to_vec()).unwrap_or_default();
        let entry_count = mmap.entry_count();
        let file_size = std::fs::metadata(&sst_path).map(|m| m.len()).unwrap_or(0);
        let filename = sst_path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("")
            .to_string();

        let handle = Arc::new(SstableHandle { mmap, bloom });

        {
            let mut leveled = self.leveled_sstables.lock().unwrap();
            leveled.add_to_level(0, handle);
        }

        // Update manifest with new SSTable
        {
            let mut manifest = self.manifest.lock().unwrap();
            manifest.add_entry(ManifestEntry::new(
                filename,
                0,
                &min_key,
                &max_key,
                file_size,
                entry_count,
            ));
            if let Err(e) = manifest.save(&self.data_dir) {
                eprintln!("Warning: Failed to save manifest: {}", e);
            }
        }

        self.mem_state.remove_immutable(&memtable);

        // Rotate WAL: create a new WAL file
        if let Err(e) = self.wal.rotate() {
            eprintln!("Warning: Failed to rotate WAL: {}", e);
        }

        // Archive old WAL files if configured
        self.archive_old_wal_files();

        println!("Flush finished.");

        // Check if compaction is needed
        self.check_compaction();

        Ok(())
    }

    /// Check if compaction should be triggered and run it if needed
    fn check_compaction(&self) {
        // Find which level needs compaction
        let compaction_level = {
            let leveled = self.leveled_sstables.lock().unwrap();

            // Check L0 file count first
            let l0_count = leveled.l0_sstables().len();
            if self.compaction_handler.l0_needs_compaction(l0_count) {
                Some(0)
            } else {
                // Check L1+ for size-based compaction
                let mut needs_compaction = None;
                for level in 1..leveled.level_count() {
                    let level_size = leveled.level_size(level);
                    if self
                        .compaction_handler
                        .level_needs_compaction(level, level_size)
                    {
                        needs_compaction = Some(level);
                        break;
                    }
                }
                needs_compaction
            }
        };

        if let Some(level) = compaction_level {
            let mut in_progress = self.compaction_in_progress.lock().unwrap();
            if !*in_progress {
                *in_progress = true;
                drop(in_progress);

                let engine_clone = self.clone();
                tokio::spawn(async move {
                    if let Err(e) = engine_clone.run_compaction_for_level(level).await {
                        eprintln!("Compaction failed for level {}: {}", level, e);
                    }
                    *engine_clone.compaction_in_progress.lock().unwrap() = false;
                });
            }
        }
    }

    /// Shutdown the engine gracefully, flushing all pending WAL writes
    pub async fn shutdown(&self) {
        self.wal.shutdown().await;
    }

    /// Archive old WAL files based on retention period and size limits.
    /// Only archives WAL files that have been flushed to SSTables.
    fn archive_old_wal_files(&self) {
        // Skip if archiving is disabled
        if self.wal_archive_config.retention_secs.is_none()
            && self.wal_archive_config.max_size_bytes.is_none()
        {
            return;
        }

        // Get the maximum WAL ID that has been flushed to SSTable
        let max_flushed_wal_id = self.get_max_flushed_wal_id();

        // Collect WAL files with their metadata
        let mut wal_files: Vec<(u64, PathBuf, u64, std::time::SystemTime)> = Vec::new();

        if let Ok(entries) = fs::read_dir(&self.data_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if let Some(filename) = path.file_name().and_then(|n| n.to_str())
                    && let Some(wal_id) = GroupCommitWalWriter::parse_wal_filename(filename)
                {
                    // Only consider WAL files that have been flushed (wal_id <= max_flushed_wal_id)
                    if wal_id <= max_flushed_wal_id
                        && let Ok(metadata) = fs::metadata(&path)
                    {
                        let size = metadata.len();
                        let modified = metadata.modified().unwrap_or(std::time::UNIX_EPOCH);
                        wal_files.push((wal_id, path, size, modified));
                    }
                }
            }
        }

        if wal_files.is_empty() {
            return;
        }

        // Sort by WAL ID (oldest first)
        wal_files.sort_by_key(|(id, _, _, _)| *id);

        // Calculate total size
        let total_size: u64 = wal_files.iter().map(|(_, _, size, _)| size).sum();
        let now = std::time::SystemTime::now();

        let mut files_to_delete: Vec<(u64, PathBuf)> = Vec::new();
        let mut remaining_size = total_size;

        for (wal_id, path, size, modified) in &wal_files {
            let mut should_delete = false;

            // Check retention period
            if let Some(retention_secs) = self.wal_archive_config.retention_secs
                && let Ok(age) = now.duration_since(*modified)
                && age.as_secs() > retention_secs
            {
                should_delete = true;
            }

            // Check size limit (delete oldest files first to get under limit)
            if let Some(max_size) = self.wal_archive_config.max_size_bytes
                && remaining_size > max_size
            {
                should_delete = true;
            }

            if should_delete {
                files_to_delete.push((*wal_id, path.clone()));
                remaining_size = remaining_size.saturating_sub(*size);
            }
        }

        // Delete the files
        for (wal_id, path) in files_to_delete {
            match fs::remove_file(&path) {
                Ok(_) => {
                    println!(
                        "Archived (deleted) old WAL file: {:?} (id={})",
                        path, wal_id
                    );
                }
                Err(e) => {
                    eprintln!("Warning: Failed to archive WAL file {:?}: {}", path, e);
                }
            }
        }
    }

    /// Get the maximum WAL ID that has been flushed to SSTables
    fn get_max_flushed_wal_id(&self) -> u64 {
        let leveled = self.leveled_sstables.lock().unwrap();
        let mut max_wal_id = 0u64;

        // Check all levels for the max WAL ID
        for level in 0..leveled.level_count() {
            for handle in leveled.get_level(level) {
                if let Some(wal_id) = sstable::extract_wal_id_from_sstable(handle.mmap.path())
                    && wal_id > max_wal_id
                {
                    max_wal_id = wal_id;
                }
            }
        }

        max_wal_id
    }

    /// Get a snapshot of engine metrics
    pub fn metrics(&self) -> MetricsSnapshot {
        self.metrics.snapshot()
    }

    /// Get block cache statistics
    pub fn cache_stats(&self) -> block_cache::CacheStats {
        self.block_cache.stats()
    }

    /// Reset all metrics counters (for testing)
    #[cfg(test)]
    pub fn reset_metrics(&self) {
        self.metrics.reset();
        self.block_cache.reset_stats();
    }

    /// Get the snapshot lock for use by ReplicationService.
    /// Hold a read lock during snapshot transfer to prevent SSTable deletion.
    pub fn snapshot_lock(&self) -> SnapshotLock {
        Arc::clone(&self.snapshot_lock)
    }

    /// Run leveled compaction for the specified level
    /// - Level 0: Compact all L0 files into L1
    /// - Level 1+: Compact one file from Ln into Ln+1
    async fn run_compaction_for_level(&self, source_level: usize) -> Result<(), Status> {
        println!("Starting leveled compaction for L{}...", source_level);

        let target_level = source_level + 1;

        // Get source and target level file paths
        let (source_paths, target_paths): (Vec<PathBuf>, Vec<PathBuf>) = {
            let leveled = self.leveled_sstables.lock().unwrap();
            let source = leveled
                .get_level(source_level)
                .iter()
                .map(|h| h.mmap.path().to_path_buf())
                .collect();
            let target = leveled
                .get_level(target_level)
                .iter()
                .map(|h| h.mmap.path().to_path_buf())
                .collect();
            (source, target)
        };

        if source_paths.is_empty() {
            println!("No L{} SSTables to compact", source_level);
            return Ok(());
        }

        // Plan compaction based on level
        let task = if source_level == 0 {
            // L0 → L1: Compact all L0 files
            match self
                .compaction_handler
                .plan_l0_compaction(&source_paths, &target_paths)?
            {
                Some(t) => t,
                None => {
                    println!("No L0 compaction needed");
                    return Ok(());
                }
            }
        } else {
            // Ln → Ln+1: Compact one file from source level
            match self.compaction_handler.plan_level_compaction(
                source_level,
                &source_paths,
                &target_paths,
            )? {
                Some(t) => t,
                None => {
                    println!("No L{} compaction needed", source_level);
                    return Ok(());
                }
            }
        };

        println!(
            "Compacting {} L{} files + {} L{} files → L{}",
            task.source_files.len(),
            source_level,
            task.target_files.len(),
            target_level,
            target_level
        );

        // Execute compaction
        let mut next_sst_id = self.next_sst_id.load(Ordering::SeqCst);
        let wal_id = self.wal.current_id();
        let result = self
            .compaction_handler
            .execute(&task, &mut next_sst_id, wal_id)?;

        // Update next_sst_id
        self.next_sst_id.store(next_sst_id, Ordering::SeqCst);

        // Record compaction metrics
        self.metrics
            .record_compaction(result.bytes_read, result.bytes_written);

        // Build handles for new SSTables
        let mut new_handles = Vec::new();
        for output in &result.new_files {
            let mmap = MappedSSTable::open(&output.path)?;
            let bloom = mmap.read_bloom_filter()?;
            new_handles.push((output.level, Arc::new(SstableHandle { mmap, bloom })));
        }

        // Update leveled sstables
        {
            let mut leveled = self.leveled_sstables.lock().unwrap();

            // Remove compacted files from their levels
            for path in &result.files_to_delete {
                // Find and remove the handle
                let filename = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
                for level_idx in 0..leveled.level_count() {
                    let level = &mut leveled.levels[level_idx];
                    if let Some(pos) = level.iter().position(|h| {
                        h.mmap
                            .path()
                            .file_name()
                            .and_then(|n| n.to_str())
                            .unwrap_or("")
                            == filename
                    }) {
                        level.remove(pos);
                        break;
                    }
                }
            }

            // Add new SSTables to their target levels
            for (level, handle) in new_handles {
                leveled.add_to_level(level, handle);
            }
        }

        // Update manifest
        {
            let mut manifest = self.manifest.lock().unwrap();

            // Remove old entries
            for path in &result.files_to_delete {
                let filename = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
                manifest.remove_entry(filename);
            }

            // Add new entries
            for output in &result.new_files {
                manifest.add_entry(output.to_manifest_entry());
            }

            if let Err(e) = manifest.save(&self.data_dir) {
                eprintln!("Warning: Failed to save manifest after compaction: {}", e);
            }
        }

        // Delete old SSTable files
        {
            let _lock = self.snapshot_lock.write().unwrap();
            for path in &result.files_to_delete {
                if let Err(e) = fs::remove_file(path) {
                    eprintln!("Failed to delete old SSTable {:?}: {}", path, e);
                } else {
                    println!("Deleted old SSTable: {:?}", path);
                    self.block_cache.invalidate_file(path);
                }
            }
        }

        println!(
            "Leveled compaction L{} → L{} finished. Created {} new SSTables.",
            source_level,
            target_level,
            result.new_files.len()
        );
        Ok(())
    }
}

impl Engine for LsmTreeEngine {
    fn set(&self, key: String, value: String) -> Result<(), Status> {
        self.metrics.record_set();

        // 0. Wait if too many immutable memtables are pending (write stall prevention)
        self.mem_state.wait_if_stalled();

        let new_val_opt = Some(value);

        // 1. Start writing to WAL
        let wal = self.wal.clone();
        let key_clone = key.clone();
        let val_clone = new_val_opt.clone();
        let (written_rx, synced_rx) = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(wal.append_pipelined(&key_clone, &val_clone))
        })?;

        // 2. Wait for WAL to be written to OS buffer
        let _ =
            tokio::task::block_in_place(|| tokio::runtime::Handle::current().block_on(written_rx));

        // 3. Update MemTable immediately (it becomes visible to Get)
        self.mem_state.insert(key, new_val_opt);
        self.trigger_flush_if_needed();

        // 4. Finally wait for disk sync for durability
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current()
                .block_on(synced_rx)
                .map_err(|_| Status::internal("WAL synced channel closed"))?
        })
    }

    fn get(&self, key: String) -> Result<String, Status> {
        self.metrics.record_get();

        // 1. Check memtable (active and immutable)
        if let Some(val_opt) = self.mem_state.get(&key) {
            return val_opt
                .as_ref()
                .cloned()
                .ok_or_else(|| Status::not_found("Key deleted"));
        }

        let leveled = self.leveled_sstables.lock().unwrap();
        let key_bytes = key.as_bytes();

        // 2. Check L0 SSTables (scan all, newest first - they may have overlapping keys)
        for handle in leveled.l0_sstables() {
            if !handle.bloom.contains(&key) {
                self.metrics.record_bloom_filter_hit();
                continue;
            }
            self.metrics.record_sstable_search();
            match sstable::search_key_mmap(&handle.mmap, &key, &self.block_cache) {
                Ok(Some(v)) => return Ok(v),
                Ok(None) => return Err(Status::not_found("Key deleted")),
                Err(e) if e.code() == tonic::Code::NotFound => {
                    self.metrics.record_bloom_filter_false_positive();
                    continue;
                }
                Err(e) => return Err(e),
            }
        }

        // 3. Check L1+ (non-overlapping, use binary search by key range)
        for level in 1..leveled.level_count() {
            if let Some(handle) = leveled.binary_search_level(level, key_bytes) {
                if !handle.bloom.contains(&key) {
                    self.metrics.record_bloom_filter_hit();
                    continue;
                }
                self.metrics.record_sstable_search();
                match sstable::search_key_mmap(&handle.mmap, &key, &self.block_cache) {
                    Ok(Some(v)) => return Ok(v),
                    Ok(None) => return Err(Status::not_found("Key deleted")),
                    Err(e) if e.code() == tonic::Code::NotFound => {
                        self.metrics.record_bloom_filter_false_positive();
                        continue;
                    }
                    Err(e) => return Err(e),
                }
            }
        }

        Err(Status::not_found("Key not found"))
    }

    fn delete(&self, key: String) -> Result<(), Status> {
        self.metrics.record_delete();

        // 0. Wait if too many immutable memtables are pending (write stall prevention)
        self.mem_state.wait_if_stalled();

        // 1. Start writing tombstone to WAL
        let wal = self.wal.clone();
        let key_clone = key.clone();
        let (written_rx, synced_rx) = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(wal.append_pipelined(&key_clone, &None))
        })?;

        // 2. Wait for WAL to be written to OS buffer
        let _ =
            tokio::task::block_in_place(|| tokio::runtime::Handle::current().block_on(written_rx));

        // 3. Update MemTable
        self.mem_state.insert(key, None);
        self.trigger_flush_if_needed();

        // 4. Wait for disk sync
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current()
                .block_on(synced_rx)
                .map_err(|_| Status::internal("WAL synced channel closed"))?
        })
    }

    fn batch_set(&self, items: Vec<(String, String)>) -> Result<usize, Status> {
        if items.is_empty() {
            return Ok(0);
        }

        // 0. Wait if too many immutable memtables are pending (write stall prevention)
        self.mem_state.wait_if_stalled();

        let count = items.len();
        self.metrics.record_batch_set(count as u64);

        // Convert to WAL format (key, Some(value))
        let entries: Vec<(String, Option<String>)> = items
            .iter()
            .map(|(k, v)| (k.clone(), Some(v.clone())))
            .collect();

        // 1. Start writing batch to WAL
        let wal = self.wal.clone();
        let (written_rx, synced_rx) = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(wal.append_batch_pipelined(entries))
        })?;

        // 2. Wait for WAL to be written to OS buffer
        let _ =
            tokio::task::block_in_place(|| tokio::runtime::Handle::current().block_on(written_rx));

        // 3. Update MemTable with all entries (they become visible to Get)
        for (key, value) in items {
            self.mem_state.insert(key, Some(value));
        }
        self.trigger_flush_if_needed();

        // 4. Wait for disk sync for durability
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current()
                .block_on(synced_rx)
                .map_err(|_| Status::internal("WAL synced channel closed"))?
        })?;

        Ok(count)
    }

    fn batch_get(&self, mut keys: Vec<String>) -> Vec<(String, String)> {
        if keys.is_empty() {
            return Vec::new();
        }

        // Sort keys for better cache locality when accessing SSTables
        keys.sort();
        // Remove duplicates to avoid redundant lookups
        keys.dedup();

        self.metrics.record_batch_get(keys.len() as u64);

        let mut results = Vec::with_capacity(keys.len());

        // First pass: check MemTable for all keys (fast, in-memory)
        // MemTable uses SkipMap which is already thread-safe, but sequential access
        // is efficient enough and maintains sorted order benefits
        let mut remaining_keys = Vec::new();
        for key in keys {
            if let Some(val_opt) = self.mem_state.get(&key) {
                if let Some(value) = val_opt {
                    results.push((key, value));
                }
                // If val_opt is None, it's a tombstone - skip
            } else {
                remaining_keys.push(key);
            }
        }

        // Second pass: check SSTables for remaining keys in parallel
        if !remaining_keys.is_empty() {
            // Get flattened SSTable list: L0 first (newest first), then L1, L2, etc.
            let handles = {
                let leveled = self.leveled_sstables.lock().unwrap();
                leveled.to_flat_list()
            };

            // Process keys in parallel using rayon
            let sstable_results: Vec<(String, String)> = remaining_keys
                .par_iter()
                .filter_map(|key| {
                    // Check each SSTable (L0 newest first, then other levels)
                    for handle in &handles {
                        // Skip if bloom filter says key is definitely not present
                        if !handle.bloom.contains(key) {
                            continue;
                        }

                        match sstable::search_key_mmap(&handle.mmap, key, &self.block_cache) {
                            Ok(Some(v)) => {
                                return Some((key.clone(), v));
                            }
                            Ok(None) => {
                                // Tombstone found - key is deleted
                                return None;
                            }
                            Err(e) if e.code() == tonic::Code::NotFound => {
                                // Key not in this SSTable, continue to next
                                continue;
                            }
                            Err(_) => {
                                // Error reading SSTable, skip this key
                                return None;
                            }
                        }
                    }
                    None
                })
                .collect();

            results.extend(sstable_results);
        }

        results
    }

    fn batch_delete(&self, keys: Vec<String>) -> Result<usize, Status> {
        if keys.is_empty() {
            return Ok(0);
        }

        // 0. Wait if too many immutable memtables are pending (write stall prevention)
        self.mem_state.wait_if_stalled();

        let count = keys.len();
        self.metrics.record_batch_delete(count as u64);

        // Convert to WAL format (key, None for tombstone)
        let entries: Vec<(String, Option<String>)> =
            keys.iter().map(|k| (k.clone(), None)).collect();

        // 1. Start writing batch of tombstones to WAL
        let wal = self.wal.clone();
        let (written_rx, synced_rx) = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(wal.append_batch_pipelined(entries))
        })?;

        // 2. Wait for WAL to be written to OS buffer
        let _ =
            tokio::task::block_in_place(|| tokio::runtime::Handle::current().block_on(written_rx));

        // 3. Update MemTable with tombstones
        for key in keys {
            self.mem_state.insert(key, None);
        }
        self.trigger_flush_if_needed();

        // 4. Wait for disk sync for durability
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current()
                .block_on(synced_rx)
                .map_err(|_| Status::internal("WAL synced channel closed"))?
        })?;

        Ok(count)
    }
}

#[cfg(test)]
mod benchmark;
#[cfg(test)]
mod tests;
