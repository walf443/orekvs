mod block_cache;
mod bloom;
mod buffer_pool;
mod common_prefix_compression_index;
mod compaction;
pub mod composite_key;
mod manifest;
mod memtable;
mod metrics;
mod mmap;
mod recovery;
mod sstable;
mod wal;

use super::{Engine, current_timestamp};
use crate::engine::wal::GroupCommitConfig;
use block_cache::{BlockCache, DEFAULT_BLOCK_CACHE_SIZE_BYTES};
use bloom::BloomFilter;
use compaction::{CompactionConfig, LeveledCompaction};
use manifest::{Manifest, ManifestEntry};
use memtable::{MemTable, MemTableState, MemValue};
pub use metrics::{EngineMetrics, MetricsSnapshot};
use rayon::prelude::*;
use sstable::MappedSSTable;
use sstable::levels::{Level0, SortedLevel, SstableLevel};
use std::collections::hash_map::DefaultHasher;
use std::fs;
use std::hash::{Hash, Hasher};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use tonic::Status;
use wal::GroupCommitWalWriter;

/// Number of stripes for CAS lock (power of 2 for efficient modulo)
const CAS_LOCK_STRIPES: usize = 256;

/// Striped lock for CAS operations.
/// Allows concurrent CAS operations on different keys while serializing
/// operations on the same key (or keys that hash to the same stripe).
struct CasLockStripe {
    locks: Vec<Mutex<()>>,
}

impl CasLockStripe {
    fn new(num_stripes: usize) -> Self {
        let locks = (0..num_stripes).map(|_| Mutex::new(())).collect();
        Self { locks }
    }

    fn get_lock(&self, key: &str) -> &Mutex<()> {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        let hash = hasher.finish() as usize;
        &self.locks[hash % self.locks.len()]
    }
}

/// Lock to prevent SSTable deletion during snapshot transfer.
/// Snapshot transfer holds a read lock; compaction deletion requires a write lock.
pub type SnapshotLock = Arc<RwLock<()>>;

/// Handle to an SSTable with mmap reader and its Bloom filter
pub(crate) struct SstableHandle {
    /// Memory-mapped SSTable reader for zero-copy access
    pub(crate) mmap: MappedSSTable,
    /// Bloom filter for fast negative lookups
    pub(crate) bloom: BloomFilter,
}

/// Default maximum level for leveled compaction
const MAX_LEVELS: usize = 7;

/// Manages SSTables organized by levels for leveled compaction.
///
/// Uses separate implementations for L0 (overlapping, time-ordered) and
/// L1+ (non-overlapping, key-sorted) levels.
pub(crate) struct LeveledSstables {
    /// Level 0: overlapping SSTables sorted by creation time (newest first)
    l0: Level0,
    /// Levels 1+: non-overlapping SSTables sorted by min_key
    sorted_levels: Vec<SortedLevel>,
}

impl LeveledSstables {
    /// Create a new empty LeveledSstables structure
    pub(crate) fn new() -> Self {
        let sorted_levels = (1..MAX_LEVELS).map(SortedLevel::new).collect();
        Self {
            l0: Level0::new(),
            sorted_levels,
        }
    }

    /// Add an SSTable to a specific level
    pub(crate) fn add_to_level(&mut self, level: usize, handle: Arc<SstableHandle>) {
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
    fn remove(&mut self, handle: &Arc<SstableHandle>) {
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
    fn level_count(&self) -> usize {
        1 + self.sorted_levels.len()
    }

    /// Get SSTables at a specific level
    fn get_level(&self, level: usize) -> &[Arc<SstableHandle>] {
        if level == 0 {
            self.l0.sstables()
        } else if level <= self.sorted_levels.len() {
            self.sorted_levels[level - 1].sstables()
        } else {
            &[]
        }
    }

    /// Get all L0 SSTables (for scanning during reads)
    fn l0_sstables(&self) -> &[Arc<SstableHandle>] {
        self.l0.sstables()
    }

    /// Get SSTables in L1+ that overlap with the given key range
    #[cfg(test)]
    fn get_overlapping(
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
    fn candidates_for_key(&self, level: usize, key: &[u8]) -> Vec<Arc<SstableHandle>> {
        if level == 0 {
            self.l0.candidates_for_key(key)
        } else if level <= self.sorted_levels.len() {
            self.sorted_levels[level - 1].candidates_for_key(key)
        } else {
            Vec::new()
        }
    }

    /// Remove an SSTable by filename from any level
    fn remove_by_filename(&mut self, filename: &str) -> bool {
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
    fn total_sstable_count(&self) -> usize {
        self.l0.len() + self.sorted_levels.iter().map(|l| l.len()).sum::<usize>()
    }

    /// Get the total size of a level in bytes
    fn level_size(&self, level: usize) -> u64 {
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
    fn from_flat_list(sstables: Vec<Arc<SstableHandle>>) -> Self {
        let mut leveled = Self::new();
        for handle in sstables {
            leveled.l0.add(handle);
        }
        leveled
    }

    /// Convert to flat SSTable list (for backward compatibility)
    /// Returns all SSTables from all levels, L0 first (newest first),
    /// then L1, L2, etc.
    fn to_flat_list(&self) -> Vec<Arc<SstableHandle>> {
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

// Re-export WalArchiveConfig and constants from common wal module
pub use crate::engine::wal::{
    DEFAULT_WAL_MAX_SIZE_BYTES, DEFAULT_WAL_RETENTION_SECS, WalArchiveConfig,
};

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
    // Striped locks for CAS (compare-and-set) atomicity
    // Allows concurrent CAS on different keys while serializing same-key operations
    cas_locks: Arc<CasLockStripe>,
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
            cas_locks: Arc::clone(&self.cas_locks),
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
        Self::new_with_full_config(
            data_dir_str,
            memtable_capacity_bytes,
            compaction_trigger_file_count,
            wal_batch_interval_micros,
            wal_archive_config,
            true, // enable_stale_compaction
        )
    }

    /// Create a new LSM-Tree engine with full configuration options
    pub fn new_with_full_config(
        data_dir_str: String,
        memtable_capacity_bytes: u64,
        compaction_trigger_file_count: usize,
        wal_batch_interval_micros: u64,
        wal_archive_config: WalArchiveConfig,
        enable_stale_compaction: bool,
    ) -> Self {
        let data_dir = PathBuf::from(&data_dir_str);

        if !data_dir.exists() {
            fs::create_dir_all(&data_dir).expect("Failed to create data directory");
        }

        // Recover state from disk (SSTables, WAL, manifest)
        let mut recovery_result = recovery::recover(&data_dir);

        // Register recovered WAL files in manifest for proper cleanup
        // This ensures old WAL files can be deleted after their entries are flushed
        if !recovery_result.wal_file_max_seqs.is_empty() {
            for (wal_id, max_seq) in &recovery_result.wal_file_max_seqs {
                recovery_result
                    .manifest
                    .record_wal_file_max_seq(*wal_id, *max_seq);
            }
            if let Err(e) = recovery_result.manifest.save(&data_dir) {
                eprintln!(
                    "Warning: Failed to save manifest after registering WAL files: {}",
                    e
                );
            } else {
                println!(
                    "Registered {} WAL files in manifest for cleanup tracking",
                    recovery_result.wal_file_max_seqs.len()
                );
            }
        }

        // Create new WAL file with group commit, starting from max recovered sequence number
        let wal_config = GroupCommitConfig::with_batch_interval(wal_batch_interval_micros);
        let wal = GroupCommitWalWriter::new_with_seq(
            &data_dir,
            recovery_result.next_wal_id,
            wal_config,
            recovery_result.max_wal_seq,
        )
        .expect("Failed to create initial WAL");

        // Re-write recovered entries to new WAL for safety
        if !recovery_result.recovered_memtable.is_empty() {
            wal.write_entries(&recovery_result.recovered_memtable)
                .expect("Failed to write recovered entries to WAL");
        }

        let mem_state = MemTableState::new(
            memtable_capacity_bytes,
            recovery_result.recovered_memtable,
            recovery_result.recovered_size,
        );

        let compaction_config = CompactionConfig {
            l0_compaction_threshold: compaction_trigger_file_count,
            ..CompactionConfig::default()
        };
        let compaction_handler = LeveledCompaction::new(data_dir.clone(), compaction_config);

        let engine = LsmTreeEngine {
            mem_state,
            leveled_sstables: Arc::new(Mutex::new(recovery_result.leveled_sstables)),
            data_dir,
            next_sst_id: Arc::new(AtomicU64::new(recovery_result.next_sst_id)),
            compaction_trigger_file_count,
            compaction_in_progress: Arc::new(Mutex::new(false)),
            wal,
            block_cache: Arc::new(BlockCache::new(DEFAULT_BLOCK_CACHE_SIZE_BYTES)),
            metrics: Arc::new(EngineMetrics::new()),
            wal_archive_config,
            snapshot_lock: Arc::new(RwLock::new(())),
            manifest: Arc::new(Mutex::new(recovery_result.manifest)),
            compaction_handler,
            cas_locks: Arc::new(CasLockStripe::new(CAS_LOCK_STRIPES)),
        };

        // Trigger background compaction for stale SSTables (non-blocking)
        if enable_stale_compaction {
            engine.trigger_stale_compaction();
        }

        engine
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
        let min_key = mmap.min_key().to_vec();
        let max_key = mmap.max_key().to_vec();
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

        // Update manifest with new SSTable and current WAL sequence
        {
            let mut manifest = self.manifest.lock().unwrap();
            manifest.add_entry(ManifestEntry::new(
                filename,
                0,
                &min_key,
                &max_key,
                file_size,
                Some(entry_count),
            ));
            // Record the current WAL sequence for LSN-based recovery
            // All entries with seq <= this value are now persisted in SSTables
            manifest.set_last_flushed_wal_seq(self.wal.current_seq().saturating_sub(1));
            if let Err(e) = manifest.save(&self.data_dir) {
                eprintln!("Warning: Failed to save manifest: {}", e);
            }
        }

        self.mem_state.remove_immutable(&memtable);

        // Record old WAL file's max sequence before rotating
        let old_wal_id = self.wal.current_id();
        let old_wal_max_seq = self.wal.current_seq().saturating_sub(1);

        // Rotate WAL: create a new WAL file
        if let Err(e) = self.wal.rotate() {
            eprintln!("Warning: Failed to rotate WAL: {}", e);
        } else {
            // Record the old WAL file's max sequence for LSN-based cleanup
            let mut manifest = self.manifest.lock().unwrap();
            manifest.record_wal_file_max_seq(old_wal_id, old_wal_max_seq);
            if let Err(e) = manifest.save(&self.data_dir) {
                eprintln!("Warning: Failed to save manifest after WAL rotation: {}", e);
            }
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

    /// Check if there are stale SSTables that need compaction.
    /// A SSTable is considered stale if its WAL ID is significantly older than the current WAL ID.
    /// Returns the count of stale SSTables.
    fn count_stale_sstables(&self, current_wal_id: u64, threshold: u64) -> usize {
        let leveled = self.leveled_sstables.lock().unwrap();
        let mut stale_count = 0;

        for level in 0..leveled.level_count() {
            for handle in leveled.get_level(level) {
                if let Some(wal_id) = sstable::extract_wal_id_from_sstable(handle.mmap.path())
                    && current_wal_id > wal_id
                    && current_wal_id - wal_id >= threshold
                {
                    stale_count += 1;
                }
            }
        }

        stale_count
    }

    /// Trigger compaction for stale SSTables asynchronously.
    /// This runs in the background and doesn't block startup.
    fn trigger_stale_compaction(&self) {
        let current_wal_id = self.wal.current_id();

        // Only check for stale files if WAL ID is high enough
        // This prevents unnecessary compaction on fresh/test setups
        const MIN_WAL_ID_FOR_STALE_CHECK: u64 = 3;
        if current_wal_id < MIN_WAL_ID_FOR_STALE_CHECK {
            return;
        }

        // Consider SSTables stale if their WAL ID is at least 2 behind current
        const STALE_THRESHOLD: u64 = 2;

        let stale_count = self.count_stale_sstables(current_wal_id, STALE_THRESHOLD);

        if stale_count == 0 {
            return;
        }

        println!(
            "Found {} stale SSTables (WAL ID < {}), triggering background compaction",
            stale_count,
            current_wal_id.saturating_sub(STALE_THRESHOLD)
        );

        let engine_clone = self.clone();
        tokio::spawn(async move {
            engine_clone.compact_stale_l0().await;
        });
    }

    /// Compact L0 level to clean up stale SSTables after recovery.
    /// L0 compaction merges all L0 files with overlapping L1 files,
    /// which effectively cleans up old SSTables by merging them into newer ones.
    async fn compact_stale_l0(&self) {
        // Wait a bit before starting to allow the engine to fully initialize
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // Wait for any ongoing compaction to finish first
        loop {
            let is_in_progress = {
                let in_progress = self.compaction_in_progress.lock().unwrap();
                *in_progress
            };
            if !is_in_progress {
                break;
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
        }

        // Compact L0 (this is where stale files accumulate)
        // L0 compaction merges all L0 files with overlapping L1 files
        let has_l0_files = {
            let leveled = self.leveled_sstables.lock().unwrap();
            !leveled.get_level(0).is_empty()
        };

        if !has_l0_files {
            println!("Stale compaction: no L0 files to compact");
            return;
        }

        {
            let mut in_progress = self.compaction_in_progress.lock().unwrap();
            *in_progress = true;
        }

        println!("Stale compaction: running L0 compaction");
        if let Err(e) = self.run_compaction_for_level(0).await {
            eprintln!("Stale compaction failed for L0: {}", e);
        }

        {
            let mut in_progress = self.compaction_in_progress.lock().unwrap();
            *in_progress = false;
        }

        println!("Background stale SSTable compaction finished");
    }

    /// Archive old WAL files based on LSN, retention period and size limits.
    /// Only archives WAL files where all entries have been flushed to SSTables.
    fn archive_old_wal_files(&self) {
        // Skip if archiving is disabled
        if self.wal_archive_config.retention_secs.is_none()
            && self.wal_archive_config.max_size_bytes.is_none()
        {
            return;
        }

        // Get WAL IDs that can be safely deleted based on LSN
        let deletable_wal_ids: std::collections::HashSet<u64> = {
            let manifest = self.manifest.lock().unwrap();
            manifest.get_deletable_wal_ids().into_iter().collect()
        };

        // Collect WAL files with their metadata
        let mut wal_files: Vec<(u64, PathBuf, u64, std::time::SystemTime)> = Vec::new();

        if let Ok(entries) = fs::read_dir(&self.data_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if let Some(filename) = path.file_name().and_then(|n| n.to_str())
                    && let Some(wal_id) = GroupCommitWalWriter::parse_wal_filename(filename)
                {
                    // Only consider WAL files that have been fully flushed (based on LSN)
                    if deletable_wal_ids.contains(&wal_id)
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

        // Delete the files and update manifest
        if !files_to_delete.is_empty() {
            let mut deleted_ids = Vec::new();
            for (wal_id, path) in &files_to_delete {
                match fs::remove_file(path) {
                    Ok(_) => {
                        println!(
                            "Archived (deleted) old WAL file: {:?} (id={})",
                            path, wal_id
                        );
                        deleted_ids.push(*wal_id);
                    }
                    Err(e) => {
                        eprintln!("Warning: Failed to archive WAL file {:?}: {}", path, e);
                    }
                }
            }

            // Remove deleted WAL files from manifest tracking
            if !deleted_ids.is_empty() {
                let mut manifest = self.manifest.lock().unwrap();
                for wal_id in deleted_ids {
                    manifest.remove_wal_file_seq(wal_id);
                }
                if let Err(e) = manifest.save(&self.data_dir) {
                    eprintln!("Warning: Failed to save manifest after WAL cleanup: {}", e);
                }
            }
        }
    }

    /// Get the maximum WAL ID that has been flushed to SSTables
    /// Note: Now using LSN-based cleanup via manifest.get_deletable_wal_ids()
    #[allow(dead_code)]
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
                let filename = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
                leveled.remove_by_filename(filename);
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

impl LsmTreeEngine {
    #[allow(clippy::result_large_err)]
    fn set_internal(&self, key: String, value: String, expire_at: u64) -> Result<(), Status> {
        self.metrics.record_set();
        self.write_entry(key, Some(value), expire_at)
    }

    /// Common write logic for SET and CAS operations
    /// Writes to WAL, updates MemTable, and waits for durability
    #[allow(clippy::result_large_err)]
    fn write_entry(
        &self,
        key: String,
        value: Option<String>,
        expire_at: u64,
    ) -> Result<(), Status> {
        // Wait if too many immutable memtables are pending (write stall prevention)
        self.mem_state.wait_if_stalled();

        // 1. Start writing to WAL
        let wal = self.wal.clone();
        let key_clone = key.clone();
        let val_clone = value.clone();
        let (written_rx, synced_rx) = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current()
                .block_on(wal.append_pipelined_with_ttl(&key_clone, &val_clone, expire_at))
        })?;

        // 2. Wait for WAL to be written to OS buffer
        let _ =
            tokio::task::block_in_place(|| tokio::runtime::Handle::current().block_on(written_rx));

        // 3. Update MemTable immediately (it becomes visible to Get)
        self.mem_state
            .insert(key, MemValue::new_with_ttl(value, expire_at));
        self.trigger_flush_if_needed();

        // 4. Finally wait for disk sync for durability
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current()
                .block_on(synced_rx)
                .map_err(|_| Status::internal("WAL synced channel closed"))?
                .map(|_seq| ()) // Discard the sequence number
        })
    }
}

impl Engine for LsmTreeEngine {
    fn set(&self, key: String, value: String) -> Result<(), Status> {
        self.set_internal(key, value, 0)
    }

    fn set_with_ttl(&self, key: String, value: String, ttl_secs: u64) -> Result<(), Status> {
        let expire_at = current_timestamp() + ttl_secs;
        self.set_internal(key, value, expire_at)
    }

    fn get_with_expire_at(&self, key: String) -> Result<(String, u64), Status> {
        self.metrics.record_get();
        let now = current_timestamp();

        // 1. Check memtable (active and immutable)
        if let Some(mem_value) = self.mem_state.get(&key) {
            if mem_value.is_tombstone() {
                return Err(Status::not_found("Key deleted"));
            }
            if mem_value.is_expired(now) {
                return Err(Status::not_found("Key expired"));
            }
            return mem_value
                .value
                .map(|v| (v, mem_value.expire_at))
                .ok_or_else(|| Status::not_found("Key deleted"));
        }

        let leveled = self.leveled_sstables.lock().unwrap();
        let key_bytes = key.as_bytes();

        // 2. Check all levels (L0 scans all, L1+ uses binary search)
        for level in 0..leveled.level_count() {
            for handle in leveled.candidates_for_key(level, key_bytes) {
                if !handle.bloom.contains(&key) {
                    self.metrics.record_bloom_filter_hit();
                    continue;
                }
                self.metrics.record_sstable_search();
                match sstable::search_key_mmap_with_expire(
                    &handle.mmap,
                    &key,
                    &self.block_cache,
                    now,
                ) {
                    Ok(Some((value_opt, expire_at))) => {
                        // Check if it's a tombstone
                        if value_opt.is_none() {
                            return Err(Status::not_found("Key deleted"));
                        }
                        // Check if expired
                        if expire_at > 0 && now > expire_at {
                            return Err(Status::not_found("Key expired"));
                        }
                        return Ok((value_opt.unwrap(), expire_at));
                    }
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
            tokio::runtime::Handle::current()
                .block_on(wal.append_pipelined_with_ttl(&key_clone, &None, 0))
        })?;

        // 2. Wait for WAL to be written to OS buffer
        let _ =
            tokio::task::block_in_place(|| tokio::runtime::Handle::current().block_on(written_rx));

        // 3. Update MemTable with tombstone
        self.mem_state.insert(key, MemValue::new(None));
        self.trigger_flush_if_needed();

        // 4. Wait for disk sync
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current()
                .block_on(synced_rx)
                .map_err(|_| Status::internal("WAL synced channel closed"))?
                .map(|_seq| ()) // Discard the sequence number
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

        // Convert to WAL format (key, Some(value), expire_at=0)
        let entries: Vec<(String, Option<String>, u64)> = items
            .iter()
            .map(|(k, v)| (k.clone(), Some(v.clone()), 0u64))
            .collect();

        // 1. Start writing batch to WAL
        let wal = self.wal.clone();
        let (written_rx, synced_rx) = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(wal.append_batch_pipelined_with_ttl(entries))
        })?;

        // 2. Wait for WAL to be written to OS buffer
        let _ =
            tokio::task::block_in_place(|| tokio::runtime::Handle::current().block_on(written_rx));

        // 3. Update MemTable with all entries (they become visible to Get)
        for (key, value) in items {
            self.mem_state.insert(key, MemValue::new(Some(value)));
        }
        self.trigger_flush_if_needed();

        // 4. Wait for disk sync for durability
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current()
                .block_on(synced_rx)
                .map_err(|_| Status::internal("WAL synced channel closed"))?
                .map(|_seq| ()) // Discard the sequence number
        })?;

        Ok(count)
    }

    fn batch_set_with_expire_at(&self, items: Vec<(String, String, u64)>) -> Result<usize, Status> {
        if items.is_empty() {
            return Ok(0);
        }

        // 0. Wait if too many immutable memtables are pending (write stall prevention)
        self.mem_state.wait_if_stalled();

        let count = items.len();
        self.metrics.record_batch_set(count as u64);

        // Convert to WAL format (key, Some(value), expire_at)
        let entries: Vec<(String, Option<String>, u64)> = items
            .iter()
            .map(|(k, v, e)| (k.clone(), Some(v.clone()), *e))
            .collect();

        // 1. Start writing batch to WAL
        let wal = self.wal.clone();
        let (written_rx, synced_rx) = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(wal.append_batch_pipelined_with_ttl(entries))
        })?;

        // 2. Wait for WAL to be written to OS buffer
        let _ =
            tokio::task::block_in_place(|| tokio::runtime::Handle::current().block_on(written_rx));

        // 3. Update MemTable with all entries (they become visible to Get)
        for (key, value, expire_at) in items {
            self.mem_state
                .insert(key, MemValue::new_with_ttl(Some(value), expire_at));
        }
        self.trigger_flush_if_needed();

        // 4. Wait for disk sync for durability
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current()
                .block_on(synced_rx)
                .map_err(|_| Status::internal("WAL synced channel closed"))?
                .map(|_seq| ()) // Discard the sequence number
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
        let now = current_timestamp();

        // First pass: check MemTable for all keys (fast, in-memory)
        // MemTable uses SkipMap which is already thread-safe, but sequential access
        // is efficient enough and maintains sorted order benefits
        let mut remaining_keys = Vec::new();
        for key in keys {
            if let Some(mem_value) = self.mem_state.get(&key) {
                if mem_value.is_valid(now)
                    && let Some(value) = mem_value.value
                {
                    results.push((key, value));
                }
                // If tombstone or expired, skip
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

                        match sstable::search_key_mmap(&handle.mmap, key, &self.block_cache, now) {
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

        // Convert to WAL format (key, None for tombstone, expire_at=0)
        let entries: Vec<(String, Option<String>, u64)> =
            keys.iter().map(|k| (k.clone(), None, 0u64)).collect();

        // 1. Start writing batch of tombstones to WAL
        let wal = self.wal.clone();
        let (written_rx, synced_rx) = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(wal.append_batch_pipelined_with_ttl(entries))
        })?;

        // 2. Wait for WAL to be written to OS buffer
        let _ =
            tokio::task::block_in_place(|| tokio::runtime::Handle::current().block_on(written_rx));

        // 3. Update MemTable with tombstones
        for key in keys {
            self.mem_state.insert(key, MemValue::new(None));
        }
        self.trigger_flush_if_needed();

        // 4. Wait for disk sync for durability
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current()
                .block_on(synced_rx)
                .map_err(|_| Status::internal("WAL synced channel closed"))?
                .map(|_seq| ()) // Discard the sequence number
        })?;

        Ok(count)
    }

    fn compare_and_set(
        &self,
        key: String,
        expected_value: Option<String>,
        new_value: String,
        expire_at: u64,
    ) -> Result<(bool, Option<String>), Status> {
        // Take striped CAS lock for atomicity (serializes CAS operations on the same key)
        let _cas_guard = self.cas_locks.get_lock(&key).lock().unwrap();

        let now = current_timestamp();

        // Get current value (from memtable or SSTables)
        let current_value = {
            // Check memtable first
            if let Some(mem_value) = self.mem_state.get(&key) {
                if mem_value.is_tombstone() || mem_value.is_expired(now) {
                    None
                } else {
                    mem_value.value
                }
            } else {
                // Check SSTables
                let leveled = self.leveled_sstables.lock().unwrap();
                let key_bytes = key.as_bytes();

                let mut found_value = None;
                'level_loop: for level in 0..leveled.level_count() {
                    for handle in leveled.candidates_for_key(level, key_bytes) {
                        if !handle.bloom.contains(&key) {
                            continue;
                        }
                        match sstable::search_key_mmap_with_expire(
                            &handle.mmap,
                            &key,
                            &self.block_cache,
                            now,
                        ) {
                            Ok(Some((value_opt, entry_expire_at))) => {
                                if value_opt.is_none() {
                                    // Tombstone
                                    break 'level_loop;
                                }
                                if entry_expire_at > 0 && now > entry_expire_at {
                                    // Expired
                                    break 'level_loop;
                                }
                                found_value = value_opt;
                                break 'level_loop;
                            }
                            Ok(None) => break 'level_loop, // Tombstone
                            Err(e) if e.code() == tonic::Code::NotFound => continue,
                            Err(_) => continue,
                        }
                    }
                }
                found_value
            }
        };

        // Check if CAS condition is met
        let should_update = match (&expected_value, &current_value) {
            (None, None) => true,     // Key should not exist, and it doesn't
            (None, Some(_)) => false, // Key should not exist, but it does
            (Some(expected), Some(current)) if expected == current => true, // Values match
            (Some(_), _) => false,    // Values don't match or key doesn't exist
        };

        if !should_update {
            return Ok((false, current_value));
        }

        // Perform the update (still holding the CAS lock)
        self.metrics.record_set();
        self.write_entry(key, Some(new_value), expire_at)?;

        Ok((true, current_value))
    }
}

/// Wrapper to hold LSM engine reference for graceful shutdown
pub struct LsmEngineHolder {
    engine: Option<Arc<LsmTreeEngine>>,
}

impl LsmEngineHolder {
    pub fn new() -> Self {
        LsmEngineHolder { engine: None }
    }

    pub fn set(&mut self, engine: Arc<LsmTreeEngine>) {
        self.engine = Some(engine);
    }

    pub fn engine(&self) -> Option<&Arc<LsmTreeEngine>> {
        self.engine.as_ref()
    }

    pub async fn shutdown(&self) {
        if let Some(ref engine) = self.engine {
            engine.shutdown().await;
        }
    }
}

impl Default for LsmEngineHolder {
    fn default() -> Self {
        Self::new()
    }
}

/// Wrapper to implement Engine for Arc<LsmTreeEngine>
pub struct LsmTreeEngineWrapper(pub Arc<LsmTreeEngine>);

impl Engine for LsmTreeEngineWrapper {
    fn set(&self, key: String, value: String) -> Result<(), Status> {
        self.0.set(key, value)
    }

    fn set_with_ttl(&self, key: String, value: String, ttl_secs: u64) -> Result<(), Status> {
        self.0.set_with_ttl(key, value, ttl_secs)
    }

    fn delete(&self, key: String) -> Result<(), Status> {
        self.0.delete(key)
    }

    fn batch_set(&self, items: Vec<(String, String)>) -> Result<usize, Status> {
        self.0.batch_set(items)
    }

    fn batch_set_with_expire_at(&self, items: Vec<(String, String, u64)>) -> Result<usize, Status> {
        self.0.batch_set_with_expire_at(items)
    }

    fn batch_get(&self, keys: Vec<String>) -> Vec<(String, String)> {
        self.0.batch_get(keys)
    }

    fn batch_delete(&self, keys: Vec<String>) -> Result<usize, Status> {
        self.0.batch_delete(keys)
    }

    fn get_with_expire_at(&self, key: String) -> Result<(String, u64), Status> {
        self.0.get_with_expire_at(key)
    }

    fn compare_and_set(
        &self,
        key: String,
        expected_value: Option<String>,
        new_value: String,
        expire_at: u64,
    ) -> Result<(bool, Option<String>), Status> {
        self.0
            .compare_and_set(key, expected_value, new_value, expire_at)
    }
}

#[cfg(test)]
mod benchmark;
#[cfg(test)]
mod tests;

#[cfg(test)]
mod leveled_sstables_tests {
    use super::*;

    #[test]
    fn test_from_flat_list_empty() {
        let leveled = LeveledSstables::from_flat_list(Vec::new());
        assert_eq!(leveled.total_sstable_count(), 0);
        assert_eq!(leveled.l0_sstables().len(), 0);

        // to_flat_list should work on empty
        let flat = leveled.to_flat_list();
        assert!(flat.is_empty());
    }
}
