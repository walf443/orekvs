mod block_cache;
mod bloom;
mod memtable;
mod metrics;
mod mmap;
mod sstable;
mod wal;

use super::Engine;
use block_cache::{BlockCache, DEFAULT_BLOCK_CACHE_SIZE_BYTES};
use bloom::BloomFilter;
use memtable::{MemTable, MemTableState};
pub use metrics::{EngineMetrics, MetricsSnapshot};
use rayon::prelude::*;
use sstable::{MappedSSTable, TimestampedEntry};
use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tonic::Status;
use wal::GroupCommitWalWriter;

/// Handle to an SSTable with mmap reader and its Bloom filter
struct SstableHandle {
    /// Memory-mapped SSTable reader for zero-copy access
    mmap: MappedSSTable,
    /// Bloom filter for fast negative lookups
    bloom: BloomFilter,
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
    // List of SSTable file handles (newest first)
    sstables: Arc<Mutex<Vec<Arc<SstableHandle>>>>,
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
}

impl Clone for LsmTreeEngine {
    fn clone(&self) -> Self {
        LsmTreeEngine {
            mem_state: self.mem_state.clone(),
            sstables: Arc::clone(&self.sstables),
            data_dir: self.data_dir.clone(),
            next_sst_id: Arc::clone(&self.next_sst_id),
            compaction_trigger_file_count: self.compaction_trigger_file_count,
            compaction_in_progress: Arc::clone(&self.compaction_in_progress),
            wal: self.wal.clone(),
            block_cache: Arc::clone(&self.block_cache),
            metrics: Arc::clone(&self.metrics),
            wal_archive_config: self.wal_archive_config.clone(),
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

        // Build SstableHandles with mmap readers and Bloom filters for existing SSTables
        let mut sst_handles = Vec::new();
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
            sst_handles.push(Arc::new(SstableHandle { mmap, bloom }));
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

        LsmTreeEngine {
            mem_state,
            sstables: Arc::new(Mutex::new(sst_handles)),
            data_dir,
            next_sst_id: Arc::new(AtomicU64::new(max_sst_id)),
            compaction_trigger_file_count,
            compaction_in_progress: Arc::new(Mutex::new(false)),
            wal,
            block_cache: Arc::new(BlockCache::new(DEFAULT_BLOCK_CACHE_SIZE_BYTES)),
            metrics: Arc::new(EngineMetrics::new()),
            wal_archive_config,
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

        let handle = Arc::new(SstableHandle { mmap, bloom });

        {
            let mut sstables = self.sstables.lock().unwrap();
            sstables.insert(0, handle);
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
        let sst_count = {
            let sstables = self.sstables.lock().unwrap();
            sstables.len()
        };

        if sst_count >= self.compaction_trigger_file_count {
            let mut in_progress = self.compaction_in_progress.lock().unwrap();
            if !*in_progress {
                *in_progress = true;
                drop(in_progress);

                let engine_clone = self.clone();
                tokio::spawn(async move {
                    if let Err(e) = engine_clone.run_compaction().await {
                        eprintln!("Compaction failed: {}", e);
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
        let sstables = self.sstables.lock().unwrap();
        let mut max_wal_id = 0u64;

        for handle in sstables.iter() {
            if let Some(wal_id) = sstable::extract_wal_id_from_sstable(handle.mmap.path())
                && wal_id > max_wal_id
            {
                max_wal_id = wal_id;
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
    #[allow(dead_code)]
    pub fn reset_metrics(&self) {
        self.metrics.reset();
        self.block_cache.reset_stats();
    }

    /// Run compaction: merge all SSTables into a single new SSTable and delete old ones
    async fn run_compaction(&self) -> Result<(), Status> {
        println!("Starting compaction...");

        // Get current SSTables to compact
        let sst_handles: Vec<Arc<SstableHandle>> = {
            let sstables = self.sstables.lock().unwrap();
            sstables.clone()
        };

        if sst_handles.len() < 2 {
            println!("Not enough SSTables to compact");
            return Ok(());
        }

        // Calculate bytes read from input SSTables
        let bytes_read: u64 = sst_handles
            .iter()
            .filter_map(|h| std::fs::metadata(h.mmap.path()).ok())
            .map(|m| m.len())
            .sum();

        // Merge all SSTables, keeping only the latest value for each key
        let mut merged: BTreeMap<String, TimestampedEntry> = BTreeMap::new();

        for handle in &sst_handles {
            let entries = sstable::read_entries(handle.mmap.path())?;
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

        // Remove tombstones (deleted entries) during compaction
        merged.retain(|_, (_, value)| value.is_some());

        // Write merged data to a new SSTable
        let new_sst_id = self.next_sst_id.fetch_add(1, Ordering::SeqCst);
        // Use the max WAL ID from compacted SSTables
        let max_wal_id = sst_handles
            .iter()
            .filter_map(|h| sstable::extract_wal_id_from_sstable(h.mmap.path()))
            .max()
            .unwrap_or(0);
        let new_sst_path = sstable::generate_path(&self.data_dir, new_sst_id, max_wal_id);

        println!("Writing compacted SSTable to {:?}...", new_sst_path);

        // write_timestamped_entries returns the embedded Bloom filter
        let bloom = sstable::write_timestamped_entries(&new_sst_path, &merged)?;

        // Record compaction metrics
        let bytes_written = std::fs::metadata(&new_sst_path)
            .map(|m| m.len())
            .unwrap_or(0);
        self.metrics.record_compaction(bytes_read, bytes_written);

        // Open mmap for the newly created SSTable
        let mmap = MappedSSTable::open(&new_sst_path)?;

        let new_handle = Arc::new(SstableHandle { mmap, bloom });

        // Update sstables list: remove compacted SSTables and add new one
        // Keep any SSTables that were added during compaction (by flush)
        let compacted_paths: std::collections::HashSet<_> = sst_handles
            .iter()
            .map(|h| h.mmap.path().to_path_buf())
            .collect();
        {
            let mut sstables = self.sstables.lock().unwrap();
            // Keep SSTables that were not part of this compaction
            let mut new_list: Vec<Arc<SstableHandle>> = sstables
                .iter()
                .filter(|h| !compacted_paths.contains(h.mmap.path()))
                .cloned()
                .collect();
            // Add the new compacted SSTable
            new_list.push(new_handle);
            // Sort by path (descending) to maintain newest-first order
            new_list.sort_by(|a, b| b.mmap.path().cmp(a.mmap.path()));
            *sstables = new_list;
        }

        // Delete old SSTable files after updating the list
        for handle in &sst_handles {
            let path = handle.mmap.path();
            if let Err(e) = fs::remove_file(path) {
                eprintln!("Failed to delete old SSTable {:?}: {}", path, e);
            } else {
                println!("Deleted old SSTable: {:?}", path);
                // Invalidate cache entries for deleted file
                self.block_cache.invalidate_file(path);
            }
        }

        println!(
            "Compaction finished. Merged {} SSTables into 1.",
            sst_handles.len()
        );
        Ok(())
    }
}

impl Engine for LsmTreeEngine {
    fn set(&self, key: String, value: String) -> Result<(), Status> {
        self.metrics.record_set();
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

        if let Some(val_opt) = self.mem_state.get(&key) {
            return val_opt
                .as_ref()
                .cloned()
                .ok_or_else(|| Status::not_found("Key deleted"));
        }

        let handles = {
            let sstables = self.sstables.lock().unwrap();
            sstables.clone()
        };
        for handle in handles {
            if !handle.bloom.contains(&key) {
                // Bloom filter says key is definitely not in this SSTable
                self.metrics.record_bloom_filter_hit();
                continue;
            }
            // Bloom filter says key might be in this SSTable, need to search
            self.metrics.record_sstable_search();
            match sstable::search_key_mmap(&handle.mmap, &key, &self.block_cache) {
                Ok(Some(v)) => return Ok(v),
                Ok(None) => return Err(Status::not_found("Key deleted")),
                Err(e) if e.code() == tonic::Code::NotFound => {
                    // Key wasn't actually in SSTable - this was a false positive
                    self.metrics.record_bloom_filter_false_positive();
                    continue;
                }
                Err(e) => return Err(e),
            }
        }
        Err(Status::not_found("Key not found"))
    }

    fn delete(&self, key: String) -> Result<(), Status> {
        self.metrics.record_delete();

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
            let handles = {
                let sstables = self.sstables.lock().unwrap();
                sstables.clone()
            };

            // Process keys in parallel using rayon
            let sstable_results: Vec<(String, String)> = remaining_keys
                .par_iter()
                .filter_map(|key| {
                    // Check each SSTable (newest first)
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
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_lsm_flush_and_get() {
        let dir = tempdir().unwrap();
        let data_dir = dir.path().to_str().unwrap().to_string();
        let engine = LsmTreeEngine::new(data_dir, 50, 4);

        engine.set("k1".to_string(), "v1".to_string()).unwrap();
        engine.set("k2".to_string(), "v2".to_string()).unwrap();
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

        assert_eq!(engine.get("k1".to_string()).unwrap(), "v1");
        assert_eq!(engine.get("k2".to_string()).unwrap(), "v2");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_lsm_recovery() {
        let dir = tempdir().unwrap();
        let data_dir = dir.path().to_str().unwrap().to_string();

        {
            let engine = LsmTreeEngine::new(data_dir.clone(), 50, 4);
            engine.set("k1".to_string(), "v1".to_string()).unwrap();
            engine.set("k2".to_string(), "v2".to_string()).unwrap();
            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
        }

        {
            let engine = LsmTreeEngine::new(data_dir.clone(), 50, 4);
            assert_eq!(engine.get("k1".to_string()).unwrap(), "v1");
            assert_eq!(engine.get("k2".to_string()).unwrap(), "v2");

            engine.set("k3".to_string(), "v3".to_string()).unwrap();
            engine.set("k4".to_string(), "v4".to_string()).unwrap();
            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

            assert_eq!(engine.get("k1".to_string()).unwrap(), "v1");
            assert_eq!(engine.get("k3".to_string()).unwrap(), "v3");
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_lsm_overwrite_size_tracking() {
        let dir = tempdir().unwrap();
        let data_dir = dir.path().to_str().unwrap().to_string();
        // Set capacity to 100 bytes
        let engine = LsmTreeEngine::new(data_dir, 100, 4);

        // 同じキーを何度も更新しても、メモリ上のサイズは増えないはず
        for _ in 0..100 {
            engine.set("key".to_string(), "value".to_string()).unwrap();
        }

        // 少し待ってもSSTableが大量に生成されていないことを確認
        // (バグがあればここで100個近いSSTableが生成される)
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

        let sst_count = engine.sstables.lock().unwrap().len();
        assert!(
            sst_count <= 1,
            "Should not flush multiple times for the same key. Count: {}",
            sst_count
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_compaction_merges_sstables() {
        let dir = tempdir().unwrap();
        let data_dir = dir.path().to_str().unwrap().to_string();
        // Low capacity to trigger multiple flushes
        let engine = LsmTreeEngine::new(data_dir.clone(), 30, 2);

        // Write data in batches to create multiple SSTables
        for i in 0..10 {
            engine
                .set(format!("key{}", i), format!("value{}", i))
                .unwrap();
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        }

        // Wait for flushes and potential compaction
        tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;

        // Verify data is still accessible after compaction
        for i in 0..10 {
            let result = engine.get(format!("key{}", i));
            assert!(result.is_ok(), "key{} should be accessible", i);
            assert_eq!(result.unwrap(), format!("value{}", i));
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_compaction_removes_deleted_keys() {
        let dir = tempdir().unwrap();
        let data_dir = dir.path().to_str().unwrap().to_string();
        let engine = LsmTreeEngine::new(data_dir.clone(), 30, 2);

        // Write some data
        engine
            .set("keep".to_string(), "keep_value".to_string())
            .unwrap();
        engine
            .set("delete_me".to_string(), "will_be_deleted".to_string())
            .unwrap();
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

        // Delete one key
        engine.delete("delete_me".to_string()).unwrap();
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

        // Add more data to trigger compaction
        for i in 0..5 {
            engine
                .set(format!("extra{}", i), format!("extra_value{}", i))
                .unwrap();
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        }

        // Wait for compaction
        tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;

        // Verify kept key is accessible
        assert_eq!(engine.get("keep".to_string()).unwrap(), "keep_value");

        // Verify deleted key is not found
        let result = engine.get("delete_me".to_string());
        assert!(result.is_err());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_compaction_deletes_old_files() {
        let dir = tempdir().unwrap();
        let data_dir = dir.path().to_str().unwrap().to_string();
        let engine = LsmTreeEngine::new(data_dir.clone(), 30, 2);

        // Create multiple SSTables
        for i in 0..8 {
            engine.set(format!("k{}", i), format!("v{}", i)).unwrap();
            tokio::time::sleep(tokio::time::Duration::from_millis(150)).await;
        }

        // Wait for compaction to complete
        tokio::time::sleep(tokio::time::Duration::from_millis(1500)).await;

        // Count SSTable files in directory
        let sst_files: Vec<_> = fs::read_dir(&data_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.path()
                    .file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|name| name.starts_with("sst_") && name.ends_with(".data"))
            })
            .collect();

        // After compaction, should have fewer SSTables
        assert!(
            sst_files.len() <= 2,
            "Expected at most 2 SSTables after compaction, got {}",
            sst_files.len()
        );

        // Data should still be accessible
        for i in 0..8 {
            assert_eq!(engine.get(format!("k{}", i)).unwrap(), format!("v{}", i));
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_wal_recovery_basic() {
        let dir = tempdir().unwrap();
        let data_dir = dir.path().to_str().unwrap().to_string();

        // Write some data without flushing (high capacity)
        {
            let engine = LsmTreeEngine::new(data_dir.clone(), 1024 * 1024, 4);
            engine.set("k1".to_string(), "v1".to_string()).unwrap();
            engine.set("k2".to_string(), "v2".to_string()).unwrap();
            // Engine dropped without flush - data should be in WAL
        }

        // Recover and verify data
        {
            let engine = LsmTreeEngine::new(data_dir.clone(), 1024 * 1024, 4);
            assert_eq!(engine.get("k1".to_string()).unwrap(), "v1");
            assert_eq!(engine.get("k2".to_string()).unwrap(), "v2");
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_wal_recovery_with_delete() {
        let dir = tempdir().unwrap();
        let data_dir = dir.path().to_str().unwrap().to_string();

        // Write and delete data without flushing
        {
            let engine = LsmTreeEngine::new(data_dir.clone(), 1024 * 1024, 4);
            engine.set("k1".to_string(), "v1".to_string()).unwrap();
            engine.set("k2".to_string(), "v2".to_string()).unwrap();
            engine.delete("k1".to_string()).unwrap();
        }

        // Recover and verify
        {
            let engine = LsmTreeEngine::new(data_dir.clone(), 1024 * 1024, 4);
            // k1 should be deleted (tombstone recovered)
            assert!(engine.get("k1".to_string()).is_err());
            // k2 should still exist
            assert_eq!(engine.get("k2".to_string()).unwrap(), "v2");
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_wal_preserved_after_flush() {
        let dir = tempdir().unwrap();
        let data_dir = dir.path().to_str().unwrap().to_string();

        // Write data and trigger flush (low capacity)
        {
            let engine = LsmTreeEngine::new(data_dir.clone(), 30, 4);
            engine.set("k1".to_string(), "v1".to_string()).unwrap();
            engine.set("k2".to_string(), "v2".to_string()).unwrap();
            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
        }

        // Check that WAL files are preserved (for replication)
        let wal_count = fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.path()
                    .file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|name| name.starts_with("wal_") && name.ends_with(".log"))
            })
            .count();

        // Should have at least 1 WAL file (current one)
        assert!(
            wal_count >= 1,
            "Expected at least 1 WAL file, got {}",
            wal_count
        );

        // Data should still be accessible
        {
            let engine = LsmTreeEngine::new(data_dir.clone(), 1024 * 1024, 4);
            assert_eq!(engine.get("k1".to_string()).unwrap(), "v1");
            assert_eq!(engine.get("k2".to_string()).unwrap(), "v2");
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_lsm_versioning_across_sstables() {
        let dir = tempdir().unwrap();
        let data_dir = dir.path().to_str().unwrap().to_string();
        // Low capacity to force flush every write
        let engine = LsmTreeEngine::new(data_dir.clone(), 10, 100);

        // v1 in SSTable 1
        engine.set("key".to_string(), "v1".to_string()).unwrap();
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

        // v2 in SSTable 2
        engine.set("key".to_string(), "v2".to_string()).unwrap();
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

        // v3 in MemTable
        engine.set("key".to_string(), "v3".to_string()).unwrap();

        assert_eq!(engine.get("key".to_string()).unwrap(), "v3");

        // Force flush v3 to SSTable 3
        engine.trigger_flush_if_needed(); // This won't work easily because of capacity check, but another set will
        engine
            .set(
                "other".to_string(),
                "large_value_to_force_flush".to_string(),
            )
            .unwrap();
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

        assert_eq!(engine.get("key".to_string()).unwrap(), "v3");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_lsm_recovery_merges_wal_and_sst() {
        let dir = tempdir().unwrap();
        let data_dir = dir.path().to_str().unwrap().to_string();

        {
            let engine = LsmTreeEngine::new(data_dir.clone(), 30, 100);
            // k1 goes to SSTable
            engine.set("k1".to_string(), "v1".to_string()).unwrap();
            tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

            // k2 triggers another flush to SSTable
            engine.set("k2".to_string(), "v2".to_string()).unwrap();
            // Wait for second flush to complete
            tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

            // k1 updated in WAL/MemTable
            engine.set("k1".to_string(), "v1_new".to_string()).unwrap();

            // Wait for WAL group commit to flush before engine drop
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

            // Properly shutdown to ensure all WAL writes are flushed
            engine.shutdown().await;
        }

        {
            let engine = LsmTreeEngine::new(data_dir.clone(), 1024, 100);
            assert_eq!(engine.get("k1".to_string()).unwrap(), "v1_new");
            assert_eq!(engine.get("k2".to_string()).unwrap(), "v2");
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_lsm_delete_after_flush() {
        let dir = tempdir().unwrap();
        let data_dir = dir.path().to_str().unwrap().to_string();
        let engine = LsmTreeEngine::new(data_dir.clone(), 30, 100);

        // Flush k1 to SSTable
        engine.set("k1".to_string(), "v1".to_string()).unwrap();
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

        assert_eq!(engine.get("k1".to_string()).unwrap(), "v1");

        // Delete k1 (tombstone in MemTable)
        engine.delete("k1".to_string()).unwrap();
        assert!(engine.get("k1".to_string()).is_err());

        // Flush tombstone
        engine
            .set("k2".to_string(), "trigger_flush".repeat(10))
            .unwrap();
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

        // Still should not find k1 (SSTable has v1, but newer SSTable has tombstone)
        assert!(engine.get("k1".to_string()).is_err());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_batch_get_basic() {
        let dir = tempdir().unwrap();
        let data_dir = dir.path().to_str().unwrap().to_string();
        let engine = LsmTreeEngine::new(data_dir, 1024 * 1024, 4);

        // Set some values
        engine.set("a".to_string(), "va".to_string()).unwrap();
        engine.set("b".to_string(), "vb".to_string()).unwrap();
        engine.set("c".to_string(), "vc".to_string()).unwrap();

        // batch_get with existing and non-existing keys
        let keys = vec![
            "c".to_string(),
            "a".to_string(),
            "nonexistent".to_string(),
            "b".to_string(),
        ];
        let results = engine.batch_get(keys);

        // Should return 3 results (excluding nonexistent)
        assert_eq!(results.len(), 3);

        // Results should be returned (order may vary due to sorting)
        let result_map: std::collections::HashMap<_, _> = results.into_iter().collect();
        assert_eq!(result_map.get("a"), Some(&"va".to_string()));
        assert_eq!(result_map.get("b"), Some(&"vb".to_string()));
        assert_eq!(result_map.get("c"), Some(&"vc".to_string()));
        assert_eq!(result_map.get("nonexistent"), None);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_batch_get_with_tombstone() {
        let dir = tempdir().unwrap();
        let data_dir = dir.path().to_str().unwrap().to_string();
        let engine = LsmTreeEngine::new(data_dir, 1024 * 1024, 4);

        engine.set("a".to_string(), "va".to_string()).unwrap();
        engine.set("b".to_string(), "vb".to_string()).unwrap();
        engine.delete("a".to_string()).unwrap();

        let keys = vec!["a".to_string(), "b".to_string()];
        let results = engine.batch_get(keys);

        // Should return only "b" (a is deleted)
        assert_eq!(results.len(), 1);
        assert_eq!(results[0], ("b".to_string(), "vb".to_string()));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_batch_get_with_duplicates() {
        let dir = tempdir().unwrap();
        let data_dir = dir.path().to_str().unwrap().to_string();
        let engine = LsmTreeEngine::new(data_dir, 1024 * 1024, 4);

        engine.set("a".to_string(), "va".to_string()).unwrap();
        engine.set("b".to_string(), "vb".to_string()).unwrap();

        // Request with duplicate keys
        let keys = vec![
            "a".to_string(),
            "b".to_string(),
            "a".to_string(),
            "a".to_string(),
            "b".to_string(),
        ];
        let results = engine.batch_get(keys);

        // Should return only unique keys (duplicates removed)
        assert_eq!(results.len(), 2);
        let result_map: std::collections::HashMap<_, _> = results.into_iter().collect();
        assert_eq!(result_map.get("a"), Some(&"va".to_string()));
        assert_eq!(result_map.get("b"), Some(&"vb".to_string()));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_batch_get_from_sstable() {
        let dir = tempdir().unwrap();
        let data_dir = dir.path().to_str().unwrap().to_string();
        // Small memtable to force flush to SSTable
        let engine = LsmTreeEngine::new(data_dir, 50, 100);

        // Insert data that will be flushed to SSTable
        engine
            .set("key1".to_string(), "value1".to_string())
            .unwrap();
        engine
            .set("key2".to_string(), "value2".to_string())
            .unwrap();
        tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;

        // batch_get should find keys in SSTable
        let keys = vec!["key2".to_string(), "key1".to_string(), "key3".to_string()];
        let results = engine.batch_get(keys);

        assert_eq!(results.len(), 2);
        let result_map: std::collections::HashMap<_, _> = results.into_iter().collect();
        assert_eq!(result_map.get("key1"), Some(&"value1".to_string()));
        assert_eq!(result_map.get("key2"), Some(&"value2".to_string()));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_batch_delete_basic() {
        let dir = tempdir().unwrap();
        let data_dir = dir.path().to_str().unwrap().to_string();
        let engine = LsmTreeEngine::new(data_dir, 1024 * 1024, 4);

        // Set some values
        engine.set("a".to_string(), "va".to_string()).unwrap();
        engine.set("b".to_string(), "vb".to_string()).unwrap();
        engine.set("c".to_string(), "vc".to_string()).unwrap();
        engine.set("d".to_string(), "vd".to_string()).unwrap();

        // Batch delete some keys
        let keys = vec!["a".to_string(), "c".to_string()];
        let deleted = engine.batch_delete(keys).unwrap();
        assert_eq!(deleted, 2);

        // Verify deleted keys are gone
        assert!(engine.get("a".to_string()).is_err());
        assert!(engine.get("c".to_string()).is_err());

        // Verify remaining keys still exist
        assert_eq!(engine.get("b".to_string()).unwrap(), "vb");
        assert_eq!(engine.get("d".to_string()).unwrap(), "vd");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_batch_delete_empty() {
        let dir = tempdir().unwrap();
        let data_dir = dir.path().to_str().unwrap().to_string();
        let engine = LsmTreeEngine::new(data_dir, 1024 * 1024, 4);

        // Batch delete with empty list should return 0
        let deleted = engine.batch_delete(vec![]).unwrap();
        assert_eq!(deleted, 0);
    }

    /// Benchmark test to compare batch_delete vs individual delete
    /// Run with: cargo test bench_batch_delete --release -- --nocapture --ignored
    #[tokio::test(flavor = "multi_thread")]
    #[ignore]
    async fn bench_batch_delete() {
        use std::time::Instant;

        let num_keys = 1000;

        // Benchmark individual delete
        {
            let dir = tempdir().unwrap();
            let data_dir = dir.path().to_str().unwrap().to_string();
            let engine = LsmTreeEngine::new(data_dir, 1024 * 1024, 100);

            // Setup: create keys
            for i in 0..num_keys {
                engine
                    .set(format!("key{:05}", i), format!("value{:05}", i))
                    .unwrap();
            }

            // Benchmark individual deletes
            let start = Instant::now();
            for i in 0..num_keys {
                engine.delete(format!("key{:05}", i)).unwrap();
            }
            let individual_duration = start.elapsed();

            println!("\n=== Batch Delete Benchmark ===");
            println!(
                "Individual delete: {} keys in {:?} ({:.2} keys/sec)",
                num_keys,
                individual_duration,
                num_keys as f64 / individual_duration.as_secs_f64()
            );
        }

        // Benchmark batch delete
        {
            let dir = tempdir().unwrap();
            let data_dir = dir.path().to_str().unwrap().to_string();
            let engine = LsmTreeEngine::new(data_dir, 1024 * 1024, 100);

            // Setup: create keys
            for i in 0..num_keys {
                engine
                    .set(format!("key{:05}", i), format!("value{:05}", i))
                    .unwrap();
            }

            // Benchmark batch delete
            let keys: Vec<String> = (0..num_keys).map(|i| format!("key{:05}", i)).collect();
            let start = Instant::now();
            engine.batch_delete(keys).unwrap();
            let batch_duration = start.elapsed();

            println!(
                "Batch delete:      {} keys in {:?} ({:.2} keys/sec)",
                num_keys,
                batch_duration,
                num_keys as f64 / batch_duration.as_secs_f64()
            );

            println!("=====================================\n");
        }
    }

    /// Benchmark test to measure block cache effectiveness
    /// Run with: cargo test bench_block_cache --release -- --nocapture --ignored
    #[tokio::test(flavor = "multi_thread")]
    #[ignore] // This is a benchmark, not a unit test - run explicitly when needed
    async fn bench_block_cache_effectiveness() {
        use std::time::Instant;

        let dir = tempdir().unwrap();
        let data_dir = dir.path().to_str().unwrap().to_string();

        // Create engine with larger memtable to batch keys into fewer SSTables
        let engine = LsmTreeEngine::new(data_dir.clone(), 50 * 1024, 100);

        // Phase 1: Write data to create SSTables
        let num_keys = 5000;
        println!("\n=== Block Cache Benchmark ===");
        println!("Writing {} keys...", num_keys);

        for i in 0..num_keys {
            engine
                .set(format!("key{:05}", i), format!("value{:05}", i))
                .unwrap();
        }

        // Force flush remaining memtable by writing more data
        for i in 0..100 {
            engine
                .set(format!("flush{:05}", i), "x".repeat(1000))
                .unwrap();
        }

        // Wait for all flushes to complete
        tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;

        let sst_handles: Vec<Arc<SstableHandle>> = engine.sstables.lock().unwrap().clone();
        println!("Created {} SSTables", sst_handles.len());

        // Phase 2: Compare non-mmap vs mmap reads directly on SSTable
        println!("\n--- Direct SSTable Read Comparison ---");

        // Test key that should be in SSTable
        let test_key = "key00100";

        // Non-mmap reads (using search_key with file path)
        let iterations = 1000;
        let start = Instant::now();
        for _ in 0..iterations {
            for handle in &sst_handles {
                let _ = sstable::search_key(handle.mmap.path(), test_key);
            }
        }
        let no_mmap_duration = start.elapsed();
        println!(
            "Without mmap: {} SSTable searches in {:?} ({:.2}/sec)",
            iterations * sst_handles.len(),
            no_mmap_duration,
            (iterations * sst_handles.len()) as f64 / no_mmap_duration.as_secs_f64()
        );

        // Mmap-based reads (using search_key_mmap)
        let start = Instant::now();
        for _ in 0..iterations {
            for handle in &sst_handles {
                let _ = sstable::search_key_mmap(&handle.mmap, test_key, &engine.block_cache);
            }
        }
        let mmap_duration = start.elapsed();
        println!(
            "With mmap:    {} SSTable searches in {:?} ({:.2}/sec)",
            iterations * sst_handles.len(),
            mmap_duration,
            (iterations * sst_handles.len()) as f64 / mmap_duration.as_secs_f64()
        );

        let stats = engine.block_cache.stats();
        println!(
            "\nCache stats: {} entries, {} bytes",
            stats.entries, stats.size_bytes
        );

        let improvement = no_mmap_duration.as_secs_f64() / mmap_duration.as_secs_f64();
        println!("\n=== Results ===");
        println!(
            "Mmap speedup: {:.2}x faster with mmap + block cache",
            improvement
        );
        println!("================================\n");
    }

    /// Benchmark test to measure Bloom filter loading improvement
    /// Run with: cargo test bench_bloom_loading --release -- --nocapture --ignored
    #[tokio::test(flavor = "multi_thread")]
    #[ignore]
    async fn bench_bloom_loading() {
        use std::time::Instant;

        let dir = tempdir().unwrap();
        let data_dir = dir.path().to_str().unwrap().to_string();

        println!("\n=== Bloom Filter Loading Benchmark ===");
        println!("Creating test data...");

        // Create engine and populate with data to create multiple SSTables
        {
            let engine = LsmTreeEngine::new(data_dir.clone(), 50 * 1024, 100);

            // Write enough data to create multiple SSTables
            for i in 0..10000 {
                engine
                    .set(format!("key{:06}", i), format!("value{:06}", i))
                    .unwrap();
            }

            // Force flush
            for i in 0..50 {
                engine
                    .set(format!("flush{:05}", i), "x".repeat(2000))
                    .unwrap();
            }

            tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;
            engine.shutdown().await;
        }

        // Count SSTable files
        let sst_files: Vec<_> = fs::read_dir(&data_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.path()
                    .file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|s| s.starts_with("sst_") && s.ends_with(".data"))
            })
            .map(|e| e.path())
            .collect();

        println!("Created {} SSTables", sst_files.len());

        // Benchmark V4 (read_bloom_filter)
        let iterations = 10;
        let start = Instant::now();
        for _ in 0..iterations {
            for path in &sst_files {
                let _ = sstable::read_bloom_filter(path);
            }
        }
        let v4_duration = start.elapsed();

        // Benchmark V3 style (read_keys + build bloom)
        let start = Instant::now();
        for _ in 0..iterations {
            for path in &sst_files {
                let keys = sstable::read_keys(path).unwrap();
                let mut bloom = BloomFilter::new(keys.len().max(1), 0.01);
                for key in &keys {
                    bloom.insert(key);
                }
            }
        }
        let v3_duration = start.elapsed();

        println!(
            "\n=== Results ({} iterations x {} SSTables) ===",
            iterations,
            sst_files.len()
        );
        println!(
            "V4 (read_bloom_filter):   {:?} ({:.2} ms/SSTable)",
            v4_duration,
            v4_duration.as_secs_f64() * 1000.0 / (iterations * sst_files.len()) as f64
        );
        println!(
            "V3 (read_keys + build):   {:?} ({:.2} ms/SSTable)",
            v3_duration,
            v3_duration.as_secs_f64() * 1000.0 / (iterations * sst_files.len()) as f64
        );
        println!(
            "Speedup: {:.2}x faster with embedded Bloom filter",
            v3_duration.as_secs_f64() / v4_duration.as_secs_f64()
        );
        println!("=====================================\n");
    }
}
