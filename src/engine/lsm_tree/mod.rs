mod block_cache;
mod bloom;
mod buffer_pool;
mod common_prefix_compression_index;
mod compaction;
pub mod composite_key;
mod flush;
mod manifest;
mod memtable;
mod metrics;
mod mmap;
mod recovery;
mod sstable;
mod wal;
mod wrapper;

use super::{Engine, current_timestamp};
use crate::engine::cas::CasLockStripe;
use crate::engine::wal::GroupCommitConfig;
use block_cache::{BlockCache, DEFAULT_BLOCK_CACHE_SIZE_BYTES};
use bloom::BloomFilter;
use compaction::{CompactionConfig, LeveledCompaction};
use manifest::Manifest;
use memtable::{MemTableState, MemValue};
pub use metrics::{EngineMetrics, MetricsSnapshot};
use rayon::prelude::*;
use sstable::MappedSSTable;
use sstable::levels::LeveledSstables;
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex, RwLock};
use tonic::Status;
use wal::GroupCommitWalWriter;
pub use wrapper::{LsmEngineHolder, LsmTreeEngineWrapper};

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
    // Lock to serialize flush operations
    flush_lock: Arc<Mutex<()>>,
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
            flush_lock: Arc::clone(&self.flush_lock),
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
            flush_lock: Arc::new(Mutex::new(())),
            wal,
            block_cache: Arc::new(BlockCache::new(DEFAULT_BLOCK_CACHE_SIZE_BYTES)),
            metrics: Arc::new(EngineMetrics::new()),
            wal_archive_config,
            snapshot_lock: Arc::new(RwLock::new(())),
            manifest: Arc::new(Mutex::new(recovery_result.manifest)),
            compaction_handler,
            cas_locks: Arc::new(CasLockStripe::new()),
        };

        // Trigger background compaction for stale SSTables (non-blocking)
        if enable_stale_compaction {
            engine.trigger_stale_compaction();
        }

        engine
    }

    /// Shutdown the engine gracefully, flushing all pending WAL writes
    pub async fn shutdown(&self) {
        self.wal.shutdown().await;
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
