//! Disk-based B-Tree storage engine.
//!
//! A B-tree implementation optimized for disk access with:
//! - Fixed-size 4KB pages
//! - LRU buffer pool for caching
//! - Write-ahead logging for crash recovery
//! - Freelist for page reuse

pub mod buffer_pool;
pub mod freelist;
pub mod node;
pub mod page;
pub mod page_manager;
pub mod wal;

#[cfg(test)]
mod benchmark;
#[cfg(test)]
mod tests;

use self::buffer_pool::{BufferPool, BufferPoolConfig, CachedNode, DEFAULT_POOL_SIZE_PAGES};
use self::freelist::Freelist;
use self::node::{InternalNode, LeafEntry, LeafNode};
use self::page::MetaPage;
use self::page_manager::PageManager;
use self::wal::{GroupCommitWalWriter, RecordType, recover_from_wal};
use crate::engine::Engine;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use tonic::Status;

/// B-tree storage engine configuration
#[derive(Debug, Clone)]
pub struct BTreeConfig {
    /// Buffer pool size in pages
    pub buffer_pool_pages: usize,
    /// Whether to enable WAL
    pub enable_wal: bool,
    /// WAL group commit batch interval in microseconds.
    /// Higher values increase throughput but also increase latency.
    pub wal_batch_interval_micros: u64,
    /// Background flush interval in milliseconds (0 to disable).
    /// Lower values reduce eviction latency but increase I/O.
    pub background_flush_interval_ms: u64,
    /// Maximum dirty pages before triggering background flush.
    pub max_dirty_pages: usize,
}

/// Default batch interval in microseconds (100us)
pub const DEFAULT_WAL_BATCH_INTERVAL_MICROS: u64 = 100;

/// Default background flush interval in milliseconds (100ms)
pub const DEFAULT_BACKGROUND_FLUSH_INTERVAL_MS: u64 = 100;

/// Default maximum dirty pages
pub const DEFAULT_MAX_DIRTY_PAGES: usize = 1000;

impl Default for BTreeConfig {
    fn default() -> Self {
        Self {
            buffer_pool_pages: DEFAULT_POOL_SIZE_PAGES,
            enable_wal: true,
            wal_batch_interval_micros: DEFAULT_WAL_BATCH_INTERVAL_MICROS,
            background_flush_interval_ms: DEFAULT_BACKGROUND_FLUSH_INTERVAL_MS,
            max_dirty_pages: DEFAULT_MAX_DIRTY_PAGES,
        }
    }
}

/// B-tree storage engine
pub struct BTreeEngine {
    /// Data directory
    data_dir: PathBuf,
    /// Buffer pool for page caching
    buffer_pool: BufferPool,
    /// Freelist for page reuse
    freelist: Mutex<Freelist>,
    /// Tree metadata (root, height, etc.)
    meta: RwLock<MetaPage>,
    /// Write-ahead log (group commit)
    wal: Option<GroupCommitWalWriter>,
    /// Configuration
    #[allow(dead_code)]
    config: BTreeConfig,
    /// Latest WAL sequence number written (for tracking during flush)
    current_wal_seq: AtomicU64,
}

#[allow(clippy::result_large_err)]
impl BTreeEngine {
    /// Open or create a B-tree at the specified path
    pub fn open<P: AsRef<Path>>(data_dir: P) -> Result<Self, Status> {
        Self::open_with_config(data_dir, BTreeConfig::default())
    }

    /// Open or create a B-tree with custom configuration
    pub fn open_with_config<P: AsRef<Path>>(
        data_dir: P,
        config: BTreeConfig,
    ) -> Result<Self, Status> {
        let data_dir = data_dir.as_ref().to_path_buf();

        // Create directory if it doesn't exist
        if !data_dir.exists() {
            std::fs::create_dir_all(&data_dir)
                .map_err(|e| Status::internal(format!("Failed to create data directory: {}", e)))?;
        }

        let db_path = data_dir.join("btree.db");
        let wal_path = data_dir.join("btree.wal");

        // Open page manager
        let page_manager = PageManager::open(&db_path)
            .map_err(|e| Status::internal(format!("Failed to open page manager: {}", e)))?;

        // Create buffer pool with write-behind configuration
        let buffer_pool = BufferPool::with_config(
            page_manager,
            BufferPoolConfig {
                max_pages: config.buffer_pool_pages,
                flush_interval_ms: config.background_flush_interval_ms,
                max_dirty_pages: config.max_dirty_pages,
            },
        );

        // Read metadata
        let meta = buffer_pool
            .with_page_manager(|pm| pm.read_meta())
            .map_err(|e| Status::internal(format!("Failed to read metadata: {}", e)))?;

        // Load freelist
        let freelist = if meta.freelist_page_id != 0 {
            buffer_pool
                .with_page_manager(|pm| Freelist::load(pm, meta.freelist_page_id))
                .map_err(|e| Status::internal(format!("Failed to load freelist: {}", e)))?
        } else {
            Freelist::new()
        };

        // Open WAL if enabled (with group commit)
        let wal = if config.enable_wal {
            Some(
                GroupCommitWalWriter::open(&wal_path, config.wal_batch_interval_micros)
                    .map_err(|e| Status::internal(format!("Failed to open WAL: {}", e)))?,
            )
        } else {
            None
        };

        // Initialize current_wal_seq from meta (for tracking during operation)
        let current_wal_seq = AtomicU64::new(meta.last_wal_seq);

        let mut engine = Self {
            data_dir,
            buffer_pool,
            freelist: Mutex::new(freelist),
            meta: RwLock::new(meta),
            wal,
            config,
            current_wal_seq,
        };

        // Perform recovery if needed
        if engine.wal.is_some() {
            engine.recover()?;
        }

        Ok(engine)
    }

    /// Perform WAL recovery using LSN-based filtering
    ///
    /// Only replays records with sequence number > last_wal_seq stored in meta page.
    /// This allows incremental recovery instead of full WAL replay.
    fn recover(&mut self) -> Result<(), Status> {
        let wal_path = self.data_dir.join("btree.wal");

        if !wal_path.exists() {
            return Ok(());
        }

        // Get the last persisted WAL sequence from meta page
        let last_persisted_seq = {
            let meta = self.meta.read().unwrap();
            meta.last_wal_seq
        };

        let records = recover_from_wal(&wal_path)
            .map_err(|e| Status::internal(format!("Failed to read WAL: {}", e)))?;

        // Filter records to only replay those after the last persisted sequence
        let records_to_replay: Vec<_> = records
            .into_iter()
            .filter(|r| r.seq > last_persisted_seq)
            .collect();

        if records_to_replay.is_empty() {
            return Ok(());
        }

        println!(
            "Recovering {} WAL records (seq > {})",
            records_to_replay.len(),
            last_persisted_seq
        );

        // Track the max sequence number we're replaying
        let mut max_replayed_seq = last_persisted_seq;

        // Replay records
        for record in records_to_replay {
            max_replayed_seq = max_replayed_seq.max(record.seq);
            match record.record_type {
                RecordType::Insert => {
                    if let Some(value) = record.value {
                        self.set_internal(record.key, value, false)?;
                    }
                }
                RecordType::Delete => {
                    // Ignore not found errors during recovery (key may have been deleted before crash)
                    let _ = self.delete_internal(record.key, false);
                }
                _ => {}
            }
        }

        // Update current_wal_seq to reflect the replayed records
        self.current_wal_seq
            .store(max_replayed_seq, Ordering::SeqCst);

        // Flush recovered data to persist the changes
        self.flush()?;

        // Optionally truncate WAL after successful recovery
        // This is safe because we've persisted all data and updated last_wal_seq
        if let Some(wal) = &self.wal {
            wal.log_checkpoint()
                .map_err(|e| Status::internal(format!("Failed to write checkpoint: {}", e)))?;
            wal.truncate_after_checkpoint()
                .map_err(|e| Status::internal(format!("Failed to truncate WAL: {}", e)))?;
        }

        Ok(())
    }

    /// Internal set implementation
    fn set_internal(&self, key: String, value: String, log_wal: bool) -> Result<(), Status> {
        // Log to WAL first
        if log_wal && let Some(wal) = &self.wal {
            let seq = wal
                .log_insert(&key, &value)
                .map_err(|e| Status::internal(format!("Failed to write WAL: {}", e)))?;
            // Track the latest WAL sequence number
            self.current_wal_seq.fetch_max(seq, Ordering::SeqCst);
        }

        let entry = LeafEntry::new(key, value);

        let mut meta = self.meta.write().unwrap();

        if meta.root_page_id == 0 {
            // Empty tree - create root leaf
            let (page_id, mut leaf) = self
                .buffer_pool
                .new_leaf()
                .map_err(|e| Status::internal(format!("Failed to create leaf: {}", e)))?;
            leaf.insert(entry);
            self.buffer_pool
                .put(page_id, CachedNode::Leaf(leaf))
                .map_err(|e| Status::internal(format!("Failed to cache leaf: {}", e)))?;
            meta.root_page_id = page_id;
            meta.tree_height = 1;
            meta.entry_count = 1;
            meta.page_count = self.buffer_pool.page_count();
            return Ok(());
        }

        // Traverse to leaf and insert
        let insert_result = self.insert_recursive(meta.root_page_id, entry, meta.tree_height)?;

        if let Some((split_key, right_page_id)) = insert_result {
            // Root split - create new root
            let (new_root_id, _) = self
                .buffer_pool
                .new_internal(meta.root_page_id, split_key, right_page_id)
                .map_err(|e| Status::internal(format!("Failed to create root: {}", e)))?;
            meta.root_page_id = new_root_id;
            meta.tree_height += 1;
        }

        meta.entry_count += 1;
        meta.page_count = self.buffer_pool.page_count();

        Ok(())
    }

    /// Recursive insert, returns split info if split occurred
    fn insert_recursive(
        &self,
        page_id: u32,
        entry: LeafEntry,
        height: u32,
    ) -> Result<Option<(String, u32)>, Status> {
        if height == 1 {
            // Leaf node
            let mut leaf = self.get_leaf(page_id)?;
            let _is_new = leaf.insert(entry.clone());

            if leaf.needs_split() {
                // Split leaf
                let (right, split_key) = leaf.split();
                let right_page_id = self.allocate_page()?;

                // Update next/prev pointers
                let old_next = leaf.next_leaf;
                leaf.next_leaf = right_page_id;
                let mut right = right;
                right.prev_leaf = page_id;
                right.next_leaf = old_next;

                self.buffer_pool
                    .put(page_id, CachedNode::Leaf(leaf))
                    .map_err(|e| Status::internal(format!("Failed to cache left leaf: {}", e)))?;
                self.buffer_pool
                    .put(right_page_id, CachedNode::Leaf(right))
                    .map_err(|e| Status::internal(format!("Failed to cache right leaf: {}", e)))?;

                return Ok(Some((split_key, right_page_id)));
            }

            self.buffer_pool
                .put(page_id, CachedNode::Leaf(leaf))
                .map_err(|e| Status::internal(format!("Failed to cache leaf: {}", e)))?;

            Ok(None)
        } else {
            // Internal node
            let internal = self.get_internal(page_id)?;
            let child_idx = internal.find_child_index(&entry.key);
            let child_page_id = internal.get_child(child_idx);
            drop(internal); // Release before recursive call

            let child_split = self.insert_recursive(child_page_id, entry, height - 1)?;

            if let Some((split_key, right_child)) = child_split {
                let mut internal = self.get_internal(page_id)?;
                internal.insert_after_split(split_key.clone(), right_child);

                if internal.needs_split() {
                    // Split internal node
                    let (right, promoted_key) = internal.split();
                    let right_page_id = self.allocate_page()?;

                    self.buffer_pool
                        .put(page_id, CachedNode::Internal(internal))
                        .map_err(|e| {
                            Status::internal(format!("Failed to cache left internal: {}", e))
                        })?;
                    self.buffer_pool
                        .put(right_page_id, CachedNode::Internal(right))
                        .map_err(|e| {
                            Status::internal(format!("Failed to cache right internal: {}", e))
                        })?;

                    return Ok(Some((promoted_key, right_page_id)));
                }

                self.buffer_pool
                    .put(page_id, CachedNode::Internal(internal))
                    .map_err(|e| Status::internal(format!("Failed to cache internal: {}", e)))?;
            }

            Ok(None)
        }
    }

    /// Internal delete implementation
    fn delete_internal(&self, key: String, log_wal: bool) -> Result<(), Status> {
        // Log to WAL first
        if log_wal && let Some(wal) = &self.wal {
            let seq = wal
                .log_delete(&key)
                .map_err(|e| Status::internal(format!("Failed to write WAL: {}", e)))?;
            // Track the latest WAL sequence number
            self.current_wal_seq.fetch_max(seq, Ordering::SeqCst);
        }

        let mut meta = self.meta.write().unwrap();

        if meta.root_page_id == 0 {
            return Err(Status::not_found("Key not found"));
        }

        // Find the leaf containing the key
        let leaf_page_id = self.find_leaf(meta.root_page_id, &key, meta.tree_height)?;
        let mut leaf = self.get_leaf(leaf_page_id)?;

        if !leaf.delete(&key) {
            return Err(Status::not_found("Key not found"));
        }

        // Check if leaf is empty and can be removed
        // For simplicity, we don't do rebalancing in this implementation
        self.buffer_pool
            .put(leaf_page_id, CachedNode::Leaf(leaf))
            .map_err(|e| Status::internal(format!("Failed to cache leaf: {}", e)))?;

        meta.entry_count = meta.entry_count.saturating_sub(1);

        Ok(())
    }

    /// Find the leaf page containing a key
    fn find_leaf(&self, page_id: u32, key: &str, height: u32) -> Result<u32, Status> {
        if height == 1 {
            return Ok(page_id);
        }

        let internal = self.get_internal(page_id)?;
        let child_idx = internal.find_child_index(key);
        let child_page_id = internal.get_child(child_idx);

        self.find_leaf(child_page_id, key, height - 1)
    }

    /// Get a leaf node from cache/disk
    fn get_leaf(&self, page_id: u32) -> Result<LeafNode, Status> {
        let node = self
            .buffer_pool
            .get(page_id)
            .map_err(|e| Status::internal(format!("Failed to read leaf: {}", e)))?;

        node.as_leaf()
            .cloned()
            .ok_or_else(|| Status::internal("Expected leaf node"))
    }

    /// Get an internal node from cache/disk
    fn get_internal(&self, page_id: u32) -> Result<InternalNode, Status> {
        let node = self
            .buffer_pool
            .get(page_id)
            .map_err(|e| Status::internal(format!("Failed to read internal: {}", e)))?;

        node.as_internal()
            .cloned()
            .ok_or_else(|| Status::internal("Expected internal node"))
    }

    /// Allocate a new page (from freelist or new)
    fn allocate_page(&self) -> Result<u32, Status> {
        let mut freelist = self.freelist.lock().unwrap();
        if let Some(page_id) = freelist.allocate() {
            return Ok(page_id);
        }
        Ok(self.buffer_pool.allocate_page())
    }

    /// Flush all dirty pages to disk
    pub fn flush(&self) -> Result<(), Status> {
        // Flush buffer pool
        self.buffer_pool
            .flush_all()
            .map_err(|e| Status::internal(format!("Failed to flush buffer pool: {}", e)))?;

        // Update last_wal_seq to record which WAL records have been persisted
        let flushed_seq = self.current_wal_seq.load(Ordering::SeqCst);

        // Update and write metadata with the flushed WAL sequence
        let mut meta = self.meta.write().unwrap();
        meta.last_wal_seq = flushed_seq;
        self.buffer_pool
            .with_page_manager(|pm| pm.write_meta(&meta))
            .map_err(|e| Status::internal(format!("Failed to write metadata: {}", e)))?;
        drop(meta);

        // Sync to disk
        self.buffer_pool
            .sync()
            .map_err(|e| Status::internal(format!("Failed to sync: {}", e)))?;

        // Sync WAL
        if let Some(wal) = &self.wal {
            wal.sync()
                .map_err(|e| Status::internal(format!("Failed to sync WAL: {}", e)))?;
        }

        Ok(())
    }

    /// Checkpoint the database
    pub fn checkpoint(&self) -> Result<(), Status> {
        self.flush()?;

        if let Some(wal) = &self.wal {
            wal.log_checkpoint()
                .map_err(|e| Status::internal(format!("Failed to write checkpoint: {}", e)))?;
            wal.truncate_after_checkpoint()
                .map_err(|e| Status::internal(format!("Failed to truncate WAL: {}", e)))?;
        }

        Ok(())
    }

    /// Get buffer pool statistics
    pub fn buffer_pool_stats(&self) -> buffer_pool::BufferPoolStats {
        self.buffer_pool.stats()
    }

    /// Get entry count
    pub fn entry_count(&self) -> u64 {
        let meta = self.meta.read().unwrap();
        meta.entry_count
    }

    /// Get tree height
    pub fn tree_height(&self) -> u32 {
        let meta = self.meta.read().unwrap();
        meta.tree_height
    }
}

impl Engine for BTreeEngine {
    fn set(&self, key: String, value: String) -> Result<(), Status> {
        self.set_internal(key, value, true)
    }

    fn get(&self, key: String) -> Result<String, Status> {
        let meta = self.meta.read().unwrap();

        if meta.root_page_id == 0 {
            return Err(Status::not_found("Key not found"));
        }

        // Traverse to leaf
        let leaf_page_id = self.find_leaf(meta.root_page_id, &key, meta.tree_height)?;
        let leaf = self.get_leaf(leaf_page_id)?;

        match leaf.get(&key) {
            Some(entry) if !entry.is_tombstone() => Ok(entry.value.clone().unwrap()),
            _ => Err(Status::not_found("Key not found")),
        }
    }

    fn delete(&self, key: String) -> Result<(), Status> {
        self.delete_internal(key, true)
    }

    fn batch_set(&self, items: Vec<(String, String)>) -> Result<usize, Status> {
        if items.is_empty() {
            return Ok(0);
        }

        // Begin batch in WAL
        if let Some(wal) = &self.wal {
            wal.begin_batch()
                .map_err(|e| Status::internal(format!("Failed to begin batch: {}", e)))?;
        }

        let mut count = 0;
        for (key, value) in items {
            self.set_internal(key, value, true)?;
            count += 1;
        }

        // End batch in WAL
        if let Some(wal) = &self.wal {
            wal.end_batch()
                .map_err(|e| Status::internal(format!("Failed to end batch: {}", e)))?;
        }

        Ok(count)
    }

    fn batch_delete(&self, keys: Vec<String>) -> Result<usize, Status> {
        if keys.is_empty() {
            return Ok(0);
        }

        // Begin batch in WAL
        if let Some(wal) = &self.wal {
            wal.begin_batch()
                .map_err(|e| Status::internal(format!("Failed to begin batch: {}", e)))?;
        }

        let mut count = 0;
        for key in keys {
            match self.delete_internal(key, true) {
                Ok(()) => count += 1,
                Err(e) if e.code() == tonic::Code::NotFound => continue,
                Err(e) => return Err(e),
            }
        }

        // End batch in WAL
        if let Some(wal) = &self.wal {
            wal.end_batch()
                .map_err(|e| Status::internal(format!("Failed to end batch: {}", e)))?;
        }

        Ok(count)
    }
}

impl Drop for BTreeEngine {
    fn drop(&mut self) {
        // Shutdown buffer pool's background flusher first
        self.buffer_pool.shutdown();

        // Try to flush on drop
        let _ = self.flush();

        // Close WAL
        if let Some(wal) = &self.wal {
            let _ = wal.close();
        }
    }
}

/// Wrapper to hold BTree engine reference for graceful shutdown
pub struct BTreeEngineHolder {
    engine: Option<Arc<BTreeEngine>>,
}

impl BTreeEngineHolder {
    pub fn new() -> Self {
        BTreeEngineHolder { engine: None }
    }

    pub fn set(&mut self, engine: Arc<BTreeEngine>) {
        self.engine = Some(engine);
    }

    pub fn shutdown(&self) {
        if let Some(ref engine) = self.engine {
            // Flush all dirty pages and close WAL
            if let Err(e) = engine.flush() {
                eprintln!("Error flushing BTree engine during shutdown: {}", e);
            }
            println!("BTree engine shutdown complete.");
        }
    }
}

impl Default for BTreeEngineHolder {
    fn default() -> Self {
        Self::new()
    }
}

/// Wrapper to implement Engine for Arc<BTreeEngine>
pub struct BTreeEngineWrapper(pub Arc<BTreeEngine>);

impl Engine for BTreeEngineWrapper {
    fn set(&self, key: String, value: String) -> Result<(), Status> {
        self.0.set(key, value)
    }

    fn get(&self, key: String) -> Result<String, Status> {
        self.0.get(key)
    }

    fn delete(&self, key: String) -> Result<(), Status> {
        self.0.delete(key)
    }

    fn batch_set(&self, items: Vec<(String, String)>) -> Result<usize, Status> {
        self.0.batch_set(items)
    }

    fn batch_get(&self, keys: Vec<String>) -> Vec<(String, String)> {
        self.0.batch_get(keys)
    }

    fn batch_delete(&self, keys: Vec<String>) -> Result<usize, Status> {
        self.0.batch_delete(keys)
    }
}
