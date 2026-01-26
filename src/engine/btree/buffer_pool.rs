//! Buffer pool with LRU eviction for B-tree pages.
//!
//! Provides caching layer between B-tree operations and disk I/O.
//! Tracks dirty pages and handles flushing.
//!
//! ## Write-Behind (No-Force) Policy
//!
//! This buffer pool implements a Write-Behind (No-Force) policy:
//! - Dirty pages are NOT immediately written to disk on each modification
//! - WAL provides durability (Force policy for WAL, No-Force for data pages)
//! - A background flusher thread periodically writes dirty pages to disk
//! - This reduces eviction latency and prepares for checkpointing

use super::node::{InternalNode, LeafNode};
use super::page::{PAGE_SIZE, Page, PageType};
use super::page_manager::PageManager;
use lru::LruCache;
use std::io;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, RwLock};
use std::thread::{self, JoinHandle};
use std::time::Duration;

/// Default buffer pool size: 64MB (16384 pages)
pub const DEFAULT_POOL_SIZE_BYTES: usize = 64 * 1024 * 1024;
pub const DEFAULT_POOL_SIZE_PAGES: usize = DEFAULT_POOL_SIZE_BYTES / PAGE_SIZE;

/// Default background flush interval in milliseconds
pub const DEFAULT_FLUSH_INTERVAL_MS: u64 = 100;

/// Default maximum dirty pages before triggering flush
pub const DEFAULT_MAX_DIRTY_PAGES: usize = 1000;

/// Cached node type
#[derive(Clone)]
pub enum CachedNode {
    Leaf(LeafNode),
    Internal(InternalNode),
}

impl CachedNode {
    pub fn as_leaf(&self) -> Option<&LeafNode> {
        match self {
            CachedNode::Leaf(node) => Some(node),
            _ => None,
        }
    }

    pub fn as_leaf_mut(&mut self) -> Option<&mut LeafNode> {
        match self {
            CachedNode::Leaf(node) => Some(node),
            _ => None,
        }
    }

    pub fn as_internal(&self) -> Option<&InternalNode> {
        match self {
            CachedNode::Internal(node) => Some(node),
            _ => None,
        }
    }

    pub fn as_internal_mut(&mut self) -> Option<&mut InternalNode> {
        match self {
            CachedNode::Internal(node) => Some(node),
            _ => None,
        }
    }

    pub fn to_page(&self, page_id: u32) -> io::Result<Page> {
        match self {
            CachedNode::Leaf(node) => node.to_page(page_id),
            CachedNode::Internal(node) => node.to_page(page_id),
        }
    }
}

/// Buffer pool entry
struct BufferEntry {
    node: CachedNode,
    dirty: bool,
}

/// Buffer pool statistics
#[derive(Debug, Default)]
pub struct BufferPoolStats {
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
    pub flushes: u64,
    pub background_flushes: u64,
}

/// Buffer pool configuration
#[derive(Debug, Clone)]
pub struct BufferPoolConfig {
    /// Maximum number of pages in the cache
    pub max_pages: usize,
    /// Background flush interval in milliseconds (0 to disable)
    pub flush_interval_ms: u64,
    /// Maximum dirty pages before triggering background flush
    pub max_dirty_pages: usize,
}

impl Default for BufferPoolConfig {
    fn default() -> Self {
        Self {
            max_pages: DEFAULT_POOL_SIZE_PAGES,
            flush_interval_ms: DEFAULT_FLUSH_INTERVAL_MS,
            max_dirty_pages: DEFAULT_MAX_DIRTY_PAGES,
        }
    }
}

/// Shared state between BufferPool and background flusher
struct SharedState {
    cache: RwLock<LruCache<u32, BufferEntry>>,
    page_manager: RwLock<PageManager>,
    // Statistics
    hits: AtomicU64,
    misses: AtomicU64,
    evictions: AtomicU64,
    flushes: AtomicU64,
    background_flushes: AtomicU64,
}

/// LRU-based buffer pool for B-tree pages
pub struct BufferPool {
    state: Arc<SharedState>,
    max_pages: usize,
    // Background flusher
    shutdown: Arc<AtomicBool>,
    flush_notify: Arc<(Mutex<bool>, Condvar)>,
    flusher_handle: Mutex<Option<JoinHandle<()>>>,
}

impl BufferPool {
    /// Create a new buffer pool
    pub fn new(page_manager: PageManager, max_pages: usize) -> Self {
        Self::with_config(
            page_manager,
            BufferPoolConfig {
                max_pages,
                ..Default::default()
            },
        )
    }

    /// Create with custom configuration
    pub fn with_config(page_manager: PageManager, config: BufferPoolConfig) -> Self {
        let capacity = NonZeroUsize::new(config.max_pages.max(16)).unwrap();
        let state = Arc::new(SharedState {
            cache: RwLock::new(LruCache::new(capacity)),
            page_manager: RwLock::new(page_manager),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
            flushes: AtomicU64::new(0),
            background_flushes: AtomicU64::new(0),
        });

        let shutdown = Arc::new(AtomicBool::new(false));
        let flush_notify = Arc::new((Mutex::new(false), Condvar::new()));
        let flusher_handle = if config.flush_interval_ms > 0 {
            let state_clone = Arc::clone(&state);
            let shutdown_clone = Arc::clone(&shutdown);
            let flush_notify_clone = Arc::clone(&flush_notify);
            let flush_interval = Duration::from_millis(config.flush_interval_ms);
            let max_dirty = config.max_dirty_pages;

            let handle = thread::spawn(move || {
                background_flusher(
                    state_clone,
                    shutdown_clone,
                    flush_notify_clone,
                    flush_interval,
                    max_dirty,
                );
            });
            Mutex::new(Some(handle))
        } else {
            Mutex::new(None)
        };

        Self {
            state,
            max_pages: config.max_pages,
            shutdown,
            flush_notify,
            flusher_handle,
        }
    }

    /// Create with default size (64MB)
    pub fn with_default_size(page_manager: PageManager) -> Self {
        Self::new(page_manager, DEFAULT_POOL_SIZE_PAGES)
    }

    /// Get a node from cache or load from disk
    pub fn get(&self, page_id: u32) -> io::Result<CachedNode> {
        // Check cache first
        {
            let mut cache = self.state.cache.write().unwrap();
            if let Some(entry) = cache.get(&page_id) {
                self.state.hits.fetch_add(1, Ordering::Relaxed);
                return Ok(entry.node.clone());
            }
        }

        // Cache miss - load from disk
        self.state.misses.fetch_add(1, Ordering::Relaxed);
        self.load_page(page_id)
    }

    /// Load page from disk and cache it
    fn load_page(&self, page_id: u32) -> io::Result<CachedNode> {
        let page = {
            let mut pm = self.state.page_manager.write().unwrap();
            pm.read_page(page_id)?
        };

        let node = match page.header.page_type {
            PageType::Leaf => CachedNode::Leaf(LeafNode::from_page(&page)?),
            PageType::Internal => CachedNode::Internal(InternalNode::from_page(&page)?),
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Unexpected page type: {:?}", page.header.page_type),
                ));
            }
        };

        // Add to cache, evicting if necessary
        self.insert_to_cache(page_id, node.clone(), false)?;

        Ok(node)
    }

    /// Insert or update a node in the cache
    fn insert_to_cache(&self, page_id: u32, node: CachedNode, dirty: bool) -> io::Result<()> {
        let mut cache = self.state.cache.write().unwrap();

        // Evict if at capacity
        while cache.len() >= self.max_pages {
            if let Some((evicted_id, entry)) = cache.pop_lru() {
                self.state.evictions.fetch_add(1, Ordering::Relaxed);
                if entry.dirty {
                    // Write dirty page to disk (no sync - write-behind)
                    drop(cache); // Release lock before I/O
                    self.flush_single_no_lock(evicted_id, &entry.node)?;
                    cache = self.state.cache.write().unwrap();
                }
            }
        }

        cache.put(page_id, BufferEntry { node, dirty });

        // Notify background flusher if dirty
        if dirty {
            let (lock, cvar) = &*self.flush_notify;
            if let Ok(mut pending) = lock.try_lock() {
                *pending = true;
                cvar.notify_one();
            }
        }

        Ok(())
    }

    /// Put a node in the cache and mark as dirty
    pub fn put(&self, page_id: u32, node: CachedNode) -> io::Result<()> {
        self.insert_to_cache(page_id, node, true)
    }

    /// Mark a page as dirty
    pub fn mark_dirty(&self, page_id: u32) {
        let mut cache = self.state.cache.write().unwrap();
        if let Some(entry) = cache.get_mut(&page_id) {
            entry.dirty = true;
        }
    }

    /// Get a node for modification (marks dirty automatically)
    pub fn get_mut(&self, page_id: u32) -> io::Result<CachedNode> {
        let node = self.get(page_id)?;
        self.mark_dirty(page_id);
        Ok(node)
    }

    /// Flush a single page to disk (without holding cache lock)
    fn flush_single_no_lock(&self, page_id: u32, node: &CachedNode) -> io::Result<()> {
        let page = node.to_page(page_id)?;
        let mut pm = self.state.page_manager.write().unwrap();
        pm.write_page(page_id, &page)?;
        self.state.flushes.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Flush all dirty pages to disk (with sync)
    pub fn flush_all(&self) -> io::Result<()> {
        let dirty_pages: Vec<(u32, CachedNode)> = {
            let mut cache = self.state.cache.write().unwrap();
            cache
                .iter_mut()
                .filter(|(_, entry)| entry.dirty)
                .map(|(&page_id, entry)| {
                    entry.dirty = false;
                    (page_id, entry.node.clone())
                })
                .collect()
        };

        let mut pm = self.state.page_manager.write().unwrap();
        for (page_id, node) in dirty_pages {
            let page = node.to_page(page_id)?;
            pm.write_page(page_id, &page)?;
            self.state.flushes.fetch_add(1, Ordering::Relaxed);
        }

        pm.sync()
    }

    /// Allocate a new page ID
    pub fn allocate_page(&self) -> u32 {
        let pm = self.state.page_manager.read().unwrap();
        pm.allocate_page()
    }

    /// Get page count
    pub fn page_count(&self) -> u32 {
        let pm = self.state.page_manager.read().unwrap();
        pm.page_count()
    }

    /// Set page count (for recovery)
    pub fn set_page_count(&self, count: u32) {
        let pm = self.state.page_manager.read().unwrap();
        pm.set_page_count(count);
    }

    /// Create a new leaf node and add to cache
    pub fn new_leaf(&self) -> io::Result<(u32, LeafNode)> {
        let page_id = self.allocate_page();
        let node = LeafNode::new();
        self.put(page_id, CachedNode::Leaf(node.clone()))?;
        Ok((page_id, node))
    }

    /// Create a new internal node and add to cache
    pub fn new_internal(
        &self,
        first_child: u32,
        key: String,
        second_child: u32,
    ) -> io::Result<(u32, InternalNode)> {
        let page_id = self.allocate_page();
        let node = InternalNode::with_children(first_child, key, second_child);
        self.put(page_id, CachedNode::Internal(node.clone()))?;
        Ok((page_id, node))
    }

    /// Invalidate (remove) a page from cache
    pub fn invalidate(&self, page_id: u32) {
        let mut cache = self.state.cache.write().unwrap();
        cache.pop(&page_id);
    }

    /// Get cache statistics
    pub fn stats(&self) -> BufferPoolStats {
        BufferPoolStats {
            hits: self.state.hits.load(Ordering::Relaxed),
            misses: self.state.misses.load(Ordering::Relaxed),
            evictions: self.state.evictions.load(Ordering::Relaxed),
            flushes: self.state.flushes.load(Ordering::Relaxed),
            background_flushes: self.state.background_flushes.load(Ordering::Relaxed),
        }
    }

    /// Get hit ratio
    pub fn hit_ratio(&self) -> f64 {
        let hits = self.state.hits.load(Ordering::Relaxed);
        let misses = self.state.misses.load(Ordering::Relaxed);
        let total = hits + misses;
        if total == 0 {
            0.0
        } else {
            hits as f64 / total as f64
        }
    }

    /// Get cache size (number of pages)
    pub fn cache_size(&self) -> usize {
        let cache = self.state.cache.read().unwrap();
        cache.len()
    }

    /// Access page manager directly (for meta operations)
    pub fn with_page_manager<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&mut PageManager) -> R,
    {
        let mut pm = self.state.page_manager.write().unwrap();
        f(&mut pm)
    }

    /// Sync underlying file
    pub fn sync(&self) -> io::Result<()> {
        let pm = self.state.page_manager.read().unwrap();
        pm.sync()
    }

    /// Shutdown the background flusher
    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::SeqCst);

        // Wake up the flusher thread
        let (lock, cvar) = &*self.flush_notify;
        if let Ok(mut pending) = lock.lock() {
            *pending = true;
            cvar.notify_one();
        }

        // Wait for the flusher thread to finish
        if let Ok(mut handle) = self.flusher_handle.lock()
            && let Some(h) = handle.take()
        {
            let _ = h.join();
        }
    }
}

impl Drop for BufferPool {
    fn drop(&mut self) {
        self.shutdown();
    }
}

/// Background flusher thread function
fn background_flusher(
    state: Arc<SharedState>,
    shutdown: Arc<AtomicBool>,
    flush_notify: Arc<(Mutex<bool>, Condvar)>,
    flush_interval: Duration,
    max_dirty: usize,
) {
    let (lock, cvar) = &*flush_notify;

    loop {
        // Wait for notification or timeout
        {
            let mut pending = lock.lock().unwrap();
            let result = cvar.wait_timeout(pending, flush_interval).unwrap();
            pending = result.0;
            *pending = false;
        }

        // Check shutdown
        if shutdown.load(Ordering::SeqCst) {
            // Final flush before exiting
            let _ = flush_dirty_pages(&state, max_dirty);
            break;
        }

        // Flush dirty pages
        let _ = flush_dirty_pages(&state, max_dirty);
    }
}

/// Flush dirty pages from the state (called by background flusher)
fn flush_dirty_pages(state: &Arc<SharedState>, max_dirty: usize) -> io::Result<usize> {
    // Count dirty pages
    let dirty_count = {
        let cache = state.cache.read().unwrap();
        cache.iter().filter(|(_, entry)| entry.dirty).count()
    };

    // Only flush if we have enough dirty pages or this is a shutdown flush
    if dirty_count == 0 {
        return Ok(0);
    }

    // Collect dirty pages (limit to max_dirty to avoid long flushes)
    let dirty_pages: Vec<(u32, CachedNode)> = {
        let mut cache = state.cache.write().unwrap();
        let mut pages = Vec::new();
        for (&page_id, entry) in cache.iter_mut() {
            if entry.dirty {
                entry.dirty = false;
                pages.push((page_id, entry.node.clone()));
                if pages.len() >= max_dirty {
                    break;
                }
            }
        }
        pages
    };

    let count = dirty_pages.len();
    if count == 0 {
        return Ok(0);
    }

    // Write pages to disk (no sync - WAL provides durability)
    let mut pm = state.page_manager.write().unwrap();
    for (page_id, node) in dirty_pages {
        let page = node.to_page(page_id)?;
        pm.write_page(page_id, &page)?;
        state.background_flushes.fetch_add(1, Ordering::Relaxed);
    }

    Ok(count)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::btree::node::LeafEntry;
    use tempfile::tempdir;

    /// Create a test buffer pool without background flusher
    fn test_pool(pm: PageManager, max_pages: usize) -> BufferPool {
        BufferPool::with_config(
            pm,
            BufferPoolConfig {
                max_pages,
                flush_interval_ms: 0, // Disable background flusher for tests
                ..Default::default()
            },
        )
    }

    #[test]
    fn test_buffer_pool_basic() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.btree");
        let pm = PageManager::open(&path).unwrap();
        let pool = test_pool(pm, 100);

        // Create a leaf node
        let (page_id, mut leaf) = pool.new_leaf().unwrap();
        leaf.insert(LeafEntry::new("key1".to_string(), "value1".to_string()));
        pool.put(page_id, CachedNode::Leaf(leaf)).unwrap();

        // Read it back
        let node = pool.get(page_id).unwrap();
        let leaf = node.as_leaf().unwrap();
        assert_eq!(leaf.entries[0].key, "key1");
    }

    #[test]
    fn test_buffer_pool_cache_hit() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.btree");
        let pm = PageManager::open(&path).unwrap();
        let pool = test_pool(pm, 100);

        let (page_id, _) = pool.new_leaf().unwrap();

        // First access - miss (but it's already cached from new_leaf)
        let _ = pool.get(page_id).unwrap();
        // Second access - hit
        let _ = pool.get(page_id).unwrap();

        assert!(pool.stats().hits >= 1);
    }

    #[test]
    fn test_buffer_pool_eviction() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.btree");
        let pm = PageManager::open(&path).unwrap();
        let pool = test_pool(pm, 5); // Small cache

        // Create more pages than cache can hold
        for _ in 0..10 {
            pool.new_leaf().unwrap();
        }

        assert!(pool.stats().evictions > 0);
    }

    #[test]
    fn test_buffer_pool_flush() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.btree");
        let pm = PageManager::open(&path).unwrap();
        let pool = test_pool(pm, 100);

        let (page_id, mut leaf) = pool.new_leaf().unwrap();
        leaf.insert(LeafEntry::new("key1".to_string(), "value1".to_string()));
        pool.put(page_id, CachedNode::Leaf(leaf)).unwrap();

        pool.flush_all().unwrap();

        // Clear cache and reload
        pool.invalidate(page_id);
        let node = pool.get(page_id).unwrap();
        let leaf = node.as_leaf().unwrap();
        assert_eq!(leaf.entries[0].key, "key1");
    }

    #[test]
    fn test_buffer_pool_background_flusher() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.btree");
        let pm = PageManager::open(&path).unwrap();

        // Enable background flusher with short interval
        let pool = BufferPool::with_config(
            pm,
            BufferPoolConfig {
                max_pages: 100,
                flush_interval_ms: 10, // 10ms interval
                max_dirty_pages: 10,
            },
        );

        // Create some dirty pages
        let (page_id, mut leaf) = pool.new_leaf().unwrap();
        leaf.insert(LeafEntry::new("key1".to_string(), "value1".to_string()));
        pool.put(page_id, CachedNode::Leaf(leaf)).unwrap();

        // Wait for background flusher
        std::thread::sleep(std::time::Duration::from_millis(50));

        // Check that background flush occurred
        let stats = pool.stats();
        assert!(stats.background_flushes > 0 || stats.flushes > 0);

        // Shutdown should work cleanly
        pool.shutdown();
    }
}
