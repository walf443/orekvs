//! Buffer pool with LRU eviction for B-tree pages.
//!
//! Provides caching layer between B-tree operations and disk I/O.
//! Tracks dirty pages and handles flushing.

use super::node::{InternalNode, LeafNode};
use super::page::{PAGE_SIZE, Page, PageType};
use super::page_manager::PageManager;
use lru::LruCache;
use std::io;
use std::num::NonZeroUsize;
use std::sync::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};

/// Default buffer pool size: 64MB (16384 pages)
pub const DEFAULT_POOL_SIZE_BYTES: usize = 64 * 1024 * 1024;
pub const DEFAULT_POOL_SIZE_PAGES: usize = DEFAULT_POOL_SIZE_BYTES / PAGE_SIZE;

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
}

/// LRU-based buffer pool for B-tree pages
pub struct BufferPool {
    cache: RwLock<LruCache<u32, BufferEntry>>,
    page_manager: RwLock<PageManager>,
    max_pages: usize,
    // Statistics
    hits: AtomicU64,
    misses: AtomicU64,
    evictions: AtomicU64,
    flushes: AtomicU64,
}

impl BufferPool {
    /// Create a new buffer pool
    pub fn new(page_manager: PageManager, max_pages: usize) -> Self {
        let capacity = NonZeroUsize::new(max_pages.max(16)).unwrap();
        Self {
            cache: RwLock::new(LruCache::new(capacity)),
            page_manager: RwLock::new(page_manager),
            max_pages,
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
            flushes: AtomicU64::new(0),
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
            let mut cache = self.cache.write().unwrap();
            if let Some(entry) = cache.get(&page_id) {
                self.hits.fetch_add(1, Ordering::Relaxed);
                return Ok(entry.node.clone());
            }
        }

        // Cache miss - load from disk
        self.misses.fetch_add(1, Ordering::Relaxed);
        self.load_page(page_id)
    }

    /// Load page from disk and cache it
    fn load_page(&self, page_id: u32) -> io::Result<CachedNode> {
        let page = {
            let mut pm = self.page_manager.write().unwrap();
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
        let mut cache = self.cache.write().unwrap();

        // Evict if at capacity
        while cache.len() >= self.max_pages {
            if let Some((evicted_id, entry)) = cache.pop_lru() {
                self.evictions.fetch_add(1, Ordering::Relaxed);
                if entry.dirty {
                    // Write dirty page to disk
                    drop(cache); // Release lock before I/O
                    self.flush_single(evicted_id, &entry.node)?;
                    cache = self.cache.write().unwrap();
                }
            }
        }

        cache.put(page_id, BufferEntry { node, dirty });
        Ok(())
    }

    /// Put a node in the cache and mark as dirty
    pub fn put(&self, page_id: u32, node: CachedNode) -> io::Result<()> {
        self.insert_to_cache(page_id, node, true)
    }

    /// Mark a page as dirty
    pub fn mark_dirty(&self, page_id: u32) {
        let mut cache = self.cache.write().unwrap();
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

    /// Flush a single page to disk
    fn flush_single(&self, page_id: u32, node: &CachedNode) -> io::Result<()> {
        let page = node.to_page(page_id)?;
        let mut pm = self.page_manager.write().unwrap();
        pm.write_page(page_id, &page)?;
        self.flushes.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Flush all dirty pages to disk
    pub fn flush_all(&self) -> io::Result<()> {
        let dirty_pages: Vec<(u32, CachedNode)> = {
            let mut cache = self.cache.write().unwrap();
            cache
                .iter_mut()
                .filter(|(_, entry)| entry.dirty)
                .map(|(&page_id, entry)| {
                    entry.dirty = false;
                    (page_id, entry.node.clone())
                })
                .collect()
        };

        let mut pm = self.page_manager.write().unwrap();
        for (page_id, node) in dirty_pages {
            let page = node.to_page(page_id)?;
            pm.write_page(page_id, &page)?;
            self.flushes.fetch_add(1, Ordering::Relaxed);
        }

        pm.sync()
    }

    /// Allocate a new page ID
    pub fn allocate_page(&self) -> u32 {
        let pm = self.page_manager.read().unwrap();
        pm.allocate_page()
    }

    /// Get page count
    pub fn page_count(&self) -> u32 {
        let pm = self.page_manager.read().unwrap();
        pm.page_count()
    }

    /// Set page count (for recovery)
    pub fn set_page_count(&self, count: u32) {
        let pm = self.page_manager.read().unwrap();
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
        let mut cache = self.cache.write().unwrap();
        cache.pop(&page_id);
    }

    /// Get cache statistics
    pub fn stats(&self) -> BufferPoolStats {
        BufferPoolStats {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            evictions: self.evictions.load(Ordering::Relaxed),
            flushes: self.flushes.load(Ordering::Relaxed),
        }
    }

    /// Get hit ratio
    pub fn hit_ratio(&self) -> f64 {
        let hits = self.hits.load(Ordering::Relaxed);
        let misses = self.misses.load(Ordering::Relaxed);
        let total = hits + misses;
        if total == 0 {
            0.0
        } else {
            hits as f64 / total as f64
        }
    }

    /// Get cache size (number of pages)
    pub fn cache_size(&self) -> usize {
        let cache = self.cache.read().unwrap();
        cache.len()
    }

    /// Access page manager directly (for meta operations)
    pub fn with_page_manager<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&mut PageManager) -> R,
    {
        let mut pm = self.page_manager.write().unwrap();
        f(&mut pm)
    }

    /// Sync underlying file
    pub fn sync(&self) -> io::Result<()> {
        let pm = self.page_manager.read().unwrap();
        pm.sync()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::btree::node::LeafEntry;
    use tempfile::tempdir;

    #[test]
    fn test_buffer_pool_basic() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.btree");
        let pm = PageManager::open(&path).unwrap();
        let pool = BufferPool::new(pm, 100);

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
        let pool = BufferPool::new(pm, 100);

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
        let pool = BufferPool::new(pm, 5); // Small cache

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
        let pool = BufferPool::new(pm, 100);

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
}
