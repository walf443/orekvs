use lru::LruCache;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use super::common_prefix_compression_index::IndexEntry;

/// Default block cache size: 64 MB
pub const DEFAULT_BLOCK_CACHE_SIZE_BYTES: usize = 64 * 1024 * 1024;

/// Key for cache lookup
#[derive(Clone, Hash, Eq, PartialEq, Debug)]
pub struct BlockCacheKey {
    pub file_path: PathBuf,
    /// Block offset in file. u64::MAX is reserved for index entries.
    pub block_offset: u64,
}

impl BlockCacheKey {
    pub fn for_block(file_path: PathBuf, block_offset: u64) -> Self {
        Self {
            file_path,
            block_offset,
        }
    }

    pub fn for_index(file_path: PathBuf) -> Self {
        Self {
            file_path,
            block_offset: u64::MAX,
        }
    }
}

/// Parsed block entry: (key, value_option, expire_at)
/// Entries are sorted by key for binary search
/// expire_at: 0 = no expiration, >0 = Unix timestamp when key expires
/// Key is Arc<str> to allow O(1) clone operations during count/scan operations
pub type ParsedBlockEntry = (Arc<str>, Option<String>, u64);

/// Cached entry types
#[derive(Clone)]
pub enum CacheEntry {
    /// Decompressed and parsed index (composite_key, block_offset, max_expire_at)
    Index(Arc<Vec<IndexEntry>>),
    /// Parsed block entries (sorted by key for binary search)
    ParsedBlock(Arc<Vec<ParsedBlockEntry>>),
}

impl CacheEntry {
    pub fn size_bytes(&self) -> usize {
        match self {
            // key.len() + block_offset(8) + max_expire_at(8)
            CacheEntry::Index(index) => index.iter().map(|(k, _, _)| k.len() + 16).sum::<usize>(),
            CacheEntry::ParsedBlock(entries) => entries
                .iter()
                .map(|(k, v, _expire_at)| k.len() + v.as_ref().map_or(0, |s| s.len()) + 8)
                .sum::<usize>(),
        }
    }
}

/// Thread-safe block cache with LRU eviction
pub struct BlockCache {
    inner: RwLock<BlockCacheInner>,
    // Metrics (atomic for lock-free access)
    hits: AtomicU64,
    misses: AtomicU64,
    evictions: AtomicU64,
}

struct BlockCacheInner {
    cache: LruCache<BlockCacheKey, CacheEntry>,
    current_size_bytes: usize,
    max_size_bytes: usize,
}

impl BlockCache {
    /// Create a new block cache with the given maximum size in bytes
    pub fn new(max_size_bytes: usize) -> Self {
        // Use a large capacity for the LRU cache; actual eviction is based on bytes
        let capacity = NonZeroUsize::new(100_000).unwrap();
        Self {
            inner: RwLock::new(BlockCacheInner {
                cache: LruCache::new(capacity),
                current_size_bytes: 0,
                max_size_bytes,
            }),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
        }
    }

    /// Get an entry from the cache, promoting it to most-recently-used
    pub fn get(&self, key: &BlockCacheKey) -> Option<CacheEntry> {
        let mut inner = self.inner.write().ok()?;
        match inner.cache.get(key).cloned() {
            Some(entry) => {
                self.hits.fetch_add(1, Ordering::Relaxed);
                Some(entry)
            }
            None => {
                self.misses.fetch_add(1, Ordering::Relaxed);
                None
            }
        }
    }

    /// Insert an entry into the cache, evicting old entries if necessary
    pub fn insert(&self, key: BlockCacheKey, value: CacheEntry) {
        let entry_size = value.size_bytes();

        let Ok(mut inner) = self.inner.write() else {
            return;
        };

        // If entry already exists, remove old size
        if let Some(old_entry) = inner.cache.peek(&key) {
            inner.current_size_bytes = inner
                .current_size_bytes
                .saturating_sub(old_entry.size_bytes());
        }

        // Evict entries until we have enough space
        while inner.current_size_bytes + entry_size > inner.max_size_bytes {
            if let Some((_, evicted)) = inner.cache.pop_lru() {
                inner.current_size_bytes = inner
                    .current_size_bytes
                    .saturating_sub(evicted.size_bytes());
                self.evictions.fetch_add(1, Ordering::Relaxed);
            } else {
                break;
            }
        }

        // Insert new entry
        inner.cache.put(key, value);
        inner.current_size_bytes += entry_size;
    }

    /// Invalidate all cache entries for a specific file
    pub fn invalidate_file(&self, file_path: &Path) {
        let Ok(mut inner) = self.inner.write() else {
            return;
        };

        // Collect keys to remove
        let keys_to_remove: Vec<BlockCacheKey> = inner
            .cache
            .iter()
            .filter(|(k, _)| k.file_path.as_path() == file_path)
            .map(|(k, _)| k.clone())
            .collect();

        // Remove entries
        for key in keys_to_remove {
            if let Some(entry) = inner.cache.pop(&key) {
                inner.current_size_bytes =
                    inner.current_size_bytes.saturating_sub(entry.size_bytes());
            }
        }
    }

    /// Get cache statistics
    pub fn stats(&self) -> CacheStats {
        let inner = self.inner.read().unwrap();
        let hits = self.hits.load(Ordering::Relaxed);
        let misses = self.misses.load(Ordering::Relaxed);
        let total = hits + misses;
        CacheStats {
            entries: inner.cache.len(),
            size_bytes: inner.current_size_bytes,
            max_size_bytes: inner.max_size_bytes,
            hits,
            misses,
            evictions: self.evictions.load(Ordering::Relaxed),
            hit_ratio: if total > 0 {
                hits as f64 / total as f64
            } else {
                0.0
            },
        }
    }

    /// Reset cache statistics counters (for testing)
    #[cfg(test)]
    pub fn reset_stats(&self) {
        self.hits.store(0, Ordering::Relaxed);
        self.misses.store(0, Ordering::Relaxed);
        self.evictions.store(0, Ordering::Relaxed);
    }
}

/// Cache statistics
#[derive(Debug, Clone)]
pub struct CacheStats {
    pub entries: usize,
    pub size_bytes: usize,
    pub max_size_bytes: usize,
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
    pub hit_ratio: f64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cache_basic_operations() {
        let cache = BlockCache::new(1024);

        let key = BlockCacheKey::for_block(PathBuf::from("/test/file.data"), 0);
        let entries = Arc::new(vec![
            (Arc::from("key1"), Some("value1".to_string()), 0),
            (Arc::from("key2"), Some("value2".to_string()), 0),
        ]);

        // Insert
        cache.insert(key.clone(), CacheEntry::ParsedBlock(Arc::clone(&entries)));

        // Get
        let result = cache.get(&key);
        assert!(result.is_some());
        if let Some(CacheEntry::ParsedBlock(retrieved)) = result {
            assert_eq!(retrieved.len(), 2);
        }

        // Stats
        let stats = cache.stats();
        assert_eq!(stats.entries, 1);
        // key1(4) + value1(6) + expire_at(8) + key2(4) + value2(6) + expire_at(8) = 36
        assert_eq!(stats.size_bytes, 36);
    }

    #[test]
    fn test_cache_eviction() {
        let cache = BlockCache::new(250);

        // Insert entries that exceed cache size
        for i in 0..5 {
            let key = BlockCacheKey::for_block(PathBuf::from("/test/file.data"), i * 100);
            // Create entries with predictable sizes: 50 + 50 + 8 = 108 bytes per entry
            let entries = Arc::new(vec![(
                Arc::from("x".repeat(50).as_str()),
                Some("y".repeat(50)),
                0,
            )]);
            cache.insert(key, CacheEntry::ParsedBlock(entries));
        }

        // Cache should have evicted older entries
        let stats = cache.stats();
        assert!(stats.size_bytes <= 250);
    }

    #[test]
    fn test_cache_invalidate_file() {
        let cache = BlockCache::new(1024);

        // Insert entries for two different files
        let file1 = PathBuf::from("/test/file1.data");
        let file2 = PathBuf::from("/test/file2.data");

        cache.insert(
            BlockCacheKey::for_block(file1.clone(), 0),
            CacheEntry::ParsedBlock(Arc::new(vec![(Arc::from("k1"), Some("v1".to_string()), 0)])),
        );
        cache.insert(
            BlockCacheKey::for_block(file1.clone(), 100),
            CacheEntry::ParsedBlock(Arc::new(vec![(Arc::from("k2"), Some("v2".to_string()), 0)])),
        );
        cache.insert(
            BlockCacheKey::for_block(file2.clone(), 0),
            CacheEntry::ParsedBlock(Arc::new(vec![(Arc::from("k3"), Some("v3".to_string()), 0)])),
        );

        assert_eq!(cache.stats().entries, 3);

        // Invalidate file1
        cache.invalidate_file(&file1);

        assert_eq!(cache.stats().entries, 1);
        assert!(cache.get(&BlockCacheKey::for_block(file2, 0)).is_some());
    }

    #[test]
    fn test_cache_index_key() {
        let file = PathBuf::from("/test/file.data");
        let index_key = BlockCacheKey::for_index(file.clone());
        let block_key = BlockCacheKey::for_block(file, 0);

        // Index key uses u64::MAX as block_offset, so it's different from any block key
        assert_ne!(index_key, block_key);

        // Two index keys for the same file should be equal
        let index_key2 = BlockCacheKey::for_index(PathBuf::from("/test/file.data"));
        assert_eq!(index_key, index_key2);
    }
}
