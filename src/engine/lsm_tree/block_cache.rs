use lru::LruCache;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

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

    #[allow(dead_code)]
    pub fn is_index(&self) -> bool {
        self.block_offset == u64::MAX
    }
}

/// Cached entry types
#[derive(Clone)]
pub enum CacheEntry {
    /// Decompressed data block
    DataBlock(Arc<Vec<u8>>),
    /// Decompressed and parsed index
    Index(Arc<Vec<(String, u64)>>),
}

impl CacheEntry {
    pub fn size_bytes(&self) -> usize {
        match self {
            CacheEntry::DataBlock(data) => data.len(),
            CacheEntry::Index(index) => index.iter().map(|(k, _)| k.len() + 8).sum::<usize>(),
        }
    }
}

/// Thread-safe block cache with LRU eviction
pub struct BlockCache {
    inner: RwLock<BlockCacheInner>,
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
        }
    }

    /// Get an entry from the cache, promoting it to most-recently-used
    pub fn get(&self, key: &BlockCacheKey) -> Option<CacheEntry> {
        let mut inner = self.inner.write().ok()?;
        inner.cache.get(key).cloned()
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
            } else {
                break;
            }
        }

        // Insert new entry
        inner.cache.put(key, value);
        inner.current_size_bytes += entry_size;
    }

    /// Invalidate all cache entries for a specific file
    pub fn invalidate_file(&self, file_path: &PathBuf) {
        let Ok(mut inner) = self.inner.write() else {
            return;
        };

        // Collect keys to remove
        let keys_to_remove: Vec<BlockCacheKey> = inner
            .cache
            .iter()
            .filter(|(k, _)| &k.file_path == file_path)
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
    #[allow(dead_code)]
    pub fn stats(&self) -> CacheStats {
        let inner = self.inner.read().unwrap();
        CacheStats {
            entries: inner.cache.len(),
            size_bytes: inner.current_size_bytes,
            max_size_bytes: inner.max_size_bytes,
        }
    }
}

/// Cache statistics
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct CacheStats {
    pub entries: usize,
    pub size_bytes: usize,
    pub max_size_bytes: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cache_basic_operations() {
        let cache = BlockCache::new(1024);

        let key = BlockCacheKey::for_block(PathBuf::from("/test/file.data"), 0);
        let data = Arc::new(vec![1u8; 100]);

        // Insert
        cache.insert(key.clone(), CacheEntry::DataBlock(Arc::clone(&data)));

        // Get
        let result = cache.get(&key);
        assert!(result.is_some());
        if let Some(CacheEntry::DataBlock(retrieved)) = result {
            assert_eq!(retrieved.len(), 100);
        }

        // Stats
        let stats = cache.stats();
        assert_eq!(stats.entries, 1);
        assert_eq!(stats.size_bytes, 100);
    }

    #[test]
    fn test_cache_eviction() {
        let cache = BlockCache::new(200);

        // Insert entries that exceed cache size
        for i in 0..5 {
            let key = BlockCacheKey::for_block(PathBuf::from("/test/file.data"), i * 100);
            let data = Arc::new(vec![i as u8; 100]);
            cache.insert(key, CacheEntry::DataBlock(data));
        }

        // Cache should have evicted older entries
        let stats = cache.stats();
        assert!(stats.size_bytes <= 200);
    }

    #[test]
    fn test_cache_invalidate_file() {
        let cache = BlockCache::new(1024);

        // Insert entries for two different files
        let file1 = PathBuf::from("/test/file1.data");
        let file2 = PathBuf::from("/test/file2.data");

        cache.insert(
            BlockCacheKey::for_block(file1.clone(), 0),
            CacheEntry::DataBlock(Arc::new(vec![1u8; 50])),
        );
        cache.insert(
            BlockCacheKey::for_block(file1.clone(), 100),
            CacheEntry::DataBlock(Arc::new(vec![2u8; 50])),
        );
        cache.insert(
            BlockCacheKey::for_block(file2.clone(), 0),
            CacheEntry::DataBlock(Arc::new(vec![3u8; 50])),
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

        assert!(index_key.is_index());
        assert!(!block_key.is_index());
        assert_ne!(index_key, block_key);
    }
}
