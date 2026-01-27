//! SSTable reader implementation.
//!
//! Provides memory-mapped SSTable reading with block cache support.

use std::collections::BTreeMap;
use std::fs::File;
use std::io::{Cursor, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};

use tonic::Status;

use crate::engine::lsm_tree::block_cache::{
    BlockCache, BlockCacheKey, CacheEntry, ParsedBlockEntry,
};
use crate::engine::lsm_tree::bloom::BloomFilter;
use crate::engine::lsm_tree::buffer_pool::PooledBuffer;
use crate::engine::lsm_tree::common_prefix_compression_index as prefix_index;
use crate::engine::lsm_tree::mmap::MappedFile;

use super::{DATA_VERSION, FOOTER_MAGIC, FOOTER_SIZE, HEADER_SIZE, MAGIC_BYTES, crc32};

// Entry with timestamp and TTL for merge-sorting during compaction
// (timestamp, expire_at, value)
pub type TimestampedEntry = (u64, u64, Option<String>);

/// Find the block offset for a key using binary search in the index.
///
/// The index contains (first_key, block_offset) pairs sorted by key.
/// This function finds the block that might contain the given key:
/// - If the key matches an index entry exactly, returns that block's offset
/// - Otherwise, returns the offset of the previous block (which could contain the key)
fn find_block_offset(index: &[(String, u64)], key: &str) -> u64 {
    match index.binary_search_by(|(k, _)| k.as_str().cmp(key)) {
        Ok(idx) => index[idx].1,
        Err(idx) => {
            if idx == 0 {
                // Key is smaller than the first block's first key.
                // If it exists, it must be in the first block.
                index[0].1
            } else {
                index[idx - 1].1
            }
        }
    }
}

/// Memory-mapped SSTable reader
///
/// Provides zero-copy access to SSTable data via mmap.
/// The SSTable file must not be modified while this struct exists.
pub struct MappedSSTable {
    mmap: MappedFile,
    path: PathBuf,
    #[allow(dead_code)]
    version: u32,
    index_offset: u64,
    bloom_offset: u64,
    /// Minimum key in this SSTable (used for leveled compaction)
    min_key: Vec<u8>,
    /// Maximum key in this SSTable (used for leveled compaction)
    max_key: Vec<u8>,
    /// Number of entries in this SSTable (used for approximate count/rank)
    entry_count: u64,
    /// Cached decompressed index (lazy loaded on first access)
    cached_index: OnceLock<Vec<(String, u64)>>,
}

impl MappedSSTable {
    /// Open an SSTable file with memory mapping
    #[allow(clippy::result_large_err)]
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, Status> {
        let path = path.as_ref();
        let mmap = MappedFile::open(path).map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                Status::not_found("SSTable file not found")
            } else {
                Status::internal(e.to_string())
            }
        })?;

        // Advise OS that we'll access this file randomly (point lookups)
        // This disables read-ahead which would be wasteful for random access
        let _ = mmap.advise_random();

        // Validate header
        if mmap.len() < HEADER_SIZE as usize {
            return Err(Status::internal("SSTable file too small"));
        }

        let magic = mmap.slice(0, 6);
        if magic != MAGIC_BYTES {
            return Err(Status::internal("Invalid SSTable magic"));
        }

        let version = u32::from_le_bytes(mmap.slice(6, 10).try_into().unwrap());
        if version != DATA_VERSION {
            return Err(Status::internal(format!(
                "Unsupported SSTable version: {}. Only version {} is supported.",
                version, DATA_VERSION
            )));
        }

        let file_len = mmap.len();
        let footer_size = FOOTER_SIZE as usize;

        if file_len < HEADER_SIZE as usize + footer_size {
            return Err(Status::internal("SSTable file too small for footer"));
        }

        // Read footer: [index_offset][bloom_offset][keyrange_offset][footer_magic]
        let footer_start = file_len - footer_size;
        let footer = mmap.slice(footer_start, footer_start + 32);
        let index_offset = u64::from_le_bytes(footer[0..8].try_into().unwrap());
        let bloom_offset = u64::from_le_bytes(footer[8..16].try_into().unwrap());
        let keyrange_offset = u64::from_le_bytes(footer[16..24].try_into().unwrap());
        let magic = u64::from_le_bytes(footer[24..32].try_into().unwrap());
        if magic != FOOTER_MAGIC {
            return Err(Status::internal("Invalid footer magic"));
        }

        // Read key range at keyrange_offset
        let keyrange_pos = keyrange_offset as usize;

        // Read min_key
        let min_key_len_bytes = mmap
            .read_at(keyrange_pos, 4)
            .ok_or_else(|| Status::internal("Failed to read min_key length"))?;
        let min_key_len = u32::from_le_bytes(min_key_len_bytes.try_into().unwrap()) as usize;
        let min_key = mmap
            .read_at(keyrange_pos + 4, min_key_len)
            .ok_or_else(|| Status::internal("Failed to read min_key"))?
            .to_vec();

        // Read max_key
        let max_key_pos = keyrange_pos + 4 + min_key_len;
        let max_key_len_bytes = mmap
            .read_at(max_key_pos, 4)
            .ok_or_else(|| Status::internal("Failed to read max_key length"))?;
        let max_key_len = u32::from_le_bytes(max_key_len_bytes.try_into().unwrap()) as usize;
        let max_key = mmap
            .read_at(max_key_pos + 4, max_key_len)
            .ok_or_else(|| Status::internal("Failed to read max_key"))?
            .to_vec();

        // Read entry_count
        let entry_count_pos = max_key_pos + 4 + max_key_len;
        let entry_count_bytes = mmap
            .read_at(entry_count_pos, 8)
            .ok_or_else(|| Status::internal("Failed to read entry_count"))?;
        let entry_count = u64::from_le_bytes(entry_count_bytes.try_into().unwrap());

        Ok(Self {
            mmap,
            path: path.to_path_buf(),
            version,
            index_offset,
            bloom_offset,
            min_key,
            max_key,
            entry_count,
            cached_index: OnceLock::new(),
        })
    }

    /// Get the file path
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Get the SSTable format version
    #[allow(dead_code)]
    pub fn version(&self) -> u32 {
        self.version
    }

    /// Get canonical path for cache keys
    pub fn canonical_path(&self) -> PathBuf {
        self.path
            .canonicalize()
            .unwrap_or_else(|_| self.path.clone())
    }

    /// Get the minimum key in this SSTable (used for leveled compaction)
    pub fn min_key(&self) -> &[u8] {
        &self.min_key
    }

    /// Get the maximum key in this SSTable (used for leveled compaction)
    pub fn max_key(&self) -> &[u8] {
        &self.max_key
    }

    /// Get the number of entries in this SSTable (used for approximate count/rank)
    pub fn entry_count(&self) -> u64 {
        self.entry_count
    }

    /// Check if a key might be in the key range of this SSTable (used for leveled compaction)
    /// Returns true if the key is within [min_key, max_key]
    pub fn key_in_range(&self, key: &[u8]) -> bool {
        key >= self.min_key.as_slice() && key <= self.max_key.as_slice()
    }

    /// Read index with caching (returns reference to cached data)
    ///
    /// The index is decompressed and parsed on first access, then cached
    /// for subsequent lookups. This avoids repeated decompression overhead.
    #[cfg(test)]
    #[allow(clippy::result_large_err)]
    pub fn read_index(&self) -> Result<&[(String, u64)], Status> {
        // Fast path: already cached
        if let Some(cached) = self.cached_index.get() {
            return Ok(cached.as_slice());
        }

        // Slow path: decompress and cache
        let index = self.decompress_index()?;
        // Try to set the cache (ignore if another thread beat us)
        let _ = self.cached_index.set(index);
        // Return from cache (guaranteed to be set now)
        Ok(self.cached_index.get().unwrap().as_slice())
    }

    /// Read index without caching (returns owned data)
    ///
    /// Use this when you need ownership of the index data.
    #[allow(clippy::result_large_err)]
    pub fn read_index_owned(&self) -> Result<Vec<(String, u64)>, Status> {
        // If already cached, clone from cache
        if let Some(cached) = self.cached_index.get() {
            return Ok(cached.clone());
        }
        // Decompress and cache
        let index = self.decompress_index()?;
        let _ = self.cached_index.set(index);
        Ok(self.cached_index.get().unwrap().clone())
    }

    /// Internal: decompress and parse index from disk
    #[allow(clippy::result_large_err)]
    fn decompress_index(&self) -> Result<Vec<(String, u64)>, Status> {
        let offset = self.index_offset as usize;

        // Read compressed size
        let len_bytes = self
            .mmap
            .read_at(offset, 8)
            .ok_or_else(|| Status::internal("Failed to read index length"))?;
        let len = u64::from_le_bytes(len_bytes.try_into().unwrap()) as usize;

        // Read compressed data
        let compressed = self
            .mmap
            .read_at(offset + 8, len)
            .ok_or_else(|| Status::internal("Failed to read index data"))?;

        // Verify checksum
        let stored_crc_bytes = self
            .mmap
            .read_at(offset + 8 + len, 4)
            .ok_or_else(|| Status::internal("Failed to read index checksum"))?;
        let stored_crc = u32::from_le_bytes(stored_crc_bytes.try_into().unwrap());
        let computed_crc = crc32(compressed);
        if stored_crc != computed_crc {
            return Err(Status::data_loss(format!(
                "Index checksum mismatch: expected {:08x}, got {:08x}",
                stored_crc, computed_crc
            )));
        }

        let decompressed = zstd::decode_all(Cursor::new(compressed))
            .map_err(|e| Status::internal(format!("Index decompression error: {}", e)))?;

        // Parse index entries using prefix compression module
        prefix_index::parse_index(&decompressed)
    }

    /// Read and decompress a block at the given offset
    #[allow(clippy::result_large_err)]
    pub fn read_block(&self, block_offset: u64) -> Result<Vec<u8>, Status> {
        let offset = block_offset as usize;

        // Read block length
        let len_bytes = self
            .mmap
            .read_at(offset, 4)
            .ok_or_else(|| Status::internal("Failed to read block length"))?;
        let len = u32::from_le_bytes(len_bytes.try_into().unwrap()) as usize;

        // Read compressed block
        let compressed = self
            .mmap
            .read_at(offset + 4, len)
            .ok_or_else(|| Status::internal("Failed to read block data"))?;

        // Verify checksum
        let stored_crc_bytes = self
            .mmap
            .read_at(offset + 4 + len, 4)
            .ok_or_else(|| Status::internal("Failed to read block checksum"))?;
        let stored_crc = u32::from_le_bytes(stored_crc_bytes.try_into().unwrap());
        let computed_crc = crc32(compressed);
        if stored_crc != computed_crc {
            return Err(Status::data_loss(format!(
                "Block checksum mismatch: expected {:08x}, got {:08x}",
                stored_crc, computed_crc
            )));
        }

        zstd::decode_all(Cursor::new(compressed))
            .map_err(|e| Status::internal(format!("Block decompression error: {}", e)))
    }

    /// Read and deserialize the Bloom filter
    #[allow(clippy::result_large_err)]
    pub fn read_bloom_filter(&self) -> Result<BloomFilter, Status> {
        let bloom_offset = self.bloom_offset as usize;

        // Read bloom filter size
        let size_bytes = self
            .mmap
            .read_at(bloom_offset, 8)
            .ok_or_else(|| Status::internal("Failed to read Bloom filter size"))?;
        let bloom_size = u64::from_le_bytes(size_bytes.try_into().unwrap()) as usize;

        // Read bloom filter data
        let bloom_data = self
            .mmap
            .read_at(bloom_offset + 8, bloom_size)
            .ok_or_else(|| Status::internal("Failed to read Bloom filter data"))?;

        BloomFilter::deserialize(bloom_data)
            .ok_or_else(|| Status::internal("Failed to deserialize Bloom filter"))
    }
}

/// Extract WAL ID from SSTable filename (sst_{sst_id}_{wal_id}.data)
pub fn extract_wal_id_from_sstable(path: &Path) -> Option<u64> {
    let filename = path.file_name()?.to_str()?;
    if filename.starts_with("sst_") && filename.ends_with(".data") {
        let name_part = &filename[4..filename.len() - 5];
        // Try new format: sst_{sst_id}_{wal_id}.data
        if let Some(pos) = name_part.rfind('_')
            && let Ok(wal_id) = name_part[pos + 1..].parse::<u64>()
        {
            return Some(wal_id);
        }
    }
    None
}

/// Search for a key in an SSTable file
#[cfg(test)]
#[allow(clippy::result_large_err)]
pub fn search_key(path: &Path, key: &str) -> Result<Option<String>, Status> {
    // If file doesn't exist (deleted by compaction), skip it
    let mut file = match File::open(path) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Err(Status::not_found("SSTable file not found"));
        }
        Err(e) => return Err(Status::internal(e.to_string())),
    };

    // Read magic
    let mut magic = [0u8; 6];
    if file.read_exact(&mut magic).is_err() || &magic != MAGIC_BYTES {
        return Err(Status::internal("Invalid SSTable magic"));
    }

    // Read version
    let mut ver_bytes = [0u8; 4];
    file.read_exact(&mut ver_bytes)
        .map_err(|e| Status::internal(e.to_string()))?;
    let version = u32::from_le_bytes(ver_bytes);

    if version != DATA_VERSION {
        return Err(Status::internal(format!(
            "Unsupported SSTable version: {}. Only version {} is supported.",
            version, DATA_VERSION
        )));
    }

    let file_len = file
        .metadata()
        .map_err(|e| Status::internal(e.to_string()))?
        .len();

    if file_len < HEADER_SIZE + FOOTER_SIZE {
        return Err(Status::internal("SSTable file too small"));
    }

    // Read footer to get index offset (index_offset is always at the start of footer)
    file.seek(SeekFrom::Start(file_len - FOOTER_SIZE))
        .map_err(|e| Status::internal(e.to_string()))?;
    let mut footer = [0u8; 8];
    file.read_exact(&mut footer)
        .map_err(|e| Status::internal(e.to_string()))?;
    let index_offset = u64::from_le_bytes(footer);

    // Read index (v5+ uses prefix compression)
    let index = prefix_index::read_index(&mut file, index_offset)?;

    // Binary search in index to find the block that might contain the key
    let start_offset = find_block_offset(&index, key);

    file.seek(SeekFrom::Start(start_offset))
        .map_err(|e| Status::internal(e.to_string()))?;

    // Read compressed block
    let mut len_bytes = [0u8; 4];
    file.read_exact(&mut len_bytes)
        .map_err(|e| Status::internal(e.to_string()))?;
    let len = u32::from_le_bytes(len_bytes);

    let mut compressed_block = PooledBuffer::new_zeroed(len as usize);
    file.read_exact(&mut compressed_block)
        .map_err(|e| Status::internal(e.to_string()))?;

    // Verify checksum
    let mut checksum_bytes = [0u8; 4];
    file.read_exact(&mut checksum_bytes)
        .map_err(|e| Status::internal(e.to_string()))?;
    let stored_checksum = u32::from_le_bytes(checksum_bytes);
    let computed_checksum = crc32(&compressed_block);
    if stored_checksum != computed_checksum {
        return Err(Status::data_loss(format!(
            "Block checksum mismatch: expected {:08x}, got {:08x}",
            stored_checksum, computed_checksum
        )));
    }

    let decompressed = zstd::decode_all(Cursor::new(&compressed_block))
        .map_err(|e| Status::internal(format!("Block decompression error: {}", e)))?;

    // Scan decompressed block
    let mut cursor = Cursor::new(decompressed);
    loop {
        // Check if EOF
        if cursor.position() == cursor.get_ref().len() as u64 {
            break;
        }

        // Skip timestamp (8 bytes)
        let mut ts_bytes = [0u8; 8];
        if cursor.read_exact(&mut ts_bytes).is_err() {
            break;
        }

        // Skip expire_at (8 bytes)
        let mut expire_at_bytes = [0u8; 8];
        cursor
            .read_exact(&mut expire_at_bytes)
            .map_err(|e| Status::internal(e.to_string()))?;

        let mut klen_bytes = [0u8; 8];
        cursor
            .read_exact(&mut klen_bytes)
            .map_err(|e| Status::internal(e.to_string()))?;
        let mut vlen_bytes = [0u8; 8];
        cursor
            .read_exact(&mut vlen_bytes)
            .map_err(|e| Status::internal(e.to_string()))?;

        let key_len = u64::from_le_bytes(klen_bytes);
        let val_len = u64::from_le_bytes(vlen_bytes);

        let mut key_buf = vec![0u8; key_len as usize];
        cursor
            .read_exact(&mut key_buf)
            .map_err(|e| Status::internal(e.to_string()))?;
        let current_key = String::from_utf8_lossy(&key_buf);

        if current_key == key {
            if val_len == u64::MAX {
                return Ok(None);
            }
            let mut val_buf = vec![0u8; val_len as usize];
            cursor
                .read_exact(&mut val_buf)
                .map_err(|e| Status::internal(e.to_string()))?;
            return Ok(Some(String::from_utf8_lossy(&val_buf).to_string()));
        }

        // Optimization: If current_key > key, we can stop (entries are sorted)
        if current_key.as_ref() > key {
            return Err(Status::not_found("Key not found in SSTable (sorted check)"));
        }

        if val_len != u64::MAX {
            cursor
                .seek(SeekFrom::Current(val_len as i64))
                .map_err(|e| Status::internal(e.to_string()))?;
        }
    }
    Err(Status::not_found("Key not found in SSTable"))
}

/// Search for a key using a memory-mapped SSTable with block cache support
/// This is more efficient than search_key_cached as it avoids file open overhead
#[allow(clippy::result_large_err)]
pub fn search_key_mmap(
    sst: &MappedSSTable,
    key: &str,
    cache: &BlockCache,
) -> Result<Option<String>, Status> {
    let canonical_path = sst.canonical_path();

    // Get or load index from cache
    let index = get_or_load_index_mmap(sst, &canonical_path, cache)?;

    // Binary search in index to find the block that might contain the key
    let block_offset = find_block_offset(&index, key);

    // Get or load parsed block from cache
    let parsed_entries = get_or_load_parsed_block_mmap(sst, &canonical_path, block_offset, cache)?;

    // Binary search within parsed block entries
    search_in_parsed_block(&parsed_entries, key)
}

/// Get index from cache or load from mmap
#[allow(clippy::result_large_err)]
fn get_or_load_index_mmap(
    sst: &MappedSSTable,
    canonical_path: &Path,
    cache: &BlockCache,
) -> Result<Arc<Vec<(String, u64)>>, Status> {
    let cache_key = BlockCacheKey::for_index(canonical_path.to_path_buf());

    // Check cache first
    if let Some(CacheEntry::Index(index)) = cache.get(&cache_key) {
        return Ok(index);
    }

    // Load from mmap (use owned version for Arc wrapping)
    let index = sst.read_index_owned()?;
    let arc_index = Arc::new(index);

    // Cache it
    cache.insert(cache_key, CacheEntry::Index(Arc::clone(&arc_index)));

    Ok(arc_index)
}

/// Get parsed block from cache or load from mmap
#[allow(clippy::result_large_err)]
fn get_or_load_parsed_block_mmap(
    sst: &MappedSSTable,
    canonical_path: &Path,
    block_offset: u64,
    cache: &BlockCache,
) -> Result<Arc<Vec<ParsedBlockEntry>>, Status> {
    let cache_key = BlockCacheKey::for_block(canonical_path.to_path_buf(), block_offset);

    // Check cache first
    if let Some(CacheEntry::ParsedBlock(entries)) = cache.get(&cache_key) {
        return Ok(entries);
    }

    // Load and decompress block from mmap
    let decompressed = sst.read_block(block_offset)?;

    // Parse entries
    let entries = parse_block_entries(&decompressed)?;
    let arc_entries = Arc::new(entries);

    // Cache parsed entries
    cache.insert(cache_key, CacheEntry::ParsedBlock(Arc::clone(&arc_entries)));

    Ok(arc_entries)
}

/// Parse all entries from a decompressed block into a sorted vector
#[allow(clippy::result_large_err)]
fn parse_block_entries(block_data: &[u8]) -> Result<Vec<ParsedBlockEntry>, Status> {
    let mut entries = Vec::new();
    let mut cursor = Cursor::new(block_data);

    loop {
        if cursor.position() == block_data.len() as u64 {
            break;
        }

        // Skip timestamp (8 bytes)
        let mut ts_bytes = [0u8; 8];
        if cursor.read_exact(&mut ts_bytes).is_err() {
            break;
        }

        // Skip expire_at (8 bytes)
        let mut expire_at_bytes = [0u8; 8];
        cursor
            .read_exact(&mut expire_at_bytes)
            .map_err(|e| Status::internal(e.to_string()))?;

        let mut klen_bytes = [0u8; 8];
        cursor
            .read_exact(&mut klen_bytes)
            .map_err(|e| Status::internal(e.to_string()))?;
        let mut vlen_bytes = [0u8; 8];
        cursor
            .read_exact(&mut vlen_bytes)
            .map_err(|e| Status::internal(e.to_string()))?;

        let key_len = u64::from_le_bytes(klen_bytes);
        let val_len = u64::from_le_bytes(vlen_bytes);

        let mut key_buf = vec![0u8; key_len as usize];
        cursor
            .read_exact(&mut key_buf)
            .map_err(|e| Status::internal(e.to_string()))?;
        let key = String::from_utf8_lossy(&key_buf).to_string();

        let value = if val_len == u64::MAX {
            None // Tombstone
        } else {
            let mut val_buf = vec![0u8; val_len as usize];
            cursor
                .read_exact(&mut val_buf)
                .map_err(|e| Status::internal(e.to_string()))?;
            Some(String::from_utf8_lossy(&val_buf).to_string())
        };

        entries.push((key, value));
    }

    // Entries are already sorted by key (written in BTreeMap order)
    Ok(entries)
}

/// Binary search within parsed block entries
/// Returns Ok(Some(value)) if found, Ok(None) if tombstone, Err if not found
#[allow(clippy::result_large_err)]
fn search_in_parsed_block(
    entries: &[ParsedBlockEntry],
    key: &str,
) -> Result<Option<String>, Status> {
    match entries.binary_search_by(|(k, _)| k.as_str().cmp(key)) {
        Ok(idx) => {
            // Found the key
            Ok(entries[idx].1.clone())
        }
        Err(_) => {
            // Key not in this block
            Err(Status::not_found("Key not found in SSTable"))
        }
    }
}

/// Read only keys from an SSTable file
#[cfg(test)]
#[allow(clippy::result_large_err)]
pub fn read_keys(path: &Path) -> Result<Vec<String>, Status> {
    let mut file = match File::open(path) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(Status::internal(e.to_string())),
    };

    // Read magic and version
    let mut header = [0u8; 10];
    file.read_exact(&mut header)
        .map_err(|e| Status::internal(e.to_string()))?;

    if &header[0..6] != MAGIC_BYTES {
        return Err(Status::internal("Invalid SSTable magic"));
    }

    let version = u32::from_le_bytes(header[6..10].try_into().unwrap());
    if version != DATA_VERSION {
        return Err(Status::internal(format!(
            "Unsupported SSTable version: {}. Only version {} is supported.",
            version, DATA_VERSION
        )));
    }

    let file_len = file
        .metadata()
        .map_err(|e| Status::internal(e.to_string()))?
        .len();

    if file_len < HEADER_SIZE + FOOTER_SIZE {
        return Ok(Vec::new());
    }

    // Read footer to get index offset (marks end of data blocks)
    file.seek(SeekFrom::Start(file_len - FOOTER_SIZE))
        .map_err(|e| Status::internal(e.to_string()))?;
    let mut footer = [0u8; 8];
    file.read_exact(&mut footer)
        .map_err(|e| Status::internal(e.to_string()))?;
    let end_offset = u64::from_le_bytes(footer);

    file.seek(SeekFrom::Start(HEADER_SIZE))
        .map_err(|e| Status::internal(e.to_string()))?;

    let mut keys = Vec::new();

    loop {
        if file
            .stream_position()
            .map_err(|e| Status::internal(e.to_string()))?
            >= end_offset
        {
            break;
        }

        let mut len_bytes = [0u8; 4];
        if file.read_exact(&mut len_bytes).is_err() {
            break;
        }
        let len = u32::from_le_bytes(len_bytes);

        let mut compressed_block = PooledBuffer::new_zeroed(len as usize);
        file.read_exact(&mut compressed_block)
            .map_err(|e| Status::internal(e.to_string()))?;

        // Skip checksum (checksum verification is optional for read_keys since it's not critical)
        let mut checksum_bytes = [0u8; 4];
        file.read_exact(&mut checksum_bytes)
            .map_err(|e| Status::internal(e.to_string()))?;

        let decompressed = zstd::decode_all(Cursor::new(&compressed_block))
            .map_err(|e| Status::internal(format!("Block decompression error: {}", e)))?;

        let mut cursor = Cursor::new(decompressed);
        loop {
            if cursor.position() == cursor.get_ref().len() as u64 {
                break;
            }

            // Skip timestamp (8) + expire_at (8) + read klen (8) + vlen (8)
            cursor
                .seek(SeekFrom::Current(16))
                .map_err(|e| Status::internal(e.to_string()))?;

            let mut lengths = [0u8; 16];
            cursor
                .read_exact(&mut lengths)
                .map_err(|e| Status::internal(e.to_string()))?;
            let key_len = u64::from_le_bytes(lengths[0..8].try_into().unwrap());
            let val_len = u64::from_le_bytes(lengths[8..16].try_into().unwrap());

            let mut key_buf = vec![0u8; key_len as usize];
            cursor
                .read_exact(&mut key_buf)
                .map_err(|e| Status::internal(e.to_string()))?;
            keys.push(String::from_utf8_lossy(&key_buf).to_string());

            if val_len != u64::MAX {
                cursor
                    .seek(SeekFrom::Current(val_len as i64))
                    .map_err(|e| Status::internal(e.to_string()))?;
            }
        }
    }

    Ok(keys)
}

/// Read all entries from an SSTable file
#[allow(clippy::result_large_err)]
pub fn read_entries(path: &Path) -> Result<BTreeMap<String, TimestampedEntry>, Status> {
    let mut file = match File::open(path) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            // File was deleted (e.g., by another compaction), skip it
            return Ok(BTreeMap::new());
        }
        Err(e) => return Err(Status::internal(e.to_string())),
    };

    // Read magic
    let mut magic = [0u8; 6];
    if file.read_exact(&mut magic).is_err() || &magic != MAGIC_BYTES {
        return Err(Status::internal("Invalid SSTable magic"));
    }

    // Read version
    let mut ver_bytes = [0u8; 4];
    file.read_exact(&mut ver_bytes)
        .map_err(|e| Status::internal(e.to_string()))?;
    let version = u32::from_le_bytes(ver_bytes);

    if version != DATA_VERSION {
        return Err(Status::internal(format!(
            "Unsupported SSTable version: {}. Only version {} is supported.",
            version, DATA_VERSION
        )));
    }

    let file_len = file
        .metadata()
        .map_err(|e| Status::internal(e.to_string()))?
        .len();

    if file_len < HEADER_SIZE + FOOTER_SIZE {
        return Ok(BTreeMap::new()); // Corrupt or empty?
    }

    // Read footer to get index offset (which marks end of data blocks)
    file.seek(SeekFrom::Start(file_len - FOOTER_SIZE))
        .map_err(|e| Status::internal(e.to_string()))?;
    let mut footer = [0u8; 8];
    file.read_exact(&mut footer)
        .map_err(|e| Status::internal(e.to_string()))?;
    let end_offset = u64::from_le_bytes(footer);

    file.seek(SeekFrom::Start(HEADER_SIZE))
        .map_err(|e| Status::internal(e.to_string()))?;

    let mut entries = BTreeMap::new();
    loop {
        // Check if we reached end of data
        if file
            .stream_position()
            .map_err(|e| Status::internal(e.to_string()))?
            >= end_offset
        {
            break;
        }

        let mut len_bytes = [0u8; 4];
        if file.read_exact(&mut len_bytes).is_err() {
            break;
        }
        let len = u32::from_le_bytes(len_bytes);

        let mut compressed_block = PooledBuffer::new_zeroed(len as usize);
        file.read_exact(&mut compressed_block)
            .map_err(|e| Status::internal(e.to_string()))?;

        // Verify checksum
        let mut checksum_bytes = [0u8; 4];
        file.read_exact(&mut checksum_bytes)
            .map_err(|e| Status::internal(e.to_string()))?;
        let stored_checksum = u32::from_le_bytes(checksum_bytes);
        let computed_checksum = crc32(&compressed_block);
        if stored_checksum != computed_checksum {
            return Err(Status::data_loss(format!(
                "Block checksum mismatch: expected {:08x}, got {:08x}",
                stored_checksum, computed_checksum
            )));
        }

        let decompressed = zstd::decode_all(Cursor::new(&compressed_block))
            .map_err(|e| Status::internal(format!("Block decompression error: {}", e)))?;

        let mut cursor = Cursor::new(decompressed);
        loop {
            // Check if EOF in block
            if cursor.position() == cursor.get_ref().len() as u64 {
                break;
            }

            let mut ts_bytes = [0u8; 8];
            if cursor.read_exact(&mut ts_bytes).is_err() {
                break;
            }
            let timestamp = u64::from_le_bytes(ts_bytes);

            let mut expire_at_bytes = [0u8; 8];
            cursor
                .read_exact(&mut expire_at_bytes)
                .map_err(|e| Status::internal(e.to_string()))?;
            let expire_at = u64::from_le_bytes(expire_at_bytes);

            let mut klen_bytes = [0u8; 8];
            cursor
                .read_exact(&mut klen_bytes)
                .map_err(|e| Status::internal(e.to_string()))?;
            let mut vlen_bytes = [0u8; 8];
            cursor
                .read_exact(&mut vlen_bytes)
                .map_err(|e| Status::internal(e.to_string()))?;

            let key_len = u64::from_le_bytes(klen_bytes);
            let val_len = u64::from_le_bytes(vlen_bytes);

            let mut key_buf = vec![0u8; key_len as usize];
            cursor
                .read_exact(&mut key_buf)
                .map_err(|e| Status::internal(e.to_string()))?;
            let key = String::from_utf8_lossy(&key_buf).to_string();

            let value = if val_len == u64::MAX {
                None
            } else {
                let mut val_buf = vec![0u8; val_len as usize];
                cursor
                    .read_exact(&mut val_buf)
                    .map_err(|e| Status::internal(e.to_string()))?;
                Some(String::from_utf8_lossy(&val_buf).to_string())
            };

            entries.insert(key, (timestamp, expire_at, value));
        }
    }

    Ok(entries)
}

/// Read Bloom filter from SSTable file
#[cfg(test)]
#[allow(clippy::result_large_err)]
pub fn read_bloom_filter(path: &Path) -> Result<BloomFilter, Status> {
    let mut file = match File::open(path) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Err(Status::not_found("SSTable file not found"));
        }
        Err(e) => return Err(Status::internal(e.to_string())),
    };

    // Read and verify header
    let mut header = [0u8; 10];
    file.read_exact(&mut header)
        .map_err(|e| Status::internal(e.to_string()))?;

    if &header[0..6] != MAGIC_BYTES {
        return Err(Status::internal("Invalid SSTable magic"));
    }

    let version = u32::from_le_bytes(header[6..10].try_into().unwrap());
    if version != DATA_VERSION {
        return Err(Status::internal(format!(
            "Unsupported SSTable version: {}. Only version {} is supported.",
            version, DATA_VERSION
        )));
    }

    let file_len = file
        .metadata()
        .map_err(|e| Status::internal(e.to_string()))?
        .len();

    if file_len < HEADER_SIZE + FOOTER_SIZE {
        return Err(Status::internal("SSTable file too small"));
    }

    // Read footer
    file.seek(SeekFrom::Start(file_len - FOOTER_SIZE))
        .map_err(|e| Status::internal(e.to_string()))?;

    let mut footer = [0u8; 32];
    file.read_exact(&mut footer)
        .map_err(|e| Status::internal(e.to_string()))?;

    // bloom_offset is at bytes 8..16
    let bloom_offset = u64::from_le_bytes(footer[8..16].try_into().unwrap());
    // footer_magic is at bytes 24..32
    let footer_magic = u64::from_le_bytes(footer[24..32].try_into().unwrap());

    if footer_magic != FOOTER_MAGIC {
        return Err(Status::internal("Invalid footer magic"));
    }

    // Read Bloom filter
    file.seek(SeekFrom::Start(bloom_offset))
        .map_err(|e| Status::internal(e.to_string()))?;

    // Read bloom filter size
    let mut size_bytes = [0u8; 8];
    file.read_exact(&mut size_bytes)
        .map_err(|e| Status::internal(e.to_string()))?;
    let bloom_size = u64::from_le_bytes(size_bytes) as usize;

    // Read bloom filter data
    let mut bloom_data = vec![0u8; bloom_size];
    file.read_exact(&mut bloom_data)
        .map_err(|e| Status::internal(e.to_string()))?;

    BloomFilter::deserialize(&bloom_data)
        .ok_or_else(|| Status::internal("Failed to deserialize Bloom filter"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::lsm_tree::sstable::writer::create_from_memtable;
    use std::collections::BTreeMap;
    use std::fs::File;
    use std::io::Write;
    use tempfile::tempdir;

    #[test]
    fn test_extract_wal_id() {
        let path = Path::new("sst_00001_00123.data");
        assert_eq!(extract_wal_id_from_sstable(path), Some(123));

        let path_old = Path::new("sst_00001.data");
        assert_eq!(extract_wal_id_from_sstable(path_old), None);
    }

    #[test]
    fn test_read_bloom_filter() {
        use crate::engine::lsm_tree::memtable::MemValue;

        let dir = tempdir().unwrap();
        let sst_path = dir.path().join("sst_bloom.data");

        let mut memtable = BTreeMap::new();
        memtable.insert("apple".to_string(), MemValue::new(Some("red".to_string())));
        memtable.insert(
            "banana".to_string(),
            MemValue::new(Some("yellow".to_string())),
        );
        memtable.insert("cherry".to_string(), MemValue::new(Some("red".to_string())));

        create_from_memtable(&sst_path, &memtable).unwrap();

        // Read the Bloom filter from the SSTable
        let bloom = read_bloom_filter(&sst_path).expect("should read successfully");

        // Verify the Bloom filter contains the keys
        assert!(bloom.contains("apple"));
        assert!(bloom.contains("banana"));
        assert!(bloom.contains("cherry"));
        // Keys not in the SSTable should (likely) not match
        // Note: false positives are possible, but unlikely for small sets
        assert!(!bloom.contains("grape"));
    }

    #[test]
    fn test_invalid_version() {
        let dir = tempdir().unwrap();
        let sst_path = dir.path().join("invalid_version.data");

        {
            let mut file = File::create(&sst_path).unwrap();
            file.write_all(MAGIC_BYTES).unwrap();
            file.write_all(&999u32.to_le_bytes()).unwrap(); // Wrong version
        }

        let result = search_key(&sst_path, "any");
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .message()
                .contains("Unsupported SSTable version")
        );
    }

    #[test]
    fn test_corrupt_block_data() {
        use crate::engine::lsm_tree::memtable::MemValue;

        let dir = tempdir().unwrap();
        let sst_path = dir.path().join("corrupt_block.data");

        let mut memtable = BTreeMap::new();
        memtable.insert(
            "key1".to_string(),
            MemValue::new(Some("value1".to_string())),
        );
        create_from_memtable(&sst_path, &memtable).unwrap();

        // Corrupt the block data (between header and index)
        let mut data = std::fs::read(&sst_path).unwrap();
        // The first block starts after HEADER_SIZE (10)
        // Let's flip some bits in the middle of what should be compressed data
        if data.len() > 20 {
            data[15] ^= 0xFF;
        }
        std::fs::write(&sst_path, data).unwrap();

        let result = search_key(&sst_path, "key1");
        assert!(result.is_err());
        // With v6 format, we now detect corruption via checksum
        let err_msg = result.unwrap_err().message().to_string();
        assert!(
            err_msg.contains("checksum mismatch") || err_msg.contains("decompression error"),
            "Expected checksum or decompression error, got: {}",
            err_msg
        );
    }

    #[test]
    fn test_corrupt_index_data() {
        use crate::engine::lsm_tree::memtable::MemValue;

        let dir = tempdir().unwrap();
        let sst_path = dir.path().join("corrupt_index.data");

        let mut memtable = BTreeMap::new();
        memtable.insert(
            "key1".to_string(),
            MemValue::new(Some("value1".to_string())),
        );
        create_from_memtable(&sst_path, &memtable).unwrap();

        // Corrupt the index offset in the footer (V7 format)
        // Footer layout: index_offset(8) + bloom_offset(8) + keyrange_offset(8) + footer_magic(8)
        // index_offset is at offset len - 32
        let mut data = std::fs::read(&sst_path).unwrap();
        let len = data.len();
        if len > 32 {
            // Change the index offset to something invalid
            data[len - 32] ^= 0xFF;
        }
        std::fs::write(&sst_path, data).unwrap();

        let result = search_key(&sst_path, "key1");
        assert!(result.is_err());
    }

    #[test]
    fn test_v7_min_max_key() {
        use crate::engine::lsm_tree::memtable::MemValue;

        let dir = tempdir().unwrap();
        let sst_path = dir.path().join("sst_v7_minmax.data");

        let mut memtable = BTreeMap::new();
        memtable.insert("apple".to_string(), MemValue::new(Some("red".to_string())));
        memtable.insert(
            "banana".to_string(),
            MemValue::new(Some("yellow".to_string())),
        );
        memtable.insert("cherry".to_string(), MemValue::new(Some("red".to_string())));
        memtable.insert("date".to_string(), MemValue::new(Some("brown".to_string())));

        create_from_memtable(&sst_path, &memtable).unwrap();

        // Open with MappedSSTable and verify min/max keys
        let sst = MappedSSTable::open(&sst_path).unwrap();
        assert_eq!(sst.min_key(), b"apple");
        assert_eq!(sst.max_key(), b"date");

        // Verify key_in_range works correctly
        assert!(sst.key_in_range(b"apple"));
        assert!(sst.key_in_range(b"banana"));
        assert!(sst.key_in_range(b"cherry"));
        assert!(sst.key_in_range(b"date"));
        assert!(sst.key_in_range(b"coconut")); // between cherry and date
        assert!(!sst.key_in_range(b"aaaa")); // before apple
        assert!(!sst.key_in_range(b"zebra")); // after date
    }

    #[test]
    fn test_v7_empty_memtable_min_max_key() {
        use crate::engine::lsm_tree::memtable::MemValue;

        let dir = tempdir().unwrap();
        let sst_path = dir.path().join("sst_v7_empty.data");

        let memtable: BTreeMap<String, MemValue> = BTreeMap::new();
        create_from_memtable(&sst_path, &memtable).unwrap();

        // Open with MappedSSTable - empty memtable should have empty min/max keys
        let sst = MappedSSTable::open(&sst_path).unwrap();
        assert_eq!(sst.min_key(), b"");
        assert_eq!(sst.max_key(), b"");
    }
}
