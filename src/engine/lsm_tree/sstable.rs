use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{Cursor, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tonic::Status;

use super::block_cache::{BlockCache, BlockCacheKey, CacheEntry, ParsedBlockEntry};
use super::bloom::BloomFilter;
use super::memtable::MemTable;
use super::mmap::MappedFile;

pub const MAGIC_BYTES: &[u8; 6] = b"ORELSM";
pub const DATA_VERSION: u32 = 5;

// Header size: 6 bytes magic + 4 bytes version
pub const HEADER_SIZE: u64 = 10;
// Footer size for V5: index_offset(8) + bloom_offset(8) + footer_magic(8)
#[allow(dead_code)]
pub const FOOTER_SIZE_V5: u64 = 24;
// Footer size for V4: index_offset(8) + bloom_offset(8) + footer_magic(8)
pub const FOOTER_SIZE_V4: u64 = 24;
// Footer size for V3 (legacy): index_offset(8)
pub const FOOTER_SIZE_V3: u64 = 8;

// Prefix compression restart interval for index
// Every N entries, store full key for random access
const INDEX_RESTART_INTERVAL: u32 = 16;
// Footer magic bytes
pub const FOOTER_MAGIC: u64 = 0x4F52454C534D4654; // "ORELSMFT" in hex
// Target block size for compression (bytes)
pub const BLOCK_SIZE: usize = 4096;

// ZSTD compression levels (0-22, higher = better compression but slower)
// Flush: prioritize speed for write performance
const COMPRESSION_LEVEL_FLUSH: i32 = 1;
// Compaction: prioritize compression ratio for storage efficiency
const COMPRESSION_LEVEL_COMPACTION: i32 = 6;
// Index: use higher compression since index is read once per search
const COMPRESSION_LEVEL_INDEX: i32 = 6;

// Entry with timestamp for merge-sorting during compaction
pub type TimestampedEntry = (u64, Option<String>); // (timestamp, value)

/// Memory-mapped SSTable reader
///
/// Provides zero-copy access to SSTable data via mmap.
/// The SSTable file must not be modified while this struct exists.
pub struct MappedSSTable {
    mmap: MappedFile,
    path: PathBuf,
    version: u32,
    index_offset: u64,
    bloom_offset: Option<u64>, // None for V3 format
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

        // Validate header
        if mmap.len() < HEADER_SIZE as usize {
            return Err(Status::internal("SSTable file too small"));
        }

        let magic = mmap.slice(0, 6);
        if magic != MAGIC_BYTES {
            return Err(Status::internal("Invalid SSTable magic"));
        }

        let version = u32::from_le_bytes(mmap.slice(6, 10).try_into().unwrap());
        if version != 3 && version != 4 && version != 5 {
            return Err(Status::internal(format!(
                "Unsupported SSTable version: {}. Only versions 3, 4, and 5 are supported.",
                version
            )));
        }

        let file_len = mmap.len();
        let footer_size = if version >= 4 {
            FOOTER_SIZE_V4 as usize // V4 and V5 have same footer size
        } else {
            FOOTER_SIZE_V3 as usize
        };

        if file_len < HEADER_SIZE as usize + footer_size {
            return Err(Status::internal("SSTable file too small for footer"));
        }

        // Read footer
        let (index_offset, bloom_offset) = if version >= 4 {
            let footer_start = file_len - FOOTER_SIZE_V4 as usize;
            let footer = mmap.slice(footer_start, footer_start + 24);
            let idx_off = u64::from_le_bytes(footer[0..8].try_into().unwrap());
            let bloom_off = u64::from_le_bytes(footer[8..16].try_into().unwrap());
            let magic = u64::from_le_bytes(footer[16..24].try_into().unwrap());
            if magic != FOOTER_MAGIC {
                return Err(Status::internal("Invalid footer magic"));
            }
            (idx_off, Some(bloom_off))
        } else {
            let footer_start = file_len - FOOTER_SIZE_V3 as usize;
            let idx_off = u64::from_le_bytes(
                mmap.slice(footer_start, footer_start + 8)
                    .try_into()
                    .unwrap(),
            );
            (idx_off, None)
        };

        Ok(Self {
            mmap,
            path: path.to_path_buf(),
            version,
            index_offset,
            bloom_offset,
        })
    }

    /// Get the file path
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Get canonical path for cache keys
    pub fn canonical_path(&self) -> PathBuf {
        self.path
            .canonicalize()
            .unwrap_or_else(|_| self.path.clone())
    }

    /// Read and decompress the index
    #[allow(clippy::result_large_err)]
    pub fn read_index(&self) -> Result<Vec<(String, u64)>, Status> {
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

        let decompressed = zstd::decode_all(Cursor::new(compressed))
            .map_err(|e| Status::internal(format!("Index decompression error: {}", e)))?;

        // Parse index entries based on version
        if self.version >= 5 {
            self.parse_index_v5(&decompressed)
        } else {
            self.parse_index_v4(&decompressed)
        }
    }

    /// Parse V4 index format (no prefix compression)
    #[allow(clippy::result_large_err)]
    fn parse_index_v4(&self, decompressed: &[u8]) -> Result<Vec<(String, u64)>, Status> {
        let mut cursor = Cursor::new(decompressed);
        let mut num_bytes = [0u8; 8];
        cursor
            .read_exact(&mut num_bytes)
            .map_err(|e| Status::internal(e.to_string()))?;
        let num_entries = u64::from_le_bytes(num_bytes);

        let mut index = Vec::with_capacity(num_entries as usize);
        for _ in 0..num_entries {
            let mut klen_bytes = [0u8; 8];
            cursor
                .read_exact(&mut klen_bytes)
                .map_err(|e| Status::internal(e.to_string()))?;
            let key_len = u64::from_le_bytes(klen_bytes);

            let mut key_buf = vec![0u8; key_len as usize];
            cursor
                .read_exact(&mut key_buf)
                .map_err(|e| Status::internal(e.to_string()))?;
            let key = String::from_utf8_lossy(&key_buf).to_string();

            let mut off_bytes = [0u8; 8];
            cursor
                .read_exact(&mut off_bytes)
                .map_err(|e| Status::internal(e.to_string()))?;
            let offset = u64::from_le_bytes(off_bytes);

            index.push((key, offset));
        }

        Ok(index)
    }

    /// Parse V5 index format (with prefix compression)
    /// Unified format: every entry uses prefix_len (u16) + suffix_len (u16) + suffix
    #[allow(clippy::result_large_err)]
    fn parse_index_v5(&self, decompressed: &[u8]) -> Result<Vec<(String, u64)>, Status> {
        let mut cursor = Cursor::new(decompressed);

        // Read header
        let mut num_bytes = [0u8; 8];
        cursor
            .read_exact(&mut num_bytes)
            .map_err(|e| Status::internal(e.to_string()))?;
        let num_entries = u64::from_le_bytes(num_bytes);

        // Read restart_interval (stored but not needed for reading with unified format)
        let mut interval_bytes = [0u8; 4];
        cursor
            .read_exact(&mut interval_bytes)
            .map_err(|e| Status::internal(e.to_string()))?;

        let mut index = Vec::with_capacity(num_entries as usize);
        let mut prev_key = Vec::new();

        for _ in 0..num_entries {
            // Unified format: prefix_len (u16) + suffix_len (u16) + suffix
            let mut prefix_len_bytes = [0u8; 2];
            cursor
                .read_exact(&mut prefix_len_bytes)
                .map_err(|e| Status::internal(e.to_string()))?;
            let prefix_len = u16::from_le_bytes(prefix_len_bytes) as usize;

            let mut suffix_len_bytes = [0u8; 2];
            cursor
                .read_exact(&mut suffix_len_bytes)
                .map_err(|e| Status::internal(e.to_string()))?;
            let suffix_len = u16::from_le_bytes(suffix_len_bytes) as usize;

            let mut suffix = vec![0u8; suffix_len];
            cursor
                .read_exact(&mut suffix)
                .map_err(|e| Status::internal(e.to_string()))?;

            // Reconstruct key from prefix + suffix
            let mut key_buf = prev_key[..prefix_len].to_vec();
            key_buf.extend_from_slice(&suffix);

            let mut off_bytes = [0u8; 8];
            cursor
                .read_exact(&mut off_bytes)
                .map_err(|e| Status::internal(e.to_string()))?;
            let offset = u64::from_le_bytes(off_bytes);

            let key_string = String::from_utf8_lossy(&key_buf).to_string();
            prev_key = key_buf;
            index.push((key_string, offset));
        }

        Ok(index)
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

        zstd::decode_all(Cursor::new(compressed))
            .map_err(|e| Status::internal(format!("Block decompression error: {}", e)))
    }

    /// Read and deserialize the Bloom filter (V4 only)
    #[allow(clippy::result_large_err)]
    pub fn read_bloom_filter(&self) -> Result<Option<BloomFilter>, Status> {
        let bloom_offset = match self.bloom_offset {
            Some(off) => off as usize,
            None => return Ok(None), // V3 format
        };

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
            .map(Some)
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

/// Generate SSTable filename
pub fn generate_filename(sst_id: u64, wal_id: u64) -> String {
    format!("sst_{:05}_{:05}.data", sst_id, wal_id)
}

/// Search for a key in an SSTable file
#[allow(clippy::result_large_err)]
#[allow(dead_code)]
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

    if version != 3 && version != 4 && version != 5 {
        return Err(Status::internal(format!(
            "Unsupported SSTable version: {}. Only versions 3, 4, and 5 are supported.",
            version
        )));
    }

    let file_len = file
        .metadata()
        .map_err(|e| Status::internal(e.to_string()))?
        .len();

    let footer_size = if version >= 4 {
        FOOTER_SIZE_V4
    } else {
        FOOTER_SIZE_V3
    };

    if file_len < HEADER_SIZE + footer_size {
        return Err(Status::internal("SSTable file too small"));
    }

    // Read footer to get index offset
    let index_offset = if version >= 4 {
        file.seek(SeekFrom::Start(file_len - FOOTER_SIZE_V4))
            .map_err(|e| Status::internal(e.to_string()))?;
        let mut footer = [0u8; 24];
        file.read_exact(&mut footer)
            .map_err(|e| Status::internal(e.to_string()))?;
        u64::from_le_bytes(footer[0..8].try_into().unwrap())
    } else {
        file.seek(SeekFrom::Start(file_len - FOOTER_SIZE_V3))
            .map_err(|e| Status::internal(e.to_string()))?;
        let mut index_offset_bytes = [0u8; 8];
        file.read_exact(&mut index_offset_bytes)
            .map_err(|e| Status::internal(e.to_string()))?;
        u64::from_le_bytes(index_offset_bytes)
    };

    // Read index (V5 uses prefix compression)
    let index = if version >= 5 {
        read_index_prefix_compressed(&mut file, index_offset)?
    } else {
        read_index_compressed(&mut file, index_offset)?
    };

    // Binary search in index
    let start_offset = match index.binary_search_by(|(k, _)| k.as_str().cmp(key)) {
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
    };

    file.seek(SeekFrom::Start(start_offset))
        .map_err(|e| Status::internal(e.to_string()))?;

    // Read compressed block
    let mut len_bytes = [0u8; 4];
    file.read_exact(&mut len_bytes)
        .map_err(|e| Status::internal(e.to_string()))?;
    let len = u32::from_le_bytes(len_bytes);

    let mut compressed_block = vec![0u8; len as usize];
    file.read_exact(&mut compressed_block)
        .map_err(|e| Status::internal(e.to_string()))?;

    let decompressed = zstd::decode_all(Cursor::new(&compressed_block))
        .map_err(|e| Status::internal(format!("Block decompression error: {}", e)))?;

    // Scan decompressed block
    let mut cursor = Cursor::new(decompressed);
    loop {
        // Check if EOF
        if cursor.position() == cursor.get_ref().len() as u64 {
            break;
        }

        let mut ts_bytes = [0u8; 8];
        if cursor.read_exact(&mut ts_bytes).is_err() {
            break;
        }

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

/// Search for a key in an SSTable file with block cache support
/// Uses binary search on parsed block entries for O(log n) lookup within blocks
#[allow(clippy::result_large_err)]
#[allow(dead_code)]
pub fn search_key_cached(
    path: &Path,
    key: &str,
    cache: &BlockCache,
) -> Result<Option<String>, Status> {
    // Try to get canonical path for consistent cache keys
    let canonical_path = path.canonicalize().unwrap_or_else(|_| path.to_path_buf());

    // Get or load index from cache
    let index = get_or_load_index(path, &canonical_path, cache)?;

    // Binary search in index to find the right block
    let block_offset = match index.binary_search_by(|(k, _)| k.as_str().cmp(key)) {
        Ok(idx) => index[idx].1,
        Err(idx) => {
            if idx == 0 {
                index[0].1
            } else {
                index[idx - 1].1
            }
        }
    };

    // Get or load parsed block from cache
    let parsed_entries = get_or_load_parsed_block(path, &canonical_path, block_offset, cache)?;

    // Binary search within parsed block entries
    search_in_parsed_block(&parsed_entries, key)
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

    // Binary search in index to find the right block
    let block_offset = match index.binary_search_by(|(k, _)| k.as_str().cmp(key)) {
        Ok(idx) => index[idx].1,
        Err(idx) => {
            if idx == 0 {
                index[0].1
            } else {
                index[idx - 1].1
            }
        }
    };

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

    // Load from mmap
    let index = sst.read_index()?;
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

/// Get index from cache or load from file
#[allow(clippy::result_large_err)]
#[allow(dead_code)]
fn get_or_load_index(
    path: &Path,
    canonical_path: &Path,
    cache: &BlockCache,
) -> Result<Arc<Vec<(String, u64)>>, Status> {
    let cache_key = BlockCacheKey::for_index(canonical_path.to_path_buf());

    // Check cache first
    if let Some(CacheEntry::Index(index)) = cache.get(&cache_key) {
        return Ok(index);
    }

    // Load from file
    let mut file = match File::open(path) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Err(Status::not_found("SSTable file not found"));
        }
        Err(e) => return Err(Status::internal(e.to_string())),
    };

    // Validate header
    let mut magic = [0u8; 6];
    if file.read_exact(&mut magic).is_err() || &magic != MAGIC_BYTES {
        return Err(Status::internal("Invalid SSTable magic"));
    }

    let mut ver_bytes = [0u8; 4];
    file.read_exact(&mut ver_bytes)
        .map_err(|e| Status::internal(e.to_string()))?;
    let version = u32::from_le_bytes(ver_bytes);

    if version != 3 && version != 4 && version != 5 {
        return Err(Status::internal(format!(
            "Unsupported SSTable version: {}. Only versions 3, 4, and 5 are supported.",
            version
        )));
    }

    let file_len = file
        .metadata()
        .map_err(|e| Status::internal(e.to_string()))?
        .len();

    let footer_size = if version >= 4 {
        FOOTER_SIZE_V4
    } else {
        FOOTER_SIZE_V3
    };

    if file_len < HEADER_SIZE + footer_size {
        return Err(Status::internal("SSTable file too small"));
    }

    // Read footer to get index offset
    let index_offset = if version >= 4 {
        file.seek(SeekFrom::Start(file_len - FOOTER_SIZE_V4))
            .map_err(|e| Status::internal(e.to_string()))?;
        let mut footer = [0u8; 24];
        file.read_exact(&mut footer)
            .map_err(|e| Status::internal(e.to_string()))?;
        u64::from_le_bytes(footer[0..8].try_into().unwrap())
    } else {
        file.seek(SeekFrom::Start(file_len - FOOTER_SIZE_V3))
            .map_err(|e| Status::internal(e.to_string()))?;
        let mut index_offset_bytes = [0u8; 8];
        file.read_exact(&mut index_offset_bytes)
            .map_err(|e| Status::internal(e.to_string()))?;
        u64::from_le_bytes(index_offset_bytes)
    };

    // Read and decompress index (V5 uses prefix compression)
    let index = if version >= 5 {
        read_index_prefix_compressed(&mut file, index_offset)?
    } else {
        read_index_compressed(&mut file, index_offset)?
    };
    let arc_index = Arc::new(index);

    // Cache it
    cache.insert(cache_key, CacheEntry::Index(Arc::clone(&arc_index)));

    Ok(arc_index)
}

/// Get parsed block from cache or load and parse from file
/// Returns parsed entries sorted by key for binary search
#[allow(clippy::result_large_err)]
#[allow(dead_code)]
fn get_or_load_parsed_block(
    path: &Path,
    canonical_path: &Path,
    block_offset: u64,
    cache: &BlockCache,
) -> Result<Arc<Vec<ParsedBlockEntry>>, Status> {
    let cache_key = BlockCacheKey::for_block(canonical_path.to_path_buf(), block_offset);

    // Check cache first for parsed block
    if let Some(CacheEntry::ParsedBlock(entries)) = cache.get(&cache_key) {
        return Ok(entries);
    }

    // Load from file
    let mut file = match File::open(path) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Err(Status::not_found("SSTable file not found"));
        }
        Err(e) => return Err(Status::internal(e.to_string())),
    };

    file.seek(SeekFrom::Start(block_offset))
        .map_err(|e| Status::internal(e.to_string()))?;

    // Read compressed block
    let mut len_bytes = [0u8; 4];
    file.read_exact(&mut len_bytes)
        .map_err(|e| Status::internal(e.to_string()))?;
    let len = u32::from_le_bytes(len_bytes);

    let mut compressed_block = vec![0u8; len as usize];
    file.read_exact(&mut compressed_block)
        .map_err(|e| Status::internal(e.to_string()))?;

    let decompressed = zstd::decode_all(Cursor::new(&compressed_block))
        .map_err(|e| Status::internal(format!("Block decompression error: {}", e)))?;

    // Parse all entries in the block
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
    if version != 3 && version != 4 && version != 5 {
        return Err(Status::internal(format!(
            "Unsupported SSTable version: {}. Only versions 3, 4, and 5 are supported.",
            version
        )));
    }

    let file_len = file
        .metadata()
        .map_err(|e| Status::internal(e.to_string()))?
        .len();

    let footer_size = if version >= 4 {
        FOOTER_SIZE_V4
    } else {
        FOOTER_SIZE_V3
    };

    if file_len < HEADER_SIZE + footer_size {
        return Ok(Vec::new());
    }

    // Read footer to get index offset (marks end of data blocks)
    let end_offset = if version >= 4 {
        file.seek(SeekFrom::Start(file_len - FOOTER_SIZE_V4))
            .map_err(|e| Status::internal(e.to_string()))?;
        let mut footer = [0u8; 24];
        file.read_exact(&mut footer)
            .map_err(|e| Status::internal(e.to_string()))?;
        u64::from_le_bytes(footer[0..8].try_into().unwrap())
    } else {
        file.seek(SeekFrom::Start(file_len - FOOTER_SIZE_V3))
            .map_err(|e| Status::internal(e.to_string()))?;
        let mut index_offset_bytes = [0u8; 8];
        file.read_exact(&mut index_offset_bytes)
            .map_err(|e| Status::internal(e.to_string()))?;
        u64::from_le_bytes(index_offset_bytes)
    };

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

        let mut compressed_block = vec![0u8; len as usize];
        file.read_exact(&mut compressed_block)
            .map_err(|e| Status::internal(e.to_string()))?;

        let decompressed = zstd::decode_all(Cursor::new(&compressed_block))
            .map_err(|e| Status::internal(format!("Block decompression error: {}", e)))?;

        let mut cursor = Cursor::new(decompressed);
        loop {
            if cursor.position() == cursor.get_ref().len() as u64 {
                break;
            }

            // Skip timestamp (8) + read klen (8) + vlen (8)
            cursor
                .seek(SeekFrom::Current(8))
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

    if version != 3 && version != 4 && version != 5 {
        return Err(Status::internal(format!(
            "Unsupported SSTable version: {}. Only versions 3, 4, and 5 are supported.",
            version
        )));
    }

    let file_len = file
        .metadata()
        .map_err(|e| Status::internal(e.to_string()))?
        .len();

    let footer_size = if version >= 4 {
        FOOTER_SIZE_V4
    } else {
        FOOTER_SIZE_V3
    };

    if file_len < HEADER_SIZE + footer_size {
        return Ok(BTreeMap::new()); // Corrupt or empty?
    }

    // Read footer to get index offset (which marks end of data blocks)
    let end_offset = if version >= 4 {
        file.seek(SeekFrom::Start(file_len - FOOTER_SIZE_V4))
            .map_err(|e| Status::internal(e.to_string()))?;
        let mut footer = [0u8; 24];
        file.read_exact(&mut footer)
            .map_err(|e| Status::internal(e.to_string()))?;
        u64::from_le_bytes(footer[0..8].try_into().unwrap())
    } else {
        file.seek(SeekFrom::Start(file_len - FOOTER_SIZE_V3))
            .map_err(|e| Status::internal(e.to_string()))?;
        let mut index_offset_bytes = [0u8; 8];
        file.read_exact(&mut index_offset_bytes)
            .map_err(|e| Status::internal(e.to_string()))?;
        u64::from_le_bytes(index_offset_bytes)
    };

    file.seek(SeekFrom::Start(HEADER_SIZE))
        .map_err(|e| Status::internal(e.to_string()))?;

    let mut entries = BTreeMap::new();

    // V3: Block Compressed
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

        let mut compressed_block = vec![0u8; len as usize];
        file.read_exact(&mut compressed_block)
            .map_err(|e| Status::internal(e.to_string()))?;

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

            entries.insert(key, (timestamp, value));
        }
    }

    Ok(entries)
}

/// Write a MemTable to an SSTable file
#[allow(clippy::result_large_err)]
pub fn create_from_memtable(path: &Path, memtable: &MemTable) -> Result<BloomFilter, Status> {
    // Write to a temporary file first, then rename for atomic operation
    let tmp_path = path.with_extension("data.tmp");

    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(&tmp_path)
        .map_err(|e| Status::internal(e.to_string()))?;

    // Write header
    file.write_all(MAGIC_BYTES)
        .map_err(|e| Status::internal(e.to_string()))?;
    file.write_all(&DATA_VERSION.to_le_bytes())
        .map_err(|e| Status::internal(e.to_string()))?;

    // Build Bloom filter from keys
    let mut bloom = BloomFilter::new(memtable.len().max(1), 0.01);
    for key in memtable.keys() {
        bloom.insert(key);
    }

    let mut current_offset = HEADER_SIZE;
    let mut index: Vec<(String, u64)> = Vec::new();
    let mut block_buffer = Vec::with_capacity(BLOCK_SIZE);
    let mut first_key_in_block = String::new();

    // Write entries
    for (key, value_opt) in memtable.iter() {
        if block_buffer.is_empty() {
            first_key_in_block = key.clone();
        }

        write_entry(&mut block_buffer, key, value_opt, None)?;

        if block_buffer.len() >= BLOCK_SIZE {
            // Compress and write block
            index.push((first_key_in_block.clone(), current_offset));

            let compressed = zstd::encode_all(Cursor::new(&block_buffer), COMPRESSION_LEVEL_FLUSH)
                .map_err(|e| Status::internal(format!("Compression error: {}", e)))?;

            let len = compressed.len() as u32;
            file.write_all(&len.to_le_bytes())
                .map_err(|e| Status::internal(e.to_string()))?;
            file.write_all(&compressed)
                .map_err(|e| Status::internal(e.to_string()))?;

            current_offset += 4 + len as u64;
            block_buffer.clear();
        }
    }

    // Write remaining block
    if !block_buffer.is_empty() {
        index.push((first_key_in_block, current_offset));

        let compressed = zstd::encode_all(Cursor::new(&block_buffer), COMPRESSION_LEVEL_FLUSH)
            .map_err(|e| Status::internal(format!("Compression error: {}", e)))?;

        let len = compressed.len() as u32;
        file.write_all(&len.to_le_bytes())
            .map_err(|e| Status::internal(e.to_string()))?;
        file.write_all(&compressed)
            .map_err(|e| Status::internal(e.to_string()))?;

        current_offset += 4 + len as u64;
    }

    // Write index with prefix compression (V5)
    let index_offset = current_offset;
    let index_size = write_index_prefix_compressed(&mut file, &index, COMPRESSION_LEVEL_INDEX)?;
    current_offset += index_size;

    // Write Bloom filter
    let bloom_offset = current_offset;
    let bloom_data = bloom.serialize();
    let bloom_len = bloom_data.len() as u64;
    file.write_all(&bloom_len.to_le_bytes())
        .map_err(|e| Status::internal(e.to_string()))?;
    file.write_all(&bloom_data)
        .map_err(|e| Status::internal(e.to_string()))?;

    // Write V5 footer: index_offset + bloom_offset + magic
    file.write_all(&index_offset.to_le_bytes())
        .map_err(|e| Status::internal(e.to_string()))?;
    file.write_all(&bloom_offset.to_le_bytes())
        .map_err(|e| Status::internal(e.to_string()))?;
    file.write_all(&FOOTER_MAGIC.to_le_bytes())
        .map_err(|e| Status::internal(e.to_string()))?;

    file.flush().map_err(|e| Status::internal(e.to_string()))?;
    file.sync_data()
        .map_err(|e| Status::internal(e.to_string()))?;

    // Atomically rename tmp file to final path
    std::fs::rename(&tmp_path, path)
        .map_err(|e| Status::internal(format!("Failed to rename SSTable: {}", e)))?;

    Ok(bloom)
}

/// Write timestamped entries to an SSTable file (used for compaction)
#[allow(clippy::result_large_err)]
pub fn write_timestamped_entries(
    path: &Path,
    entries: &BTreeMap<String, TimestampedEntry>,
) -> Result<BloomFilter, Status> {
    // Write to a temporary file first, then rename for atomic operation
    let tmp_path = path.with_extension("data.tmp");

    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(&tmp_path)
        .map_err(|e| Status::internal(e.to_string()))?;

    // Write header
    file.write_all(MAGIC_BYTES)
        .map_err(|e| Status::internal(e.to_string()))?;
    file.write_all(&DATA_VERSION.to_le_bytes())
        .map_err(|e| Status::internal(e.to_string()))?;

    // Build Bloom filter from keys
    let mut bloom = BloomFilter::new(entries.len().max(1), 0.01);
    for key in entries.keys() {
        bloom.insert(key);
    }

    let mut current_offset = HEADER_SIZE;
    let mut index: Vec<(String, u64)> = Vec::new();
    let mut block_buffer = Vec::with_capacity(BLOCK_SIZE);
    let mut first_key_in_block = String::new();

    // Write entries
    for (key, (timestamp, value_opt)) in entries {
        if block_buffer.is_empty() {
            first_key_in_block = key.clone();
        }

        write_entry(&mut block_buffer, key, value_opt, Some(*timestamp))?;

        if block_buffer.len() >= BLOCK_SIZE {
            index.push((first_key_in_block.clone(), current_offset));

            let compressed =
                zstd::encode_all(Cursor::new(&block_buffer), COMPRESSION_LEVEL_COMPACTION)
                    .map_err(|e| Status::internal(format!("Compression error: {}", e)))?;

            let len = compressed.len() as u32;
            file.write_all(&len.to_le_bytes())
                .map_err(|e| Status::internal(e.to_string()))?;
            file.write_all(&compressed)
                .map_err(|e| Status::internal(e.to_string()))?;

            current_offset += 4 + len as u64;
            block_buffer.clear();
        }
    }

    // Write remaining block
    if !block_buffer.is_empty() {
        index.push((first_key_in_block, current_offset));

        let compressed = zstd::encode_all(Cursor::new(&block_buffer), COMPRESSION_LEVEL_COMPACTION)
            .map_err(|e| Status::internal(format!("Compression error: {}", e)))?;

        let len = compressed.len() as u32;
        file.write_all(&len.to_le_bytes())
            .map_err(|e| Status::internal(e.to_string()))?;
        file.write_all(&compressed)
            .map_err(|e| Status::internal(e.to_string()))?;

        current_offset += 4 + len as u64;
    }

    // Write index with prefix compression (V5)
    let index_offset = current_offset;
    let index_size = write_index_prefix_compressed(&mut file, &index, COMPRESSION_LEVEL_INDEX)?;
    current_offset += index_size;

    // Write Bloom filter
    let bloom_offset = current_offset;
    let bloom_data = bloom.serialize();
    let bloom_len = bloom_data.len() as u64;
    file.write_all(&bloom_len.to_le_bytes())
        .map_err(|e| Status::internal(e.to_string()))?;
    file.write_all(&bloom_data)
        .map_err(|e| Status::internal(e.to_string()))?;

    // Write V5 footer: index_offset + bloom_offset + magic
    file.write_all(&index_offset.to_le_bytes())
        .map_err(|e| Status::internal(e.to_string()))?;
    file.write_all(&bloom_offset.to_le_bytes())
        .map_err(|e| Status::internal(e.to_string()))?;
    file.write_all(&FOOTER_MAGIC.to_le_bytes())
        .map_err(|e| Status::internal(e.to_string()))?;

    file.flush().map_err(|e| Status::internal(e.to_string()))?;
    file.sync_data()
        .map_err(|e| Status::internal(e.to_string()))?;

    // Atomically rename tmp file to final path
    std::fs::rename(&tmp_path, path)
        .map_err(|e| Status::internal(format!("Failed to rename SSTable: {}", e)))?;

    Ok(bloom)
}

/// Write a single entry to a writer
#[allow(clippy::result_large_err)]
fn write_entry(
    writer: &mut impl Write,
    key: &str,
    value_opt: &Option<String>,
    timestamp: Option<u64>,
) -> Result<u64, Status> {
    let key_bytes = key.as_bytes();
    let key_len = key_bytes.len() as u64;
    let (val_len, val_bytes) = match value_opt {
        Some(v) => (v.len() as u64, v.as_bytes()),
        None => (u64::MAX, &[] as &[u8]),
    };

    let ts = timestamp.unwrap_or_else(|| {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
    });

    writer
        .write_all(&ts.to_le_bytes())
        .map_err(|e| Status::internal(e.to_string()))?;
    writer
        .write_all(&key_len.to_le_bytes())
        .map_err(|e| Status::internal(e.to_string()))?;
    writer
        .write_all(&val_len.to_le_bytes())
        .map_err(|e| Status::internal(e.to_string()))?;
    writer
        .write_all(key_bytes)
        .map_err(|e| Status::internal(e.to_string()))?;
    if val_len != u64::MAX {
        writer
            .write_all(val_bytes)
            .map_err(|e| Status::internal(e.to_string()))?;
    }

    let written = 8 + 8 + 8 + key_len + if val_len == u64::MAX { 0 } else { val_len };
    Ok(written)
}

/// Parse SSTable filename and return (sst_id, wal_id) if valid
pub fn parse_filename(filename: &str) -> Option<(u64, Option<u64>)> {
    if filename.starts_with("sst_") && filename.ends_with(".data") {
        let name_part = &filename[4..filename.len() - 5];
        // Try new format first: sst_{sst_id}_{wal_id}.data
        if let Some(pos) = name_part.find('_') {
            if let (Ok(sst_id), Ok(wal_id)) = (
                name_part[..pos].parse::<u64>(),
                name_part[pos + 1..].parse::<u64>(),
            ) {
                return Some((sst_id, Some(wal_id)));
            }
        } else if let Ok(sst_id) = name_part.parse::<u64>() {
            // Old format: sst_{id}.data
            return Some((sst_id, None));
        }
    }
    None
}

/// Generate SSTable path from data directory, SSTable ID, and WAL ID
pub fn generate_path(data_dir: &Path, sst_id: u64, wal_id: u64) -> PathBuf {
    data_dir.join(generate_filename(sst_id, wal_id))
}

/// Read Bloom filter from SSTable file (V4 format)
/// Returns None for V3 format (Bloom filter not embedded)
#[allow(clippy::result_large_err)]
#[allow(dead_code)]
pub fn read_bloom_filter(path: &Path) -> Result<Option<BloomFilter>, Status> {
    let mut file = match File::open(path) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Ok(None);
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

    if version < 4 {
        // V3 or earlier: no embedded Bloom filter
        return Ok(None);
    }

    let file_len = file
        .metadata()
        .map_err(|e| Status::internal(e.to_string()))?
        .len();

    if file_len < HEADER_SIZE + FOOTER_SIZE_V4 {
        return Err(Status::internal("SSTable file too small for V4 format"));
    }

    // Read footer
    file.seek(SeekFrom::Start(file_len - FOOTER_SIZE_V4))
        .map_err(|e| Status::internal(e.to_string()))?;

    let mut footer = [0u8; 24];
    file.read_exact(&mut footer)
        .map_err(|e| Status::internal(e.to_string()))?;

    let _index_offset = u64::from_le_bytes(footer[0..8].try_into().unwrap());
    let bloom_offset = u64::from_le_bytes(footer[8..16].try_into().unwrap());
    let footer_magic = u64::from_le_bytes(footer[16..24].try_into().unwrap());

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
        .map(Some)
        .ok_or_else(|| Status::internal("Failed to deserialize Bloom filter"))
}

#[allow(clippy::result_large_err)]
#[allow(dead_code)]
fn write_index_compressed(
    file: &mut File,
    index: &[(String, u64)],
    compression_level: i32,
) -> Result<u64, Status> {
    let mut buffer = Vec::new();
    let num_entries = index.len() as u64;
    buffer
        .write_all(&num_entries.to_le_bytes())
        .map_err(|e| Status::internal(e.to_string()))?;

    for (key, offset) in index {
        let key_bytes = key.as_bytes();
        let key_len = key_bytes.len() as u64;

        buffer
            .write_all(&key_len.to_le_bytes())
            .map_err(|e| Status::internal(e.to_string()))?;
        buffer
            .write_all(key_bytes)
            .map_err(|e| Status::internal(e.to_string()))?;
        buffer
            .write_all(&offset.to_le_bytes())
            .map_err(|e| Status::internal(e.to_string()))?;
    }

    let compressed = zstd::encode_all(Cursor::new(&buffer), compression_level)
        .map_err(|e| Status::internal(format!("Index compression error: {}", e)))?;

    // Write compressed size then data
    let len = compressed.len() as u64;
    file.write_all(&len.to_le_bytes())
        .map_err(|e| Status::internal(e.to_string()))?;
    file.write_all(&compressed)
        .map_err(|e| Status::internal(e.to_string()))?;

    // Return total bytes written: 8 (size) + len (compressed data)
    Ok(8 + len)
}

/// Compute the length of the common prefix between two byte slices
fn common_prefix_len(a: &[u8], b: &[u8]) -> usize {
    a.iter().zip(b.iter()).take_while(|(x, y)| x == y).count()
}

/// Write index with prefix compression (V5 format)
/// Format:
/// - num_entries: u64
/// - restart_interval: u32
/// - For each entry (unified format):
///   - prefix_len (u16) + suffix_len (u16) + suffix + offset (u64)
///   - Restart points have prefix_len = 0, suffix = full key
#[allow(clippy::result_large_err)]
fn write_index_prefix_compressed(
    file: &mut File,
    index: &[(String, u64)],
    compression_level: i32,
) -> Result<u64, Status> {
    let mut buffer = Vec::new();
    let num_entries = index.len() as u64;

    buffer
        .write_all(&num_entries.to_le_bytes())
        .map_err(|e| Status::internal(e.to_string()))?;
    buffer
        .write_all(&INDEX_RESTART_INTERVAL.to_le_bytes())
        .map_err(|e| Status::internal(e.to_string()))?;

    let mut prev_key: &[u8] = &[];

    for (i, (key, offset)) in index.iter().enumerate() {
        let key_bytes = key.as_bytes();

        // Unified format: prefix_len (u16) + suffix_len (u16) + suffix
        // Restart points use prefix_len = 0
        let prefix_len = if (i as u32).is_multiple_of(INDEX_RESTART_INTERVAL) {
            0
        } else {
            common_prefix_len(prev_key, key_bytes)
        };
        let suffix = &key_bytes[prefix_len..];

        buffer
            .write_all(&(prefix_len as u16).to_le_bytes())
            .map_err(|e| Status::internal(e.to_string()))?;
        buffer
            .write_all(&(suffix.len() as u16).to_le_bytes())
            .map_err(|e| Status::internal(e.to_string()))?;
        buffer
            .write_all(suffix)
            .map_err(|e| Status::internal(e.to_string()))?;
        buffer
            .write_all(&offset.to_le_bytes())
            .map_err(|e| Status::internal(e.to_string()))?;

        prev_key = key_bytes;
    }

    let compressed = zstd::encode_all(Cursor::new(&buffer), compression_level)
        .map_err(|e| Status::internal(format!("Index compression error: {}", e)))?;

    // Write compressed size then data
    let len = compressed.len() as u64;
    file.write_all(&len.to_le_bytes())
        .map_err(|e| Status::internal(e.to_string()))?;
    file.write_all(&compressed)
        .map_err(|e| Status::internal(e.to_string()))?;

    Ok(8 + len)
}

/// Read index with prefix compression (V5 format)
/// Unified format: every entry uses prefix_len (u16) + suffix_len (u16) + suffix
#[allow(clippy::result_large_err)]
#[allow(dead_code)]
fn read_index_prefix_compressed(
    file: &mut File,
    index_offset: u64,
) -> Result<Vec<(String, u64)>, Status> {
    file.seek(SeekFrom::Start(index_offset))
        .map_err(|e| Status::internal(e.to_string()))?;

    let mut len_bytes = [0u8; 8];
    file.read_exact(&mut len_bytes)
        .map_err(|e| Status::internal(e.to_string()))?;
    let len = u64::from_le_bytes(len_bytes);

    let mut compressed = vec![0u8; len as usize];
    file.read_exact(&mut compressed)
        .map_err(|e| Status::internal(e.to_string()))?;

    let decompressed = zstd::decode_all(Cursor::new(&compressed))
        .map_err(|e| Status::internal(format!("Index decompression error: {}", e)))?;

    let mut cursor = Cursor::new(decompressed);

    // Read header
    let mut num_bytes = [0u8; 8];
    cursor
        .read_exact(&mut num_bytes)
        .map_err(|e| Status::internal(e.to_string()))?;
    let num_entries = u64::from_le_bytes(num_bytes);

    // Read restart_interval (stored but not needed for reading with unified format)
    let mut interval_bytes = [0u8; 4];
    cursor
        .read_exact(&mut interval_bytes)
        .map_err(|e| Status::internal(e.to_string()))?;

    let mut index = Vec::with_capacity(num_entries as usize);
    let mut prev_key = Vec::new();

    for _ in 0..num_entries {
        // Unified format: prefix_len (u16) + suffix_len (u16) + suffix
        let mut prefix_len_bytes = [0u8; 2];
        cursor
            .read_exact(&mut prefix_len_bytes)
            .map_err(|e| Status::internal(e.to_string()))?;
        let prefix_len = u16::from_le_bytes(prefix_len_bytes) as usize;

        let mut suffix_len_bytes = [0u8; 2];
        cursor
            .read_exact(&mut suffix_len_bytes)
            .map_err(|e| Status::internal(e.to_string()))?;
        let suffix_len = u16::from_le_bytes(suffix_len_bytes) as usize;

        let mut suffix = vec![0u8; suffix_len];
        cursor
            .read_exact(&mut suffix)
            .map_err(|e| Status::internal(e.to_string()))?;

        // Reconstruct key from prefix + suffix
        let mut key_buf = prev_key[..prefix_len].to_vec();
        key_buf.extend_from_slice(&suffix);

        let mut off_bytes = [0u8; 8];
        cursor
            .read_exact(&mut off_bytes)
            .map_err(|e| Status::internal(e.to_string()))?;
        let offset = u64::from_le_bytes(off_bytes);

        let key_string = String::from_utf8_lossy(&key_buf).to_string();
        prev_key = key_buf;
        index.push((key_string, offset));
    }

    Ok(index)
}

#[allow(clippy::result_large_err)]
#[allow(dead_code)]
fn read_index_compressed(file: &mut File, index_offset: u64) -> Result<Vec<(String, u64)>, Status> {
    file.seek(SeekFrom::Start(index_offset))
        .map_err(|e| Status::internal(e.to_string()))?;

    let mut len_bytes = [0u8; 8];
    file.read_exact(&mut len_bytes)
        .map_err(|e| Status::internal(e.to_string()))?;
    let len = u64::from_le_bytes(len_bytes);

    let mut compressed = vec![0u8; len as usize];
    file.read_exact(&mut compressed)
        .map_err(|e| Status::internal(e.to_string()))?;

    let decompressed = zstd::decode_all(Cursor::new(&compressed))
        .map_err(|e| Status::internal(format!("Index decompression error: {}", e)))?;

    let mut cursor = Cursor::new(decompressed);
    let mut num_bytes = [0u8; 8];
    cursor
        .read_exact(&mut num_bytes)
        .map_err(|e| Status::internal(e.to_string()))?;
    let num_entries = u64::from_le_bytes(num_bytes);

    let mut index = Vec::with_capacity(num_entries as usize);

    for _ in 0..num_entries {
        let mut klen_bytes = [0u8; 8];
        cursor
            .read_exact(&mut klen_bytes)
            .map_err(|e| Status::internal(e.to_string()))?;
        let key_len = u64::from_le_bytes(klen_bytes);

        let mut key_buf = vec![0u8; key_len as usize];
        cursor
            .read_exact(&mut key_buf)
            .map_err(|e| Status::internal(e.to_string()))?;
        let key = String::from_utf8_lossy(&key_buf).to_string();

        let mut off_bytes = [0u8; 8];
        cursor
            .read_exact(&mut off_bytes)
            .map_err(|e| Status::internal(e.to_string()))?;
        let offset = u64::from_le_bytes(off_bytes);

        index.push((key, offset));
    }

    Ok(index)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_parse_filename() {
        assert_eq!(parse_filename("sst_00001_00002.data"), Some((1, Some(2))));
        assert_eq!(parse_filename("sst_00010.data"), Some((10, None)));
        assert_eq!(parse_filename("invalid"), None);
        assert_eq!(parse_filename("sst_abc.data"), None);
    }

    #[test]
    fn test_sstable_creation_and_search() {
        let dir = tempdir().unwrap();
        let sst_path = dir.path().join("sst_00001_00001.data");

        let mut memtable = BTreeMap::new();
        memtable.insert("key1".to_string(), Some("value1".to_string()));
        memtable.insert("key2".to_string(), Some("value2".to_string()));
        memtable.insert("key3".to_string(), None); // Tombstone

        create_from_memtable(&sst_path, &memtable).unwrap();

        assert_eq!(
            search_key(&sst_path, "key1").unwrap(),
            Some("value1".to_string())
        );
        assert_eq!(
            search_key(&sst_path, "key2").unwrap(),
            Some("value2".to_string())
        );
        assert_eq!(search_key(&sst_path, "key3").unwrap(), None); // Should return None for tombstone
        assert!(search_key(&sst_path, "nonexistent").is_err());
    }

    #[test]
    fn test_sstable_multiple_blocks() {
        let dir = tempdir().unwrap();
        let sst_path = dir.path().join("sst_00002_00001.data");

        // Use a small block size for testing if possible, but BLOCK_SIZE is constant.
        // We'll insert enough data to trigger multiple blocks.
        let mut memtable = BTreeMap::new();
        let entry_count = 500;
        for i in 0..entry_count {
            let key = format!("key{:05}", i);
            let value = format!("value{:05}", i);
            memtable.insert(key, Some(value));
        }

        create_from_memtable(&sst_path, &memtable).unwrap();

        // Check some keys
        for i in (0..entry_count).step_by(50) {
            let key = format!("key{:05}", i);
            let expected_value = format!("value{:05}", i);
            assert_eq!(search_key(&sst_path, &key).unwrap(), Some(expected_value));
        }

        // Read all entries and verify
        let read_back = read_entries(&sst_path).unwrap();
        assert_eq!(read_back.len(), entry_count);
        for (key, (_, value)) in read_back {
            let i_str = &key[3..];
            let expected_value = format!("value{}", i_str);
            assert_eq!(value, Some(expected_value));
        }
    }

    #[test]
    fn test_read_entries_empty_file() {
        let dir = tempdir().unwrap();
        let sst_path = dir.path().join("empty.data");

        // create_from_memtable with empty memtable
        let memtable = BTreeMap::new();
        create_from_memtable(&sst_path, &memtable).unwrap();

        let entries = read_entries(&sst_path).unwrap();
        assert!(entries.is_empty());
    }

    #[test]
    fn test_extract_wal_id() {
        let path = Path::new("sst_00001_00123.data");
        assert_eq!(extract_wal_id_from_sstable(path), Some(123));

        let path_old = Path::new("sst_00001.data");
        assert_eq!(extract_wal_id_from_sstable(path_old), None);
    }

    #[test]
    fn test_write_timestamped_entries() {
        let dir = tempdir().unwrap();
        let sst_path = dir.path().join("sst_ts.data");

        let mut entries = BTreeMap::new();
        entries.insert("k1".to_string(), (100, Some("v1".to_string())));
        entries.insert("k2".to_string(), (200, None)); // Tombstone with specific TS

        write_timestamped_entries(&sst_path, &entries).unwrap();

        let read_back = read_entries(&sst_path).unwrap();
        assert_eq!(read_back.get("k1").unwrap(), &(100, Some("v1".to_string())));
        assert_eq!(read_back.get("k2").unwrap(), &(200, None));
    }

    #[test]
    fn test_read_bloom_filter_v4() {
        let dir = tempdir().unwrap();
        let sst_path = dir.path().join("sst_bloom_v4.data");

        let mut memtable = BTreeMap::new();
        memtable.insert("apple".to_string(), Some("red".to_string()));
        memtable.insert("banana".to_string(), Some("yellow".to_string()));
        memtable.insert("cherry".to_string(), Some("red".to_string()));

        // This creates a V4 SSTable with embedded Bloom filter
        create_from_memtable(&sst_path, &memtable).unwrap();

        // Read the Bloom filter from the SSTable
        let bloom = read_bloom_filter(&sst_path)
            .expect("should read successfully")
            .expect("should have Bloom filter (V4)");

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
    fn test_sstable_large_entries() {
        let dir = tempdir().unwrap();
        let sst_path = dir.path().join("sst_large.data");

        let mut memtable = BTreeMap::new();
        let large_key = "k".repeat(1024); // 1KB key
        let large_value = "v".repeat(1024 * 1024); // 1MB value
        memtable.insert(large_key.clone(), Some(large_value.clone()));
        memtable.insert("short".to_string(), Some("value".to_string()));

        create_from_memtable(&sst_path, &memtable).unwrap();

        assert_eq!(
            search_key(&sst_path, &large_key).unwrap(),
            Some(large_value)
        );
        assert_eq!(
            search_key(&sst_path, "short").unwrap(),
            Some("value".to_string())
        );
    }

    #[test]
    fn test_corrupt_block_data() {
        let dir = tempdir().unwrap();
        let sst_path = dir.path().join("corrupt_block.data");

        let mut memtable = BTreeMap::new();
        memtable.insert("key1".to_string(), Some("value1".to_string()));
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
        assert!(
            result
                .unwrap_err()
                .message()
                .contains("decompression error")
        );
    }

    #[test]
    fn test_corrupt_index_data() {
        let dir = tempdir().unwrap();
        let sst_path = dir.path().join("corrupt_index.data");

        let mut memtable = BTreeMap::new();
        memtable.insert("key1".to_string(), Some("value1".to_string()));
        create_from_memtable(&sst_path, &memtable).unwrap();

        // Corrupt the index offset in the footer (V4 format)
        // Footer layout: index_offset(8) + bloom_offset(8) + footer_magic(8)
        // index_offset is at offset len - 24
        let mut data = std::fs::read(&sst_path).unwrap();
        let len = data.len();
        if len > 24 {
            // Change the index offset to something invalid
            data[len - 24] ^= 0xFF;
        }
        std::fs::write(&sst_path, data).unwrap();

        let result = search_key(&sst_path, "key1");
        assert!(result.is_err());
    }

    #[test]
    fn test_prefix_compression_roundtrip() {
        let dir = tempdir().unwrap();
        let index_path = dir.path().join("index_test.data");

        // Create index with keys that have common prefixes
        let index: Vec<(String, u64)> = (0..50)
            .map(|i| (format!("user:profile:{:05}", i), i * 100))
            .collect();

        // Write index with prefix compression
        {
            let mut file = File::create(&index_path).unwrap();
            write_index_prefix_compressed(&mut file, &index, 3).unwrap();
        }

        // Read it back
        let read_index = {
            let mut file = File::open(&index_path).unwrap();
            read_index_prefix_compressed(&mut file, 0).unwrap()
        };

        // Verify all entries match
        assert_eq!(index.len(), read_index.len());
        for (original, read) in index.iter().zip(read_index.iter()) {
            assert_eq!(original.0, read.0, "Key mismatch");
            assert_eq!(original.1, read.1, "Offset mismatch");
        }
    }

    #[test]
    fn test_prefix_compression_restart_points() {
        let dir = tempdir().unwrap();
        let index_path = dir.path().join("restart_test.data");

        // Create index with 20 entries (more than restart interval of 16)
        // to verify restart points work correctly
        let index: Vec<(String, u64)> =
            (0..20).map(|i| (format!("key:{:03}", i), i * 50)).collect();

        {
            let mut file = File::create(&index_path).unwrap();
            write_index_prefix_compressed(&mut file, &index, 3).unwrap();
        }

        let read_index = {
            let mut file = File::open(&index_path).unwrap();
            read_index_prefix_compressed(&mut file, 0).unwrap()
        };

        // Entry 0 and 16 are restart points (prefix_len = 0)
        // Verify they are correctly reconstructed
        assert_eq!(read_index[0].0, "key:000");
        assert_eq!(read_index[16].0, "key:016");
        assert_eq!(read_index.len(), 20);
    }

    #[test]
    fn test_prefix_compression_no_common_prefix() {
        let dir = tempdir().unwrap();
        let index_path = dir.path().join("no_prefix_test.data");

        // Keys with no common prefix
        let index: Vec<(String, u64)> = vec![
            ("apple".to_string(), 100),
            ("banana".to_string(), 200),
            ("cherry".to_string(), 300),
        ];

        {
            let mut file = File::create(&index_path).unwrap();
            write_index_prefix_compressed(&mut file, &index, 3).unwrap();
        }

        let read_index = {
            let mut file = File::open(&index_path).unwrap();
            read_index_prefix_compressed(&mut file, 0).unwrap()
        };

        assert_eq!(read_index, index);
    }

    #[test]
    fn test_prefix_compression_empty_index() {
        let dir = tempdir().unwrap();
        let index_path = dir.path().join("empty_index_test.data");

        let index: Vec<(String, u64)> = vec![];

        {
            let mut file = File::create(&index_path).unwrap();
            write_index_prefix_compressed(&mut file, &index, 3).unwrap();
        }

        let read_index = {
            let mut file = File::open(&index_path).unwrap();
            read_index_prefix_compressed(&mut file, 0).unwrap()
        };

        assert!(read_index.is_empty());
    }
}
