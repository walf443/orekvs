use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{Cursor, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tonic::Status;

use super::block_cache::{BlockCache, BlockCacheKey, CacheEntry};
use super::memtable::MemTable;

pub const MAGIC_BYTES: &[u8; 6] = b"ORELSM";
pub const DATA_VERSION: u32 = 3;

// Header size: 6 bytes magic + 4 bytes version
pub const HEADER_SIZE: u64 = 10;
// Footer size: 8 bytes (index offset)
pub const FOOTER_SIZE: u64 = 8;
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

    if version != 3 {
        return Err(Status::internal(format!(
            "Unsupported SSTable version: {}. Only version 3 is supported.",
            version
        )));
    }

    // V3: Block Compression + Compressed Index
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
    let mut index_offset_bytes = [0u8; 8];
    file.read_exact(&mut index_offset_bytes)
        .map_err(|e| Status::internal(e.to_string()))?;
    let index_offset = u64::from_le_bytes(index_offset_bytes);

    // Read index
    let index = read_index_compressed(&mut file, index_offset)?;

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
#[allow(clippy::result_large_err)]
pub fn search_key_cached(
    path: &Path,
    key: &str,
    cache: &BlockCache,
) -> Result<Option<String>, Status> {
    // Try to get canonical path for consistent cache keys
    let canonical_path = path.canonicalize().unwrap_or_else(|_| path.to_path_buf());

    // Get or load index from cache
    let index = get_or_load_index(path, &canonical_path, cache)?;

    // Binary search in index
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

    // Get or load block from cache
    let block_data = get_or_load_block(path, &canonical_path, block_offset, cache)?;

    // Search within block
    search_in_block(&block_data, key)
}

/// Get index from cache or load from file
#[allow(clippy::result_large_err)]
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

    if version != 3 {
        return Err(Status::internal(format!(
            "Unsupported SSTable version: {}. Only version 3 is supported.",
            version
        )));
    }

    let file_len = file
        .metadata()
        .map_err(|e| Status::internal(e.to_string()))?
        .len();
    if file_len < HEADER_SIZE + FOOTER_SIZE {
        return Err(Status::internal("SSTable file too small"));
    }

    // Read footer to get index offset
    file.seek(SeekFrom::Start(file_len - FOOTER_SIZE))
        .map_err(|e| Status::internal(e.to_string()))?;
    let mut index_offset_bytes = [0u8; 8];
    file.read_exact(&mut index_offset_bytes)
        .map_err(|e| Status::internal(e.to_string()))?;
    let index_offset = u64::from_le_bytes(index_offset_bytes);

    // Read and decompress index
    let index = read_index_compressed(&mut file, index_offset)?;
    let arc_index = Arc::new(index);

    // Cache it
    cache.insert(cache_key, CacheEntry::Index(Arc::clone(&arc_index)));

    Ok(arc_index)
}

/// Get block from cache or load from file
#[allow(clippy::result_large_err)]
fn get_or_load_block(
    path: &Path,
    canonical_path: &Path,
    block_offset: u64,
    cache: &BlockCache,
) -> Result<Arc<Vec<u8>>, Status> {
    let cache_key = BlockCacheKey::for_block(canonical_path.to_path_buf(), block_offset);

    // Check cache first
    if let Some(CacheEntry::DataBlock(data)) = cache.get(&cache_key) {
        return Ok(data);
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

    let arc_data = Arc::new(decompressed);

    // Cache it
    cache.insert(cache_key, CacheEntry::DataBlock(Arc::clone(&arc_data)));

    Ok(arc_data)
}

/// Search for a key within a decompressed block
#[allow(clippy::result_large_err)]
fn search_in_block(block_data: &[u8], key: &str) -> Result<Option<String>, Status> {
    let mut cursor = Cursor::new(block_data);

    loop {
        if cursor.position() == block_data.len() as u64 {
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
    if version != 3 {
        return Err(Status::internal(format!("Unsupported SSTable version: {}", version)));
    }

    let file_len = file.metadata().map_err(|e| Status::internal(e.to_string()))?.len();
    if file_len < HEADER_SIZE + FOOTER_SIZE {
        return Ok(Vec::new());
    }

    // Read footer for index offset
    file.seek(SeekFrom::Start(file_len - FOOTER_SIZE))
        .map_err(|e| Status::internal(e.to_string()))?;
    let mut index_offset_bytes = [0u8; 8];
    file.read_exact(&mut index_offset_bytes)
        .map_err(|e| Status::internal(e.to_string()))?;
    let end_offset = u64::from_le_bytes(index_offset_bytes);

    file.seek(SeekFrom::Start(HEADER_SIZE))
        .map_err(|e| Status::internal(e.to_string()))?;

    let mut keys = Vec::new();

    loop {
        if file.stream_position().map_err(|e| Status::internal(e.to_string()))? >= end_offset {
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
            cursor.seek(SeekFrom::Current(8)).map_err(|e| Status::internal(e.to_string()))?;
            
            let mut lengths = [0u8; 16];
            cursor.read_exact(&mut lengths).map_err(|e| Status::internal(e.to_string()))?;
            let key_len = u64::from_le_bytes(lengths[0..8].try_into().unwrap());
            let val_len = u64::from_le_bytes(lengths[8..16].try_into().unwrap());

            let mut key_buf = vec![0u8; key_len as usize];
            cursor.read_exact(&mut key_buf).map_err(|e| Status::internal(e.to_string()))?;
            keys.push(String::from_utf8_lossy(&key_buf).to_string());

            if val_len != u64::MAX {
                cursor.seek(SeekFrom::Current(val_len as i64)).map_err(|e| Status::internal(e.to_string()))?;
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

    if version != 3 {
        return Err(Status::internal(format!(
            "Unsupported SSTable version: {}. Only version 3 is supported.",
            version
        )));
    }

    let file_len = file
        .metadata()
        .map_err(|e| Status::internal(e.to_string()))?
        .len();
    if file_len < HEADER_SIZE + FOOTER_SIZE {
        return Ok(BTreeMap::new()); // Corrupt or empty?
    }
    // Read footer
    file.seek(SeekFrom::Start(file_len - FOOTER_SIZE))
        .map_err(|e| Status::internal(e.to_string()))?;
    let mut index_offset_bytes = [0u8; 8];
    file.read_exact(&mut index_offset_bytes)
        .map_err(|e| Status::internal(e.to_string()))?;
    let end_offset = u64::from_le_bytes(index_offset_bytes);

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
pub fn create_from_memtable(path: &Path, memtable: &MemTable) -> Result<(), Status> {
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

    // Write index
    let index_offset = current_offset;
    write_index_compressed(&mut file, &index, COMPRESSION_LEVEL_INDEX)?;

    // Write footer
    file.write_all(&index_offset.to_le_bytes())
        .map_err(|e| Status::internal(e.to_string()))?;

    file.flush().map_err(|e| Status::internal(e.to_string()))?;
    file.sync_all()
        .map_err(|e| Status::internal(e.to_string()))?;

    // Atomically rename tmp file to final path
    std::fs::rename(&tmp_path, path)
        .map_err(|e| Status::internal(format!("Failed to rename SSTable: {}", e)))?;

    Ok(())
}

/// Write timestamped entries to an SSTable file (used for compaction)
#[allow(clippy::result_large_err)]
pub fn write_timestamped_entries(
    path: &Path,
    entries: &BTreeMap<String, TimestampedEntry>,
) -> Result<(), Status> {
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

    // Write index
    let index_offset = current_offset;
    write_index_compressed(&mut file, &index, COMPRESSION_LEVEL_INDEX)?;

    // Write footer
    file.write_all(&index_offset.to_le_bytes())
        .map_err(|e| Status::internal(e.to_string()))?;

    file.flush().map_err(|e| Status::internal(e.to_string()))?;
    file.sync_all()
        .map_err(|e| Status::internal(e.to_string()))?;

    // Atomically rename tmp file to final path
    std::fs::rename(&tmp_path, path)
        .map_err(|e| Status::internal(format!("Failed to rename SSTable: {}", e)))?;

    Ok(())
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

#[allow(clippy::result_large_err)]
fn write_index_compressed(
    file: &mut File,
    index: &[(String, u64)],
    compression_level: i32,
) -> Result<(), Status> {
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

    Ok(())
}

#[allow(clippy::result_large_err)]
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

        // Corrupt the index offset in the footer
        let mut data = std::fs::read(&sst_path).unwrap();
        let len = data.len();
        if len > 8 {
            // Change the index offset to something invalid
            data[len - 4] ^= 0xFF;
        }
        std::fs::write(&sst_path, data).unwrap();

        let result = search_key(&sst_path, "key1");
        assert!(result.is_err());
    }
}
