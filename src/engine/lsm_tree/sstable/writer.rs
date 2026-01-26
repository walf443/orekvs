//! SSTable writer implementation.
//!
//! Provides SSTable creation from MemTable and timestamped entries.

use std::collections::BTreeMap;
use std::fs::OpenOptions;
use std::io::{Cursor, Write};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use tonic::Status;

use crate::engine::lsm_tree::bloom::BloomFilter;
use crate::engine::lsm_tree::common_prefix_compression_index as prefix_index;
use crate::engine::lsm_tree::memtable::MemTable;

use super::reader::TimestampedEntry;
use super::{BLOCK_SIZE, DATA_VERSION, FOOTER_MAGIC, HEADER_SIZE, MAGIC_BYTES, crc32};

// ZSTD compression levels (0-22, higher = better compression but slower)
// Flush: prioritize speed for write performance
const COMPRESSION_LEVEL_FLUSH: i32 = 1;
// Compaction: prioritize compression ratio for storage efficiency
const COMPRESSION_LEVEL_COMPACTION: i32 = 6;
// Index: use higher compression since index is read once per search
const COMPRESSION_LEVEL_INDEX: i32 = 6;

/// Write a compressed block to file and return the number of bytes written.
///
/// Block format: [len: u32][compressed_data][checksum: u32]
#[allow(clippy::result_large_err)]
fn write_compressed_block<W: Write>(
    file: &mut W,
    block_buffer: &[u8],
    compression_level: i32,
) -> Result<u64, Status> {
    let compressed = zstd::encode_all(Cursor::new(block_buffer), compression_level)
        .map_err(|e| Status::internal(format!("Compression error: {}", e)))?;

    let len = compressed.len() as u32;
    let checksum = crc32(&compressed);

    file.write_all(&len.to_le_bytes())
        .map_err(|e| Status::internal(e.to_string()))?;
    file.write_all(&compressed)
        .map_err(|e| Status::internal(e.to_string()))?;
    file.write_all(&checksum.to_le_bytes())
        .map_err(|e| Status::internal(e.to_string()))?;

    Ok(4 + len as u64 + 4) // len + data + checksum
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
            index.push((first_key_in_block.clone(), current_offset));
            current_offset +=
                write_compressed_block(&mut file, &block_buffer, COMPRESSION_LEVEL_FLUSH)?;
            block_buffer.clear();
        }
    }

    // Write remaining block
    if !block_buffer.is_empty() {
        index.push((first_key_in_block, current_offset));
        current_offset +=
            write_compressed_block(&mut file, &block_buffer, COMPRESSION_LEVEL_FLUSH)?;
    }

    // Write index with prefix compression (V6 with checksum)
    let index_offset = current_offset;
    let index_size = prefix_index::write_index(&mut file, &index, COMPRESSION_LEVEL_INDEX)?;
    current_offset += index_size;

    // Write Bloom filter
    let bloom_offset = current_offset;
    let bloom_data = bloom.serialize();
    let bloom_len = bloom_data.len() as u64;
    file.write_all(&bloom_len.to_le_bytes())
        .map_err(|e| Status::internal(e.to_string()))?;
    file.write_all(&bloom_data)
        .map_err(|e| Status::internal(e.to_string()))?;
    current_offset += 8 + bloom_len;

    // Write key range section (V8)
    let keyrange_offset = current_offset;
    // Get min/max keys from memtable (BTreeMap is sorted)
    let min_key = memtable.keys().next().map(|k| k.as_bytes()).unwrap_or(&[]);
    let max_key = memtable
        .keys()
        .next_back()
        .map(|k| k.as_bytes())
        .unwrap_or(&[]);
    let entry_count = memtable.len() as u64;

    // Write min_key: [len: u32][key: bytes]
    file.write_all(&(min_key.len() as u32).to_le_bytes())
        .map_err(|e| Status::internal(e.to_string()))?;
    file.write_all(min_key)
        .map_err(|e| Status::internal(e.to_string()))?;

    // Write max_key: [len: u32][key: bytes]
    file.write_all(&(max_key.len() as u32).to_le_bytes())
        .map_err(|e| Status::internal(e.to_string()))?;
    file.write_all(max_key)
        .map_err(|e| Status::internal(e.to_string()))?;

    // Write entry_count: [count: u64] (V8)
    file.write_all(&entry_count.to_le_bytes())
        .map_err(|e| Status::internal(e.to_string()))?;

    // Write V8 footer: index_offset + bloom_offset + keyrange_offset + magic
    file.write_all(&index_offset.to_le_bytes())
        .map_err(|e| Status::internal(e.to_string()))?;
    file.write_all(&bloom_offset.to_le_bytes())
        .map_err(|e| Status::internal(e.to_string()))?;
    file.write_all(&keyrange_offset.to_le_bytes())
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
            current_offset +=
                write_compressed_block(&mut file, &block_buffer, COMPRESSION_LEVEL_COMPACTION)?;
            block_buffer.clear();
        }
    }

    // Write remaining block
    if !block_buffer.is_empty() {
        index.push((first_key_in_block, current_offset));
        current_offset +=
            write_compressed_block(&mut file, &block_buffer, COMPRESSION_LEVEL_COMPACTION)?;
    }

    // Write index with prefix compression (V6 with checksum)
    let index_offset = current_offset;
    let index_size = prefix_index::write_index(&mut file, &index, COMPRESSION_LEVEL_INDEX)?;
    current_offset += index_size;

    // Write Bloom filter
    let bloom_offset = current_offset;
    let bloom_data = bloom.serialize();
    let bloom_len = bloom_data.len() as u64;
    file.write_all(&bloom_len.to_le_bytes())
        .map_err(|e| Status::internal(e.to_string()))?;
    file.write_all(&bloom_data)
        .map_err(|e| Status::internal(e.to_string()))?;
    current_offset += 8 + bloom_len;

    // Write key range section (V8)
    let keyrange_offset = current_offset;
    // Get min/max keys from entries (BTreeMap is sorted)
    let min_key = entries.keys().next().map(|k| k.as_bytes()).unwrap_or(&[]);
    let max_key = entries
        .keys()
        .next_back()
        .map(|k| k.as_bytes())
        .unwrap_or(&[]);
    let entry_count = entries.len() as u64;

    // Write min_key: [len: u32][key: bytes]
    file.write_all(&(min_key.len() as u32).to_le_bytes())
        .map_err(|e| Status::internal(e.to_string()))?;
    file.write_all(min_key)
        .map_err(|e| Status::internal(e.to_string()))?;

    // Write max_key: [len: u32][key: bytes]
    file.write_all(&(max_key.len() as u32).to_le_bytes())
        .map_err(|e| Status::internal(e.to_string()))?;
    file.write_all(max_key)
        .map_err(|e| Status::internal(e.to_string()))?;

    // Write entry_count: [count: u64] (V8)
    file.write_all(&entry_count.to_le_bytes())
        .map_err(|e| Status::internal(e.to_string()))?;

    // Write V8 footer: index_offset + bloom_offset + keyrange_offset + magic
    file.write_all(&index_offset.to_le_bytes())
        .map_err(|e| Status::internal(e.to_string()))?;
    file.write_all(&bloom_offset.to_le_bytes())
        .map_err(|e| Status::internal(e.to_string()))?;
    file.write_all(&keyrange_offset.to_le_bytes())
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::lsm_tree::sstable::reader::read_entries;
    use tempfile::tempdir;

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
}
