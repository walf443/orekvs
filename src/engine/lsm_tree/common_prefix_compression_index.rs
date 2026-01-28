//! Prefix-compressed index for SSTable
//!
//! This module implements prefix compression for SSTable index entries.
//! Keys are stored with common prefix elimination to reduce storage size.
//!
//! Format (v11):
//! - num_entries: u64
//! - restart_interval: u32
//! - For each entry:
//!   - prefix_len (u16) + suffix_len (u16) + suffix + offset (u64) + max_expire_at (u64)
//!   - Restart points (every N entries) have prefix_len = 0
//!
//! max_expire_at allows skipping entire blocks during compaction if all entries are expired.

use std::fs::File;
use std::io::{Cursor, Read, Write};
use tonic::Status;

use super::composite_key;

/// Prefix compression restart interval for index.
/// Every N entries, store full key for random access.
pub const INDEX_RESTART_INTERVAL: u32 = 16;

/// Compute common prefix length between two byte slices
pub fn common_prefix_len(a: &[u8], b: &[u8]) -> usize {
    a.iter().zip(b.iter()).take_while(|(x, y)| x == y).count()
}

/// Index entry type: (composite_key, block_offset, max_expire_at)
pub type IndexEntry = (String, u64, u64);

/// Parse prefix-compressed index from decompressed data (v11 format with max_expire_at)
#[allow(clippy::result_large_err)]
pub fn parse_index(decompressed: &[u8]) -> Result<Vec<IndexEntry>, Status> {
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

        // Read max_expire_at (v11)
        let mut expire_bytes = [0u8; 8];
        cursor
            .read_exact(&mut expire_bytes)
            .map_err(|e| Status::internal(e.to_string()))?;
        let max_expire_at = u64::from_le_bytes(expire_bytes);

        // Convert bytes to String (composite key may contain non-UTF-8 bytes in expire_at portion)
        let key_string = composite_key::string_from_bytes(key_buf.clone());
        prev_key = key_buf;
        index.push((key_string, offset, max_expire_at));
    }

    Ok(index)
}

/// Write index with prefix compression and checksum (v11 format with max_expire_at)
#[allow(clippy::result_large_err)]
pub fn write_index(
    file: &mut File,
    index: &[IndexEntry],
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

    for (i, (key, offset, max_expire_at)) in index.iter().enumerate() {
        let key_bytes = key.as_bytes();

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
        // Write max_expire_at (v11)
        buffer
            .write_all(&max_expire_at.to_le_bytes())
            .map_err(|e| Status::internal(e.to_string()))?;

        prev_key = key_bytes;
    }

    let compressed = zstd::encode_all(Cursor::new(&buffer), compression_level)
        .map_err(|e| Status::internal(format!("Index compression error: {}", e)))?;

    // Write compressed size, data, and checksum
    let len = compressed.len() as u64;
    let checksum = crc32(&compressed);
    file.write_all(&len.to_le_bytes())
        .map_err(|e| Status::internal(e.to_string()))?;
    file.write_all(&compressed)
        .map_err(|e| Status::internal(e.to_string()))?;
    file.write_all(&checksum.to_le_bytes())
        .map_err(|e| Status::internal(e.to_string()))?;

    Ok(8 + len + 4) // size + data + checksum
}

/// Read index with prefix compression and checksum verification (v11 format)
#[allow(dead_code)]
#[allow(clippy::result_large_err)]
pub fn read_index(file: &mut File, index_offset: u64) -> Result<Vec<IndexEntry>, Status> {
    use std::io::{Seek, SeekFrom};
    file.seek(SeekFrom::Start(index_offset))
        .map_err(|e| Status::internal(e.to_string()))?;

    let mut len_bytes = [0u8; 8];
    file.read_exact(&mut len_bytes)
        .map_err(|e| Status::internal(e.to_string()))?;
    let len = u64::from_le_bytes(len_bytes);

    // Sanity check: index should not be larger than 1GB
    const MAX_INDEX_SIZE: u64 = 1024 * 1024 * 1024;
    if len > MAX_INDEX_SIZE {
        return Err(Status::data_loss(format!(
            "Index size {} exceeds maximum allowed size {}",
            len, MAX_INDEX_SIZE
        )));
    }

    let mut compressed = vec![0u8; len as usize];
    file.read_exact(&mut compressed)
        .map_err(|e| Status::internal(e.to_string()))?;

    // Read and verify checksum
    let mut checksum_bytes = [0u8; 4];
    file.read_exact(&mut checksum_bytes)
        .map_err(|e| Status::internal(e.to_string()))?;
    let stored_checksum = u32::from_le_bytes(checksum_bytes);
    let computed_checksum = crc32(&compressed);
    if stored_checksum != computed_checksum {
        return Err(Status::data_loss(format!(
            "Index checksum mismatch: expected {:08x}, got {:08x}",
            stored_checksum, computed_checksum
        )));
    }

    let decompressed = zstd::decode_all(Cursor::new(&compressed))
        .map_err(|e| Status::internal(format!("Index decompression error: {}", e)))?;

    parse_index(&decompressed)
}

/// Compute CRC32C checksum
fn crc32(data: &[u8]) -> u32 {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(data);
    hasher.finalize()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_common_prefix_len() {
        assert_eq!(common_prefix_len(b"hello", b"hello"), 5);
        assert_eq!(common_prefix_len(b"hello", b"help"), 3);
        assert_eq!(common_prefix_len(b"hello", b"world"), 0);
        assert_eq!(common_prefix_len(b"", b"hello"), 0);
        assert_eq!(common_prefix_len(b"hello", b""), 0);
    }

    #[test]
    fn test_prefix_compression_roundtrip() {
        let dir = tempdir().unwrap();
        let index_path = dir.path().join("index_test.data");

        // Create index with keys that have common prefixes (v11 format with max_expire_at)
        let index: Vec<IndexEntry> = (0..50)
            .map(|i| (format!("user:profile:{:05}", i), i * 100, 1000 + i))
            .collect();

        // Write index with prefix compression
        {
            let mut file = File::create(&index_path).unwrap();
            write_index(&mut file, &index, 3).unwrap();
        }

        // Read it back
        let read_index = {
            let mut file = File::open(&index_path).unwrap();
            read_index(&mut file, 0).unwrap()
        };

        // Verify all entries match
        assert_eq!(index.len(), read_index.len());
        for (original, read) in index.iter().zip(read_index.iter()) {
            assert_eq!(original.0, read.0, "Key mismatch");
            assert_eq!(original.1, read.1, "Offset mismatch");
            assert_eq!(original.2, read.2, "max_expire_at mismatch");
        }
    }

    #[test]
    fn test_prefix_compression_restart_points() {
        let dir = tempdir().unwrap();
        let index_path = dir.path().join("restart_test.data");

        // Create index with 20 entries (more than restart interval of 16)
        // to verify restart points work correctly
        let index: Vec<IndexEntry> = (0..20)
            .map(|i| (format!("key:{:03}", i), i * 50, 0))
            .collect();

        {
            let mut file = File::create(&index_path).unwrap();
            write_index(&mut file, &index, 3).unwrap();
        }

        let read_index = {
            let mut file = File::open(&index_path).unwrap();
            read_index(&mut file, 0).unwrap()
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

        // Keys with no common prefix (with max_expire_at)
        let index: Vec<IndexEntry> = vec![
            ("apple".to_string(), 100, 500),
            ("banana".to_string(), 200, 600),
            ("cherry".to_string(), 300, 0), // no TTL
        ];

        {
            let mut file = File::create(&index_path).unwrap();
            write_index(&mut file, &index, 3).unwrap();
        }

        let read_index = {
            let mut file = File::open(&index_path).unwrap();
            read_index(&mut file, 0).unwrap()
        };

        assert_eq!(read_index, index);
    }

    #[test]
    fn test_prefix_compression_empty_index() {
        let dir = tempdir().unwrap();
        let index_path = dir.path().join("empty_index_test.data");

        let index: Vec<IndexEntry> = vec![];

        {
            let mut file = File::create(&index_path).unwrap();
            write_index(&mut file, &index, 3).unwrap();
        }

        let read_index = {
            let mut file = File::open(&index_path).unwrap();
            read_index(&mut file, 0).unwrap()
        };

        assert!(read_index.is_empty());
    }
}
