//! SSTable (Sorted String Table) implementation.
//!
//! This module provides SSTable functionality split into reader and writer components:
//! - `reader`: Memory-mapped SSTable reading with block cache support
//! - `writer`: SSTable creation from MemTable and timestamped entries
//! - `levels`: Level management for L0 and L1+ SSTables

pub mod levels;
mod reader;
mod writer;

#[allow(unused_imports)] // read_entries is used in tests
pub use reader::{
    MappedSSTable, TimestampedEntry, extract_wal_id_from_sstable, read_entries,
    read_entries_for_compaction, search_key_mmap, search_key_mmap_with_expire,
};
pub use writer::{create_from_memtable, write_timestamped_entries};

use std::path::{Path, PathBuf};

// Re-export for tests
#[cfg(test)]
pub use reader::{read_bloom_filter, read_keys, search_key};

pub const MAGIC_BYTES: &[u8; 6] = b"OREKVS";
pub const DATA_VERSION: u32 = 11; // v11: block-level max_expire_at in index
pub const FOOTER_MAGIC: u64 = 0x4F52454B56534654; // "OREKVSFT" in hex

/// Compute CRC32C checksum
pub(crate) fn crc32(data: &[u8]) -> u32 {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(data);
    hasher.finalize()
}

// Header size: 6 bytes magic + 4 bytes version
pub const HEADER_SIZE: u64 = 10;
// Footer size: index_offset(8) + bloom_offset(8) + keyrange_offset(8) + footer_magic(8)
pub const FOOTER_SIZE: u64 = 32;

// Footer magic bytes - defined above
// Target block size for compression (bytes)
pub const BLOCK_SIZE: usize = 4096;

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

/// Generate SSTable filename
pub fn generate_filename(sst_id: u64, wal_id: u64) -> String {
    format!("sst_{:05}_{:05}.data", sst_id, wal_id)
}

/// Generate SSTable path from data directory, SSTable ID, and WAL ID
pub fn generate_path(data_dir: &Path, sst_id: u64, wal_id: u64) -> PathBuf {
    data_dir.join(generate_filename(sst_id, wal_id))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::lsm_tree::memtable::MemValue;
    use std::collections::BTreeMap;
    use tempfile::tempdir;

    #[test]
    fn test_parse_filename() {
        assert_eq!(parse_filename("sst_00001_00002.data"), Some((1, Some(2))));
        assert_eq!(parse_filename("sst_00010.data"), Some((10, None)));
        assert_eq!(parse_filename("invalid"), None);
        assert_eq!(parse_filename("sst_abc.data"), None);
    }

    // Integration tests that test both reader and writer functionality

    #[test]
    fn test_sstable_creation_and_search() {
        let dir = tempdir().unwrap();
        let sst_path = dir.path().join("sst_00001_00001.data");

        let mut memtable = BTreeMap::new();
        memtable.insert(
            "key1".to_string(),
            MemValue::new(Some("value1".to_string())),
        );
        memtable.insert(
            "key2".to_string(),
            MemValue::new(Some("value2".to_string())),
        );
        memtable.insert("key3".to_string(), MemValue::new(None)); // Tombstone

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
            memtable.insert(key, MemValue::new(Some(value)));
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
        for (key, (_, _, value)) in read_back {
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
    fn test_sstable_large_entries() {
        let dir = tempdir().unwrap();
        let sst_path = dir.path().join("sst_large.data");

        let mut memtable = BTreeMap::new();
        let large_key = "k".repeat(1024); // 1KB key
        let large_value = "v".repeat(1024 * 1024); // 1MB value
        memtable.insert(large_key.clone(), MemValue::new(Some(large_value.clone())));
        memtable.insert(
            "short".to_string(),
            MemValue::new(Some("value".to_string())),
        );

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
}
