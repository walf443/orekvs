use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};
use tonic::Status;

use super::memtable::MemTable;

pub const MAGIC_BYTES: &[u8; 6] = b"ORELSM";
pub const DATA_VERSION: u32 = 2;

// Header size: 6 bytes magic + 4 bytes version
pub const HEADER_SIZE: u64 = 10;
// Footer size: 8 bytes (index offset)
pub const FOOTER_SIZE: u64 = 8;
// Sparse index block size (bytes)
pub const INDEX_BLOCK_SIZE: u64 = 4096;

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

    if version == 2 {
        // V2: Use Sparse Index
        let file_len = file.metadata().map_err(|e| Status::internal(e.to_string()))?.len();
        if file_len < HEADER_SIZE + FOOTER_SIZE {
             return Err(Status::internal("SSTable file too small"));
        }

        // Read footer (last 8 bytes)
        file.seek(SeekFrom::Start(file_len - FOOTER_SIZE))
            .map_err(|e| Status::internal(e.to_string()))?;
        let mut index_offset_bytes = [0u8; 8];
        file.read_exact(&mut index_offset_bytes)
            .map_err(|e| Status::internal(e.to_string()))?;
        let index_offset = u64::from_le_bytes(index_offset_bytes);

        // Read index
        let index = read_index(&mut file, index_offset)?;

        // Binary search in index to find the starting block
        // Find the last entry where entry.key <= key
        let start_offset = match index.binary_search_by(|(k, _)| k.as_str().cmp(key)) {
            Ok(idx) => index[idx].1,
            Err(idx) => {
                if idx == 0 {
                    HEADER_SIZE // Key is smaller than first index entry, check from start
                } else {
                    index[idx - 1].1 // Check from the previous block
                }
            }
        };

        file.seek(SeekFrom::Start(start_offset))
             .map_err(|e| Status::internal(e.to_string()))?;

        // Linear scan from start_offset until key found or key > target
        loop {
            // Check if we reached index offset (end of data)
            if file.stream_position().map_err(|e| Status::internal(e.to_string()))? >= index_offset {
                break;
            }
            
            let mut ts_bytes = [0u8; 8];
            if file.read_exact(&mut ts_bytes).is_err() {
                break;
            }

            let mut klen_bytes = [0u8; 8];
            file.read_exact(&mut klen_bytes)
                .map_err(|e| Status::internal(e.to_string()))?;
            let mut vlen_bytes = [0u8; 8];
            file.read_exact(&mut vlen_bytes)
                .map_err(|e| Status::internal(e.to_string()))?;

            let key_len = u64::from_le_bytes(klen_bytes);
            let val_len = u64::from_le_bytes(vlen_bytes);

            let mut key_buf = vec![0u8; key_len as usize];
            file.read_exact(&mut key_buf)
                .map_err(|e| Status::internal(e.to_string()))?;
            let current_key = String::from_utf8_lossy(&key_buf);

            if current_key == key {
                if val_len == u64::MAX {
                    return Ok(None);
                }
                let mut val_buf = vec![0u8; val_len as usize];
                file.read_exact(&mut val_buf)
                    .map_err(|e| Status::internal(e.to_string()))?;
                return Ok(Some(String::from_utf8_lossy(&val_buf).to_string()));
            }

            // Optimization: If current_key > key, we can stop (entries are sorted)
            if current_key.as_ref() > key {
                 return Err(Status::not_found("Key not found in SSTable (sorted check)"));
            }

            if val_len != u64::MAX {
                file.seek(SeekFrom::Current(val_len as i64))
                    .map_err(|e| Status::internal(e.to_string()))?;
            }
        }
        Err(Status::not_found("Key not found in SSTable"))

    } else {
        // V1: Linear scan
        loop {
            let mut ts_bytes = [0u8; 8];
            if file.read_exact(&mut ts_bytes).is_err() {
                break;
            }
    
            let mut klen_bytes = [0u8; 8];
            file.read_exact(&mut klen_bytes)
                .map_err(|e| Status::internal(e.to_string()))?;
            let mut vlen_bytes = [0u8; 8];
            file.read_exact(&mut vlen_bytes)
                .map_err(|e| Status::internal(e.to_string()))?;
    
            let key_len = u64::from_le_bytes(klen_bytes);
            let val_len = u64::from_le_bytes(vlen_bytes);
    
            let mut key_buf = vec![0u8; key_len as usize];
            file.read_exact(&mut key_buf)
                .map_err(|e| Status::internal(e.to_string()))?;
            let current_key = String::from_utf8_lossy(&key_buf);
    
            if current_key == key {
                if val_len == u64::MAX {
                    return Ok(None);
                }
                let mut val_buf = vec![0u8; val_len as usize];
                file.read_exact(&mut val_buf)
                    .map_err(|e| Status::internal(e.to_string()))?;
                return Ok(Some(String::from_utf8_lossy(&val_buf).to_string()));
            }
    
            if val_len != u64::MAX {
                file.seek(SeekFrom::Current(val_len as i64))
                    .map_err(|e| Status::internal(e.to_string()))?;
            }
        }
        Err(Status::not_found("Key not found in SSTable"))
    }
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

    let end_offset = if version == 2 {
        let file_len = file.metadata().map_err(|e| Status::internal(e.to_string()))?.len();
        if file_len < HEADER_SIZE + FOOTER_SIZE {
             return Ok(BTreeMap::new()); // Corrupt or empty?
        }
        // Read footer
        file.seek(SeekFrom::Start(file_len - FOOTER_SIZE))
            .map_err(|e| Status::internal(e.to_string()))?;
        let mut index_offset_bytes = [0u8; 8];
        file.read_exact(&mut index_offset_bytes)
            .map_err(|e| Status::internal(e.to_string()))?;
        u64::from_le_bytes(index_offset_bytes)
    } else {
        u64::MAX
    };

    file.seek(SeekFrom::Start(HEADER_SIZE))
        .map_err(|e| Status::internal(e.to_string()))?;

    let mut entries = BTreeMap::new();

    loop {
        // Check if we reached end of data
        if file.stream_position().map_err(|e| Status::internal(e.to_string()))? >= end_offset {
            break;
        }

        let mut ts_bytes = [0u8; 8];
        if file.read_exact(&mut ts_bytes).is_err() {
            break;
        }
        let timestamp = u64::from_le_bytes(ts_bytes);

        let mut klen_bytes = [0u8; 8];
        file.read_exact(&mut klen_bytes)
            .map_err(|e| Status::internal(e.to_string()))?;
        let mut vlen_bytes = [0u8; 8];
        file.read_exact(&mut vlen_bytes)
            .map_err(|e| Status::internal(e.to_string()))?;

        let key_len = u64::from_le_bytes(klen_bytes);
        let val_len = u64::from_le_bytes(vlen_bytes);

        let mut key_buf = vec![0u8; key_len as usize];
        file.read_exact(&mut key_buf)
            .map_err(|e| Status::internal(e.to_string()))?;
        let key = String::from_utf8_lossy(&key_buf).to_string();

        let value = if val_len == u64::MAX {
            None
        } else {
            let mut val_buf = vec![0u8; val_len as usize];
            file.read_exact(&mut val_buf)
                .map_err(|e| Status::internal(e.to_string()))?;
            Some(String::from_utf8_lossy(&val_buf).to_string())
        };

        entries.insert(key, (timestamp, value));
    }

    Ok(entries)
}

/// Write a MemTable to an SSTable file
#[allow(clippy::result_large_err)]
pub fn create_from_memtable(path: &Path, memtable: &MemTable) -> Result<(), Status> {
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)
        .map_err(|e| Status::internal(e.to_string()))?;

    // Write header
    file.write_all(MAGIC_BYTES)
        .map_err(|e| Status::internal(e.to_string()))?;
    file.write_all(&DATA_VERSION.to_le_bytes())
        .map_err(|e| Status::internal(e.to_string()))?;

    let mut current_offset = HEADER_SIZE;
    let mut index: Vec<(String, u64)> = Vec::new();
    let mut next_index_threshold = HEADER_SIZE;

    // Write entries
    for (key, value_opt) in memtable.iter() {
        // Add to index if we crossed the threshold
        if current_offset >= next_index_threshold {
            index.push((key.clone(), current_offset));
            next_index_threshold += INDEX_BLOCK_SIZE;
        }

        let written_bytes = write_entry(&mut file, key, value_opt, None)?;
        current_offset += written_bytes;
    }

    // Write index
    let index_offset = current_offset;
    write_index(&mut file, &index)?;

    // Write footer
    file.write_all(&index_offset.to_le_bytes())
        .map_err(|e| Status::internal(e.to_string()))?;

    file.flush().map_err(|e| Status::internal(e.to_string()))?;
    file.sync_all()
        .map_err(|e| Status::internal(e.to_string()))?;

    Ok(())
}

/// Write timestamped entries to an SSTable file (used for compaction)
#[allow(clippy::result_large_err)]
pub fn write_timestamped_entries(
    path: &Path,
    entries: &BTreeMap<String, TimestampedEntry>,
) -> Result<(), Status> {
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)
        .map_err(|e| Status::internal(e.to_string()))?;

    // Write header
    file.write_all(MAGIC_BYTES)
        .map_err(|e| Status::internal(e.to_string()))?;
    file.write_all(&DATA_VERSION.to_le_bytes())
        .map_err(|e| Status::internal(e.to_string()))?;

    let mut current_offset = HEADER_SIZE;
    let mut index: Vec<(String, u64)> = Vec::new();
    let mut next_index_threshold = HEADER_SIZE;

    // Write entries
    for (key, (timestamp, value_opt)) in entries {
        // Add to index if we crossed the threshold
        if current_offset >= next_index_threshold {
            index.push((key.clone(), current_offset));
            next_index_threshold += INDEX_BLOCK_SIZE;
        }

        let written_bytes = write_entry(&mut file, key, value_opt, Some(*timestamp))?;
        current_offset += written_bytes;
    }

    // Write index
    let index_offset = current_offset;
    write_index(&mut file, &index)?;

    // Write footer
    file.write_all(&index_offset.to_le_bytes())
        .map_err(|e| Status::internal(e.to_string()))?;

    file.flush().map_err(|e| Status::internal(e.to_string()))?;
    file.sync_all()
        .map_err(|e| Status::internal(e.to_string()))?;

    Ok(())
}

/// Write a single entry to an SSTable file
#[allow(clippy::result_large_err)]
fn write_entry(
    file: &mut File,
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

    file.write_all(&ts.to_le_bytes())
        .map_err(|e| Status::internal(e.to_string()))?;
    file.write_all(&key_len.to_le_bytes())
        .map_err(|e| Status::internal(e.to_string()))?;
    file.write_all(&val_len.to_le_bytes())
        .map_err(|e| Status::internal(e.to_string()))?;
    file.write_all(key_bytes)
        .map_err(|e| Status::internal(e.to_string()))?;
    if val_len != u64::MAX {
        file.write_all(val_bytes)
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

fn write_index(file: &mut File, index: &[(String, u64)]) -> Result<(), Status> {
    let num_entries = index.len() as u64;
    file.write_all(&num_entries.to_le_bytes())
        .map_err(|e| Status::internal(e.to_string()))?;

    for (key, offset) in index {
        let key_bytes = key.as_bytes();
        let key_len = key_bytes.len() as u64;

        file.write_all(&key_len.to_le_bytes())
            .map_err(|e| Status::internal(e.to_string()))?;
        file.write_all(key_bytes)
            .map_err(|e| Status::internal(e.to_string()))?;
        file.write_all(&offset.to_le_bytes())
            .map_err(|e| Status::internal(e.to_string()))?;
    }
    Ok(())
}

fn read_index(file: &mut File, index_offset: u64) -> Result<Vec<(String, u64)>, Status> {
    file.seek(SeekFrom::Start(index_offset))
        .map_err(|e| Status::internal(e.to_string()))?;

    let mut num_bytes = [0u8; 8];
    file.read_exact(&mut num_bytes)
        .map_err(|e| Status::internal(e.to_string()))?;
    let num_entries = u64::from_le_bytes(num_bytes);

    let mut index = Vec::with_capacity(num_entries as usize);

    for _ in 0..num_entries {
        let mut klen_bytes = [0u8; 8];
        file.read_exact(&mut klen_bytes)
            .map_err(|e| Status::internal(e.to_string()))?;
        let key_len = u64::from_le_bytes(klen_bytes);

        let mut key_buf = vec![0u8; key_len as usize];
        file.read_exact(&mut key_buf)
            .map_err(|e| Status::internal(e.to_string()))?;
        let key = String::from_utf8_lossy(&key_buf).to_string();

        let mut off_bytes = [0u8; 8];
        file.read_exact(&mut off_bytes)
            .map_err(|e| Status::internal(e.to_string()))?;
        let offset = u64::from_le_bytes(off_bytes);

        index.push((key, offset));
    }

    Ok(index)
}
