use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};
use tonic::Status;

use super::wal::MemTable;

pub const MAGIC_BYTES: &[u8; 6] = b"ORELSM";
pub const DATA_VERSION: u32 = 1;

// Header size: 6 bytes magic + 4 bytes version
pub const HEADER_SIZE: u64 = 10;

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
    file.seek(SeekFrom::Start(HEADER_SIZE))
        .map_err(|e| Status::internal(e.to_string()))?;

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
    file.seek(SeekFrom::Start(HEADER_SIZE))
        .map_err(|e| Status::internal(e.to_string()))?;

    let mut entries = BTreeMap::new();

    loop {
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
pub fn write_memtable(path: &Path, memtable: &MemTable) -> Result<(), Status> {
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

    // Write entries
    for (key, value_opt) in memtable.iter() {
        write_entry(&mut file, key, value_opt, None)?;
    }

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

    // Write entries
    for (key, (timestamp, value_opt)) in entries {
        write_entry(&mut file, key, value_opt, Some(*timestamp))?;
    }

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
) -> Result<(), Status> {
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

    Ok(())
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
