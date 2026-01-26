//! Write-Ahead Log (WAL) for B-tree crash recovery.
//!
//! Records all modifications before they are applied to the B-tree.
//! On recovery, replays the WAL to restore the tree to a consistent state.

use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

/// WAL magic number
const WAL_MAGIC: u32 = 0x4257_414C; // "BWAL"

/// WAL version
const WAL_VERSION: u8 = 1;

/// Record types
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum RecordType {
    /// Insert or update a key-value pair
    Insert = 1,
    /// Delete a key
    Delete = 2,
    /// Checkpoint marker (tree is consistent up to this point)
    Checkpoint = 3,
    /// Begin batch operation
    BeginBatch = 4,
    /// End batch operation (commit point)
    EndBatch = 5,
}

impl TryFrom<u8> for RecordType {
    type Error = io::Error;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(RecordType::Insert),
            2 => Ok(RecordType::Delete),
            3 => Ok(RecordType::Checkpoint),
            4 => Ok(RecordType::BeginBatch),
            5 => Ok(RecordType::EndBatch),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Invalid record type: {}", value),
            )),
        }
    }
}

/// WAL record structure
#[derive(Debug, Clone)]
pub struct WalRecord {
    /// Sequence number
    pub seq: u64,
    /// Record type
    pub record_type: RecordType,
    /// Key (empty for checkpoint/batch records)
    pub key: String,
    /// Value (None for delete/checkpoint/batch records)
    pub value: Option<String>,
}

impl WalRecord {
    pub fn insert(seq: u64, key: String, value: String) -> Self {
        Self {
            seq,
            record_type: RecordType::Insert,
            key,
            value: Some(value),
        }
    }

    pub fn delete(seq: u64, key: String) -> Self {
        Self {
            seq,
            record_type: RecordType::Delete,
            key,
            value: None,
        }
    }

    pub fn checkpoint(seq: u64) -> Self {
        Self {
            seq,
            record_type: RecordType::Checkpoint,
            key: String::new(),
            value: None,
        }
    }

    pub fn begin_batch(seq: u64) -> Self {
        Self {
            seq,
            record_type: RecordType::BeginBatch,
            key: String::new(),
            value: None,
        }
    }

    pub fn end_batch(seq: u64) -> Self {
        Self {
            seq,
            record_type: RecordType::EndBatch,
            key: String::new(),
            value: None,
        }
    }

    /// Serialize record to bytes
    /// Format: [seq: u64][type: u8][key_len: u32][value_len: u32][key][value][checksum: u32]
    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::new();

        buf.extend_from_slice(&self.seq.to_le_bytes());
        buf.push(self.record_type as u8);
        buf.extend_from_slice(&(self.key.len() as u32).to_le_bytes());

        let value_len = self.value.as_ref().map_or(u32::MAX, |v| v.len() as u32);
        buf.extend_from_slice(&value_len.to_le_bytes());

        buf.extend_from_slice(self.key.as_bytes());
        if let Some(value) = &self.value {
            buf.extend_from_slice(value.as_bytes());
        }

        // Checksum
        let checksum = crc32fast::hash(&buf);
        buf.extend_from_slice(&checksum.to_le_bytes());

        buf
    }

    /// Deserialize record from bytes
    pub fn deserialize(data: &[u8]) -> io::Result<(Self, usize)> {
        if data.len() < 21 {
            // Minimum: 8 + 1 + 4 + 4 + 4 = 21 bytes
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "Record too short",
            ));
        }

        let seq = u64::from_le_bytes(data[0..8].try_into().unwrap());
        let record_type = RecordType::try_from(data[8])?;
        let key_len = u32::from_le_bytes(data[9..13].try_into().unwrap()) as usize;
        let value_len = u32::from_le_bytes(data[13..17].try_into().unwrap());

        let header_size = 17;
        let key_end = header_size + key_len;

        if data.len() < key_end {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "Record truncated (key)",
            ));
        }

        let key = String::from_utf8(data[header_size..key_end].to_vec())
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        let (value, value_end) = if value_len == u32::MAX {
            (None, key_end)
        } else {
            let value_end = key_end + value_len as usize;
            if data.len() < value_end {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "Record truncated (value)",
                ));
            }
            let value = String::from_utf8(data[key_end..value_end].to_vec())
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            (Some(value), value_end)
        };

        let checksum_start = value_end;
        let checksum_end = checksum_start + 4;

        if data.len() < checksum_end {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "Record truncated (checksum)",
            ));
        }

        let stored_checksum =
            u32::from_le_bytes(data[checksum_start..checksum_end].try_into().unwrap());
        let computed_checksum = crc32fast::hash(&data[0..checksum_start]);

        if stored_checksum != computed_checksum {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Checksum mismatch: stored={:#x}, computed={:#x}",
                    stored_checksum, computed_checksum
                ),
            ));
        }

        Ok((
            Self {
                seq,
                record_type,
                key,
                value,
            },
            checksum_end,
        ))
    }
}

/// WAL file header
struct WalHeader {
    magic: u32,
    version: u8,
    _reserved: [u8; 3],
}

impl WalHeader {
    fn new() -> Self {
        Self {
            magic: WAL_MAGIC,
            version: WAL_VERSION,
            _reserved: [0; 3],
        }
    }

    fn serialize(&self) -> [u8; 8] {
        let mut buf = [0u8; 8];
        buf[0..4].copy_from_slice(&self.magic.to_le_bytes());
        buf[4] = self.version;
        buf
    }

    fn deserialize(data: &[u8]) -> io::Result<Self> {
        if data.len() < 8 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "WAL header too short",
            ));
        }

        let magic = u32::from_le_bytes(data[0..4].try_into().unwrap());
        if magic != WAL_MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Invalid WAL magic: {:#x}", magic),
            ));
        }

        let version = data[4];
        if version != WAL_VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Unsupported WAL version: {}", version),
            ));
        }

        Ok(Self {
            magic,
            version,
            _reserved: [0; 3],
        })
    }
}

/// WAL writer for B-tree operations
pub struct BTreeWalWriter {
    /// WAL file path
    path: PathBuf,
    /// Buffered writer
    writer: Mutex<Option<BufWriter<File>>>,
    /// Next sequence number
    next_seq: AtomicU64,
    /// Current batch sequence (0 if not in batch)
    batch_seq: AtomicU64,
}

impl BTreeWalWriter {
    /// Create or open WAL file
    pub fn open<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        let file_exists = path.exists();

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)?;

        let mut writer = BufWriter::new(file);

        let next_seq = if file_exists {
            // Read existing WAL to find last sequence
            let mut max_seq = 0u64;
            if let Ok(records) = Self::read_records_internal(&path) {
                for record in records {
                    max_seq = max_seq.max(record.seq);
                }
            }
            // Seek to end for appending
            writer.seek(SeekFrom::End(0))?;
            max_seq + 1
        } else {
            // Write header
            let header = WalHeader::new();
            writer.write_all(&header.serialize())?;
            writer.flush()?;
            1
        };

        Ok(Self {
            path,
            writer: Mutex::new(Some(writer)),
            next_seq: AtomicU64::new(next_seq),
            batch_seq: AtomicU64::new(0),
        })
    }

    /// Get WAL path
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Get next sequence number
    pub fn next_seq(&self) -> u64 {
        self.next_seq.load(Ordering::SeqCst)
    }

    /// Append a record
    fn append_record(&self, record: &WalRecord) -> io::Result<u64> {
        let mut guard = self.writer.lock().unwrap();
        let writer = guard
            .as_mut()
            .ok_or_else(|| io::Error::other("WAL writer closed"))?;

        let data = record.serialize();
        writer.write_all(&data)?;
        writer.flush()?;

        Ok(record.seq)
    }

    /// Log an insert operation
    pub fn log_insert(&self, key: &str, value: &str) -> io::Result<u64> {
        let seq = self.next_seq.fetch_add(1, Ordering::SeqCst);
        let record = WalRecord::insert(seq, key.to_string(), value.to_string());
        self.append_record(&record)
    }

    /// Log a delete operation
    pub fn log_delete(&self, key: &str) -> io::Result<u64> {
        let seq = self.next_seq.fetch_add(1, Ordering::SeqCst);
        let record = WalRecord::delete(seq, key.to_string());
        self.append_record(&record)
    }

    /// Log a checkpoint
    pub fn log_checkpoint(&self) -> io::Result<u64> {
        let seq = self.next_seq.fetch_add(1, Ordering::SeqCst);
        let record = WalRecord::checkpoint(seq);
        self.append_record(&record)
    }

    /// Begin a batch operation
    pub fn begin_batch(&self) -> io::Result<u64> {
        let seq = self.next_seq.fetch_add(1, Ordering::SeqCst);
        self.batch_seq.store(seq, Ordering::SeqCst);
        let record = WalRecord::begin_batch(seq);
        self.append_record(&record)
    }

    /// End a batch operation
    pub fn end_batch(&self) -> io::Result<u64> {
        let seq = self.next_seq.fetch_add(1, Ordering::SeqCst);
        self.batch_seq.store(0, Ordering::SeqCst);
        let record = WalRecord::end_batch(seq);
        self.append_record(&record)?;
        self.sync()?;
        Ok(seq)
    }

    /// Check if currently in a batch
    pub fn in_batch(&self) -> bool {
        self.batch_seq.load(Ordering::SeqCst) != 0
    }

    /// Sync WAL to disk
    pub fn sync(&self) -> io::Result<()> {
        let mut guard = self.writer.lock().unwrap();
        if let Some(writer) = guard.as_mut() {
            writer.flush()?;
            writer.get_ref().sync_all()?;
        }
        Ok(())
    }

    /// Read all records from WAL file
    fn read_records_internal<P: AsRef<Path>>(path: P) -> io::Result<Vec<WalRecord>> {
        let file = File::open(path)?;
        let mut reader = BufReader::new(file);

        // Read and validate header
        let mut header_buf = [0u8; 8];
        reader.read_exact(&mut header_buf)?;
        let _header = WalHeader::deserialize(&header_buf)?;

        // Read all records
        let mut records = Vec::new();
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf)?;

        let mut offset = 0;
        while offset < buf.len() {
            match WalRecord::deserialize(&buf[offset..]) {
                Ok((record, size)) => {
                    records.push(record);
                    offset += size;
                }
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                    // Truncated record at end, stop here
                    break;
                }
                Err(e) if e.kind() == io::ErrorKind::InvalidData => {
                    // Corrupted record, stop here
                    break;
                }
                Err(e) => return Err(e),
            }
        }

        Ok(records)
    }

    /// Read all records from this WAL
    pub fn read_records(&self) -> io::Result<Vec<WalRecord>> {
        Self::read_records_internal(&self.path)
    }

    /// Truncate WAL after checkpoint
    pub fn truncate_after_checkpoint(&self) -> io::Result<()> {
        // Close current writer
        {
            let mut guard = self.writer.lock().unwrap();
            *guard = None;
        }

        // Rewrite WAL with just header
        let file = OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&self.path)?;

        let mut writer = BufWriter::new(file);
        let header = WalHeader::new();
        writer.write_all(&header.serialize())?;
        writer.flush()?;
        writer.get_ref().sync_all()?;

        // Reopen for appending
        let file = OpenOptions::new().read(true).write(true).open(&self.path)?;
        let writer = BufWriter::new(file);

        {
            let mut guard = self.writer.lock().unwrap();
            *guard = Some(writer);
        }

        Ok(())
    }

    /// Close the WAL writer
    pub fn close(&self) -> io::Result<()> {
        self.sync()?;
        let mut guard = self.writer.lock().unwrap();
        *guard = None;
        Ok(())
    }
}

/// Read WAL records for recovery (from last checkpoint)
pub fn recover_from_wal<P: AsRef<Path>>(path: P) -> io::Result<Vec<WalRecord>> {
    let all_records = BTreeWalWriter::read_records_internal(path)?;

    // Find last checkpoint
    let checkpoint_idx = all_records
        .iter()
        .rposition(|r| r.record_type == RecordType::Checkpoint);

    // Find last complete batch
    let mut last_complete_batch_end = None;
    let mut batch_stack = Vec::new();

    for (i, record) in all_records.iter().enumerate() {
        match record.record_type {
            RecordType::BeginBatch => {
                batch_stack.push(i);
            }
            RecordType::EndBatch => {
                batch_stack.pop();
                last_complete_batch_end = Some(i);
            }
            _ => {}
        }
    }

    // Determine recovery start point
    let start_idx = match (checkpoint_idx, last_complete_batch_end) {
        (Some(cp), Some(batch)) => cp.max(batch) + 1,
        (Some(cp), None) => cp + 1,
        (None, Some(batch)) => batch + 1,
        (None, None) => 0,
    };

    // Return records after recovery point, excluding incomplete batches
    let mut records = Vec::new();
    let mut in_batch = false;

    for record in all_records.into_iter().skip(start_idx) {
        match record.record_type {
            RecordType::BeginBatch => {
                in_batch = true;
            }
            RecordType::EndBatch => {
                in_batch = false;
            }
            RecordType::Checkpoint => {
                // Skip checkpoint records during recovery
            }
            _ => {
                if !in_batch {
                    records.push(record);
                }
            }
        }
    }

    Ok(records)
}

/// Delete WAL file
pub fn delete_wal<P: AsRef<Path>>(path: P) -> io::Result<()> {
    if path.as_ref().exists() {
        fs::remove_file(path)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_wal_record_roundtrip() {
        let record = WalRecord::insert(1, "key1".to_string(), "value1".to_string());
        let data = record.serialize();
        let (decoded, _) = WalRecord::deserialize(&data).unwrap();

        assert_eq!(decoded.seq, 1);
        assert_eq!(decoded.record_type, RecordType::Insert);
        assert_eq!(decoded.key, "key1");
        assert_eq!(decoded.value, Some("value1".to_string()));
    }

    #[test]
    fn test_wal_write_read() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.wal");

        {
            let wal = BTreeWalWriter::open(&path).unwrap();
            wal.log_insert("key1", "value1").unwrap();
            wal.log_insert("key2", "value2").unwrap();
            wal.log_delete("key1").unwrap();
            wal.sync().unwrap();
        }

        let wal = BTreeWalWriter::open(&path).unwrap();
        let records = wal.read_records().unwrap();

        assert_eq!(records.len(), 3);
        assert_eq!(records[0].record_type, RecordType::Insert);
        assert_eq!(records[0].key, "key1");
        assert_eq!(records[1].record_type, RecordType::Insert);
        assert_eq!(records[1].key, "key2");
        assert_eq!(records[2].record_type, RecordType::Delete);
        assert_eq!(records[2].key, "key1");
    }

    #[test]
    fn test_wal_checkpoint() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.wal");

        {
            let wal = BTreeWalWriter::open(&path).unwrap();
            wal.log_insert("key1", "value1").unwrap();
            wal.log_checkpoint().unwrap();
            wal.log_insert("key2", "value2").unwrap();
            wal.sync().unwrap();
        }

        let records = recover_from_wal(&path).unwrap();

        // Should only have records after checkpoint
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].key, "key2");
    }

    #[test]
    fn test_wal_batch() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.wal");

        {
            let wal = BTreeWalWriter::open(&path).unwrap();
            wal.begin_batch().unwrap();
            wal.log_insert("key1", "value1").unwrap();
            wal.log_insert("key2", "value2").unwrap();
            wal.end_batch().unwrap();
            wal.sync().unwrap();
        }

        let wal = BTreeWalWriter::open(&path).unwrap();
        let records = wal.read_records().unwrap();

        assert_eq!(records.len(), 4); // begin + 2 inserts + end
    }

    #[test]
    fn test_wal_truncate() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.wal");

        {
            let wal = BTreeWalWriter::open(&path).unwrap();
            wal.log_insert("key1", "value1").unwrap();
            wal.log_checkpoint().unwrap();
            wal.truncate_after_checkpoint().unwrap();
        }

        let wal = BTreeWalWriter::open(&path).unwrap();
        let records = wal.read_records().unwrap();

        assert!(records.is_empty());
    }
}
