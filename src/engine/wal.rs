//! WAL common utilities
//!
//! Shared functionality for Write-Ahead Log implementations across different
//! storage engines (LSM-Tree, B-Tree).

use std::io::{self, Cursor, Read, Write};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

/// Default batch interval in microseconds (1ms)
pub const DEFAULT_BATCH_INTERVAL_MICROS: u64 = 1000;

/// Configuration for group commit WAL writers
#[derive(Debug, Clone, Copy)]
pub struct GroupCommitConfig {
    /// Batch interval in microseconds
    ///
    /// Controls how long to wait for batching writes before flushing.
    /// Higher values increase throughput but also increase latency.
    pub batch_interval_micros: u64,
}

impl Default for GroupCommitConfig {
    fn default() -> Self {
        Self {
            batch_interval_micros: DEFAULT_BATCH_INTERVAL_MICROS,
        }
    }
}

impl GroupCommitConfig {
    /// Create a new config with the specified batch interval
    pub fn with_batch_interval(batch_interval_micros: u64) -> Self {
        Self {
            batch_interval_micros,
        }
    }
}

// ============================================================================
// WAL Header format
// ============================================================================

/// Common WAL magic bytes (same as LSM-Tree)
pub const WAL_MAGIC: &[u8; 9] = b"ORELSMWAL";

/// WAL header size in bytes
pub const WAL_HEADER_SIZE: usize = 13; // 9 (magic) + 4 (version)

/// Current WAL format version (v4: block-based with compression)
pub const WAL_FORMAT_VERSION: u32 = 4;

/// Write WAL header to a writer
///
/// Header format: [magic: 9 bytes][version: u32 LE]
pub fn write_wal_header<W: Write>(writer: &mut W, version: u32) -> io::Result<()> {
    writer.write_all(WAL_MAGIC)?;
    writer.write_all(&version.to_le_bytes())?;
    Ok(())
}

/// Read and validate WAL header from a reader
///
/// Returns the version number if valid, or an error if the header is invalid.
pub fn read_wal_header<R: Read>(reader: &mut R) -> io::Result<u32> {
    let mut magic = [0u8; 9];
    reader.read_exact(&mut magic)?;

    if &magic != WAL_MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "Invalid WAL magic: expected {:?}, got {:?}",
                WAL_MAGIC, magic
            ),
        ));
    }

    let mut version_bytes = [0u8; 4];
    reader.read_exact(&mut version_bytes)?;
    let version = u32::from_le_bytes(version_bytes);

    Ok(version)
}

// ============================================================================
// Block format for WAL entries
// ============================================================================

/// Block flag: data is compressed with zstd
pub const BLOCK_FLAG_COMPRESSED: u8 = 0x01;

/// Minimum data size in bytes to attempt compression
pub const COMPRESSION_THRESHOLD: usize = 512;

/// Write a block of data with optional compression
///
/// Block format: `[flags: u8][uncompressed_size: u32][data_size: u32][data][crc32: u32]`
///
/// If `enable_compression` is true and the data size exceeds `COMPRESSION_THRESHOLD`,
/// the data will be compressed with zstd. Compression is only used if it actually
/// reduces the data size.
pub fn write_block<W: Write>(
    writer: &mut W,
    data: &[u8],
    enable_compression: bool,
) -> io::Result<()> {
    let uncompressed_size = data.len() as u32;

    if enable_compression
        && data.len() >= COMPRESSION_THRESHOLD
        && let Ok(compressed) = zstd::encode_all(Cursor::new(data), 3)
        && compressed.len() < data.len()
    {
        // Compression was beneficial
        let flags = BLOCK_FLAG_COMPRESSED;
        let data_size = compressed.len() as u32;
        let checksum = crc32(&compressed);

        writer.write_all(&[flags])?;
        writer.write_all(&uncompressed_size.to_le_bytes())?;
        writer.write_all(&data_size.to_le_bytes())?;
        writer.write_all(&compressed)?;
        writer.write_all(&checksum.to_le_bytes())?;

        return Ok(());
    }

    // Write uncompressed block
    let flags = 0u8;
    let data_size = data.len() as u32;
    let checksum = crc32(data);

    writer.write_all(&[flags])?;
    writer.write_all(&uncompressed_size.to_le_bytes())?;
    writer.write_all(&data_size.to_le_bytes())?;
    writer.write_all(data)?;
    writer.write_all(&checksum.to_le_bytes())?;

    Ok(())
}

/// Read a block of data and decompress if necessary
///
/// Returns the decompressed data, or an error if:
/// - The block header is truncated
/// - The checksum doesn't match
/// - Decompression fails
///
/// Returns `Ok(None)` if EOF is reached (no more blocks).
pub fn read_block<R: Read>(reader: &mut R) -> io::Result<Option<Vec<u8>>> {
    // Read flags
    let mut flags = [0u8; 1];
    match reader.read_exact(&mut flags) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e),
    }

    // Read sizes
    let mut uncompressed_size_bytes = [0u8; 4];
    let mut data_size_bytes = [0u8; 4];
    reader.read_exact(&mut uncompressed_size_bytes)?;
    reader.read_exact(&mut data_size_bytes)?;

    let _uncompressed_size = u32::from_le_bytes(uncompressed_size_bytes);
    let data_size = u32::from_le_bytes(data_size_bytes) as usize;

    // Read data
    let mut data = vec![0u8; data_size];
    reader.read_exact(&mut data)?;

    // Read and verify checksum
    let mut checksum_bytes = [0u8; 4];
    reader.read_exact(&mut checksum_bytes)?;
    let stored_checksum = u32::from_le_bytes(checksum_bytes);
    let computed_checksum = crc32(&data);

    if stored_checksum != computed_checksum {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "Block checksum mismatch: stored={:#x}, computed={:#x}",
                stored_checksum, computed_checksum
            ),
        ));
    }

    // Decompress if needed
    if flags[0] & BLOCK_FLAG_COMPRESSED != 0 {
        let decompressed = zstd::decode_all(Cursor::new(&data)).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Decompression error: {}", e),
            )
        })?;
        Ok(Some(decompressed))
    } else {
        Ok(Some(data))
    }
}

/// Common trait for WAL writers
///
/// Provides a common interface for WAL state queries and management operations.
/// Write operations are not included in this trait because B-Tree uses
/// synchronous I/O while LSM-Tree uses asynchronous I/O.
pub trait WalWriter {
    /// Get current WAL ID
    fn current_id(&self) -> u64;

    /// Get current sequence number (next to be allocated)
    fn current_seq(&self) -> u64;

    /// Get data directory path
    fn data_dir(&self) -> &Path;

    /// Rotate to a new WAL file
    ///
    /// Creates a new WAL file with an incremented ID and switches to it.
    /// Returns the new WAL ID.
    fn rotate(&self) -> io::Result<u64>;

    /// Delete WAL files up to (and including) the given ID
    fn delete_wals_up_to(&self, max_id: u64) -> io::Result<()>;
}

/// Compute CRC32 checksum using crc32fast
pub fn crc32(data: &[u8]) -> u32 {
    crc32fast::hash(data)
}

/// Verify checksum matches expected value
pub fn verify_checksum(data: &[u8], expected: u32) -> bool {
    crc32(data) == expected
}

/// Parse WAL filename and extract the WAL ID
///
/// Expected format: `wal_NNNNN.log` where NNNNN is a zero-padded number.
/// Returns `None` if the filename doesn't match the expected format.
pub fn parse_wal_filename(filename: &str) -> Option<u64> {
    if filename.starts_with("wal_") && filename.ends_with(".log") {
        filename[4..filename.len() - 4].parse::<u64>().ok()
    } else {
        None
    }
}

/// Sequence number generator using atomic operations
///
/// Thread-safe generator for allocating monotonically increasing sequence numbers.
#[derive(Debug)]
pub struct SeqGenerator {
    next: AtomicU64,
}

impl SeqGenerator {
    /// Create a new generator starting at the given initial value
    ///
    /// The first call to `allocate()` will return `initial + 1`.
    pub fn new(initial: u64) -> Self {
        Self {
            next: AtomicU64::new(initial + 1),
        }
    }

    /// Allocate the next sequence number
    pub fn allocate(&self) -> u64 {
        self.next.fetch_add(1, Ordering::SeqCst)
    }

    /// Get the current (next to be allocated) sequence number without incrementing
    pub fn current(&self) -> u64 {
        self.next.load(Ordering::SeqCst)
    }

    /// Set the next sequence number
    pub fn set(&self, value: u64) {
        self.next.store(value, Ordering::SeqCst);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_crc32() {
        let data = b"hello world";
        let checksum = crc32(data);
        assert!(verify_checksum(data, checksum));
        assert!(!verify_checksum(data, checksum + 1));
    }

    #[test]
    fn test_seq_generator() {
        let seq_gen = SeqGenerator::new(0);
        assert_eq!(seq_gen.current(), 1);
        assert_eq!(seq_gen.allocate(), 1);
        assert_eq!(seq_gen.allocate(), 2);
        assert_eq!(seq_gen.allocate(), 3);
        assert_eq!(seq_gen.current(), 4);

        seq_gen.set(100);
        assert_eq!(seq_gen.current(), 100);
        assert_eq!(seq_gen.allocate(), 100);
        assert_eq!(seq_gen.current(), 101);
    }

    #[test]
    fn test_seq_generator_with_initial() {
        let seq_gen = SeqGenerator::new(99);
        assert_eq!(seq_gen.current(), 100);
        assert_eq!(seq_gen.allocate(), 100);
        assert_eq!(seq_gen.allocate(), 101);
    }

    #[test]
    fn test_parse_wal_filename() {
        assert_eq!(parse_wal_filename("wal_00001.log"), Some(1));
        assert_eq!(parse_wal_filename("wal_12345.log"), Some(12345));
        assert_eq!(parse_wal_filename("wal_00000.log"), Some(0));
        assert_eq!(parse_wal_filename("other.log"), None);
        assert_eq!(parse_wal_filename("wal_123.data"), None);
    }

    #[test]
    fn test_write_read_block_uncompressed() {
        let data = b"hello world";
        let mut buf = Vec::new();

        // Write block without compression
        write_block(&mut buf, data, false).unwrap();

        // Read it back
        let mut cursor = std::io::Cursor::new(&buf);
        let result = read_block(&mut cursor).unwrap().unwrap();

        assert_eq!(result, data);
    }

    #[test]
    fn test_write_read_block_small_data_no_compress() {
        // Data smaller than COMPRESSION_THRESHOLD should not be compressed
        let data = b"small data";
        let mut buf = Vec::new();

        write_block(&mut buf, data, true).unwrap();

        // First byte (flags) should be 0 (no compression)
        assert_eq!(buf[0], 0);

        // Read it back
        let mut cursor = std::io::Cursor::new(&buf);
        let result = read_block(&mut cursor).unwrap().unwrap();

        assert_eq!(result, data);
    }

    #[test]
    fn test_write_read_block_large_data_compressed() {
        // Create data larger than COMPRESSION_THRESHOLD that compresses well
        let data: Vec<u8> = (0..1000).map(|i| (i % 256) as u8).collect();
        let mut buf = Vec::new();

        write_block(&mut buf, &data, true).unwrap();

        // First byte (flags) should indicate compression
        assert_eq!(buf[0] & BLOCK_FLAG_COMPRESSED, BLOCK_FLAG_COMPRESSED);

        // Compressed size should be smaller
        let data_size = u32::from_le_bytes(buf[5..9].try_into().unwrap()) as usize;
        assert!(data_size < data.len());

        // Read it back
        let mut cursor = std::io::Cursor::new(&buf);
        let result = read_block(&mut cursor).unwrap().unwrap();

        assert_eq!(result, data);
    }

    #[test]
    fn test_read_block_eof() {
        let buf: Vec<u8> = Vec::new();
        let mut cursor = std::io::Cursor::new(&buf);
        let result = read_block(&mut cursor).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_read_block_checksum_error() {
        let data = b"test data";
        let mut buf = Vec::new();
        write_block(&mut buf, data, false).unwrap();

        // Corrupt the checksum (last 4 bytes)
        let len = buf.len();
        buf[len - 1] ^= 0xFF;

        let mut cursor = std::io::Cursor::new(&buf);
        let result = read_block(&mut cursor);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("checksum"));
    }
}
