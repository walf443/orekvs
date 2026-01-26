//! WAL common utilities
//!
//! Shared functionality for Write-Ahead Log implementations across different
//! storage engines (LSM-Tree, B-Tree).

use std::sync::atomic::{AtomicU64, Ordering};

/// Compute CRC32 checksum using crc32fast
pub fn crc32(data: &[u8]) -> u32 {
    crc32fast::hash(data)
}

/// Verify checksum matches expected value
pub fn verify_checksum(data: &[u8], expected: u32) -> bool {
    crc32(data) == expected
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
}
