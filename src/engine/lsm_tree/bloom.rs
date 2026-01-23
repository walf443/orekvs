use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

/// Current serialization format version
const BLOOM_FORMAT_VERSION: u8 = 1;

pub struct BloomFilter {
    bits: Vec<u64>,
    num_hashes: u32,
    num_bits: usize,
}

impl BloomFilter {
    /// Create a new Bloom filter with optimal size for expected_entries and false_positive_rate
    pub fn new(expected_entries: usize, false_positive_rate: f64) -> Self {
        // m = - (n * ln(p)) / (ln(2)^2)
        let n = expected_entries as f64;
        let p = false_positive_rate;
        let m = (-(n * p.ln()) / (2.0f64.ln().powi(2))).ceil() as usize;

        // k = (m / n) * ln(2)
        let k = ((m as f64 / n) * 2.0f64.ln()).ceil() as u32;
        let k = k.clamp(1, 30); // Sanity check

        let num_u64s = m.div_ceil(64);
        Self {
            bits: vec![0u64; num_u64s],
            num_hashes: k,
            num_bits: num_u64s * 64,
        }
    }

    pub fn insert(&mut self, key: &str) {
        for i in 0..self.num_hashes {
            let h = self.calculate_hash(key, i);
            let bit_pos = (h as usize) % self.num_bits;
            self.bits[bit_pos / 64] |= 1 << (bit_pos % 64);
        }
    }

    pub fn contains(&self, key: &str) -> bool {
        if self.num_bits == 0 {
            return false;
        }
        for i in 0..self.num_hashes {
            let h = self.calculate_hash(key, i);
            let bit_pos = (h as usize) % self.num_bits;
            if (self.bits[bit_pos / 64] & (1 << (bit_pos % 64))) == 0 {
                return false;
            }
        }
        true
    }

    fn calculate_hash(&self, key: &str, i: u32) -> u64 {
        let mut s = DefaultHasher::new();
        key.hash(&mut s);
        i.hash(&mut s);
        s.finish()
    }

    /// Serialize the Bloom filter to bytes
    /// Format (v1): version(1) + num_hashes(4) + num_bits(8) + bits_len(8) + bits_data
    pub fn serialize(&self) -> Vec<u8> {
        let mut data = Vec::new();

        // version (u8)
        data.push(BLOOM_FORMAT_VERSION);
        // num_hashes (u32)
        data.extend_from_slice(&self.num_hashes.to_le_bytes());
        // num_bits (u64)
        data.extend_from_slice(&(self.num_bits as u64).to_le_bytes());
        // bits length (u64)
        data.extend_from_slice(&(self.bits.len() as u64).to_le_bytes());
        // bits data
        for &word in &self.bits {
            data.extend_from_slice(&word.to_le_bytes());
        }

        data
    }

    /// Deserialize a Bloom filter from bytes
    /// Supports format version 1
    /// Returns None for unsupported versions (caller should fall back to building from keys)
    pub fn deserialize(data: &[u8]) -> Option<Self> {
        if data.len() < 21 {
            // Minimum: 1 + 4 + 8 + 8 = 21 bytes for header
            return None;
        }

        let version = data[0];
        if version != 1 {
            eprintln!(
                "Warning: Unknown Bloom filter version {}. Falling back to building from keys.",
                version
            );
            return None;
        }

        let num_hashes = u32::from_le_bytes(data[1..5].try_into().ok()?);
        let num_bits = u64::from_le_bytes(data[5..13].try_into().ok()?) as usize;
        let bits_len = u64::from_le_bytes(data[13..21].try_into().ok()?) as usize;

        let expected_data_len = 21 + bits_len * 8;
        if data.len() < expected_data_len {
            return None;
        }

        let mut bits = Vec::with_capacity(bits_len);
        for i in 0..bits_len {
            let offset = 21 + i * 8;
            let word = u64::from_le_bytes(data[offset..offset + 8].try_into().ok()?);
            bits.push(word);
        }

        Some(Self {
            bits,
            num_hashes,
            num_bits,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bloom_filter_basic() {
        let mut bf = BloomFilter::new(100, 0.01);
        bf.insert("apple");
        bf.insert("orange");

        assert!(bf.contains("apple"));
        assert!(bf.contains("orange"));
        assert!(!bf.contains("banana"));
    }

    #[test]
    fn test_bloom_filter_empty() {
        let bf = BloomFilter::new(10, 0.01);
        assert!(!bf.contains("anything"));
    }

    #[test]
    fn test_bloom_filter_serialize_deserialize() {
        let mut bf = BloomFilter::new(100, 0.01);
        bf.insert("apple");
        bf.insert("orange");
        bf.insert("banana");

        // Serialize
        let data = bf.serialize();

        // Deserialize
        let bf2 = BloomFilter::deserialize(&data).expect("deserialization should succeed");

        // Check that the deserialized filter has the same behavior
        assert!(bf2.contains("apple"));
        assert!(bf2.contains("orange"));
        assert!(bf2.contains("banana"));
        assert!(!bf2.contains("grape")); // Not inserted
    }

    #[test]
    fn test_bloom_filter_deserialize_invalid() {
        // Too short data
        let short_data = vec![0u8; 10];
        assert!(BloomFilter::deserialize(&short_data).is_none());

        // Empty data
        let empty_data: Vec<u8> = vec![];
        assert!(BloomFilter::deserialize(&empty_data).is_none());
    }

    #[test]
    fn test_bloom_filter_version() {
        let mut bf = BloomFilter::new(100, 0.01);
        bf.insert("test");

        let data = bf.serialize();

        // First byte should be version 1
        assert_eq!(data[0], 1);

        // Unsupported version should fail
        let mut bad_version_data = data.clone();
        bad_version_data[0] = 99; // Unknown version
        assert!(BloomFilter::deserialize(&bad_version_data).is_none());
    }
}
