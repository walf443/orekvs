use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

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

        let num_u64s = (m + 63) / 64;
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
}
