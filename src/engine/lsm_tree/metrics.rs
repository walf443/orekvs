use std::sync::atomic::{AtomicU64, Ordering};

/// Centralized metrics collection for LSM-tree engine
pub struct EngineMetrics {
    // Operation counts
    pub get_count: AtomicU64,
    pub set_count: AtomicU64,
    pub delete_count: AtomicU64,
    pub batch_get_count: AtomicU64,
    pub batch_set_count: AtomicU64,
    pub batch_delete_count: AtomicU64,

    // SSTable metrics
    pub sstable_searches: AtomicU64,
    pub bloom_filter_hits: AtomicU64,
    pub bloom_filter_false_positives: AtomicU64,

    // MemTable metrics
    pub memtable_flushes: AtomicU64,
    pub memtable_flush_bytes: AtomicU64,

    // Compaction metrics
    pub compaction_count: AtomicU64,
    pub compaction_bytes_read: AtomicU64,
    pub compaction_bytes_written: AtomicU64,
}

impl Default for EngineMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl EngineMetrics {
    pub fn new() -> Self {
        Self {
            get_count: AtomicU64::new(0),
            set_count: AtomicU64::new(0),
            delete_count: AtomicU64::new(0),
            batch_get_count: AtomicU64::new(0),
            batch_set_count: AtomicU64::new(0),
            batch_delete_count: AtomicU64::new(0),

            sstable_searches: AtomicU64::new(0),
            bloom_filter_hits: AtomicU64::new(0),
            bloom_filter_false_positives: AtomicU64::new(0),

            memtable_flushes: AtomicU64::new(0),
            memtable_flush_bytes: AtomicU64::new(0),

            compaction_count: AtomicU64::new(0),
            compaction_bytes_read: AtomicU64::new(0),
            compaction_bytes_written: AtomicU64::new(0),
        }
    }

    /// Record a get operation
    pub fn record_get(&self) {
        self.get_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a set operation
    pub fn record_set(&self) {
        self.set_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a delete operation
    pub fn record_delete(&self) {
        self.delete_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a batch get operation
    pub fn record_batch_get(&self, count: u64) {
        self.batch_get_count.fetch_add(count, Ordering::Relaxed);
    }

    /// Record a batch set operation
    pub fn record_batch_set(&self, count: u64) {
        self.batch_set_count.fetch_add(count, Ordering::Relaxed);
    }

    /// Record a batch delete operation
    pub fn record_batch_delete(&self, count: u64) {
        self.batch_delete_count.fetch_add(count, Ordering::Relaxed);
    }

    /// Record SSTable search
    pub fn record_sstable_search(&self) {
        self.sstable_searches.fetch_add(1, Ordering::Relaxed);
    }

    /// Record Bloom filter hit (key definitely not in SSTable)
    pub fn record_bloom_filter_hit(&self) {
        self.bloom_filter_hits.fetch_add(1, Ordering::Relaxed);
    }

    /// Record Bloom filter false positive (key might be in SSTable but wasn't)
    pub fn record_bloom_filter_false_positive(&self) {
        self.bloom_filter_false_positives
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Record a MemTable flush
    pub fn record_flush(&self, bytes: u64) {
        self.memtable_flushes.fetch_add(1, Ordering::Relaxed);
        self.memtable_flush_bytes
            .fetch_add(bytes, Ordering::Relaxed);
    }

    /// Record a compaction
    pub fn record_compaction(&self, bytes_read: u64, bytes_written: u64) {
        self.compaction_count.fetch_add(1, Ordering::Relaxed);
        self.compaction_bytes_read
            .fetch_add(bytes_read, Ordering::Relaxed);
        self.compaction_bytes_written
            .fetch_add(bytes_written, Ordering::Relaxed);
    }

    /// Get a snapshot of all metrics
    pub fn snapshot(&self) -> MetricsSnapshot {
        let bloom_checks = self.bloom_filter_hits.load(Ordering::Relaxed)
            + self.sstable_searches.load(Ordering::Relaxed);
        let bloom_effectiveness = if bloom_checks > 0 {
            self.bloom_filter_hits.load(Ordering::Relaxed) as f64 / bloom_checks as f64
        } else {
            0.0
        };

        MetricsSnapshot {
            get_count: self.get_count.load(Ordering::Relaxed),
            set_count: self.set_count.load(Ordering::Relaxed),
            delete_count: self.delete_count.load(Ordering::Relaxed),
            batch_get_count: self.batch_get_count.load(Ordering::Relaxed),
            batch_set_count: self.batch_set_count.load(Ordering::Relaxed),
            batch_delete_count: self.batch_delete_count.load(Ordering::Relaxed),

            sstable_searches: self.sstable_searches.load(Ordering::Relaxed),
            bloom_filter_hits: self.bloom_filter_hits.load(Ordering::Relaxed),
            bloom_filter_false_positives: self.bloom_filter_false_positives.load(Ordering::Relaxed),
            bloom_effectiveness,

            memtable_flushes: self.memtable_flushes.load(Ordering::Relaxed),
            memtable_flush_bytes: self.memtable_flush_bytes.load(Ordering::Relaxed),

            compaction_count: self.compaction_count.load(Ordering::Relaxed),
            compaction_bytes_read: self.compaction_bytes_read.load(Ordering::Relaxed),
            compaction_bytes_written: self.compaction_bytes_written.load(Ordering::Relaxed),
        }
    }

    /// Reset all metrics (for testing)
    #[allow(dead_code)]
    pub fn reset(&self) {
        self.get_count.store(0, Ordering::Relaxed);
        self.set_count.store(0, Ordering::Relaxed);
        self.delete_count.store(0, Ordering::Relaxed);
        self.batch_get_count.store(0, Ordering::Relaxed);
        self.batch_set_count.store(0, Ordering::Relaxed);
        self.batch_delete_count.store(0, Ordering::Relaxed);

        self.sstable_searches.store(0, Ordering::Relaxed);
        self.bloom_filter_hits.store(0, Ordering::Relaxed);
        self.bloom_filter_false_positives
            .store(0, Ordering::Relaxed);

        self.memtable_flushes.store(0, Ordering::Relaxed);
        self.memtable_flush_bytes.store(0, Ordering::Relaxed);

        self.compaction_count.store(0, Ordering::Relaxed);
        self.compaction_bytes_read.store(0, Ordering::Relaxed);
        self.compaction_bytes_written.store(0, Ordering::Relaxed);
    }
}

/// Snapshot of metrics at a point in time
#[derive(Debug, Clone)]
pub struct MetricsSnapshot {
    // Operation counts
    pub get_count: u64,
    pub set_count: u64,
    pub delete_count: u64,
    pub batch_get_count: u64,
    pub batch_set_count: u64,
    pub batch_delete_count: u64,

    // SSTable metrics
    pub sstable_searches: u64,
    pub bloom_filter_hits: u64,
    pub bloom_filter_false_positives: u64,
    /// Ratio of bloom filter hits to total checks (0.0 - 1.0)
    pub bloom_effectiveness: f64,

    // MemTable metrics
    pub memtable_flushes: u64,
    pub memtable_flush_bytes: u64,

    // Compaction metrics
    pub compaction_count: u64,
    pub compaction_bytes_read: u64,
    pub compaction_bytes_written: u64,
}

impl std::fmt::Display for MetricsSnapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "=== Engine Metrics ===")?;
        writeln!(f, "Operations:")?;
        writeln!(f, "  GET: {}", self.get_count)?;
        writeln!(f, "  SET: {}", self.set_count)?;
        writeln!(f, "  DELETE: {}", self.delete_count)?;
        writeln!(f, "  Batch GET: {}", self.batch_get_count)?;
        writeln!(f, "  Batch SET: {}", self.batch_set_count)?;
        writeln!(f, "  Batch DELETE: {}", self.batch_delete_count)?;
        writeln!(f)?;
        writeln!(f, "SSTable:")?;
        writeln!(f, "  Searches: {}", self.sstable_searches)?;
        writeln!(f, "  Bloom filter hits: {}", self.bloom_filter_hits)?;
        writeln!(
            f,
            "  Bloom filter false positives: {}",
            self.bloom_filter_false_positives
        )?;
        writeln!(
            f,
            "  Bloom effectiveness: {:.1}%",
            self.bloom_effectiveness * 100.0
        )?;
        writeln!(f)?;
        writeln!(f, "MemTable:")?;
        writeln!(f, "  Flushes: {}", self.memtable_flushes)?;
        writeln!(
            f,
            "  Flush bytes: {} ({:.2} MB)",
            self.memtable_flush_bytes,
            self.memtable_flush_bytes as f64 / 1024.0 / 1024.0
        )?;
        writeln!(f)?;
        writeln!(f, "Compaction:")?;
        writeln!(f, "  Count: {}", self.compaction_count)?;
        writeln!(
            f,
            "  Bytes read: {} ({:.2} MB)",
            self.compaction_bytes_read,
            self.compaction_bytes_read as f64 / 1024.0 / 1024.0
        )?;
        writeln!(
            f,
            "  Bytes written: {} ({:.2} MB)",
            self.compaction_bytes_written,
            self.compaction_bytes_written as f64 / 1024.0 / 1024.0
        )?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_basic() {
        let metrics = EngineMetrics::new();

        metrics.record_get();
        metrics.record_get();
        metrics.record_set();

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.get_count, 2);
        assert_eq!(snapshot.set_count, 1);
    }

    #[test]
    fn test_metrics_reset() {
        let metrics = EngineMetrics::new();

        metrics.record_get();
        metrics.record_set();

        metrics.reset();

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.get_count, 0);
        assert_eq!(snapshot.set_count, 0);
    }

    #[test]
    fn test_bloom_effectiveness() {
        let metrics = EngineMetrics::new();

        // 7 bloom filter hits (skipped SSTable search)
        for _ in 0..7 {
            metrics.record_bloom_filter_hit();
        }
        // 3 SSTable searches (bloom filter said "maybe")
        for _ in 0..3 {
            metrics.record_sstable_search();
        }

        let snapshot = metrics.snapshot();
        // 7 hits / (7 hits + 3 searches) = 0.7
        assert!((snapshot.bloom_effectiveness - 0.7).abs() < 0.01);
    }
}
