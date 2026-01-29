//! Benchmark tests for LSM-tree engine.
//!
//! Run with: cargo test bench_ --release -- --nocapture --ignored

use super::*;
use tempfile::tempdir;

/// Benchmark test to compare batch_delete vs individual delete
/// Run with: cargo test bench_batch_delete --release -- --nocapture --ignored
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn bench_batch_delete() {
    use std::time::Instant;

    let num_keys = 1000;

    // Benchmark individual delete
    {
        let dir = tempdir().unwrap();
        let data_dir = dir.path().to_str().unwrap().to_string();
        let engine = LsmTreeEngine::new(data_dir, 1024 * 1024, 100);

        // Setup: create keys
        for i in 0..num_keys {
            engine
                .set(format!("key{:05}", i), format!("value{:05}", i))
                .unwrap();
        }

        // Benchmark individual deletes
        let start = Instant::now();
        for i in 0..num_keys {
            engine.delete(format!("key{:05}", i)).unwrap();
        }
        let individual_duration = start.elapsed();

        println!("\n=== Batch Delete Benchmark ===");
        println!(
            "Individual delete: {} keys in {:?} ({:.2} keys/sec)",
            num_keys,
            individual_duration,
            num_keys as f64 / individual_duration.as_secs_f64()
        );
    }

    // Benchmark batch delete
    {
        let dir = tempdir().unwrap();
        let data_dir = dir.path().to_str().unwrap().to_string();
        let engine = LsmTreeEngine::new(data_dir, 1024 * 1024, 100);

        // Setup: create keys
        for i in 0..num_keys {
            engine
                .set(format!("key{:05}", i), format!("value{:05}", i))
                .unwrap();
        }

        // Benchmark batch delete
        let keys: Vec<String> = (0..num_keys).map(|i| format!("key{:05}", i)).collect();
        let start = Instant::now();
        engine.batch_delete(keys).unwrap();
        let batch_duration = start.elapsed();

        println!(
            "Batch delete:      {} keys in {:?} ({:.2} keys/sec)",
            num_keys,
            batch_duration,
            num_keys as f64 / batch_duration.as_secs_f64()
        );

        println!("=====================================\n");
    }
}

/// Benchmark test to measure block cache effectiveness
/// Run with: cargo test bench_block_cache --release -- --nocapture --ignored
#[tokio::test(flavor = "multi_thread")]
#[ignore] // This is a benchmark, not a unit test - run explicitly when needed
async fn bench_block_cache_effectiveness() {
    use std::time::Instant;

    let dir = tempdir().unwrap();
    let data_dir = dir.path().to_str().unwrap().to_string();

    // Create engine with larger memtable to batch keys into fewer SSTables
    let engine = LsmTreeEngine::new(data_dir.clone(), 50 * 1024, 100);

    // Phase 1: Write data to create SSTables
    let num_keys = 5000;
    println!("\n=== Block Cache Benchmark ===");
    println!("Writing {} keys...", num_keys);

    for i in 0..num_keys {
        engine
            .set(format!("key{:05}", i), format!("value{:05}", i))
            .unwrap();
    }

    // Force flush remaining memtable by writing more data
    for i in 0..100 {
        engine
            .set(format!("flush{:05}", i), "x".repeat(1000))
            .unwrap();
    }

    // Wait for all flushes to complete
    tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;

    let sst_handles: Vec<Arc<SstableHandle>> =
        engine.leveled_sstables.lock().unwrap().to_flat_list();
    println!("Created {} SSTables", sst_handles.len());

    // Phase 2: Compare non-mmap vs mmap reads directly on SSTable
    println!("\n--- Direct SSTable Read Comparison ---");

    // Test key that should be in SSTable
    let test_key = "key00100";

    // Non-mmap reads (using search_key with file path)
    let iterations = 1000;
    let start = Instant::now();
    for _ in 0..iterations {
        for handle in &sst_handles {
            let _ = sstable::search_key(handle.mmap.path(), test_key);
        }
    }
    let no_mmap_duration = start.elapsed();
    println!(
        "Without mmap: {} SSTable searches in {:?} ({:.2}/sec)",
        iterations * sst_handles.len(),
        no_mmap_duration,
        (iterations * sst_handles.len()) as f64 / no_mmap_duration.as_secs_f64()
    );

    // Mmap-based reads (using search_key_mmap)
    let now = crate::engine::current_timestamp();
    let start = Instant::now();
    for _ in 0..iterations {
        for handle in &sst_handles {
            let _ = sstable::search_key_mmap(&handle.mmap, test_key, &engine.block_cache, now);
        }
    }
    let mmap_duration = start.elapsed();
    println!(
        "With mmap:    {} SSTable searches in {:?} ({:.2}/sec)",
        iterations * sst_handles.len(),
        mmap_duration,
        (iterations * sst_handles.len()) as f64 / mmap_duration.as_secs_f64()
    );

    let stats = engine.block_cache.stats();
    println!(
        "\nCache stats: {} entries, {} bytes",
        stats.entries, stats.size_bytes
    );

    let improvement = no_mmap_duration.as_secs_f64() / mmap_duration.as_secs_f64();
    println!("\n=== Results ===");
    println!(
        "Mmap speedup: {:.2}x faster with mmap + block cache",
        improvement
    );
    println!("================================\n");
}

/// Benchmark test to measure Bloom filter loading improvement
/// Run with: cargo test bench_bloom_loading --release -- --nocapture --ignored
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn bench_bloom_loading() {
    use std::time::Instant;

    let dir = tempdir().unwrap();
    let data_dir = dir.path().to_str().unwrap().to_string();

    println!("\n=== Bloom Filter Loading Benchmark ===");
    println!("Creating test data...");

    // Create engine and populate with data to create multiple SSTables
    {
        let engine = LsmTreeEngine::new(data_dir.clone(), 50 * 1024, 100);

        // Write enough data to create multiple SSTables
        for i in 0..10000 {
            engine
                .set(format!("key{:06}", i), format!("value{:06}", i))
                .unwrap();
        }

        // Force flush
        for i in 0..50 {
            engine
                .set(format!("flush{:05}", i), "x".repeat(2000))
                .unwrap();
        }

        tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;
        engine.shutdown().await;
    }

    // Count SSTable files
    let sst_files: Vec<_> = fs::read_dir(&data_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.path()
                .file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|s| s.starts_with("sst_") && s.ends_with(".data"))
        })
        .map(|e| e.path())
        .collect();

    println!("Created {} SSTables", sst_files.len());

    // Benchmark V4 (read_bloom_filter)
    let iterations = 10;
    let start = Instant::now();
    for _ in 0..iterations {
        for path in &sst_files {
            let _ = sstable::read_bloom_filter(path);
        }
    }
    let v4_duration = start.elapsed();

    // Benchmark V3 style (read_keys + build bloom)
    let start = Instant::now();
    for _ in 0..iterations {
        for path in &sst_files {
            let keys = sstable::read_keys(path).unwrap();
            let mut bloom = BloomFilter::new(keys.len().max(1), 0.01);
            for key in &keys {
                bloom.insert(key);
            }
        }
    }
    let v3_duration = start.elapsed();

    println!(
        "\n=== Results ({} iterations x {} SSTables) ===",
        iterations,
        sst_files.len()
    );
    println!(
        "V4 (read_bloom_filter):   {:?} ({:.2} ms/SSTable)",
        v4_duration,
        v4_duration.as_secs_f64() * 1000.0 / (iterations * sst_files.len()) as f64
    );
    println!(
        "V3 (read_keys + build):   {:?} ({:.2} ms/SSTable)",
        v3_duration,
        v3_duration.as_secs_f64() * 1000.0 / (iterations * sst_files.len()) as f64
    );
    println!(
        "Speedup: {:.2}x faster with embedded Bloom filter",
        v3_duration.as_secs_f64() / v4_duration.as_secs_f64()
    );
    println!("=====================================\n");
}

/// Benchmark test to measure Index Block Cache effectiveness
/// Run with: cargo test bench_index_cache --release -- --nocapture --ignored
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn bench_index_cache() {
    use super::sstable::MappedSSTable;
    use std::time::Instant;

    let dir = tempdir().unwrap();
    let data_dir = dir.path().to_str().unwrap().to_string();

    println!("\n=== Index Cache Benchmark ===");
    println!("Creating test data...");

    // Create engine and populate with data to create SSTables
    let engine = LsmTreeEngine::new(data_dir.clone(), 50 * 1024, 100);

    // Write enough data to create multiple SSTables
    for i in 0..5000 {
        engine
            .set(format!("key{:05}", i), format!("value{:05}", i))
            .unwrap();
    }

    // Force flush
    for i in 0..100 {
        engine
            .set(format!("flush{:05}", i), "x".repeat(1000))
            .unwrap();
    }
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    // Get SSTable paths
    let sst_files: Vec<_> = std::fs::read_dir(&data_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().map_or(false, |ext| ext == "data"))
        .map(|e| e.path())
        .collect();

    println!("Created {} SSTables", sst_files.len());

    if sst_files.is_empty() {
        println!("No SSTables created, skipping benchmark");
        return;
    }

    let iterations = 1000;

    // Benchmark: First read (cold cache) vs subsequent reads (warm cache)
    println!("\n--- Index Read Performance ---");

    // Test with fresh MappedSSTable instances (simulates cold cache)
    let start = Instant::now();
    for _ in 0..iterations {
        for path in &sst_files {
            let sst = MappedSSTable::open(path).unwrap();
            let _ = sst.read_index().unwrap();
        }
    }
    let cold_duration = start.elapsed();
    println!(
        "Cold cache (new SSTable each time): {} reads in {:?} ({:.2}/sec)",
        iterations * sst_files.len(),
        cold_duration,
        (iterations * sst_files.len()) as f64 / cold_duration.as_secs_f64()
    );

    // Open SSTables once and reuse (warm cache)
    let sstables: Vec<_> = sst_files
        .iter()
        .map(|p| MappedSSTable::open(p).unwrap())
        .collect();

    // First read to populate cache
    for sst in &sstables {
        let _ = sst.read_index().unwrap();
    }

    // Benchmark warm cache reads
    let start = Instant::now();
    for _ in 0..iterations {
        for sst in &sstables {
            let _ = sst.read_index().unwrap();
        }
    }
    let warm_duration = start.elapsed();
    println!(
        "Warm cache (reuse SSTable):         {} reads in {:?} ({:.2}/sec)",
        iterations * sst_files.len(),
        warm_duration,
        (iterations * sst_files.len()) as f64 / warm_duration.as_secs_f64()
    );

    let improvement = cold_duration.as_secs_f64() / warm_duration.as_secs_f64();
    println!("\n=== Results ===");
    println!(
        "Index cache speedup: {:.2}x faster with OnceLock cache",
        improvement
    );
    println!(
        "Cold: {:.3} ms/read, Warm: {:.3} ms/read",
        cold_duration.as_secs_f64() * 1000.0 / (iterations * sst_files.len()) as f64,
        warm_duration.as_secs_f64() * 1000.0 / (iterations * sst_files.len()) as f64
    );
    println!("=====================================\n");
}

/// Benchmark test to measure compaction optimization for expired entries
/// Compares read_entries vs read_entries_for_compaction at different expiration rates
/// Run with: cargo test bench_compaction_expired --release -- --nocapture --ignored
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn bench_compaction_expired_entries() {
    use crate::engine::current_timestamp;
    use crate::engine::lsm_tree::memtable::MemValue;
    use std::collections::BTreeMap;
    use std::time::Instant;

    println!("\n=== Compaction Expired Entries Benchmark ===");

    // Parameters
    let num_entries = 5000;
    let value_size = 1024; // 1KB values
    let expired_ratios = [0.0, 0.25, 0.50, 0.75, 0.90, 1.0];

    println!("Parameters:");
    println!("  - Entries: {}", num_entries);
    println!("  - Value size: {} bytes", value_size);
    println!("\n| Expired % | Standard | Optimized | Speedup |");
    println!("|-----------|----------|-----------|---------|");

    for expired_ratio in expired_ratios {
        let dir = tempdir().unwrap();
        let now = current_timestamp();
        let value = "x".repeat(value_size);

        // Create memtable with mixed expired/non-expired entries
        let mut memtable: BTreeMap<String, MemValue> = BTreeMap::new();
        for i in 0..num_entries {
            let expire_at = if (i as f64) < (num_entries as f64 * expired_ratio) {
                // Expired entry
                now - 1
            } else {
                // Valid entry (has TTL but not expired yet)
                now + 3600
            };
            memtable.insert(
                format!("key{:06}", i),
                MemValue::new_with_ttl(Some(value.clone()), expire_at),
            );
        }

        // Write SSTable
        let sst_path = dir.path().join("test_bench.data");
        sstable::create_from_memtable(&sst_path, &memtable).unwrap();

        let iterations = 30;

        // Benchmark: read_entries (without optimization)
        let start = Instant::now();
        for _ in 0..iterations {
            let _ = sstable::read_entries(&sst_path).unwrap();
        }
        let standard_duration = start.elapsed();

        // Benchmark: read_entries_for_compaction (with optimization)
        let start = Instant::now();
        for _ in 0..iterations {
            let _ = sstable::read_entries_for_compaction(&sst_path, now).unwrap();
        }
        let optimized_duration = start.elapsed();

        let standard_ms = standard_duration.as_secs_f64() * 1000.0 / iterations as f64;
        let optimized_ms = optimized_duration.as_secs_f64() * 1000.0 / iterations as f64;
        let speedup = standard_duration.as_secs_f64() / optimized_duration.as_secs_f64();

        println!(
            "| {:>8}% | {:>6.2}ms | {:>7.2}ms | {:>6.2}x |",
            (expired_ratio * 100.0) as u32,
            standard_ms,
            optimized_ms,
            speedup
        );
    }

    println!("\n=====================================\n");
}

/// Benchmark test to measure GET optimization for expired blocks
/// Compares GET on expired vs non-expired blocks
/// Run with: cargo test bench_get_expired_block --release -- --nocapture --ignored
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn bench_get_expired_block() {
    use crate::engine::current_timestamp;
    use crate::engine::lsm_tree::block_cache::BlockCache;
    use crate::engine::lsm_tree::memtable::MemValue;
    use std::collections::BTreeMap;
    use std::time::Instant;

    println!("\n=== GET Expired Block Optimization Benchmark ===");

    let dir = tempdir().unwrap();
    let now = current_timestamp();

    // Parameters
    let num_entries = 1000;
    let value_size = 1024;
    let iterations = 10000;

    println!("Parameters:");
    println!("  - Entries per SSTable: {}", num_entries);
    println!("  - Value size: {} bytes", value_size);
    println!("  - GET iterations: {}", iterations);

    let value = "x".repeat(value_size);

    // Create SSTable with expired entries
    let expired_sst_path = dir.path().join("expired.data");
    {
        let mut memtable: BTreeMap<String, MemValue> = BTreeMap::new();
        for i in 0..num_entries {
            memtable.insert(
                format!("expired_key{:06}", i),
                MemValue::new_with_ttl(Some(value.clone()), now - 100), // Expired
            );
        }
        sstable::create_from_memtable(&expired_sst_path, &memtable).unwrap();
    }

    // Create SSTable with non-expired entries
    let valid_sst_path = dir.path().join("valid.data");
    {
        let mut memtable: BTreeMap<String, MemValue> = BTreeMap::new();
        for i in 0..num_entries {
            memtable.insert(
                format!("valid_key{:06}", i),
                MemValue::new_with_ttl(Some(value.clone()), now + 3600), // Not expired
            );
        }
        sstable::create_from_memtable(&valid_sst_path, &memtable).unwrap();
    }

    // Create SSTable with no-TTL entries
    let no_ttl_sst_path = dir.path().join("no_ttl.data");
    {
        let mut memtable: BTreeMap<String, MemValue> = BTreeMap::new();
        for i in 0..num_entries {
            memtable.insert(
                format!("no_ttl_key{:06}", i),
                MemValue::new(Some(value.clone())), // No TTL
            );
        }
        sstable::create_from_memtable(&no_ttl_sst_path, &memtable).unwrap();
    }

    // Open SSTables
    let expired_sst = sstable::MappedSSTable::open(&expired_sst_path).unwrap();
    let valid_sst = sstable::MappedSSTable::open(&valid_sst_path).unwrap();
    let no_ttl_sst = sstable::MappedSSTable::open(&no_ttl_sst_path).unwrap();

    let cache = BlockCache::new(64 * 1024 * 1024);

    // Warm up caches (load index)
    let _ = sstable::search_key_mmap(&expired_sst, "expired_key000000", &cache, now);
    let _ = sstable::search_key_mmap(&valid_sst, "valid_key000000", &cache, now);
    let _ = sstable::search_key_mmap(&no_ttl_sst, "no_ttl_key000000", &cache, now);
    cache.reset_stats();

    println!("\n| Scenario        | Time     | Ops/sec    | Block Decompressions |");
    println!("|-----------------|----------|------------|----------------------|");

    // Benchmark: GET on expired block (should skip decompression)
    let start = Instant::now();
    for i in 0..iterations {
        let key = format!("expired_key{:06}", i % num_entries);
        let _ = sstable::search_key_mmap(&expired_sst, &key, &cache, now);
    }
    let expired_duration = start.elapsed();
    let expired_stats = cache.stats();
    let expired_block_loads = expired_stats.misses; // Block cache misses = decompressions
    cache.reset_stats();

    println!(
        "| Expired block   | {:>6.2}ms | {:>10.0} | {:>20} |",
        expired_duration.as_secs_f64() * 1000.0,
        iterations as f64 / expired_duration.as_secs_f64(),
        expired_block_loads
    );

    // Benchmark: GET on valid (non-expired) block
    let start = Instant::now();
    for i in 0..iterations {
        let key = format!("valid_key{:06}", i % num_entries);
        let _ = sstable::search_key_mmap(&valid_sst, &key, &cache, now);
    }
    let valid_duration = start.elapsed();
    let valid_stats = cache.stats();
    let valid_block_loads = valid_stats.misses;
    cache.reset_stats();

    println!(
        "| Valid block     | {:>6.2}ms | {:>10.0} | {:>20} |",
        valid_duration.as_secs_f64() * 1000.0,
        iterations as f64 / valid_duration.as_secs_f64(),
        valid_block_loads
    );

    // Benchmark: GET on no-TTL block
    let start = Instant::now();
    for i in 0..iterations {
        let key = format!("no_ttl_key{:06}", i % num_entries);
        let _ = sstable::search_key_mmap(&no_ttl_sst, &key, &cache, now);
    }
    let no_ttl_duration = start.elapsed();
    let no_ttl_stats = cache.stats();
    let no_ttl_block_loads = no_ttl_stats.misses;

    println!(
        "| No-TTL block    | {:>6.2}ms | {:>10.0} | {:>20} |",
        no_ttl_duration.as_secs_f64() * 1000.0,
        iterations as f64 / no_ttl_duration.as_secs_f64(),
        no_ttl_block_loads
    );

    let speedup = valid_duration.as_secs_f64() / expired_duration.as_secs_f64();
    println!("\n=== Results ===");
    println!(
        "Expired block GET is {:.2}x faster than valid block GET",
        speedup
    );
    println!("(Block decompression is skipped for expired blocks)");

    // Cold cache benchmark (no caching, each GET requires decompression)
    println!("\n--- Cold Cache Benchmark (fresh cache each iteration) ---");

    let cold_iterations = 100;

    // Expired block - cold cache
    let start = Instant::now();
    for i in 0..cold_iterations {
        let fresh_cache = BlockCache::new(64 * 1024 * 1024);
        let key = format!("expired_key{:06}", i % num_entries);
        let _ = sstable::search_key_mmap(&expired_sst, &key, &fresh_cache, now);
    }
    let expired_cold_duration = start.elapsed();

    // Valid block - cold cache
    let start = Instant::now();
    for i in 0..cold_iterations {
        let fresh_cache = BlockCache::new(64 * 1024 * 1024);
        let key = format!("valid_key{:06}", i % num_entries);
        let _ = sstable::search_key_mmap(&valid_sst, &key, &fresh_cache, now);
    }
    let valid_cold_duration = start.elapsed();

    println!(
        "Expired block (cold): {:.2}ms ({:.0} ops/sec)",
        expired_cold_duration.as_secs_f64() * 1000.0,
        cold_iterations as f64 / expired_cold_duration.as_secs_f64()
    );
    println!(
        "Valid block (cold):   {:.2}ms ({:.0} ops/sec)",
        valid_cold_duration.as_secs_f64() * 1000.0,
        cold_iterations as f64 / valid_cold_duration.as_secs_f64()
    );

    let cold_speedup = valid_cold_duration.as_secs_f64() / expired_cold_duration.as_secs_f64();
    println!(
        "Cold cache speedup: {:.2}x faster for expired blocks",
        cold_speedup
    );

    // Overhead measurement: compare no-TTL (max_expire_at=0) vs valid TTL (max_expire_at>0)
    // Both require full processing, so difference shows the overhead of the expiration check
    println!("\n--- Optimization Overhead (non-expired cases) ---");

    let overhead_iterations = 50000;

    // No-TTL block - baseline (max_expire_at = 0, check short-circuits immediately)
    let shared_cache = BlockCache::new(64 * 1024 * 1024);
    // Warm up
    let _ = sstable::search_key_mmap(&no_ttl_sst, "no_ttl_key000000", &shared_cache, now);

    let start = Instant::now();
    for i in 0..overhead_iterations {
        let key = format!("no_ttl_key{:06}", i % num_entries);
        let _ = sstable::search_key_mmap(&no_ttl_sst, &key, &shared_cache, now);
    }
    let no_ttl_time = start.elapsed();

    // Valid TTL block (max_expire_at > now, full check but not skipped)
    let shared_cache2 = BlockCache::new(64 * 1024 * 1024);
    // Warm up
    let _ = sstable::search_key_mmap(&valid_sst, "valid_key000000", &shared_cache2, now);

    let start = Instant::now();
    for i in 0..overhead_iterations {
        let key = format!("valid_key{:06}", i % num_entries);
        let _ = sstable::search_key_mmap(&valid_sst, &key, &shared_cache2, now);
    }
    let valid_time = start.elapsed();

    let no_ttl_ns_per_op = no_ttl_time.as_nanos() as f64 / overhead_iterations as f64;
    let valid_ns_per_op = valid_time.as_nanos() as f64 / overhead_iterations as f64;
    let overhead_ns = valid_ns_per_op - no_ttl_ns_per_op;
    let overhead_percent = (overhead_ns / no_ttl_ns_per_op) * 100.0;

    println!(
        "No-TTL (max_expire_at=0):     {:.0} ns/op ({:.0} ops/sec)",
        no_ttl_ns_per_op,
        overhead_iterations as f64 / no_ttl_time.as_secs_f64()
    );
    println!(
        "Valid TTL (max_expire_at>0):  {:.0} ns/op ({:.0} ops/sec)",
        valid_ns_per_op,
        overhead_iterations as f64 / valid_time.as_secs_f64()
    );
    println!(
        "Overhead: {:.0} ns/op ({:.2}%)",
        overhead_ns.max(0.0),
        overhead_percent.max(0.0)
    );
    println!("=====================================\n");
}

/// Benchmark to compare scan_prefix_mmap vs scan_prefix_keys_mmap
/// Run with: cargo test bench_scan_prefix --release -- --nocapture --ignored
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn bench_scan_prefix_comparison() {
    use super::block_cache::BlockCache;
    use super::memtable::MemValue;
    use std::collections::BTreeMap;
    use std::time::Instant;

    println!("\n=== SSTable Scan Prefix Comparison ===");
    println!(
        "Comparing scan_prefix_mmap (returns values) vs scan_prefix_keys_mmap (returns keys only)"
    );
    println!("Both use block cache now\n");

    let value_sizes = [100, 1000, 10000];
    let num_keys = 1000;
    let iterations = 100;
    let now = 0u64; // No expiration

    for value_size in value_sizes {
        println!("--- Value size: {} bytes ---", value_size);

        let dir = tempdir().unwrap();
        let sst_path = dir.path().join("test.data");

        // Create SSTable with test data
        let mut memtable: BTreeMap<String, MemValue> = BTreeMap::new();
        let value = "x".repeat(value_size);

        for i in 0..num_keys {
            let key = format!("log:{:05}", i);
            memtable.insert(key, MemValue::new(Some(value.clone())));
        }

        sstable::create_from_memtable(&sst_path, &memtable).unwrap();

        // Open SSTable
        let sst = sstable::MappedSSTable::open(&sst_path).unwrap();

        // === Cold Cache Comparison ===
        println!("  Cold cache (fresh cache each iteration):");

        // scan_prefix_mmap cold
        let start = Instant::now();
        for _ in 0..iterations {
            let cache = BlockCache::new(64 * 1024 * 1024);
            let _ = sstable::scan_prefix_mmap(&sst, "log:", &cache, now).unwrap();
        }
        let cold_with_values = start.elapsed();

        // scan_prefix_keys_mmap cold
        let start = Instant::now();
        for _ in 0..iterations {
            let cache = BlockCache::new(64 * 1024 * 1024);
            let _ = sstable::scan_prefix_keys_mmap(&sst, "log:", &cache, now).unwrap();
        }
        let cold_keys_only = start.elapsed();

        println!(
            "    with values: {:.3}ms ({:.0} ops/sec)",
            cold_with_values.as_secs_f64() * 1000.0,
            iterations as f64 / cold_with_values.as_secs_f64()
        );
        println!(
            "    keys only:   {:.3}ms ({:.0} ops/sec)",
            cold_keys_only.as_secs_f64() * 1000.0,
            iterations as f64 / cold_keys_only.as_secs_f64()
        );

        // === Warm Cache Comparison ===
        println!("  Warm cache (shared cache across iterations):");

        let cache = BlockCache::new(64 * 1024 * 1024);

        // Warm up
        let _ = sstable::scan_prefix_mmap(&sst, "log:", &cache, now).unwrap();

        // scan_prefix_mmap warm
        let start = Instant::now();
        for _ in 0..iterations {
            let _ = sstable::scan_prefix_mmap(&sst, "log:", &cache, now).unwrap();
        }
        let warm_with_values = start.elapsed();

        // scan_prefix_keys_mmap warm (same cache, already warm)
        let start = Instant::now();
        for _ in 0..iterations {
            let _ = sstable::scan_prefix_keys_mmap(&sst, "log:", &cache, now).unwrap();
        }
        let warm_keys_only = start.elapsed();

        println!(
            "    with values: {:.3}ms ({:.0} ops/sec)",
            warm_with_values.as_secs_f64() * 1000.0,
            iterations as f64 / warm_with_values.as_secs_f64()
        );
        println!(
            "    keys only:   {:.3}ms ({:.0} ops/sec)",
            warm_keys_only.as_secs_f64() * 1000.0,
            iterations as f64 / warm_keys_only.as_secs_f64()
        );
        println!();
    }

    println!("=====================================\n");
}
