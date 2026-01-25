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
    let start = Instant::now();
    for _ in 0..iterations {
        for handle in &sst_handles {
            let _ = sstable::search_key_mmap(&handle.mmap, test_key, &engine.block_cache);
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
