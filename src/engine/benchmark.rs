//! Cross-engine benchmark tests.
//!
//! Run with: cargo test bench_ --release -- --nocapture --ignored

use tempfile::tempdir;

/// Benchmark test to measure count performance across all engines
/// Run with: cargo test bench_count --release -- --nocapture --ignored
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn bench_count_all_engines() {
    use super::Engine;
    use super::btree::BTreeEngine;
    use super::log::LogEngine;
    use super::lsm_tree::LsmTreeEngine;
    use super::memory::MemoryEngine;
    use std::sync::Arc;
    use std::time::Instant;

    println!("\n=== Count Performance Benchmark ===");

    let num_keys = 10000;
    let iterations = 100;

    // Different prefix patterns to test
    // Keys are: log:00000, log:00001, ... log:01999 (2000 each prefix)
    let prefixes = [
        ("log:", 2000),      // ~20% match
        ("log:001", 100),    // ~1% match
        ("log:0010", 10),    // 0.1% match
        ("nonexistent:", 0), // 0% match
    ];

    println!("Parameters:");
    println!("  - Total keys: {}", num_keys);
    println!("  - Iterations per test: {}", iterations);
    println!();

    // Helper function to create test data (sorted by prefix for consistent results)
    fn populate_data(engine: &dyn Engine, num_keys: usize) {
        let prefix_groups = ["log:", "order:", "product:", "session:", "user:"];
        let keys_per_prefix = num_keys / prefix_groups.len();
        for (prefix_idx, prefix) in prefix_groups.iter().enumerate() {
            for i in 0..keys_per_prefix {
                let key = format!("{}{:05}", prefix, i);
                engine
                    .set(key, format!("value{:05}", prefix_idx * keys_per_prefix + i))
                    .unwrap();
            }
        }
    }

    println!("| Engine   | Prefix       | Expected | Actual | Time (ms) | ops/sec   |");
    println!("|----------|--------------|----------|--------|-----------|-----------|");

    // 1. Memory Engine
    {
        let engine = MemoryEngine::new();
        populate_data(&engine, num_keys);

        for (prefix, expected) in &prefixes {
            let start = Instant::now();
            let mut actual = 0u64;
            for _ in 0..iterations {
                actual = engine.count(prefix).unwrap();
            }
            let duration = start.elapsed();
            let ops_per_sec = iterations as f64 / duration.as_secs_f64();
            println!(
                "| Memory   | {:12} | {:>8} | {:>6} | {:>9.3} | {:>9.0} |",
                prefix,
                expected,
                actual,
                duration.as_secs_f64() * 1000.0,
                ops_per_sec
            );
        }
    }

    println!("|----------|--------------|----------|--------|-----------|-----------|");

    // 2. Log Engine
    {
        let dir = tempdir().unwrap();
        let data_dir = dir.path().to_str().unwrap().to_string();
        let engine = LogEngine::new(data_dir, 100 * 1024 * 1024);
        populate_data(&engine, num_keys);

        for (prefix, expected) in &prefixes {
            let start = Instant::now();
            let mut actual = 0u64;
            for _ in 0..iterations {
                actual = engine.count(prefix).unwrap();
            }
            let duration = start.elapsed();
            let ops_per_sec = iterations as f64 / duration.as_secs_f64();
            println!(
                "| Log      | {:12} | {:>8} | {:>6} | {:>9.3} | {:>9.0} |",
                prefix,
                expected,
                actual,
                duration.as_secs_f64() * 1000.0,
                ops_per_sec
            );
        }
    }

    println!("|----------|--------------|----------|--------|-----------|-----------|");

    // 3. BTree Engine
    {
        let dir = tempdir().unwrap();
        let data_dir = dir.path().to_str().unwrap().to_string();
        let engine = Arc::new(BTreeEngine::open(data_dir).unwrap());
        populate_data(&*engine, num_keys);

        // Wait for writes to settle
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

        for (prefix, expected) in &prefixes {
            let start = Instant::now();
            let mut actual = 0u64;
            for _ in 0..iterations {
                actual = engine.count(prefix).unwrap();
            }
            let duration = start.elapsed();
            let ops_per_sec = iterations as f64 / duration.as_secs_f64();
            println!(
                "| BTree    | {:12} | {:>8} | {:>6} | {:>9.3} | {:>9.0} |",
                prefix,
                expected,
                actual,
                duration.as_secs_f64() * 1000.0,
                ops_per_sec
            );
        }
    }

    println!("|----------|--------------|----------|--------|-----------|-----------|");

    // 4. LSM-tree Engine
    {
        let dir = tempdir().unwrap();
        let data_dir = dir.path().to_str().unwrap().to_string();
        let engine = LsmTreeEngine::new(data_dir, 1024 * 1024, 100);
        populate_data(&engine, num_keys);

        // Wait for flushes to complete
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

        for (prefix, expected) in &prefixes {
            let start = Instant::now();
            let mut actual = 0u64;
            for _ in 0..iterations {
                actual = engine.count(prefix).unwrap();
            }
            let duration = start.elapsed();
            let ops_per_sec = iterations as f64 / duration.as_secs_f64();
            println!(
                "| LSM-tree | {:12} | {:>8} | {:>6} | {:>9.3} | {:>9.0} |",
                prefix,
                expected,
                actual,
                duration.as_secs_f64() * 1000.0,
                ops_per_sec
            );
        }

        engine.shutdown().await;
    }

    println!("=====================================\n");
}
