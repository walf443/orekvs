//! Benchmark tests for BTree vs LSM-tree comparison.
//!
//! Run with: cargo test bench_ --release -- --nocapture --ignored

use super::*;
use crate::engine::Engine;
use crate::engine::lsm_tree::LsmTreeEngine;
use std::sync::Arc;
use std::thread;
use std::time::Instant;
use tempfile::tempdir;

/// Benchmark: Single-threaded write performance comparison
/// Run with: cargo test bench_single_thread_write --release -- --nocapture --ignored
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn bench_single_thread_write() {
    let num_keys = 10000;

    println!("\n=== Single-Threaded Write Benchmark ===");
    println!("Keys: {}", num_keys);

    // BTree benchmark
    {
        let dir = tempdir().unwrap();
        let engine = BTreeEngine::open(dir.path()).unwrap();

        let start = Instant::now();
        for i in 0..num_keys {
            engine
                .set(format!("key{:06}", i), format!("value{:06}", i))
                .unwrap();
        }
        let duration = start.elapsed();

        println!(
            "BTree:    {:?} ({:.2} keys/sec)",
            duration,
            num_keys as f64 / duration.as_secs_f64()
        );
    }

    // LSM-tree benchmark
    {
        let dir = tempdir().unwrap();
        let engine = LsmTreeEngine::new(
            dir.path().to_str().unwrap().to_string(),
            4 * 1024 * 1024, // 4MB memtable
            100,             // batch interval
        );

        let start = Instant::now();
        for i in 0..num_keys {
            engine
                .set(format!("key{:06}", i), format!("value{:06}", i))
                .unwrap();
        }
        let duration = start.elapsed();

        println!(
            "LSM-tree: {:?} ({:.2} keys/sec)",
            duration,
            num_keys as f64 / duration.as_secs_f64()
        );

        engine.shutdown().await;
    }

    println!("=========================================\n");
}

/// Benchmark: Multi-threaded write performance comparison
/// Run with: cargo test bench_multi_thread_write --release -- --nocapture --ignored
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn bench_multi_thread_write() {
    let num_keys = 10000;
    let thread_counts = [1, 2, 4, 8, 16];

    println!("\n=== Multi-Threaded Write Benchmark ===");
    println!("Keys per thread: {}", num_keys);
    println!();

    for &num_threads in &thread_counts {
        // BTree benchmark
        let btree_throughput = {
            let dir = tempdir().unwrap();
            let engine = Arc::new(BTreeEngine::open(dir.path()).unwrap());

            let start = Instant::now();
            let handles: Vec<_> = (0..num_threads)
                .map(|t| {
                    let engine = Arc::clone(&engine);
                    thread::spawn(move || {
                        for i in 0..num_keys {
                            engine
                                .set(
                                    format!("key{:02}_{:06}", t, i),
                                    format!("value{:02}_{:06}", t, i),
                                )
                                .unwrap();
                        }
                    })
                })
                .collect();

            for h in handles {
                h.join().unwrap();
            }
            let duration = start.elapsed();
            (num_keys * num_threads) as f64 / duration.as_secs_f64()
        };

        // LSM-tree benchmark
        let lsm_throughput = {
            let dir = tempdir().unwrap();
            let engine = Arc::new(LsmTreeEngine::new(
                dir.path().to_str().unwrap().to_string(),
                4 * 1024 * 1024,
                100,
            ));

            let start = Instant::now();
            let handles: Vec<_> = (0..num_threads)
                .map(|t| {
                    let engine = Arc::clone(&engine);
                    tokio::task::spawn_blocking(move || {
                        for i in 0..num_keys {
                            engine
                                .set(
                                    format!("key{:02}_{:06}", t, i),
                                    format!("value{:02}_{:06}", t, i),
                                )
                                .unwrap();
                        }
                    })
                })
                .collect();

            for h in handles {
                h.await.unwrap();
            }
            let duration = start.elapsed();
            let throughput = (num_keys * num_threads) as f64 / duration.as_secs_f64();

            engine.shutdown().await;
            throughput
        };

        println!(
            "{:2} threads: BTree {:>10.0}/sec  LSM-tree {:>10.0}/sec  (LSM/BTree: {:.2}x)",
            num_threads,
            btree_throughput,
            lsm_throughput,
            lsm_throughput / btree_throughput
        );
    }

    println!("=========================================\n");
}

/// Benchmark: Read performance comparison
/// Run with: cargo test bench_read_performance --release -- --nocapture --ignored
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn bench_read_performance() {
    let num_keys = 10000;
    let read_iterations = 100000;

    println!("\n=== Read Performance Benchmark ===");
    println!("Keys: {}, Read iterations: {}", num_keys, read_iterations);

    // BTree benchmark
    {
        let dir = tempdir().unwrap();
        let engine = BTreeEngine::open(dir.path()).unwrap();

        // Write data
        for i in 0..num_keys {
            engine
                .set(format!("key{:06}", i), format!("value{:06}", i))
                .unwrap();
        }

        // Read benchmark
        let start = Instant::now();
        for i in 0..read_iterations {
            let key = format!("key{:06}", i % num_keys);
            let _ = engine.get(key);
        }
        let duration = start.elapsed();

        println!(
            "BTree:    {:?} ({:.2} reads/sec)",
            duration,
            read_iterations as f64 / duration.as_secs_f64()
        );
    }

    // LSM-tree benchmark
    {
        let dir = tempdir().unwrap();
        let engine = LsmTreeEngine::new(
            dir.path().to_str().unwrap().to_string(),
            4 * 1024 * 1024,
            100,
        );

        // Write data
        for i in 0..num_keys {
            engine
                .set(format!("key{:06}", i), format!("value{:06}", i))
                .unwrap();
        }

        // Wait for flush
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

        // Read benchmark
        let start = Instant::now();
        for i in 0..read_iterations {
            let key = format!("key{:06}", i % num_keys);
            let _ = engine.get(key);
        }
        let duration = start.elapsed();

        println!(
            "LSM-tree: {:?} ({:.2} reads/sec)",
            duration,
            read_iterations as f64 / duration.as_secs_f64()
        );

        engine.shutdown().await;
    }

    println!("=====================================\n");
}
