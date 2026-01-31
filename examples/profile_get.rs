//! GET operation profiling benchmark
//!
//! Run with: cargo run --release --features chrome --example profile_get

use orekvs::engine::Engine;
use orekvs::engine::lsm_tree::LsmTreeEngine;
use std::sync::Arc;
use tempfile::TempDir;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

#[tokio::main]
async fn main() {
    // Setup tracing with chrome output
    let (chrome_layer, _guard) = tracing_chrome::ChromeLayerBuilder::new()
        .file("./profile_get_trace.json")
        .include_args(true)
        .build();

    tracing_subscriber::registry().with(chrome_layer).init();

    // Create temp directory for test data
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let data_dir = temp_dir.path().to_string_lossy().to_string();

    println!("Profiling LSM-Tree GET operations...");
    println!("Data directory: {:?}", data_dir);

    // Create LSM-Tree engine with small memtable to force flushes
    let engine: Arc<dyn Engine> = Arc::new(LsmTreeEngine::new(
        data_dir.clone(),
        256 * 1024, // 256KB memtable (small to force frequent flushes)
        4,          // compaction trigger file count
    ));

    // Phase 1: Insert test data with large values to force SSTable flushes
    println!("\n=== Phase 1: Inserting test data (forcing SSTable flushes) ===");
    let num_keys = 5000;
    let large_value = "x".repeat(200); // 200 byte values to fill memtable faster
    for i in 0..num_keys {
        let key = format!("key_{:08}", i);
        let value = format!("{}_{}", large_value, i);
        engine.set(key, value).expect("Failed to set");
    }
    println!("Inserted {} keys with ~200B values", num_keys);

    // Wait a bit for flushes to complete
    std::thread::sleep(std::time::Duration::from_millis(500));

    // Phase 2: Insert more recent data that stays in memtable
    println!("\n=== Phase 2: Inserting recent data (stays in memtable) ===");
    let recent_start = num_keys;
    for i in recent_start..(recent_start + 500) {
        let key = format!("recent_{:08}", i);
        let value = format!("value_{}", i);
        engine.set(key, value).expect("Failed to set");
    }
    println!("Inserted 500 recent keys");

    // Phase 3: Profile GET operations from MemTable (recent keys)
    println!("\n=== Phase 3: Profiling GET from MemTable ===");
    {
        let _span = tracing::info_span!("memtable_gets").entered();
        for i in recent_start..(recent_start + 500) {
            let key = format!("recent_{:08}", i);
            let _ = engine.get(key);
        }
    }
    println!("Completed 500 MemTable GETs");

    // Phase 4: Profile GET operations from SSTable (older keys)
    println!("\n=== Phase 4: Profiling GET from SSTable ===");
    {
        let _span = tracing::info_span!("sstable_gets").entered();
        for i in 0..1000 {
            let key = format!("key_{:08}", i);
            let _ = engine.get(key);
        }
    }
    println!("Completed 1000 SSTable GETs");

    // Phase 5: Profile GET operations for non-existent keys (bloom filter test)
    println!("\n=== Phase 5: Profiling GET for non-existent keys ===");
    {
        let _span = tracing::info_span!("nonexistent_gets").entered();
        for i in 0..1000 {
            let key = format!("nonexistent_{:08}", i);
            let _ = engine.get(key);
        }
    }
    println!("Completed 1000 non-existent key GETs");

    // Phase 6: Random access pattern (mix of memtable and sstable)
    println!("\n=== Phase 6: Random access pattern ===");
    {
        let _span = tracing::info_span!("random_gets").entered();
        use rand::Rng;
        let mut rng = rand::thread_rng();
        for _ in 0..1000 {
            let i = rng.gen_range(0..num_keys);
            let key = format!("key_{:08}", i);
            let _ = engine.get(key);
        }
    }
    println!("Completed 1000 random GETs");

    println!("\n=== Profiling complete ===");
    println!("Trace file written to: ./profile_get_trace.json");
    println!("Open in Chrome: chrome://tracing");
}
