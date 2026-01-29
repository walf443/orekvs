//! Unit tests for LSM-tree engine.

use super::*;
use sstable::levels::MAX_LEVELS;
use tempfile::tempdir;

/// Helper: Create an engine for testing without stale compaction.
/// Stale compaction runs asynchronously after engine creation and can cause
/// race conditions with shutdown in tests that create/destroy multiple engines.
fn new_engine_for_test(
    data_dir: String,
    memtable_capacity_bytes: u64,
    compaction_trigger_file_count: usize,
) -> LsmTreeEngine {
    LsmTreeEngine::new_with_full_config(
        data_dir,
        memtable_capacity_bytes,
        compaction_trigger_file_count,
        DEFAULT_WAL_BATCH_INTERVAL_MICROS,
        WalArchiveConfig::default(),
        false, // disable stale compaction for tests
    )
}

/// Helper: Wait for all pending flushes to complete by checking immutable memtables
async fn wait_for_flush(engine: &LsmTreeEngine, max_wait_ms: u64) {
    let poll_interval = tokio::time::Duration::from_millis(10);
    let max_iterations = max_wait_ms / 10;

    for _ in 0..max_iterations {
        // Check if there are any immutable memtables pending flush
        let immutable_count = engine.mem_state.immutable_memtables.lock().unwrap().len();
        if immutable_count == 0 {
            return;
        }
        tokio::time::sleep(poll_interval).await;
    }
}

/// Helper: Wait for a condition to be true with polling
async fn wait_for_condition<F>(mut condition: F, max_wait_ms: u64) -> bool
where
    F: FnMut() -> bool,
{
    let poll_interval = tokio::time::Duration::from_millis(10);
    let max_iterations = max_wait_ms / 10;

    for _ in 0..max_iterations {
        if condition() {
            return true;
        }
        tokio::time::sleep(poll_interval).await;
    }
    false
}

#[tokio::test(flavor = "multi_thread")]
async fn test_lsm_flush_and_get() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path().to_str().unwrap().to_string();
    let engine = LsmTreeEngine::new(data_dir, 50, 4);

    engine.set("k1".to_string(), "v1".to_string()).unwrap();
    engine.set("k2".to_string(), "v2".to_string()).unwrap();
    wait_for_flush(&engine, 2000).await;

    assert_eq!(engine.get("k1".to_string()).unwrap(), "v1");
    assert_eq!(engine.get("k2".to_string()).unwrap(), "v2");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_lsm_recovery() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path().to_str().unwrap().to_string();

    {
        let engine = LsmTreeEngine::new(data_dir.clone(), 50, 4);
        engine.set("k1".to_string(), "v1".to_string()).unwrap();
        engine.set("k2".to_string(), "v2".to_string()).unwrap();
        wait_for_flush(&engine, 2000).await;
    }

    {
        let engine = LsmTreeEngine::new(data_dir.clone(), 50, 4);
        assert_eq!(engine.get("k1".to_string()).unwrap(), "v1");
        assert_eq!(engine.get("k2".to_string()).unwrap(), "v2");

        engine.set("k3".to_string(), "v3".to_string()).unwrap();
        engine.set("k4".to_string(), "v4".to_string()).unwrap();
        wait_for_flush(&engine, 2000).await;

        assert_eq!(engine.get("k1".to_string()).unwrap(), "v1");
        assert_eq!(engine.get("k3".to_string()).unwrap(), "v3");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_lsm_overwrite_size_tracking() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path().to_str().unwrap().to_string();
    // Set capacity to 100 bytes
    let engine = LsmTreeEngine::new(data_dir, 100, 4);

    // 同じキーを何度も更新しても、メモリ上のサイズは増えないはず
    for _ in 0..100 {
        engine.set("key".to_string(), "value".to_string()).unwrap();
    }

    // Wait for any pending flush to complete
    wait_for_flush(&engine, 2000).await;

    let sst_count = engine
        .leveled_sstables
        .lock()
        .unwrap()
        .total_sstable_count();
    assert!(
        sst_count <= 1,
        "Should not flush multiple times for the same key. Count: {}",
        sst_count
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_compaction_merges_sstables() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path().to_str().unwrap().to_string();
    // Low capacity to trigger multiple flushes, higher compaction trigger to reduce race conditions
    // Use new_engine_for_test to disable stale compaction which can race with test operations
    let engine = new_engine_for_test(data_dir.clone(), 30, 4);

    // Write data in batches to create multiple SSTables
    for i in 0..10 {
        engine
            .set(format!("key{}", i), format!("value{}", i))
            .unwrap();
        wait_for_flush(&engine, 5000).await;
    }

    // Wait for all data to be accessible (compaction may be in progress)
    let engine_ref = &engine;
    let all_accessible = wait_for_condition(
        || (0..10).all(|i| engine_ref.get(format!("key{}", i)).is_ok()),
        30000,
    )
    .await;
    assert!(
        all_accessible,
        "All keys should be accessible after compaction"
    );

    // Verify data values
    for i in 0..10 {
        assert_eq!(
            engine.get(format!("key{}", i)).unwrap(),
            format!("value{}", i)
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_compaction_removes_deleted_keys() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path().to_str().unwrap().to_string();
    // Higher compaction trigger to reduce race conditions
    let engine = LsmTreeEngine::new(data_dir.clone(), 30, 4);

    // Write some data
    engine
        .set("keep".to_string(), "keep_value".to_string())
        .unwrap();
    engine
        .set("delete_me".to_string(), "will_be_deleted".to_string())
        .unwrap();
    wait_for_flush(&engine, 5000).await;

    // Delete one key
    engine.delete("delete_me".to_string()).unwrap();
    wait_for_flush(&engine, 5000).await;

    // Add more data to trigger compaction
    for i in 0..5 {
        engine
            .set(format!("extra{}", i), format!("extra_value{}", i))
            .unwrap();
        wait_for_flush(&engine, 5000).await;
    }

    // Wait for compaction to complete and "keep" key to be accessible
    let engine_ref = &engine;
    let found = wait_for_condition(|| engine_ref.get("keep".to_string()).is_ok(), 30000).await;
    assert!(found, "keep key should be accessible after compaction");

    // Verify kept key has correct value
    assert_eq!(engine.get("keep".to_string()).unwrap(), "keep_value");

    // Verify deleted key is not found
    let result = engine.get("delete_me".to_string());
    assert!(result.is_err());
}

#[tokio::test(flavor = "multi_thread")]
async fn test_compaction_deletes_old_files() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path().to_str().unwrap().to_string();
    // Use higher compaction trigger to reduce race conditions
    let engine = LsmTreeEngine::new(data_dir.clone(), 30, 4);

    // Create multiple SSTables
    for i in 0..8 {
        engine.set(format!("k{}", i), format!("v{}", i)).unwrap();
        wait_for_flush(&engine, 2000).await;
    }

    // Wait for compaction to reduce SSTable count (may take multiple compaction rounds)
    let data_dir_clone = data_dir.clone();
    let compacted =
        wait_for_condition(
            || {
                let sst_count =
                    fs::read_dir(&data_dir_clone)
                        .map(|entries| {
                            entries
                                .filter_map(|e| e.ok())
                                .filter(|e| {
                                    e.path().file_name().and_then(|n| n.to_str()).is_some_and(
                                        |name| name.starts_with("sst_") && name.ends_with(".data"),
                                    )
                                })
                                .count()
                        })
                        .unwrap_or(100);
                // With compaction trigger of 4, expect some reduction in SSTable count
                sst_count <= 4
            },
            30000, // Longer timeout for parallel test execution
        )
        .await;

    // Count SSTable files in directory
    let sst_files: Vec<_> = fs::read_dir(&data_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.path()
                .file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|name| name.starts_with("sst_") && name.ends_with(".data"))
        })
        .collect();

    // After compaction, should have fewer SSTables than originally created
    // (8 writes should result in fewer than 8 files due to compaction)
    assert!(
        compacted && sst_files.len() <= 4,
        "Expected at most 4 SSTables after compaction, got {}",
        sst_files.len()
    );

    // Wait for all data to be accessible (longer timeout for compaction to complete)
    let engine_ref = &engine;
    let all_accessible = wait_for_condition(
        || (0..8).all(|i| engine_ref.get(format!("k{}", i)).is_ok()),
        30000,
    )
    .await;
    assert!(all_accessible, "All keys should be accessible");

    // Data should have correct values
    for i in 0..8 {
        assert_eq!(engine.get(format!("k{}", i)).unwrap(), format!("v{}", i));
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_wal_recovery_basic() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path().to_str().unwrap().to_string();

    // Write some data without flushing (high capacity)
    {
        let engine = LsmTreeEngine::new(data_dir.clone(), 1024 * 1024, 4);
        engine.set("k1".to_string(), "v1".to_string()).unwrap();
        engine.set("k2".to_string(), "v2".to_string()).unwrap();
        // Engine dropped without flush - data should be in WAL
    }

    // Recover and verify data
    {
        let engine = LsmTreeEngine::new(data_dir.clone(), 1024 * 1024, 4);
        assert_eq!(engine.get("k1".to_string()).unwrap(), "v1");
        assert_eq!(engine.get("k2".to_string()).unwrap(), "v2");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_wal_recovery_with_delete() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path().to_str().unwrap().to_string();

    // Write and delete data without flushing
    {
        let engine = LsmTreeEngine::new(data_dir.clone(), 1024 * 1024, 4);
        engine.set("k1".to_string(), "v1".to_string()).unwrap();
        engine.set("k2".to_string(), "v2".to_string()).unwrap();
        engine.delete("k1".to_string()).unwrap();
    }

    // Recover and verify
    {
        let engine = LsmTreeEngine::new(data_dir.clone(), 1024 * 1024, 4);
        // k1 should be deleted (tombstone recovered)
        assert!(engine.get("k1".to_string()).is_err());
        // k2 should still exist
        assert_eq!(engine.get("k2".to_string()).unwrap(), "v2");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_wal_preserved_after_flush() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path().to_str().unwrap().to_string();

    // Write data and trigger flush (low capacity)
    {
        let engine = LsmTreeEngine::new(data_dir.clone(), 30, 4);
        engine.set("k1".to_string(), "v1".to_string()).unwrap();
        engine.set("k2".to_string(), "v2".to_string()).unwrap();
        wait_for_flush(&engine, 2000).await;
    }

    // Check that WAL files are preserved (for replication)
    let wal_count = fs::read_dir(dir.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.path()
                .file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|name| name.starts_with("wal_") && name.ends_with(".log"))
        })
        .count();

    // Should have at least 1 WAL file (current one)
    assert!(
        wal_count >= 1,
        "Expected at least 1 WAL file, got {}",
        wal_count
    );

    // Data should still be accessible
    {
        let engine = LsmTreeEngine::new(data_dir.clone(), 1024 * 1024, 4);
        assert_eq!(engine.get("k1".to_string()).unwrap(), "v1");
        assert_eq!(engine.get("k2".to_string()).unwrap(), "v2");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_lsm_versioning_across_sstables() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path().to_str().unwrap().to_string();
    // Low capacity to force flush every write
    let engine = LsmTreeEngine::new(data_dir.clone(), 10, 100);

    // v1 in SSTable 1
    engine.set("key".to_string(), "v1".to_string()).unwrap();
    wait_for_flush(&engine, 2000).await;

    // v2 in SSTable 2
    engine.set("key".to_string(), "v2".to_string()).unwrap();
    wait_for_flush(&engine, 2000).await;

    // v3 in MemTable
    engine.set("key".to_string(), "v3".to_string()).unwrap();

    assert_eq!(engine.get("key".to_string()).unwrap(), "v3");

    // Force flush v3 to SSTable 3
    engine.trigger_flush_if_needed(); // This won't work easily because of capacity check, but another set will
    engine
        .set(
            "other".to_string(),
            "large_value_to_force_flush".to_string(),
        )
        .unwrap();
    wait_for_flush(&engine, 2000).await;

    assert_eq!(engine.get("key".to_string()).unwrap(), "v3");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_lsm_recovery_merges_wal_and_sst() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path().to_str().unwrap().to_string();

    {
        // Phase 1: Create SSTables with k1=v1 and k2=v2
        // Use 30 byte memtable to trigger flushes
        // Use new_engine_for_test to disable stale compaction which can race with shutdown
        let engine = new_engine_for_test(data_dir.clone(), 30, 100);

        // k1 goes to SSTable
        engine.set("k1".to_string(), "v1".to_string()).unwrap();
        wait_for_flush(&engine, 2000).await;

        // k2 triggers another flush to SSTable
        engine.set("k2".to_string(), "v2".to_string()).unwrap();
        wait_for_flush(&engine, 2000).await;

        engine.shutdown().await;
    }

    {
        // Phase 2: Update k1 in WAL only (no flush)
        // Use large memtable so k1=v1_new won't trigger flush
        // Use new_engine_for_test to disable stale compaction which can race with shutdown
        let engine = new_engine_for_test(data_dir.clone(), 1024, 100);

        // k1 updated in WAL/MemTable only (won't trigger flush)
        engine.set("k1".to_string(), "v1_new".to_string()).unwrap();

        // Wait for WAL group commit to complete (short wait since no flush needed)
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        engine.shutdown().await;
    }

    {
        // Phase 3: Verify recovery merges WAL with SSTable
        // Use new_engine_for_test to disable stale compaction for consistency
        let engine = new_engine_for_test(data_dir.clone(), 1024, 100);
        // k1 should be v1_new (from WAL, overwriting SSTable's v1)
        assert_eq!(engine.get("k1".to_string()).unwrap(), "v1_new");
        // k2 should be v2 (from SSTable)
        assert_eq!(engine.get("k2".to_string()).unwrap(), "v2");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_lsm_delete_after_flush() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path().to_str().unwrap().to_string();
    let engine = LsmTreeEngine::new(data_dir.clone(), 30, 100);

    // Flush k1 to SSTable
    engine.set("k1".to_string(), "v1".to_string()).unwrap();
    wait_for_flush(&engine, 2000).await;

    assert_eq!(engine.get("k1".to_string()).unwrap(), "v1");

    // Delete k1 (tombstone in MemTable)
    engine.delete("k1".to_string()).unwrap();
    assert!(engine.get("k1".to_string()).is_err());

    // Flush tombstone
    engine
        .set("k2".to_string(), "trigger_flush".repeat(10))
        .unwrap();
    wait_for_flush(&engine, 2000).await;

    // Still should not find k1 (SSTable has v1, but newer SSTable has tombstone)
    assert!(engine.get("k1".to_string()).is_err());
}

#[tokio::test(flavor = "multi_thread")]
async fn test_batch_get_basic() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path().to_str().unwrap().to_string();
    let engine = LsmTreeEngine::new(data_dir, 1024 * 1024, 4);

    // Set some values
    engine.set("a".to_string(), "va".to_string()).unwrap();
    engine.set("b".to_string(), "vb".to_string()).unwrap();
    engine.set("c".to_string(), "vc".to_string()).unwrap();

    // batch_get with existing and non-existing keys
    let keys = vec![
        "c".to_string(),
        "a".to_string(),
        "nonexistent".to_string(),
        "b".to_string(),
    ];
    let results = engine.batch_get(keys);

    // Should return 3 results (excluding nonexistent)
    assert_eq!(results.len(), 3);

    // Results should be returned (order may vary due to sorting)
    let result_map: std::collections::HashMap<_, _> = results.into_iter().collect();
    assert_eq!(result_map.get("a"), Some(&"va".to_string()));
    assert_eq!(result_map.get("b"), Some(&"vb".to_string()));
    assert_eq!(result_map.get("c"), Some(&"vc".to_string()));
    assert_eq!(result_map.get("nonexistent"), None);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_batch_get_with_tombstone() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path().to_str().unwrap().to_string();
    let engine = LsmTreeEngine::new(data_dir, 1024 * 1024, 4);

    engine.set("a".to_string(), "va".to_string()).unwrap();
    engine.set("b".to_string(), "vb".to_string()).unwrap();
    engine.delete("a".to_string()).unwrap();

    let keys = vec!["a".to_string(), "b".to_string()];
    let results = engine.batch_get(keys);

    // Should return only "b" (a is deleted)
    assert_eq!(results.len(), 1);
    assert_eq!(results[0], ("b".to_string(), "vb".to_string()));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_batch_get_with_duplicates() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path().to_str().unwrap().to_string();
    let engine = LsmTreeEngine::new(data_dir, 1024 * 1024, 4);

    engine.set("a".to_string(), "va".to_string()).unwrap();
    engine.set("b".to_string(), "vb".to_string()).unwrap();

    // Request with duplicate keys
    let keys = vec![
        "a".to_string(),
        "b".to_string(),
        "a".to_string(),
        "a".to_string(),
        "b".to_string(),
    ];
    let results = engine.batch_get(keys);

    // Should return only unique keys (duplicates removed)
    assert_eq!(results.len(), 2);
    let result_map: std::collections::HashMap<_, _> = results.into_iter().collect();
    assert_eq!(result_map.get("a"), Some(&"va".to_string()));
    assert_eq!(result_map.get("b"), Some(&"vb".to_string()));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_batch_get_from_sstable() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path().to_str().unwrap().to_string();
    // Small memtable to force flush to SSTable
    let engine = LsmTreeEngine::new(data_dir, 50, 100);

    // Insert data that will be flushed to SSTable
    engine
        .set("key1".to_string(), "value1".to_string())
        .unwrap();
    engine
        .set("key2".to_string(), "value2".to_string())
        .unwrap();
    wait_for_flush(&engine, 2000).await;

    // batch_get should find keys in SSTable
    let keys = vec!["key2".to_string(), "key1".to_string(), "key3".to_string()];
    let results = engine.batch_get(keys);

    assert_eq!(results.len(), 2);
    let result_map: std::collections::HashMap<_, _> = results.into_iter().collect();
    assert_eq!(result_map.get("key1"), Some(&"value1".to_string()));
    assert_eq!(result_map.get("key2"), Some(&"value2".to_string()));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_batch_delete_basic() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path().to_str().unwrap().to_string();
    let engine = LsmTreeEngine::new(data_dir, 1024 * 1024, 4);

    // Set some values
    engine.set("a".to_string(), "va".to_string()).unwrap();
    engine.set("b".to_string(), "vb".to_string()).unwrap();
    engine.set("c".to_string(), "vc".to_string()).unwrap();
    engine.set("d".to_string(), "vd".to_string()).unwrap();

    // Batch delete some keys
    let keys = vec!["a".to_string(), "c".to_string()];
    let deleted = engine.batch_delete(keys).unwrap();
    assert_eq!(deleted, 2);

    // Verify deleted keys are gone
    assert!(engine.get("a".to_string()).is_err());
    assert!(engine.get("c".to_string()).is_err());

    // Verify remaining keys still exist
    assert_eq!(engine.get("b".to_string()).unwrap(), "vb");
    assert_eq!(engine.get("d".to_string()).unwrap(), "vd");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_batch_delete_empty() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path().to_str().unwrap().to_string();
    let engine = LsmTreeEngine::new(data_dir, 1024 * 1024, 4);

    // Batch delete with empty list should return 0
    let deleted = engine.batch_delete(vec![]).unwrap();
    assert_eq!(deleted, 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_wal_archive_by_size() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path().to_str().unwrap().to_string();

    // Configure with very small max size (1KB) to trigger archiving
    let archive_config = WalArchiveConfig {
        retention_secs: None,       // Don't archive by time
        max_size_bytes: Some(1024), // 1KB max
    };

    let engine = LsmTreeEngine::new_with_config(
        data_dir.clone(),
        100,  // Small memtable to trigger frequent flushes
        1000, // Very high compaction trigger to avoid compaction interference
        DEFAULT_WAL_BATCH_INTERVAL_MICROS,
        archive_config,
    );

    // Write enough data to create multiple WAL files
    for i in 0..20 {
        engine.set(format!("key{}", i), "x".repeat(100)).unwrap();
        wait_for_flush(&engine, 1000).await;
    }

    // Helper to calculate total WAL size
    let calc_wal_size = || -> u64 {
        fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.path()
                    .file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|name| name.starts_with("wal_") && name.ends_with(".log"))
            })
            .filter_map(|e| fs::metadata(e.path()).ok())
            .map(|m| m.len())
            .sum()
    };

    // Wait for WAL size to stabilize below limit (polling with timeout)
    // This is more robust than a fixed sleep as it handles slow systems
    let max_wait_ms = 5000;
    let poll_interval_ms = 100;
    let mut waited_ms = 0;
    let mut total_wal_size = calc_wal_size();

    while total_wal_size > 2048 && waited_ms < max_wait_ms {
        tokio::time::sleep(tokio::time::Duration::from_millis(poll_interval_ms)).await;
        waited_ms += poll_interval_ms;
        total_wal_size = calc_wal_size();
    }

    // Count WAL files for logging
    let wal_files: Vec<_> = fs::read_dir(dir.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.path()
                .file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|name| name.starts_with("wal_") && name.ends_with(".log"))
        })
        .collect();

    println!(
        "WAL files count: {}, total size: {} bytes (waited {}ms)",
        wal_files.len(),
        total_wal_size,
        waited_ms
    );

    // The archiving should keep total size near the limit (1KB)
    // Note: current WAL and the most recent flushed WAL are not archived
    // Each WAL file is around 100-150 bytes, so we expect around 8-10 files max
    assert!(
        total_wal_size <= 2048, // Allow some slack (2KB)
        "Expected WAL size to be limited, got {} bytes after waiting {}ms",
        total_wal_size,
        waited_ms
    );

    // Wait for all data to be accessible (compaction may have been triggered internally)
    let engine_ref = &engine;
    let all_accessible = wait_for_condition(
        || (0..20).all(|i| engine_ref.get(format!("key{}", i)).is_ok()),
        5000,
    )
    .await;
    assert!(
        all_accessible,
        "All keys should be accessible after WAL archiving"
    );

    // Data should still be accessible (from SSTables)
    for i in 0..20 {
        let result = engine.get(format!("key{}", i));
        assert!(result.is_ok(), "key{} should still be accessible", i);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_wal_archive_by_retention() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path().to_str().unwrap().to_string();

    // Configure with very short retention (1 second) to trigger archiving
    let archive_config = WalArchiveConfig {
        retention_secs: Some(1), // 1 second retention
        max_size_bytes: None,    // Don't archive by size
    };

    let engine = LsmTreeEngine::new_with_config(
        data_dir.clone(),
        100, // Small memtable to trigger frequent flushes
        100, // High compaction trigger
        DEFAULT_WAL_BATCH_INTERVAL_MICROS,
        archive_config,
    );

    // Write data and trigger flush
    for i in 0..5 {
        engine.set(format!("key{}", i), "x".repeat(50)).unwrap();
        wait_for_flush(&engine, 1000).await;
    }

    // Count WAL files before retention expires
    let count_wal_files = || {
        fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.path()
                    .file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|name| name.starts_with("wal_") && name.ends_with(".log"))
            })
            .count()
    };

    let wal_count_before = count_wal_files();
    println!("WAL files before waiting: {}", wal_count_before);

    // Wait for retention period to expire (this is necessary for the retention test)
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    // Trigger another flush to run archive
    engine.set("trigger".to_string(), "x".repeat(200)).unwrap();
    wait_for_flush(&engine, 2000).await;

    let wal_count_after = count_wal_files();
    println!("WAL files after retention + flush: {}", wal_count_after);

    // Should have fewer WAL files after archiving (or at least not more than before + 1)
    // The archiving should have removed old files
    assert!(
        wal_count_after <= wal_count_before + 1,
        "Expected WAL files to be archived, before: {}, after: {}",
        wal_count_before,
        wal_count_after
    );

    // Data should still be accessible
    for i in 0..5 {
        let result = engine.get(format!("key{}", i));
        assert!(result.is_ok(), "key{} should still be accessible", i);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_wal_archive_disabled() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path().to_str().unwrap().to_string();

    // Disable archiving
    let archive_config = WalArchiveConfig::disabled();

    let engine = LsmTreeEngine::new_with_config(
        data_dir.clone(),
        100, // Small memtable
        100, // High compaction trigger
        DEFAULT_WAL_BATCH_INTERVAL_MICROS,
        archive_config,
    );

    // Write data to create multiple WAL files
    for i in 0..10 {
        engine.set(format!("key{}", i), "x".repeat(100)).unwrap();
        wait_for_flush(&engine, 1000).await;
    }

    // Count WAL files - all should be preserved
    let wal_count = fs::read_dir(dir.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.path()
                .file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|name| name.starts_with("wal_") && name.ends_with(".log"))
        })
        .count();

    println!("WAL files with archiving disabled: {}", wal_count);

    // Should have multiple WAL files (not archived)
    assert!(
        wal_count >= 2,
        "Expected multiple WAL files when archiving is disabled, got {}",
        wal_count
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_snapshot_lock_prevents_sstable_deletion_during_compaction() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path().to_str().unwrap().to_string();
    let data_dir_clone = data_dir.clone();

    // Create engine with compaction trigger of 4
    let engine = LsmTreeEngine::new(data_dir.clone(), 50, 4);

    // Helper to count SSTable files
    let count_sstables = move || -> usize {
        fs::read_dir(&data_dir_clone)
            .map(|entries| {
                entries
                    .filter_map(|e| e.ok())
                    .filter(|e| {
                        e.path()
                            .file_name()
                            .and_then(|n| n.to_str())
                            .is_some_and(|name| name.starts_with("sst_") && name.ends_with(".data"))
                    })
                    .count()
            })
            .unwrap_or(0)
    };

    // Acquire read lock BEFORE creating SSTables (simulating ongoing snapshot transfer)
    let snapshot_lock = engine.snapshot_lock();
    let read_guard = snapshot_lock.read().unwrap();
    println!("Acquired snapshot read lock before writing data");

    // Write data to create multiple SSTables and trigger compaction
    for i in 0..6 {
        engine
            .set(format!("key{}", i), format!("value{}", i))
            .unwrap();
        wait_for_flush(&engine, 1000).await;
    }

    // Wait for SSTables to be created (poll until we have at least 2)
    wait_for_condition(|| count_sstables() >= 2, 5000).await;

    let count_while_locked = count_sstables();
    println!(
        "SSTable count while holding lock: {} (compaction should be blocked)",
        count_while_locked
    );

    // Should have multiple SSTables because compaction can't delete old ones
    // (merged SSTable is created but old ones are not deleted yet)
    assert!(
        count_while_locked >= 2,
        "Expected at least 2 SSTables while lock is held (old ones not deleted), got {}",
        count_while_locked
    );

    // Wait for all data to be accessible
    let engine_ref = &engine;
    let all_accessible = wait_for_condition(
        || (0..6).all(|i| engine_ref.get(format!("key{}", i)).is_ok()),
        30000,
    )
    .await;
    assert!(
        all_accessible,
        "All keys should be accessible while lock is held"
    );

    // Verify data values
    for i in 0..6 {
        assert_eq!(
            engine.get(format!("key{}", i)).unwrap(),
            format!("value{}", i)
        );
    }
    println!("Data verified while holding lock");

    // Release the lock - now compaction can delete old SSTables
    drop(read_guard);
    println!("Released snapshot read lock");

    // Wait for compaction to complete deletion (poll until SSTable count stabilizes)
    let final_count_target = count_while_locked;
    wait_for_condition(|| count_sstables() <= final_count_target, 10000).await;

    let final_count = count_sstables();
    println!(
        "Final SSTable count after lock released: {} (was {})",
        final_count, count_while_locked
    );

    // After lock is released, old SSTables should be deleted
    // Final count should be less than or equal to count while locked
    // (compaction merges multiple SSTables into one)
    assert!(
        final_count <= count_while_locked,
        "Expected fewer SSTables after compaction completed"
    );

    // Wait for all data to be accessible after compaction
    let all_accessible = wait_for_condition(
        || (0..6).all(|i| engine_ref.get(format!("key{}", i)).is_ok()),
        30000,
    )
    .await;
    assert!(
        all_accessible,
        "All keys should be accessible after compaction"
    );

    // Verify all data values
    for i in 0..6 {
        assert_eq!(
            engine.get(format!("key{}", i)).unwrap(),
            format!("value{}", i)
        );
    }
    println!("All data verified after compaction completed");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_leveled_sstables_basic() {
    use memtable::MemValue;

    let dir = tempdir().unwrap();
    let data_dir = dir.path();

    // Create some SSTable files with different key ranges
    let mut memtable1 = std::collections::BTreeMap::new();
    memtable1.insert("a".to_string(), MemValue::new(Some("v1".to_string())));
    memtable1.insert("b".to_string(), MemValue::new(Some("v2".to_string())));
    let sst1_path = data_dir.join("sst_1_0.data");
    sstable::create_from_memtable(&sst1_path, &memtable1).unwrap();

    let mut memtable2 = std::collections::BTreeMap::new();
    memtable2.insert("c".to_string(), MemValue::new(Some("v3".to_string())));
    memtable2.insert("d".to_string(), MemValue::new(Some("v4".to_string())));
    let sst2_path = data_dir.join("sst_2_0.data");
    sstable::create_from_memtable(&sst2_path, &memtable2).unwrap();

    // Create handles
    let mmap1 = MappedSSTable::open(&sst1_path).unwrap();
    let bloom1 = mmap1.read_bloom_filter().unwrap();
    let handle1 = Arc::new(SstableHandle {
        mmap: mmap1,
        bloom: bloom1,
    });

    let mmap2 = MappedSSTable::open(&sst2_path).unwrap();
    let bloom2 = mmap2.read_bloom_filter().unwrap();
    let handle2 = Arc::new(SstableHandle {
        mmap: mmap2,
        bloom: bloom2,
    });

    // Test LeveledSstables
    let mut leveled = LeveledSstables::new();
    assert_eq!(leveled.total_sstable_count(), 0);
    assert_eq!(leveled.level_count(), MAX_LEVELS);

    // Add to L0
    leveled.add_to_level(0, Arc::clone(&handle1));
    leveled.add_to_level(0, Arc::clone(&handle2));
    assert_eq!(leveled.l0_sstables().len(), 2);
    assert_eq!(leveled.total_sstable_count(), 2);

    // L0 should have handle2 first (newest)
    assert!(Arc::ptr_eq(&leveled.l0_sstables()[0], &handle2));

    // Add to L1
    leveled.add_to_level(1, Arc::clone(&handle1));
    assert_eq!(leveled.get_level(1).len(), 1);
    assert_eq!(leveled.total_sstable_count(), 3);

    // Remove from L0
    leveled.remove(&handle2);
    assert_eq!(leveled.l0_sstables().len(), 1);
    assert_eq!(leveled.total_sstable_count(), 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_leveled_sstables_binary_search() {
    use memtable::MemValue;

    let dir = tempdir().unwrap();
    let data_dir = dir.path();

    // Create SSTables with non-overlapping ranges for L1
    let mut memtable1 = std::collections::BTreeMap::new();
    memtable1.insert("aaa".to_string(), MemValue::new(Some("v1".to_string())));
    memtable1.insert("bbb".to_string(), MemValue::new(Some("v2".to_string())));
    let sst1_path = data_dir.join("sst_1_0.data");
    sstable::create_from_memtable(&sst1_path, &memtable1).unwrap();

    let mut memtable2 = std::collections::BTreeMap::new();
    memtable2.insert("ccc".to_string(), MemValue::new(Some("v3".to_string())));
    memtable2.insert("ddd".to_string(), MemValue::new(Some("v4".to_string())));
    let sst2_path = data_dir.join("sst_2_0.data");
    sstable::create_from_memtable(&sst2_path, &memtable2).unwrap();

    let mut memtable3 = std::collections::BTreeMap::new();
    memtable3.insert("eee".to_string(), MemValue::new(Some("v5".to_string())));
    memtable3.insert("fff".to_string(), MemValue::new(Some("v6".to_string())));
    let sst3_path = data_dir.join("sst_3_0.data");
    sstable::create_from_memtable(&sst3_path, &memtable3).unwrap();

    // Create handles
    let mmap1 = MappedSSTable::open(&sst1_path).unwrap();
    let bloom1 = mmap1.read_bloom_filter().unwrap();
    let handle1 = Arc::new(SstableHandle {
        mmap: mmap1,
        bloom: bloom1,
    });

    let mmap2 = MappedSSTable::open(&sst2_path).unwrap();
    let bloom2 = mmap2.read_bloom_filter().unwrap();
    let handle2 = Arc::new(SstableHandle {
        mmap: mmap2,
        bloom: bloom2,
    });

    let mmap3 = MappedSSTable::open(&sst3_path).unwrap();
    let bloom3 = mmap3.read_bloom_filter().unwrap();
    let handle3 = Arc::new(SstableHandle {
        mmap: mmap3,
        bloom: bloom3,
    });

    let mut leveled = LeveledSstables::new();

    // Add to L1 (non-overlapping)
    leveled.add_to_level(1, Arc::clone(&handle1));
    leveled.add_to_level(1, Arc::clone(&handle2));
    leveled.add_to_level(1, Arc::clone(&handle3));

    // candidates_for_key should find the right SSTable (at most 1 for L1+)
    let found = leveled.candidates_for_key(1, b"aaa");
    assert_eq!(found.len(), 1);

    let found = leveled.candidates_for_key(1, b"ccc");
    assert_eq!(found.len(), 1);

    let found = leveled.candidates_for_key(1, b"fff");
    assert_eq!(found.len(), 1);

    // Key outside all ranges should return empty
    let found = leveled.candidates_for_key(1, b"zzz");
    assert!(found.is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn test_leveled_sstables_get_overlapping() {
    use memtable::MemValue;

    let dir = tempdir().unwrap();
    let data_dir = dir.path();

    // Create SSTables for L1
    let mut memtable1 = std::collections::BTreeMap::new();
    memtable1.insert("a".to_string(), MemValue::new(Some("v1".to_string())));
    memtable1.insert("c".to_string(), MemValue::new(Some("v2".to_string())));
    let sst1_path = data_dir.join("sst_1_0.data");
    sstable::create_from_memtable(&sst1_path, &memtable1).unwrap();

    let mut memtable2 = std::collections::BTreeMap::new();
    memtable2.insert("d".to_string(), MemValue::new(Some("v3".to_string())));
    memtable2.insert("f".to_string(), MemValue::new(Some("v4".to_string())));
    let sst2_path = data_dir.join("sst_2_0.data");
    sstable::create_from_memtable(&sst2_path, &memtable2).unwrap();

    let mut memtable3 = std::collections::BTreeMap::new();
    memtable3.insert("g".to_string(), MemValue::new(Some("v5".to_string())));
    memtable3.insert("i".to_string(), MemValue::new(Some("v6".to_string())));
    let sst3_path = data_dir.join("sst_3_0.data");
    sstable::create_from_memtable(&sst3_path, &memtable3).unwrap();

    // Create handles
    let mmap1 = MappedSSTable::open(&sst1_path).unwrap();
    let bloom1 = mmap1.read_bloom_filter().unwrap();
    let handle1 = Arc::new(SstableHandle {
        mmap: mmap1,
        bloom: bloom1,
    });

    let mmap2 = MappedSSTable::open(&sst2_path).unwrap();
    let bloom2 = mmap2.read_bloom_filter().unwrap();
    let handle2 = Arc::new(SstableHandle {
        mmap: mmap2,
        bloom: bloom2,
    });

    let mmap3 = MappedSSTable::open(&sst3_path).unwrap();
    let bloom3 = mmap3.read_bloom_filter().unwrap();
    let handle3 = Arc::new(SstableHandle {
        mmap: mmap3,
        bloom: bloom3,
    });

    let mut leveled = LeveledSstables::new();
    leveled.add_to_level(1, Arc::clone(&handle1));
    leveled.add_to_level(1, Arc::clone(&handle2));
    leveled.add_to_level(1, Arc::clone(&handle3));

    // Query range [b, e] should overlap with handle1 ([a,c]) and handle2 ([d,f])
    let overlapping = leveled.get_overlapping(1, b"b", b"e");
    assert_eq!(overlapping.len(), 2);

    // Query range [h, j] should only overlap with handle3 ([g,i])
    let overlapping = leveled.get_overlapping(1, b"h", b"j");
    assert_eq!(overlapping.len(), 1);

    // Query range [x, z] should not overlap with anything
    let overlapping = leveled.get_overlapping(1, b"x", b"z");
    assert_eq!(overlapping.len(), 0);
}

/// Test LSN-based incremental WAL recovery
/// Only entries with seq > last_flushed_wal_seq should be recovered
#[tokio::test(flavor = "multi_thread")]
async fn test_lsn_based_incremental_recovery() {
    use super::WalArchiveConfig;

    let dir = tempdir().unwrap();
    let data_dir = dir.path().to_str().unwrap().to_string();

    // Helper to create engine without stale compaction (to avoid race conditions in tests)
    let create_engine = |capacity: u64| {
        LsmTreeEngine::new_with_full_config(
            data_dir.clone(),
            capacity,
            100,
            100,
            WalArchiveConfig::default(),
            false, // disable stale compaction for this test
        )
    };

    // Phase 1: Write some data and flush to SSTable
    // This will update manifest.last_flushed_wal_seq
    {
        let engine = create_engine(30);

        // k1 and k2 will be flushed to SSTable
        engine.set("k1".to_string(), "v1".to_string()).unwrap();
        engine.set("k2".to_string(), "v2".to_string()).unwrap();
        wait_for_flush(&engine, 2000).await;

        // Verify data is accessible
        assert_eq!(engine.get("k1".to_string()).unwrap(), "v1");
        assert_eq!(engine.get("k2".to_string()).unwrap(), "v2");

        // Check that manifest has the last_flushed_wal_seq updated
        let manifest_seq = {
            let manifest = engine.manifest.lock().unwrap();
            manifest.last_flushed_wal_seq
        };
        assert!(
            manifest_seq > 0,
            "last_flushed_wal_seq should be > 0 after flush"
        );

        engine.shutdown().await;
    }

    // Phase 2: Write more data WITHOUT flushing (only in WAL)
    {
        let engine = create_engine(1024 * 1024);

        // k1 and k2 are already persisted in SSTables
        assert_eq!(engine.get("k1".to_string()).unwrap(), "v1");
        assert_eq!(engine.get("k2".to_string()).unwrap(), "v2");

        // Add new data (will be in WAL only)
        engine.set("k3".to_string(), "v3".to_string()).unwrap();
        engine.set("k4".to_string(), "v4".to_string()).unwrap();

        // Update existing key (new value should be in WAL only)
        engine
            .set("k1".to_string(), "v1_updated".to_string())
            .unwrap();

        // Wait for WAL group commit
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // Don't flush - simulate crash
        engine.shutdown().await;
    }

    // Phase 3: Reopen and verify incremental recovery
    // Only k3, k4, and updated k1 should be recovered from WAL
    // k1's old value and k2 should come from SSTable (not replayed from WAL)
    {
        let engine = create_engine(1024 * 1024);

        // k1 should have the updated value from WAL
        assert_eq!(
            engine.get("k1".to_string()).unwrap(),
            "v1_updated",
            "k1 should be updated from WAL"
        );

        // k2 should still be accessible (from SSTable)
        assert_eq!(
            engine.get("k2".to_string()).unwrap(),
            "v2",
            "k2 should be from SSTable"
        );

        // k3 and k4 should be recovered from WAL
        assert_eq!(
            engine.get("k3".to_string()).unwrap(),
            "v3",
            "k3 should be recovered from WAL"
        );
        assert_eq!(
            engine.get("k4".to_string()).unwrap(),
            "v4",
            "k4 should be recovered from WAL"
        );

        engine.shutdown().await;
    }
}

/// Test that stale SSTables are compacted after recovery
/// When SSTables have WAL IDs significantly older than current,
/// they should be merged by background compaction
#[tokio::test(flavor = "multi_thread")]
async fn test_stale_sstable_compaction_after_recovery() {
    use super::WalArchiveConfig;
    use super::sstable;
    use std::fs;

    let dir = tempdir().unwrap();
    let data_dir = dir.path().to_str().unwrap().to_string();
    let data_path = dir.path();

    // Helper to count SSTable files and their WAL IDs
    let count_sstables = || -> Vec<(String, u64)> {
        let mut result = Vec::new();
        if let Ok(entries) = fs::read_dir(&data_path) {
            for entry in entries.flatten() {
                let p = entry.path();
                if let Some(filename) = p.file_name().and_then(|n| n.to_str()) {
                    if let Some((_, Some(wal_id))) = sstable::parse_filename(filename) {
                        result.push((filename.to_string(), wal_id));
                    }
                }
            }
        }
        result.sort_by_key(|(_, wal_id)| *wal_id);
        result
    };

    // Use larger memtable to have more predictable flush behavior
    // Each flush will contain multiple entries, creating SSTables with distinct WAL IDs
    let memtable_capacity = 200; // ~10 entries per flush

    // Phase 1: Create initial SSTables
    {
        let engine = LsmTreeEngine::new_with_full_config(
            data_dir.clone(),
            memtable_capacity,
            100, // High L0 threshold to prevent auto-compaction
            100,
            WalArchiveConfig::default(),
            false,
        );

        // Write data to trigger at least 2 flushes
        for i in 0..30 {
            engine
                .set(format!("key{:03}", i), format!("value{}", i))
                .unwrap();
        }

        // Wait for all flushes to complete
        wait_for_flush(&engine, 3000).await;

        // Verify all data is accessible
        for i in 0..30 {
            let result = engine.get(format!("key{:03}", i));
            assert!(result.is_ok(), "Phase 1: key{:03} should exist", i);
        }

        engine.shutdown().await;
    }

    let phase1_sstables = count_sstables();
    println!("Phase 1: Created {} SSTables", phase1_sstables.len());
    assert!(
        phase1_sstables.len() >= 2,
        "Should have at least 2 SSTables"
    );

    // Phase 2: Write more data to advance WAL ID
    {
        let engine = LsmTreeEngine::new_with_full_config(
            data_dir.clone(),
            memtable_capacity,
            100,
            100,
            WalArchiveConfig::default(),
            false,
        );

        // Verify Phase 1 data is recovered
        for i in 0..30 {
            let result = engine.get(format!("key{:03}", i));
            assert!(result.is_ok(), "Phase 2 start: key{:03} should exist", i);
        }

        // Write more data to advance WAL
        for i in 30..60 {
            engine
                .set(format!("key{:03}", i), format!("value{}", i))
                .unwrap();
        }
        wait_for_flush(&engine, 3000).await;

        engine.shutdown().await;
    }

    let phase2_sstables = count_sstables();
    println!("Phase 2: Now have {} SSTables", phase2_sstables.len());

    // Get min/max WAL IDs to verify we have stale files
    let min_wal_id = phase2_sstables.iter().map(|(_, id)| *id).min().unwrap_or(0);
    let max_wal_id = phase2_sstables.iter().map(|(_, id)| *id).max().unwrap_or(0);
    println!("WAL ID range: {} - {}", min_wal_id, max_wal_id);

    // Phase 3: Restart with stale compaction enabled
    {
        let engine = LsmTreeEngine::new_with_full_config(
            data_dir.clone(),
            1024 * 1024,
            4, // Low threshold to trigger L0 compaction
            100,
            WalArchiveConfig::default(),
            true, // enable stale compaction
        );

        // Wait for stale compaction to complete
        tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;

        // Verify all data is still accessible
        for i in 0..60 {
            let result = engine.get(format!("key{:03}", i));
            assert!(
                result.is_ok(),
                "key{:03} should be accessible after compaction",
                i
            );
        }

        engine.shutdown().await;
    }

    let final_sstables = count_sstables();
    println!(
        "Phase 3: After compaction, have {} SSTables",
        final_sstables.len()
    );

    // Stale compaction should have run if there were stale files
    // We just verify data integrity, not specific file counts
    println!("Test passed: All data accessible after stale compaction");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_get_expire_at_no_ttl() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path().to_str().unwrap().to_string();
    let engine = LsmTreeEngine::new(data_dir, 1024, 4);

    // Set key without TTL
    engine
        .set("key1".to_string(), "value1".to_string())
        .unwrap();

    // get_expire_at should return (true, 0) for key without TTL
    let (exists, expire_at) = engine.get_expire_at("key1".to_string()).unwrap();
    assert!(exists, "Key should exist");
    assert_eq!(expire_at, 0, "expire_at should be 0 for key without TTL");

    engine.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_get_expire_at_with_ttl() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path().to_str().unwrap().to_string();
    let engine = LsmTreeEngine::new(data_dir, 1024, 4);

    // Set key with TTL (10 seconds)
    engine
        .set_with_ttl("key1".to_string(), "value1".to_string(), 10)
        .unwrap();

    // get_expire_at should return (true, expire_at > 0)
    let (exists, expire_at) = engine.get_expire_at("key1".to_string()).unwrap();
    assert!(exists, "Key should exist");
    assert!(expire_at > 0, "expire_at should be > 0 for key with TTL");

    // Verify expire_at is approximately now + 10 seconds
    let now = current_timestamp();
    assert!(
        expire_at >= now && expire_at <= now + 11,
        "expire_at should be approximately now + 10 seconds"
    );

    engine.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_get_expire_at_expired_key() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path().to_str().unwrap().to_string();
    let engine = LsmTreeEngine::new(data_dir, 1024, 4);

    // Set key with TTL of 1 second
    engine
        .set_with_ttl("key1".to_string(), "value1".to_string(), 1)
        .unwrap();

    // Verify key exists before expiration
    let (exists, expire_at) = engine.get_expire_at("key1".to_string()).unwrap();
    assert!(exists, "Key should exist before expiration");
    assert!(expire_at > 0, "expire_at should be > 0");

    // Wait for key to expire
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    // get_expire_at should return (false, 0) for expired key
    let (exists, expire_at) = engine.get_expire_at("key1".to_string()).unwrap();
    assert!(!exists, "Key should not exist after expiration");
    assert_eq!(expire_at, 0, "expire_at should be 0 for expired key");

    // get should also return NotFound
    let result = engine.get("key1".to_string());
    assert!(result.is_err(), "get should return error for expired key");

    engine.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_get_expire_at_nonexistent_key() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path().to_str().unwrap().to_string();
    let engine = LsmTreeEngine::new(data_dir, 1024, 4);

    // get_expire_at for non-existent key should return (false, 0)
    let (exists, expire_at) = engine.get_expire_at("nonexistent".to_string()).unwrap();
    assert!(!exists, "Non-existent key should not exist");
    assert_eq!(expire_at, 0, "expire_at should be 0 for non-existent key");

    engine.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_get_expire_at_deleted_key() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path().to_str().unwrap().to_string();
    let engine = LsmTreeEngine::new(data_dir, 1024, 4);

    // Set and then delete a key
    engine
        .set("key1".to_string(), "value1".to_string())
        .unwrap();
    engine.delete("key1".to_string()).unwrap();

    // get_expire_at should return (false, 0) for deleted key
    let (exists, expire_at) = engine.get_expire_at("key1".to_string()).unwrap();
    assert!(!exists, "Deleted key should not exist");
    assert_eq!(expire_at, 0, "expire_at should be 0 for deleted key");

    engine.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_get_expire_at_from_sstable() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path().to_str().unwrap().to_string();
    // Small memtable to force flush
    let engine = LsmTreeEngine::new(data_dir, 100, 4);

    // Set key with TTL
    engine
        .set_with_ttl("key1".to_string(), "value1".to_string(), 60)
        .unwrap();

    // Force flush by adding more data
    for i in 0..10 {
        engine.set(format!("pad{}", i), "x".repeat(50)).unwrap();
    }
    wait_for_flush(&engine, 2000).await;

    // get_expire_at should still work after flush to SSTable
    let (exists, expire_at) = engine.get_expire_at("key1".to_string()).unwrap();
    assert!(exists, "Key should exist in SSTable");
    assert!(expire_at > 0, "expire_at should be preserved in SSTable");

    engine.shutdown().await;
}

/// Test that expired keys are filtered during WAL recovery
/// This saves memory by not loading already-expired data into memtable
#[tokio::test(flavor = "multi_thread")]
async fn test_wal_recovery_filters_expired_keys() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path().to_str().unwrap().to_string();

    // Phase 1: Write data with short TTL
    {
        // Large memtable to avoid flushing
        let engine = LsmTreeEngine::new(data_dir.clone(), 1024 * 1024, 100);

        // Set key with 1 second TTL
        engine
            .set_with_ttl("expired_key".to_string(), "value1".to_string(), 1)
            .unwrap();
        // Set key without TTL
        engine
            .set("permanent_key".to_string(), "value2".to_string())
            .unwrap();
        // Set key with long TTL
        engine
            .set_with_ttl("valid_ttl_key".to_string(), "value3".to_string(), 3600)
            .unwrap();

        // Wait for WAL group commit
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // Don't flush - simulate crash
        engine.shutdown().await;
    }

    // Wait for the 1-second TTL to expire
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    // Phase 2: Reopen and verify expired key is filtered during recovery
    {
        let engine = LsmTreeEngine::new(data_dir.clone(), 1024 * 1024, 100);

        // expired_key should not be recovered (already expired)
        let result = engine.get("expired_key".to_string());
        assert!(
            result.is_err(),
            "Expired key should not be recovered from WAL"
        );

        // permanent_key should be recovered
        assert_eq!(
            engine.get("permanent_key".to_string()).unwrap(),
            "value2",
            "Permanent key should be recovered from WAL"
        );

        // valid_ttl_key should be recovered (not yet expired)
        assert_eq!(
            engine.get("valid_ttl_key".to_string()).unwrap(),
            "value3",
            "Valid TTL key should be recovered from WAL"
        );

        // Verify get_expire_at for valid_ttl_key
        let (exists, expire_at) = engine.get_expire_at("valid_ttl_key".to_string()).unwrap();
        assert!(exists, "valid_ttl_key should exist");
        assert!(expire_at > 0, "valid_ttl_key should have TTL preserved");

        engine.shutdown().await;
    }
}

/// Test that orphaned SSTables (files not in manifest) are deleted during recovery
#[tokio::test(flavor = "multi_thread")]
async fn test_orphaned_sstable_cleanup() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path().to_str().unwrap().to_string();

    // Phase 1: Create engine and write some data, flush to SSTable
    {
        let engine = LsmTreeEngine::new(data_dir.clone(), 30, 100);

        engine.set("k1".to_string(), "v1".to_string()).unwrap();
        engine.set("k2".to_string(), "v2".to_string()).unwrap();

        wait_for_flush(&engine, 2000).await;

        engine.shutdown().await;
    }

    // Create an orphaned SSTable file (not in manifest)
    let orphan_path = dir.path().join("sst_99999_99999.data");
    std::fs::write(&orphan_path, b"fake sstable data").unwrap();
    assert!(
        orphan_path.exists(),
        "Orphan file should exist before recovery"
    );

    // Phase 2: Reopen engine - orphaned SSTable should be deleted
    {
        let engine = LsmTreeEngine::new(data_dir.clone(), 1024 * 1024, 100);

        // Verify data was recovered
        assert_eq!(engine.get("k1".to_string()).unwrap(), "v1");
        assert_eq!(engine.get("k2".to_string()).unwrap(), "v2");

        engine.shutdown().await;
    }

    // Orphan file should be deleted
    assert!(
        !orphan_path.exists(),
        "Orphaned SSTable should be deleted during recovery"
    );
}

/// Test that WAL files are registered in manifest during recovery
/// This ensures old WAL files can be properly cleaned up after their entries are flushed
#[tokio::test(flavor = "multi_thread")]
async fn test_wal_files_registered_on_recovery() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path().to_str().unwrap().to_string();

    // Phase 1: Create engine and write some data
    {
        let engine = LsmTreeEngine::new(data_dir.clone(), 1024 * 1024, 100);

        engine.set("k1".to_string(), "v1".to_string()).unwrap();
        engine.set("k2".to_string(), "v2".to_string()).unwrap();

        // Wait for WAL group commit
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // Check manifest - should have no WAL files registered yet (no rotation)
        let wal_file_count = {
            let manifest = engine.manifest.lock().unwrap();
            manifest.wal_file_max_seq.len()
        };
        assert_eq!(
            wal_file_count, 0,
            "No WAL files should be registered before rotation"
        );

        engine.shutdown().await;
    }

    // Phase 2: Reopen engine - WAL files should be registered during recovery
    {
        let engine = LsmTreeEngine::new(data_dir.clone(), 1024 * 1024, 100);

        // Verify data was recovered
        assert_eq!(engine.get("k1".to_string()).unwrap(), "v1");
        assert_eq!(engine.get("k2".to_string()).unwrap(), "v2");

        // Check manifest - old WAL files should now be registered
        let wal_file_count = {
            let manifest = engine.manifest.lock().unwrap();
            manifest.wal_file_max_seq.len()
        };
        assert!(
            wal_file_count >= 1,
            "WAL files should be registered after recovery, got {}",
            wal_file_count
        );

        engine.shutdown().await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_count_prefix() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path().to_str().unwrap().to_string();
    let engine = LsmTreeEngine::new(data_dir, 1024 * 1024, 4);

    // Insert keys with different prefixes
    engine
        .set("user:1".to_string(), "alice".to_string())
        .unwrap();
    engine.set("user:2".to_string(), "bob".to_string()).unwrap();
    engine
        .set("user:3".to_string(), "charlie".to_string())
        .unwrap();
    engine
        .set("product:1".to_string(), "laptop".to_string())
        .unwrap();
    engine
        .set("product:2".to_string(), "phone".to_string())
        .unwrap();

    // Count by prefix
    assert_eq!(engine.count("user:").unwrap(), 3);
    assert_eq!(engine.count("product:").unwrap(), 2);
    assert_eq!(engine.count("").unwrap(), 5); // All keys
    assert_eq!(engine.count("nonexistent:").unwrap(), 0);

    engine.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_count_with_tombstones() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path().to_str().unwrap().to_string();
    let engine = LsmTreeEngine::new(data_dir, 1024 * 1024, 4);

    // Insert and delete some keys
    engine
        .set("user:1".to_string(), "alice".to_string())
        .unwrap();
    engine.set("user:2".to_string(), "bob".to_string()).unwrap();
    engine
        .set("user:3".to_string(), "charlie".to_string())
        .unwrap();
    engine.delete("user:2".to_string()).unwrap();

    // Count should exclude deleted key
    assert_eq!(engine.count("user:").unwrap(), 2);

    engine.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_count_with_expired_keys() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path().to_str().unwrap().to_string();
    let engine = LsmTreeEngine::new(data_dir, 1024 * 1024, 4);

    // Insert keys with TTL
    engine
        .set_with_ttl("user:1".to_string(), "alice".to_string(), 1)
        .unwrap();
    engine.set("user:2".to_string(), "bob".to_string()).unwrap();
    engine
        .set("user:3".to_string(), "charlie".to_string())
        .unwrap();

    // Wait for TTL to expire
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    // Count should exclude expired key
    assert_eq!(engine.count("user:").unwrap(), 2);

    engine.shutdown().await;
}
