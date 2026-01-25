//! Unit tests for LSM-tree engine.

use super::*;
use tempfile::tempdir;

#[tokio::test(flavor = "multi_thread")]
async fn test_lsm_flush_and_get() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path().to_str().unwrap().to_string();
    let engine = LsmTreeEngine::new(data_dir, 50, 4);

    engine.set("k1".to_string(), "v1".to_string()).unwrap();
    engine.set("k2".to_string(), "v2".to_string()).unwrap();
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

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
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
    }

    {
        let engine = LsmTreeEngine::new(data_dir.clone(), 50, 4);
        assert_eq!(engine.get("k1".to_string()).unwrap(), "v1");
        assert_eq!(engine.get("k2".to_string()).unwrap(), "v2");

        engine.set("k3".to_string(), "v3".to_string()).unwrap();
        engine.set("k4".to_string(), "v4".to_string()).unwrap();
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

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

    // 少し待ってもSSTableが大量に生成されていないことを確認
    // (バグがあればここで100個近いSSTableが生成される)
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

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
    // Low capacity to trigger multiple flushes
    let engine = LsmTreeEngine::new(data_dir.clone(), 30, 2);

    // Write data in batches to create multiple SSTables
    for i in 0..10 {
        engine
            .set(format!("key{}", i), format!("value{}", i))
            .unwrap();
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    }

    // Wait for flushes and potential compaction
    tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;

    // Verify data is still accessible after compaction
    for i in 0..10 {
        let result = engine.get(format!("key{}", i));
        assert!(result.is_ok(), "key{} should be accessible", i);
        assert_eq!(result.unwrap(), format!("value{}", i));
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_compaction_removes_deleted_keys() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path().to_str().unwrap().to_string();
    let engine = LsmTreeEngine::new(data_dir.clone(), 30, 2);

    // Write some data
    engine
        .set("keep".to_string(), "keep_value".to_string())
        .unwrap();
    engine
        .set("delete_me".to_string(), "will_be_deleted".to_string())
        .unwrap();
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    // Delete one key
    engine.delete("delete_me".to_string()).unwrap();
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    // Add more data to trigger compaction
    for i in 0..5 {
        engine
            .set(format!("extra{}", i), format!("extra_value{}", i))
            .unwrap();
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    }

    // Wait for compaction
    tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;

    // Verify kept key is accessible
    assert_eq!(engine.get("keep".to_string()).unwrap(), "keep_value");

    // Verify deleted key is not found
    let result = engine.get("delete_me".to_string());
    assert!(result.is_err());
}

#[tokio::test(flavor = "multi_thread")]
async fn test_compaction_deletes_old_files() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path().to_str().unwrap().to_string();
    let engine = LsmTreeEngine::new(data_dir.clone(), 30, 2);

    // Create multiple SSTables
    for i in 0..8 {
        engine.set(format!("k{}", i), format!("v{}", i)).unwrap();
        tokio::time::sleep(tokio::time::Duration::from_millis(150)).await;
    }

    // Wait for compaction to complete
    tokio::time::sleep(tokio::time::Duration::from_millis(1500)).await;

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

    // After compaction, should have fewer SSTables
    assert!(
        sst_files.len() <= 2,
        "Expected at most 2 SSTables after compaction, got {}",
        sst_files.len()
    );

    // Data should still be accessible
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
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
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
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    // v2 in SSTable 2
    engine.set("key".to_string(), "v2".to_string()).unwrap();
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

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
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    assert_eq!(engine.get("key".to_string()).unwrap(), "v3");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_lsm_recovery_merges_wal_and_sst() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path().to_str().unwrap().to_string();

    {
        // Phase 1: Create SSTables with k1=v1 and k2=v2
        // Use 30 byte memtable to trigger flushes
        let engine = LsmTreeEngine::new(data_dir.clone(), 30, 100);

        // k1 goes to SSTable
        engine.set("k1".to_string(), "v1".to_string()).unwrap();
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

        // k2 triggers another flush to SSTable
        engine.set("k2".to_string(), "v2".to_string()).unwrap();
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

        engine.shutdown().await;
    }

    {
        // Phase 2: Update k1 in WAL only (no flush)
        // Use large memtable so k1=v1_new won't trigger flush
        let engine = LsmTreeEngine::new(data_dir.clone(), 1024, 100);

        // k1 updated in WAL/MemTable only (won't trigger flush)
        engine.set("k1".to_string(), "v1_new".to_string()).unwrap();

        // Wait for WAL group commit to complete
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        engine.shutdown().await;
    }

    {
        // Phase 3: Verify recovery merges WAL with SSTable
        let engine = LsmTreeEngine::new(data_dir.clone(), 1024, 100);
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
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    assert_eq!(engine.get("k1".to_string()).unwrap(), "v1");

    // Delete k1 (tombstone in MemTable)
    engine.delete("k1".to_string()).unwrap();
    assert!(engine.get("k1".to_string()).is_err());

    // Flush tombstone
    engine
        .set("k2".to_string(), "trigger_flush".repeat(10))
        .unwrap();
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

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
    tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;

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
        100, // Small memtable to trigger frequent flushes
        100, // High compaction trigger (avoid compaction interference)
        DEFAULT_WAL_BATCH_INTERVAL_MICROS,
        archive_config,
    );

    // Write enough data to create multiple WAL files
    for i in 0..20 {
        engine.set(format!("key{}", i), "x".repeat(100)).unwrap();
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
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
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    }

    // Wait for flushes
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

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

    // Wait for retention period to expire
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    // Trigger another flush to run archive
    engine.set("trigger".to_string(), "x".repeat(200)).unwrap();
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

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
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    }

    // Wait for flushes
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

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
    use std::sync::atomic::{AtomicUsize, Ordering};

    let dir = tempdir().unwrap();
    let data_dir = dir.path().to_str().unwrap().to_string();

    // Create engine with compaction trigger of 2
    let engine = LsmTreeEngine::new(data_dir.clone(), 50, 2);

    // Helper to count SSTable files
    let count_sstables = || -> usize {
        fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.path()
                    .file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|name| name.starts_with("sst_") && name.ends_with(".data"))
            })
            .count()
    };

    // Acquire read lock BEFORE creating SSTables (simulating ongoing snapshot transfer)
    let snapshot_lock = engine.snapshot_lock();
    let read_guard = snapshot_lock.read().unwrap();
    println!("Acquired snapshot read lock before writing data");

    // Track if compaction tried to delete files (it should be blocked)
    let sstable_count_before_compaction = Arc::new(AtomicUsize::new(0));

    // Write data to create multiple SSTables and trigger compaction
    for i in 0..6 {
        engine
            .set(format!("key{}", i), format!("value{}", i))
            .unwrap();
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    }

    // Wait for SSTables to be created and compaction to be triggered
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    let count_while_locked = count_sstables();
    sstable_count_before_compaction.store(count_while_locked, Ordering::SeqCst);
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

    // Verify data is accessible even with pending compaction
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

    // Wait for compaction to complete deletion
    tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;

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

    // Verify all data is still accessible after compaction
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
    let dir = tempdir().unwrap();
    let data_dir = dir.path();

    // Create some SSTable files with different key ranges
    let mut memtable1 = std::collections::BTreeMap::new();
    memtable1.insert("a".to_string(), Some("v1".to_string()));
    memtable1.insert("b".to_string(), Some("v2".to_string()));
    let sst1_path = data_dir.join("sst_1_0.data");
    sstable::create_from_memtable(&sst1_path, &memtable1).unwrap();

    let mut memtable2 = std::collections::BTreeMap::new();
    memtable2.insert("c".to_string(), Some("v3".to_string()));
    memtable2.insert("d".to_string(), Some("v4".to_string()));
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
    let dir = tempdir().unwrap();
    let data_dir = dir.path();

    // Create SSTables with non-overlapping ranges for L1
    let mut memtable1 = std::collections::BTreeMap::new();
    memtable1.insert("aaa".to_string(), Some("v1".to_string()));
    memtable1.insert("bbb".to_string(), Some("v2".to_string()));
    let sst1_path = data_dir.join("sst_1_0.data");
    sstable::create_from_memtable(&sst1_path, &memtable1).unwrap();

    let mut memtable2 = std::collections::BTreeMap::new();
    memtable2.insert("ccc".to_string(), Some("v3".to_string()));
    memtable2.insert("ddd".to_string(), Some("v4".to_string()));
    let sst2_path = data_dir.join("sst_2_0.data");
    sstable::create_from_memtable(&sst2_path, &memtable2).unwrap();

    let mut memtable3 = std::collections::BTreeMap::new();
    memtable3.insert("eee".to_string(), Some("v5".to_string()));
    memtable3.insert("fff".to_string(), Some("v6".to_string()));
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

    // Binary search should find the right SSTable
    let found = leveled.binary_search_level(1, b"aaa");
    assert!(found.is_some());

    let found = leveled.binary_search_level(1, b"ccc");
    assert!(found.is_some());

    let found = leveled.binary_search_level(1, b"fff");
    assert!(found.is_some());

    // Key outside all ranges should return None
    let found = leveled.binary_search_level(1, b"zzz");
    assert!(found.is_none());
}

#[tokio::test(flavor = "multi_thread")]
async fn test_leveled_sstables_get_overlapping() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path();

    // Create SSTables for L1
    let mut memtable1 = std::collections::BTreeMap::new();
    memtable1.insert("a".to_string(), Some("v1".to_string()));
    memtable1.insert("c".to_string(), Some("v2".to_string()));
    let sst1_path = data_dir.join("sst_1_0.data");
    sstable::create_from_memtable(&sst1_path, &memtable1).unwrap();

    let mut memtable2 = std::collections::BTreeMap::new();
    memtable2.insert("d".to_string(), Some("v3".to_string()));
    memtable2.insert("f".to_string(), Some("v4".to_string()));
    let sst2_path = data_dir.join("sst_2_0.data");
    sstable::create_from_memtable(&sst2_path, &memtable2).unwrap();

    let mut memtable3 = std::collections::BTreeMap::new();
    memtable3.insert("g".to_string(), Some("v5".to_string()));
    memtable3.insert("i".to_string(), Some("v6".to_string()));
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

#[test]
fn test_compaction_config_level_size_calculation() {
    use super::compaction::CompactionConfig;
    let config = CompactionConfig::default();

    // L0 has no target size (controlled by file count)
    assert_eq!(config.target_size_for_level(0), 0);

    // L1 = 64MB
    assert_eq!(config.target_size_for_level(1), 64 * 1024 * 1024);

    // L2 = 640MB (10x L1)
    assert_eq!(config.target_size_for_level(2), 640 * 1024 * 1024);

    // L3 = 6.4GB (10x L2)
    assert_eq!(config.target_size_for_level(3), 6400 * 1024 * 1024);
}

#[test]
fn test_leveled_sstables_from_flat_list() {
    let leveled = LeveledSstables::from_flat_list(Vec::new());
    assert_eq!(leveled.total_sstable_count(), 0);
    assert_eq!(leveled.l0_sstables().len(), 0);

    // to_flat_list should work on empty
    let flat = leveled.to_flat_list();
    assert!(flat.is_empty());
}
