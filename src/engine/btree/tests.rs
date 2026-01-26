//! Tests for B-tree engine

use super::*;
use tempfile::tempdir;

#[test]
fn test_btree_basic_operations() {
    let dir = tempdir().unwrap();
    let engine = BTreeEngine::open(dir.path()).unwrap();

    // Set and get
    engine
        .set("key1".to_string(), "value1".to_string())
        .unwrap();
    assert_eq!(engine.get("key1".to_string()).unwrap(), "value1");

    // Update
    engine
        .set("key1".to_string(), "value1_updated".to_string())
        .unwrap();
    assert_eq!(engine.get("key1".to_string()).unwrap(), "value1_updated");

    // Delete
    engine.delete("key1".to_string()).unwrap();
    assert!(engine.get("key1".to_string()).is_err());
}

#[test]
fn test_btree_not_found() {
    let dir = tempdir().unwrap();
    let engine = BTreeEngine::open(dir.path()).unwrap();

    let result = engine.get("nonexistent".to_string());
    assert!(result.is_err());

    let result = engine.delete("nonexistent".to_string());
    assert!(result.is_err());
}

#[test]
fn test_btree_multiple_keys() {
    let dir = tempdir().unwrap();
    let engine = BTreeEngine::open(dir.path()).unwrap();

    for i in 0..100 {
        let key = format!("key{:03}", i);
        let value = format!("value{}", i);
        engine.set(key, value).unwrap();
    }

    for i in 0..100 {
        let key = format!("key{:03}", i);
        let expected = format!("value{}", i);
        assert_eq!(engine.get(key).unwrap(), expected);
    }
}

#[test]
fn test_btree_sorted_keys() {
    let dir = tempdir().unwrap();
    let engine = BTreeEngine::open(dir.path()).unwrap();

    // Insert keys in random order
    let keys = vec!["delta", "alpha", "charlie", "bravo", "echo"];
    for key in &keys {
        engine
            .set(key.to_string(), format!("value_{}", key))
            .unwrap();
    }

    // Verify all keys exist
    for key in &keys {
        assert_eq!(
            engine.get(key.to_string()).unwrap(),
            format!("value_{}", key)
        );
    }
}

#[test]
fn test_btree_persistence() {
    let dir = tempdir().unwrap();

    // Write data
    {
        let engine = BTreeEngine::open(dir.path()).unwrap();
        engine
            .set("key1".to_string(), "value1".to_string())
            .unwrap();
        engine
            .set("key2".to_string(), "value2".to_string())
            .unwrap();
        engine.flush().unwrap();
    }

    // Reopen and verify
    {
        let engine = BTreeEngine::open(dir.path()).unwrap();
        assert_eq!(engine.get("key1".to_string()).unwrap(), "value1");
        assert_eq!(engine.get("key2".to_string()).unwrap(), "value2");
    }
}

#[test]
fn test_btree_batch_set() {
    let dir = tempdir().unwrap();
    let engine = BTreeEngine::open(dir.path()).unwrap();

    let items: Vec<(String, String)> = (0..50)
        .map(|i| (format!("key{:03}", i), format!("value{}", i)))
        .collect();

    let count = engine.batch_set(items).unwrap();
    assert_eq!(count, 50);

    for i in 0..50 {
        let key = format!("key{:03}", i);
        let expected = format!("value{}", i);
        assert_eq!(engine.get(key).unwrap(), expected);
    }
}

#[test]
fn test_btree_batch_get() {
    let dir = tempdir().unwrap();
    let engine = BTreeEngine::open(dir.path()).unwrap();

    for i in 0..10 {
        engine
            .set(format!("key{}", i), format!("value{}", i))
            .unwrap();
    }

    let keys: Vec<String> = (0..10).map(|i| format!("key{}", i)).collect();
    let results = engine.batch_get(keys);

    assert_eq!(results.len(), 10);
}

#[test]
fn test_btree_batch_delete() {
    let dir = tempdir().unwrap();
    let engine = BTreeEngine::open(dir.path()).unwrap();

    for i in 0..10 {
        engine
            .set(format!("key{}", i), format!("value{}", i))
            .unwrap();
    }

    let keys: Vec<String> = (0..5).map(|i| format!("key{}", i)).collect();
    let count = engine.batch_delete(keys).unwrap();
    assert_eq!(count, 5);

    // Verify deleted
    for i in 0..5 {
        assert!(engine.get(format!("key{}", i)).is_err());
    }

    // Verify remaining
    for i in 5..10 {
        assert!(engine.get(format!("key{}", i)).is_ok());
    }
}

#[test]
fn test_btree_large_values() {
    let dir = tempdir().unwrap();
    let engine = BTreeEngine::open(dir.path()).unwrap();

    // Create a large value (1KB)
    let large_value: String = (0..1024).map(|_| 'x').collect();

    engine
        .set("large_key".to_string(), large_value.clone())
        .unwrap();
    assert_eq!(engine.get("large_key".to_string()).unwrap(), large_value);
}

#[test]
fn test_btree_node_splits() {
    let dir = tempdir().unwrap();
    let engine = BTreeEngine::open(dir.path()).unwrap();

    // Insert enough keys to cause splits (with reasonably sized keys/values)
    for i in 0..500 {
        let key = format!("key_{:05}", i);
        let value = format!("value_{:05}_{}", i, "x".repeat(50));
        engine.set(key, value).unwrap();
    }

    // Verify tree height increased
    assert!(engine.tree_height() > 1, "Tree should have split");

    // Verify all keys accessible
    for i in 0..500 {
        let key = format!("key_{:05}", i);
        assert!(engine.get(key).is_ok());
    }
}

#[test]
fn test_btree_entry_count() {
    let dir = tempdir().unwrap();
    let engine = BTreeEngine::open(dir.path()).unwrap();

    assert_eq!(engine.entry_count(), 0);

    engine
        .set("key1".to_string(), "value1".to_string())
        .unwrap();
    assert_eq!(engine.entry_count(), 1);

    engine
        .set("key2".to_string(), "value2".to_string())
        .unwrap();
    assert_eq!(engine.entry_count(), 2);

    engine.delete("key1".to_string()).unwrap();
    assert_eq!(engine.entry_count(), 1);
}

#[test]
fn test_btree_buffer_pool_stats() {
    let dir = tempdir().unwrap();
    let engine = BTreeEngine::open(dir.path()).unwrap();

    for i in 0..100 {
        engine
            .set(format!("key{}", i), format!("value{}", i))
            .unwrap();
    }

    for i in 0..100 {
        let _ = engine.get(format!("key{}", i));
    }

    let stats = engine.buffer_pool_stats();
    assert!(stats.hits > 0 || stats.misses > 0);
}

#[test]
fn test_btree_checkpoint() {
    let dir = tempdir().unwrap();
    let engine = BTreeEngine::open(dir.path()).unwrap();

    engine
        .set("key1".to_string(), "value1".to_string())
        .unwrap();
    engine.checkpoint().unwrap();
    engine
        .set("key2".to_string(), "value2".to_string())
        .unwrap();

    // Both keys should be accessible
    assert_eq!(engine.get("key1".to_string()).unwrap(), "value1");
    assert_eq!(engine.get("key2".to_string()).unwrap(), "value2");
}

#[test]
fn test_btree_wal_recovery() {
    let dir = tempdir().unwrap();

    // Write data and don't flush (simulate crash)
    {
        let engine = BTreeEngine::open(dir.path()).unwrap();
        engine
            .set("key1".to_string(), "value1".to_string())
            .unwrap();
        engine
            .set("key2".to_string(), "value2".to_string())
            .unwrap();
        // Don't call flush() - data only in WAL
    }

    // Reopen - should recover from WAL
    {
        let engine = BTreeEngine::open(dir.path()).unwrap();
        assert_eq!(engine.get("key1".to_string()).unwrap(), "value1");
        assert_eq!(engine.get("key2".to_string()).unwrap(), "value2");
    }
}

#[test]
fn test_btree_without_wal() {
    let dir = tempdir().unwrap();
    let config = BTreeConfig {
        buffer_pool_pages: 100,
        enable_wal: false,
        wal_batch_interval_micros: 100,
    };

    let engine = BTreeEngine::open_with_config(dir.path(), config).unwrap();
    engine
        .set("key1".to_string(), "value1".to_string())
        .unwrap();
    assert_eq!(engine.get("key1".to_string()).unwrap(), "value1");
}

#[test]
fn test_btree_unicode_keys() {
    let dir = tempdir().unwrap();
    let engine = BTreeEngine::open(dir.path()).unwrap();

    engine
        .set("日本語キー".to_string(), "日本語値".to_string())
        .unwrap();
    engine
        .set("emoji_🎉".to_string(), "party_value".to_string())
        .unwrap();
    engine
        .set("混合mixed".to_string(), "混合value".to_string())
        .unwrap();

    assert_eq!(engine.get("日本語キー".to_string()).unwrap(), "日本語値");
    assert_eq!(engine.get("emoji_🎉".to_string()).unwrap(), "party_value");
    assert_eq!(engine.get("混合mixed".to_string()).unwrap(), "混合value");
}

#[test]
fn test_btree_empty_values() {
    let dir = tempdir().unwrap();
    let engine = BTreeEngine::open(dir.path()).unwrap();

    engine.set("key1".to_string(), "".to_string()).unwrap();
    assert_eq!(engine.get("key1".to_string()).unwrap(), "");
}

#[test]
fn test_btree_sequential_vs_random_insert() {
    let dir = tempdir().unwrap();
    let engine = BTreeEngine::open(dir.path()).unwrap();

    // Sequential insert
    for i in 0..100 {
        engine
            .set(format!("{:05}", i), format!("value{}", i))
            .unwrap();
    }

    // Random access
    for i in (0..100).rev().step_by(3) {
        assert!(engine.get(format!("{:05}", i)).is_ok());
    }
}
