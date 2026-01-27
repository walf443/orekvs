use std::net::{SocketAddr, TcpListener};
use std::time::Duration;
use tempfile::TempDir;
use tokio::time::sleep;

use orelsm::engine::lsm_tree::WalArchiveConfig;
use orelsm::server::kv::key_value_client::KeyValueClient;
use orelsm::server::kv::{GetRequest, PromoteRequest, SetRequest};

/// Get an available port by binding to port 0 and letting the OS assign one
fn get_available_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("Failed to bind to port 0");
    listener.local_addr().unwrap().port()
}

/// Test configuration
struct TestNode {
    data_dir: TempDir,
    addr: SocketAddr,
    replication_port: Option<u16>,
}

impl TestNode {
    fn new(with_replication: bool) -> Self {
        let port = get_available_port();
        let replication_port = if with_replication {
            Some(get_available_port())
        } else {
            None
        };
        Self {
            data_dir: TempDir::new().expect("Failed to create temp dir"),
            addr: format!("127.0.0.1:{}", port).parse().unwrap(),
            replication_port,
        }
    }

    fn data_path(&self) -> String {
        self.data_dir.path().to_str().unwrap().to_string()
    }
}

/// Wait for a server to be ready by trying to connect
async fn wait_for_server(addr: &str, timeout: Duration) -> bool {
    let start = std::time::Instant::now();
    while start.elapsed() < timeout {
        if KeyValueClient::connect(addr.to_string()).await.is_ok() {
            return true;
        }
        sleep(Duration::from_millis(100)).await;
    }
    false
}

/// Helper to set a key-value pair
async fn set_key(addr: &str, key: &str, value: &str) -> Result<(), Box<dyn std::error::Error>> {
    let mut client = KeyValueClient::connect(addr.to_string()).await?;
    client
        .set(SetRequest {
            key: key.to_string(),
            value: value.to_string(),
            ttl_seconds: 0,
        })
        .await?;
    Ok(())
}

/// Helper to set a key-value pair with TTL
async fn set_key_with_ttl(
    addr: &str,
    key: &str,
    value: &str,
    ttl_seconds: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut client = KeyValueClient::connect(addr.to_string()).await?;
    client
        .set(SetRequest {
            key: key.to_string(),
            value: value.to_string(),
            ttl_seconds,
        })
        .await?;
    Ok(())
}

/// Helper to check if a key exists (returns None if not found or expired)
async fn get_key_optional(
    addr: &str,
    key: &str,
) -> Result<Option<String>, Box<dyn std::error::Error>> {
    let mut client = KeyValueClient::connect(addr.to_string()).await?;
    let response = client
        .get(GetRequest {
            key: key.to_string(),
        })
        .await;
    match response {
        Ok(resp) => {
            let value = resp.into_inner().value;
            if value.is_empty() {
                Ok(None)
            } else {
                Ok(Some(value))
            }
        }
        Err(status) if status.code() == tonic::Code::NotFound => Ok(None),
        Err(e) => Err(e.into()),
    }
}

/// Helper to get a value
async fn get_key(addr: &str, key: &str) -> Result<String, Box<dyn std::error::Error>> {
    let mut client = KeyValueClient::connect(addr.to_string()).await?;
    let response = client
        .get(GetRequest {
            key: key.to_string(),
        })
        .await?;
    Ok(response.into_inner().value)
}

/// Helper to promote a follower to leader
async fn promote_to_leader(
    addr: &str,
    enable_replication: bool,
    replication_port: u32,
) -> Result<String, Box<dyn std::error::Error>> {
    let mut client = KeyValueClient::connect(addr.to_string()).await?;
    let response = client
        .promote_to_leader(PromoteRequest {
            enable_replication_service: enable_replication,
            replication_port,
        })
        .await?;
    let inner = response.into_inner();
    if inner.success {
        Ok(inner.message)
    } else {
        Err(inner.message.into())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_leader_follower_replication() {
    // Configure nodes with random ports
    let leader = TestNode::new(true);
    let follower1 = TestNode::new(false);
    let follower2 = TestNode::new(false);

    let leader_addr = format!("http://{}", leader.addr);
    let leader_repl_addr = format!("http://127.0.0.1:{}", leader.replication_port.unwrap());
    let follower1_addr = format!("http://{}", follower1.addr);
    let follower2_addr = format!("http://{}", follower2.addr);

    // Start leader
    let leader_data = leader.data_path();
    let leader_socket = leader.addr;
    let leader_repl_port = leader.replication_port.unwrap();
    let leader_handle = tokio::spawn(async move {
        orelsm::server::run_server(
            leader_socket,
            orelsm::server::EngineType::LsmTree,
            leader_data,
            1024 * 1024, // log capacity
            64 * 1024,   // memtable capacity (small for testing)
            2,           // compaction trigger
            100,         // wal batch interval micros
            Some(format!("127.0.0.1:{}", leader_repl_port).parse().unwrap()),
            WalArchiveConfig::disabled(), // Keep all WAL for replication
        )
        .await;
    });

    // Wait for leader to start
    assert!(
        wait_for_server(&leader_addr, Duration::from_secs(5)).await,
        "Leader failed to start"
    );
    println!("Leader started on {}", leader.addr);

    // Write enough initial data to trigger SSTable flush before followers connect
    // memtable capacity is 64KB, so we write enough data to exceed that
    println!("Writing initial data to leader (enough to trigger SSTable flush)...");
    let large_value = "x".repeat(1024); // 1KB value
    for i in 0..100 {
        // 100 * ~1KB = ~100KB > 64KB memtable capacity
        set_key(
            &leader_addr,
            &format!("initial_key{}", i),
            &format!("{}_{}", large_value, i),
        )
        .await
        .expect("Failed to write initial data to leader");
    }
    // Wait for flush to complete
    sleep(Duration::from_millis(500)).await;
    println!("Wrote 100 initial key-value pairs to leader (SSTable should be created)");

    // Start follower1
    let follower1_data = follower1.data_path();
    let follower1_socket = follower1.addr;
    let leader_repl_clone = leader_repl_addr.clone();
    let follower1_handle = tokio::spawn(async move {
        orelsm::server::run_follower(
            leader_repl_clone,
            follower1_data,
            64 * 1024, // memtable capacity
            2,         // compaction trigger
            100,       // wal batch interval micros
            follower1_socket,
            WalArchiveConfig::disabled(),
        )
        .await;
    });

    // Start follower2
    let follower2_data = follower2.data_path();
    let follower2_socket = follower2.addr;
    let leader_repl_clone2 = leader_repl_addr.clone();
    let follower2_handle = tokio::spawn(async move {
        orelsm::server::run_follower(
            leader_repl_clone2,
            follower2_data,
            64 * 1024, // memtable capacity
            2,         // compaction trigger
            100,       // wal batch interval micros
            follower2_socket,
            WalArchiveConfig::disabled(),
        )
        .await;
    });

    // Wait for followers to start
    assert!(
        wait_for_server(&follower1_addr, Duration::from_secs(5)).await,
        "Follower1 failed to start"
    );
    println!("Follower1 started on {}", follower1.addr);

    assert!(
        wait_for_server(&follower2_addr, Duration::from_secs(5)).await,
        "Follower2 failed to start"
    );
    println!("Follower2 started on {}", follower2.addr);

    // Give replication time to establish connection
    sleep(Duration::from_millis(500)).await;

    // Write data to leader
    println!("Writing data to leader...");
    for i in 0..10 {
        set_key(&leader_addr, &format!("key{}", i), &format!("value{}", i))
            .await
            .expect("Failed to write to leader");
    }
    println!("Wrote 10 key-value pairs to leader");

    // Wait for replication
    sleep(Duration::from_secs(1)).await;

    // Verify data on followers (including initial data written before followers started)
    println!("Verifying data on followers...");

    // Verify initial data (written before followers connected, includes SSTable data)
    let large_value = "x".repeat(1024);
    for i in 0..100 {
        let key = format!("initial_key{}", i);
        let expected = format!("{}_{}", large_value, i);

        let value1 = get_key(&follower1_addr, &key)
            .await
            .expect("Failed to read initial data from follower1");
        assert_eq!(
            value1, expected,
            "Follower1 initial data mismatch for {}",
            key
        );

        let value2 = get_key(&follower2_addr, &key)
            .await
            .expect("Failed to read initial data from follower2");
        assert_eq!(
            value2, expected,
            "Follower2 initial data mismatch for {}",
            key
        );
    }
    println!("Initial data (including SSTable data) verified on both followers");

    // Verify live data (written after followers connected)
    for i in 0..10 {
        let key = format!("key{}", i);
        let expected = format!("value{}", i);

        let value1 = get_key(&follower1_addr, &key)
            .await
            .expect("Failed to read from follower1");
        assert_eq!(value1, expected, "Follower1 data mismatch for {}", key);

        let value2 = get_key(&follower2_addr, &key)
            .await
            .expect("Failed to read from follower2");
        assert_eq!(value2, expected, "Follower2 data mismatch for {}", key);
    }
    println!("All data verified on both followers");

    // Test: Follower should reject writes
    let write_result = set_key(&follower1_addr, "test", "should_fail").await;
    assert!(write_result.is_err(), "Follower should reject writes");
    println!("Verified follower rejects writes");

    // Promote follower1 to leader
    println!("Promoting follower1 to leader...");
    let new_repl_port = get_available_port();
    let promote_result = promote_to_leader(&follower1_addr, true, new_repl_port as u32).await;
    assert!(promote_result.is_ok(), "Failed to promote follower1");
    println!("Follower1 promoted: {}", promote_result.unwrap());

    // Wait for promotion to complete
    sleep(Duration::from_millis(500)).await;

    // Test: Promoted follower should accept writes
    set_key(&follower1_addr, "promoted_key", "promoted_value")
        .await
        .expect("Promoted follower should accept writes");
    println!("Verified promoted follower accepts writes");

    // Verify the new data can be read
    let value = get_key(&follower1_addr, "promoted_key")
        .await
        .expect("Failed to read from promoted leader");
    assert_eq!(value, "promoted_value");
    println!("Verified data written to new leader");

    // Cleanup: abort all handles (they run forever otherwise)
    leader_handle.abort();
    follower1_handle.abort();
    follower2_handle.abort();

    println!("Test completed successfully!");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_follower_failover() {
    // This test simulates a failover scenario:
    // 1. Leader writes data
    // 2. Leader stops
    // 3. Follower is promoted to leader
    // 4. New leader continues serving

    let leader = TestNode::new(true);
    let follower = TestNode::new(false);

    let leader_addr = format!("http://{}", leader.addr);
    let leader_repl_addr = format!("http://127.0.0.1:{}", leader.replication_port.unwrap());
    let follower_addr = format!("http://{}", follower.addr);

    // Start leader
    let leader_data = leader.data_path();
    let leader_socket = leader.addr;
    let leader_repl_port = leader.replication_port.unwrap();
    let leader_handle = tokio::spawn(async move {
        orelsm::server::run_server(
            leader_socket,
            orelsm::server::EngineType::LsmTree,
            leader_data,
            1024 * 1024, // log capacity
            64 * 1024,   // memtable capacity
            2,           // compaction trigger
            100,         // wal batch interval micros
            Some(format!("127.0.0.1:{}", leader_repl_port).parse().unwrap()),
            WalArchiveConfig::disabled(),
        )
        .await;
    });

    assert!(
        wait_for_server(&leader_addr, Duration::from_secs(5)).await,
        "Leader failed to start"
    );
    println!("[Failover] Leader started");

    // Write enough initial data to trigger SSTable flush before follower starts
    let large_value = "x".repeat(1024); // 1KB value
    for i in 0..100 {
        // 100 * ~1KB = ~100KB > 64KB memtable capacity
        set_key(
            &leader_addr,
            &format!("initial_failover_key{}", i),
            &format!("{}_{}", large_value, i),
        )
        .await
        .expect("Failed to write initial data to leader");
    }
    // Wait for flush to complete
    sleep(Duration::from_millis(500)).await;
    println!("[Failover] Wrote 100 initial keys to leader (SSTable should be created)");

    // Start follower
    let follower_data = follower.data_path();
    let follower_socket = follower.addr;
    let follower_handle = tokio::spawn(async move {
        orelsm::server::run_follower(
            leader_repl_addr,
            follower_data,
            64 * 1024, // memtable capacity
            2,         // compaction trigger
            100,       // wal batch interval micros
            follower_socket,
            WalArchiveConfig::disabled(),
        )
        .await;
    });

    assert!(
        wait_for_server(&follower_addr, Duration::from_secs(5)).await,
        "Follower failed to start"
    );
    println!("[Failover] Follower started");

    sleep(Duration::from_millis(500)).await;

    // Write data to leader
    for i in 0..5 {
        set_key(
            &leader_addr,
            &format!("failover_key{}", i),
            &format!("failover_value{}", i),
        )
        .await
        .expect("Failed to write to leader");
    }
    println!("[Failover] Wrote 5 keys to leader");

    // Wait for replication
    sleep(Duration::from_secs(1)).await;

    // Verify follower has the data (including initial SSTable data written before follower started)
    let large_value = "x".repeat(1024);
    for i in 0..100 {
        let value = get_key(&follower_addr, &format!("initial_failover_key{}", i))
            .await
            .expect("Failed to read initial data from follower");
        assert_eq!(value, format!("{}_{}", large_value, i));
    }
    println!("[Failover] Initial data (including SSTable data) verified on follower");

    for i in 0..5 {
        let value = get_key(&follower_addr, &format!("failover_key{}", i))
            .await
            .expect("Failed to read from follower");
        assert_eq!(value, format!("failover_value{}", i));
    }
    println!("[Failover] All data verified on follower");

    // Simulate leader failure by aborting
    leader_handle.abort();
    println!("[Failover] Leader stopped (simulating failure)");

    // Wait a bit
    sleep(Duration::from_millis(500)).await;

    // Promote follower to leader
    let promote_result = promote_to_leader(&follower_addr, false, 0).await;
    assert!(promote_result.is_ok(), "Failed to promote follower");
    println!("[Failover] Follower promoted to leader");

    // Verify new leader has all the data (including initial SSTable data)
    for i in 0..100 {
        let value = get_key(&follower_addr, &format!("initial_failover_key{}", i))
            .await
            .expect("Failed to read initial data from new leader");
        assert_eq!(value, format!("{}_{}", large_value, i));
    }
    for i in 0..5 {
        let value = get_key(&follower_addr, &format!("failover_key{}", i))
            .await
            .expect("Failed to read from new leader");
        assert_eq!(value, format!("failover_value{}", i));
    }
    println!("[Failover] All data preserved after failover");

    // Write new data to the new leader
    set_key(&follower_addr, "new_leader_key", "new_leader_value")
        .await
        .expect("New leader should accept writes");
    println!("[Failover] New leader accepts writes");

    follower_handle.abort();
    println!("[Failover] Test completed successfully!");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_ttl_replication() {
    // This test verifies that TTL is preserved during replication:
    // 1. Leader sets key with TTL
    // 2. Follower receives the key via replication
    // 3. TTL expires on both leader and follower at the same time

    let leader = TestNode::new(true);
    let follower = TestNode::new(false);

    let leader_addr = format!("http://{}", leader.addr);
    let leader_repl_addr = format!("http://127.0.0.1:{}", leader.replication_port.unwrap());
    let follower_addr = format!("http://{}", follower.addr);

    // Start leader
    let leader_data = leader.data_path();
    let leader_socket = leader.addr;
    let leader_repl_port = leader.replication_port.unwrap();
    let leader_handle = tokio::spawn(async move {
        orelsm::server::run_server(
            leader_socket,
            orelsm::server::EngineType::LsmTree,
            leader_data,
            1024 * 1024, // log capacity
            64 * 1024,   // memtable capacity
            2,           // compaction trigger
            100,         // wal batch interval micros
            Some(format!("127.0.0.1:{}", leader_repl_port).parse().unwrap()),
            WalArchiveConfig::disabled(),
        )
        .await;
    });

    assert!(
        wait_for_server(&leader_addr, Duration::from_secs(5)).await,
        "Leader failed to start"
    );
    println!("[TTL Replication] Leader started");

    // Start follower
    let follower_data = follower.data_path();
    let follower_socket = follower.addr;
    let follower_handle = tokio::spawn(async move {
        orelsm::server::run_follower(
            leader_repl_addr,
            follower_data,
            64 * 1024, // memtable capacity
            2,         // compaction trigger
            100,       // wal batch interval micros
            follower_socket,
            WalArchiveConfig::disabled(),
        )
        .await;
    });

    assert!(
        wait_for_server(&follower_addr, Duration::from_secs(5)).await,
        "Follower failed to start"
    );
    println!("[TTL Replication] Follower started");

    // Wait for replication connection
    sleep(Duration::from_millis(500)).await;

    // Set key with 3 second TTL on leader
    let ttl_seconds = 3;
    set_key_with_ttl(&leader_addr, "ttl_key", "ttl_value", ttl_seconds)
        .await
        .expect("Failed to set key with TTL on leader");
    println!(
        "[TTL Replication] Set key with {} second TTL on leader",
        ttl_seconds
    );

    // Also set a key without TTL for comparison
    set_key(&leader_addr, "no_ttl_key", "no_ttl_value")
        .await
        .expect("Failed to set key without TTL");
    println!("[TTL Replication] Set key without TTL on leader");

    // Wait for replication
    sleep(Duration::from_secs(1)).await;

    // Verify both keys exist on follower
    let ttl_value = get_key_optional(&follower_addr, "ttl_key")
        .await
        .expect("Failed to read from follower");
    assert_eq!(
        ttl_value,
        Some("ttl_value".to_string()),
        "TTL key should exist on follower before expiration"
    );
    println!("[TTL Replication] TTL key verified on follower before expiration");

    let no_ttl_value = get_key_optional(&follower_addr, "no_ttl_key")
        .await
        .expect("Failed to read from follower");
    assert_eq!(
        no_ttl_value,
        Some("no_ttl_value".to_string()),
        "Non-TTL key should exist on follower"
    );
    println!("[TTL Replication] Non-TTL key verified on follower");

    // Wait for TTL to expire (3 seconds + buffer)
    println!("[TTL Replication] Waiting for TTL to expire...");
    sleep(Duration::from_secs(3)).await;

    // Verify TTL key has expired on leader
    let leader_ttl_value = get_key_optional(&leader_addr, "ttl_key")
        .await
        .expect("Failed to read from leader");
    assert_eq!(
        leader_ttl_value, None,
        "TTL key should have expired on leader"
    );
    println!("[TTL Replication] TTL key verified as expired on leader");

    // Verify TTL key has expired on follower (the main test!)
    let follower_ttl_value = get_key_optional(&follower_addr, "ttl_key")
        .await
        .expect("Failed to read from follower");
    assert_eq!(
        follower_ttl_value, None,
        "TTL key should have expired on follower - TTL was preserved during replication!"
    );
    println!("[TTL Replication] TTL key verified as expired on follower - TTL preserved!");

    // Verify non-TTL key still exists on both
    let leader_no_ttl = get_key_optional(&leader_addr, "no_ttl_key")
        .await
        .expect("Failed to read from leader");
    assert_eq!(
        leader_no_ttl,
        Some("no_ttl_value".to_string()),
        "Non-TTL key should still exist on leader"
    );

    let follower_no_ttl = get_key_optional(&follower_addr, "no_ttl_key")
        .await
        .expect("Failed to read from follower");
    assert_eq!(
        follower_no_ttl,
        Some("no_ttl_value".to_string()),
        "Non-TTL key should still exist on follower"
    );
    println!("[TTL Replication] Non-TTL key still exists on both nodes");

    // Cleanup
    leader_handle.abort();
    follower_handle.abort();
    println!("[TTL Replication] Test completed successfully!");
}
