use clap::{Parser, Subcommand};
use orelsm::engine::lsm_tree::WalArchiveConfig;
use orelsm::server;
use rand::distributions::Alphanumeric;
use rand::{Rng, thread_rng};
use serde::Serialize;
use server::EngineType;
use server::kv::key_value_client::KeyValueClient;
use server::kv::{
    BatchDeleteRequest, BatchGetRequest, BatchSetRequest, CompareAndSetRequest, DeleteRequest,
    GetExpireAtRequest, GetMetricsRequest, GetRequest, KeyValuePair, SetRequest,
};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Semaphore;

#[derive(Parser)]
#[command(name = "orelsm")]
#[command(about = "A simple LSM-tree implementation", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Start the key-value store server
    Server {
        #[arg(long, default_value = "127.0.0.1:50051")]
        addr: String,

        /// Storage engine to use (ignored if --leader-addr is set, uses lsm-tree)
        #[arg(long, value_enum, default_value_t = EngineType::Memory)]
        engine: EngineType,

        /// Data directory for storage engines
        #[arg(long, default_value = "kv_data")]
        data_dir: String,

        /// Max size for log file in bytes for Log engine
        #[arg(long = "log-engine-capacity-bytes", default_value_t = 1024)]
        log_capacity_bytes: u64,

        /// MemTable flush trigger size in bytes for LSM-tree engine
        #[arg(long = "lsm-engine-memtable-capacity-bytes", default_value_t = 4194304)]
        // 4MB
        lsm_memtable_capacity_bytes: u64,

        /// Compaction trigger (number of SSTables) for LSM-tree engine
        #[arg(long = "lsm-engine-compaction-trigger-file-count", default_value_t = 4)]
        lsm_compaction_trigger_file_count: usize,

        /// WAL group commit batch interval in microseconds for LSM-tree engine
        /// Higher values increase throughput but also increase latency
        #[arg(long = "lsm-engine-wal-batch-interval-micros", default_value_t = 100)]
        lsm_wal_batch_interval_micros: u64,

        /// Enable replication service for followers to connect (leader mode)
        #[arg(long)]
        enable_replication: bool,

        /// Port for replication service (default: main port + 1)
        #[arg(long)]
        replication_port: Option<u16>,

        /// Leader address to replicate from (follower mode, e.g., http://127.0.0.1:50052)
        /// When set, this server becomes a read-only follower
        #[arg(long)]
        leader_addr: Option<String>,

        /// WAL retention period in seconds for LSM-tree engine (default: 604800 = 7 days, 0 = disabled)
        #[arg(long = "lsm-engine-wal-retention-secs")]
        lsm_wal_retention_secs: Option<u64>,

        /// Maximum total WAL size in bytes for LSM-tree engine (default: 1073741824 = 1GB, 0 = disabled)
        #[arg(long = "lsm-engine-wal-max-size-bytes")]
        lsm_wal_max_size_bytes: Option<u64>,

        /// Disable WAL archiving for LSM-tree engine (keep all WAL files for replication)
        #[arg(long = "lsm-engine-disable-wal-archive")]
        lsm_disable_wal_archive: bool,
    },
    /// Test commands
    Test {
        /// Server address to connect to
        #[arg(long, default_value = "http://127.0.0.1:50051")]
        addr: String,

        #[command(subcommand)]
        command: TestCommands,
    },
}

#[derive(Subcommand)]
enum TestCommands {
    /// Set multiple random key-value pairs
    RandomSet {
        /// Number of key-value pairs to generate and set
        count: usize,
        /// Concurrency level
        #[arg(short, long, default_value_t = 1)]
        parallel: usize,
        /// Maximum key value (1..N)
        #[arg(long, default_value_t = 1000000)]
        key_range: u32,
        /// Value size in bytes
        #[arg(long, default_value_t = 20)]
        value_size: usize,
        /// TTL in seconds (0 = no expiration)
        #[arg(long, default_value_t = 0)]
        ttl: u64,
    },
    /// Get multiple random keys
    RandomGet {
        /// Number of keys to generate and get
        count: usize,
        /// Concurrency level
        #[arg(short, long, default_value_t = 1)]
        parallel: usize,
        /// Maximum key value (1..N)
        #[arg(long, default_value_t = 1000000)]
        key_range: u32,
    },
    /// Batch set multiple random key-value pairs
    BatchSet {
        /// Total number of key-value pairs to set
        count: usize,
        /// Number of items per batch
        #[arg(short, long, default_value_t = 100)]
        batch_size: usize,
        /// Maximum key value (1..N)
        #[arg(long, default_value_t = 1000000)]
        key_range: u32,
    },
    /// Batch get multiple random keys
    BatchGet {
        /// Total number of keys to get
        count: usize,
        /// Number of keys per batch
        #[arg(short, long, default_value_t = 100)]
        batch_size: usize,
        /// Maximum key value (1..N)
        #[arg(long, default_value_t = 1000000)]
        key_range: u32,
        /// Duplicate rate (0.0 = no duplicates, 0.5 = 50% duplicates)
        #[arg(long, default_value_t = 0.0)]
        duplicate_rate: f64,
    },
    /// Batch delete multiple random keys
    BatchDelete {
        /// Total number of keys to delete
        count: usize,
        /// Number of keys per batch
        #[arg(short, long, default_value_t = 100)]
        batch_size: usize,
        /// Maximum key value (1..N)
        #[arg(long, default_value_t = 1000000)]
        key_range: u32,
    },
    /// Set a key-value pair
    Set {
        key: String,
        value: String,
        /// TTL in seconds (0 = no expiration)
        #[arg(long, default_value_t = 0)]
        ttl: u64,
        /// Only set if key does not exist (like Redis NX)
        #[arg(long, conflicts_with = "if_match")]
        if_not_exists: bool,
        /// Only set if current value matches this value (CAS)
        #[arg(long, conflicts_with = "if_not_exists")]
        if_match: Option<String>,
    },
    /// Get the value of a key
    Get {
        key: String,
        /// Include expiration timestamp in the response
        #[arg(long)]
        with_expire_at: bool,
    },
    /// Get the expiration timestamp of a key
    GetExpireAt { key: String },
    /// Delete a key
    Delete { key: String },
    /// Get engine metrics
    Metrics {
        /// Output in JSON format
        #[arg(long)]
        json: bool,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    match &cli.command {
        Commands::Server {
            addr,
            engine,
            data_dir,
            log_capacity_bytes,
            lsm_memtable_capacity_bytes,
            lsm_compaction_trigger_file_count,
            lsm_wal_batch_interval_micros,
            enable_replication,
            replication_port,
            leader_addr,
            lsm_wal_retention_secs,
            lsm_wal_max_size_bytes,
            lsm_disable_wal_archive,
        } => {
            let addr: std::net::SocketAddr = addr.parse()?;

            // Build WAL archive config
            let wal_archive_config = if *lsm_disable_wal_archive {
                WalArchiveConfig::disabled()
            } else {
                WalArchiveConfig {
                    retention_secs: match lsm_wal_retention_secs {
                        Some(0) => None,
                        Some(secs) => Some(*secs),
                        None => Some(orelsm::engine::lsm_tree::DEFAULT_WAL_RETENTION_SECS),
                    },
                    max_size_bytes: match lsm_wal_max_size_bytes {
                        Some(0) => None,
                        Some(bytes) => Some(*bytes),
                        None => Some(orelsm::engine::lsm_tree::DEFAULT_WAL_MAX_SIZE_BYTES),
                    },
                }
            };

            // If leader_addr is set, run as follower
            if let Some(leader) = leader_addr {
                server::run_follower(
                    leader.clone(),
                    data_dir.clone(),
                    *lsm_memtable_capacity_bytes,
                    *lsm_compaction_trigger_file_count,
                    *lsm_wal_batch_interval_micros,
                    addr,
                    wal_archive_config,
                )
                .await;
            } else {
                // Run as leader/standalone
                let repl_addr = if *enable_replication {
                    let port = replication_port.unwrap_or(addr.port() + 1);
                    Some(std::net::SocketAddr::new(addr.ip(), port))
                } else {
                    None
                };
                server::run_server(
                    addr,
                    engine.clone(),
                    data_dir.clone(),
                    *log_capacity_bytes,
                    *lsm_memtable_capacity_bytes,
                    *lsm_compaction_trigger_file_count,
                    *lsm_wal_batch_interval_micros,
                    repl_addr,
                    wal_archive_config,
                )
                .await;
            }
            Ok(())
        }
        Commands::Test { addr, command } => match command {
            TestCommands::RandomSet {
                count,
                parallel,
                key_range,
                value_size,
                ttl,
            } => {
                run_test_set(
                    addr.clone(),
                    *count,
                    *parallel,
                    *key_range,
                    *value_size,
                    *ttl,
                )
                .await
            }
            TestCommands::RandomGet {
                count,
                parallel,
                key_range,
            } => run_test_get(addr.clone(), *count, *parallel, *key_range).await,
            TestCommands::BatchSet {
                count,
                batch_size,
                key_range,
            } => run_test_batch_set(addr.clone(), *count, *batch_size, *key_range).await,
            TestCommands::BatchGet {
                count,
                batch_size,
                key_range,
                duplicate_rate,
            } => {
                run_test_batch_get(
                    addr.clone(),
                    *count,
                    *batch_size,
                    *key_range,
                    *duplicate_rate,
                )
                .await
            }
            TestCommands::BatchDelete {
                count,
                batch_size,
                key_range,
            } => run_test_batch_delete(addr.clone(), *count, *batch_size, *key_range).await,
            TestCommands::Set {
                key,
                value,
                ttl,
                if_not_exists,
                if_match,
            } => {
                let mut client = KeyValueClient::connect(addr.clone()).await?;

                // Use CAS if conditional options are specified
                if *if_not_exists || if_match.is_some() {
                    let (expected_value, expect_exists) = if *if_not_exists {
                        (String::new(), false)
                    } else {
                        (if_match.clone().unwrap(), true)
                    };

                    let request = tonic::Request::new(CompareAndSetRequest {
                        key: key.clone(),
                        expected_value,
                        new_value: value.clone(),
                        expect_exists,
                        ttl_seconds: *ttl,
                    });

                    match client.compare_and_set(request).await {
                        Ok(response) => {
                            let resp = response.into_inner();
                            if resp.success {
                                println!("OK");
                            } else {
                                println!(
                                    "FAILED: current_value={}",
                                    if resp.current_value.is_empty() {
                                        "(key not found)".to_string()
                                    } else {
                                        format!("\"{}\"", resp.current_value)
                                    }
                                );
                            }
                        }
                        Err(e) => {
                            println!("Error: {}", e);
                        }
                    }
                } else {
                    // Normal set
                    let request = tonic::Request::new(SetRequest {
                        key: key.clone(),
                        value: value.clone(),
                        ttl_seconds: *ttl,
                    });
                    let response = client.set(request).await?;
                    if response.into_inner().success {
                        println!("OK");
                    }
                }
                Ok(())
            }
            TestCommands::Get {
                key,
                with_expire_at,
            } => {
                let mut client = KeyValueClient::connect(addr.clone()).await?;
                let request = tonic::Request::new(GetRequest {
                    key: key.clone(),
                    include_expire_at: *with_expire_at,
                });
                match client.get(request).await {
                    Ok(response) => {
                        let resp = response.into_inner();
                        if *with_expire_at {
                            println!("value={} expire_at={}", resp.value, resp.expire_at);
                        } else {
                            println!("{}", resp.value);
                        }
                    }
                    Err(e) => {
                        println!("Error: {}", e);
                    }
                }
                Ok(())
            }
            TestCommands::GetExpireAt { key } => {
                let mut client = KeyValueClient::connect(addr.clone()).await?;
                let request = tonic::Request::new(GetExpireAtRequest { key: key.clone() });
                match client.get_expire_at(request).await {
                    Ok(response) => {
                        let resp = response.into_inner();
                        if resp.exists {
                            if resp.expire_at == 0 {
                                println!("Key exists, no expiration");
                            } else {
                                println!("Key expires at: {} (Unix timestamp)", resp.expire_at);
                            }
                        } else {
                            println!("Key does not exist");
                        }
                    }
                    Err(e) => {
                        println!("Error: {}", e);
                    }
                }
                Ok(())
            }
            TestCommands::Delete { key } => {
                let mut client = KeyValueClient::connect(addr.clone()).await?;
                let request = tonic::Request::new(DeleteRequest { key: key.clone() });
                let response = client.delete(request).await?;
                println!("RESPONSE={:?}", response);
                Ok(())
            }
            TestCommands::Metrics { json } => run_test_metrics(addr.clone(), *json).await,
        },
    }
}

async fn run_test_set(
    addr: String,
    count: usize,
    parallel: usize,
    key_range: u32,
    value_size: usize,
    ttl: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    if ttl > 0 {
        println!(
            "Generating and setting {} random key-value pairs (range 1..={}, value_size={}, ttl={}s) with parallelism {} to {}...",
            count, key_range, value_size, ttl, parallel, addr
        );
    } else {
        println!(
            "Generating and setting {} random key-value pairs (range 1..={}, value_size={}) with parallelism {} to {}...",
            count, key_range, value_size, parallel, addr
        );
    }

    let client = KeyValueClient::connect(addr).await?;
    let semaphore = Arc::new(Semaphore::new(parallel));
    let mut handles = Vec::with_capacity(count);
    let start_time = Instant::now();

    for i in 0..count {
        let permit = semaphore.clone().acquire_owned().await.unwrap();
        let mut client_clone = client.clone();
        let handle = tokio::spawn(async move {
            let (key, value) = {
                let mut rng = thread_rng();
                let key = rng.gen_range(1..=key_range).to_string();
                let value: String = (0..value_size)
                    .map(|_| rng.sample(Alphanumeric) as char)
                    .collect();
                (key, value)
            };

            let request = tonic::Request::new(SetRequest {
                key: key.clone(),
                value: value.clone(),
                ttl_seconds: ttl,
            });

            let req_start = Instant::now();
            let res = client_clone.set(request).await;
            let duration = req_start.elapsed();

            let result = if let Err(e) = res {
                println!("Failed to set key {}: {}", key, e);
                None
            } else {
                if count <= 10 || i < 5 || i >= count - 5 {
                    println!("[{}/{}] Setting {} = {}", i + 1, count, key, value);
                } else if i == 5 {
                    println!("...");
                }
                Some(duration)
            };
            drop(permit);
            result
        });
        handles.push(handle);
    }

    let mut successful_durations = Vec::new();
    for handle in handles {
        if let Ok(Some(duration)) = handle.await {
            successful_durations.push(duration);
        }
    }

    let total_elapsed = start_time.elapsed();
    let success_count = successful_durations.len();

    println!("\nSummary:");
    println!("Total count: {}", count);
    println!("Success count: {}", success_count);
    println!("Total elapsed time: {:?}", total_elapsed);

    if success_count > 0 {
        let avg_duration: std::time::Duration =
            successful_durations.iter().sum::<std::time::Duration>() / success_count as u32;
        println!("Average request time (success only): {:?}", avg_duration);
        println!(
            "Throughput: {:.2} req/sec",
            success_count as f64 / total_elapsed.as_secs_f64()
        );
    }

    Ok(())
}

async fn run_test_get(
    addr: String,
    count: usize,
    parallel: usize,
    key_range: u32,
) -> Result<(), Box<dyn std::error::Error>> {
    println!(
        "Generating and getting {} random keys (range 1..={}) with parallelism {} from {}...",
        count, key_range, parallel, addr
    );

    let client = KeyValueClient::connect(addr).await?;
    let semaphore = Arc::new(Semaphore::new(parallel));
    let mut handles = Vec::with_capacity(count);
    let start_time = Instant::now();

    for i in 0..count {
        let permit = semaphore.clone().acquire_owned().await.unwrap();
        let mut client_clone = client.clone();
        let handle = tokio::spawn(async move {
            let key = {
                let mut rng = thread_rng();
                rng.gen_range(1..=key_range).to_string()
            };

            let request = tonic::Request::new(GetRequest {
                key: key.clone(),
                include_expire_at: false,
            });

            let req_start = Instant::now();
            let res = client_clone.get(request).await;
            let duration = req_start.elapsed();

            let result = match res {
                Ok(_) => {
                    if count <= 10 || i < 5 || i >= count - 5 {
                        println!("[{}/{}] Get key: {} -> Status: OK", i + 1, count, key);
                    } else if i == 5 {
                        println!("...");
                    }
                    Some(duration)
                }
                Err(e) => {
                    println!("Request failed for key {}: {}", key, e);
                    None
                }
            };
            drop(permit);
            result
        });
        handles.push(handle);
    }

    let mut successful_durations = Vec::new();
    for handle in handles {
        if let Ok(Some(duration)) = handle.await {
            successful_durations.push(duration);
        }
    }

    let total_elapsed = start_time.elapsed();
    let success_count = successful_durations.len();

    println!("\nSummary:");
    println!("Total count: {}", count);
    println!("Success count: {}", success_count);
    println!("Total elapsed time: {:?}", total_elapsed);

    if success_count > 0 {
        let avg_duration: std::time::Duration =
            successful_durations.iter().sum::<std::time::Duration>() / success_count as u32;
        println!("Average request time (success only): {:?}", avg_duration);
        println!(
            "Throughput: {:.2} req/sec",
            success_count as f64 / total_elapsed.as_secs_f64()
        );
    }

    Ok(())
}

async fn run_test_batch_set(
    addr: String,
    count: usize,
    batch_size: usize,
    key_range: u32,
) -> Result<(), Box<dyn std::error::Error>> {
    let num_batches = count.div_ceil(batch_size);
    println!(
        "Batch setting {} key-value pairs (range 1..={}) in {} batches of {} to {}...",
        count, key_range, num_batches, batch_size, addr
    );

    let mut client = KeyValueClient::connect(addr).await?;
    let start_time = Instant::now();
    let mut total_set = 0usize;

    for batch_num in 0..num_batches {
        let items_in_batch = std::cmp::min(batch_size, count - batch_num * batch_size);
        let mut items = Vec::with_capacity(items_in_batch);

        for _ in 0..items_in_batch {
            let mut rng = thread_rng();
            let key = rng.gen_range(1..=key_range).to_string();
            let value: String = (0..20).map(|_| rng.sample(Alphanumeric) as char).collect();
            items.push(KeyValuePair { key, value });
        }

        let request = tonic::Request::new(BatchSetRequest { items });
        let response = client.batch_set(request).await?;
        total_set += response.into_inner().count as usize;

        if num_batches <= 10 || batch_num < 3 || batch_num >= num_batches - 3 {
            println!(
                "[Batch {}/{}] Set {} items",
                batch_num + 1,
                num_batches,
                items_in_batch
            );
        } else if batch_num == 3 {
            println!("...");
        }
    }

    let total_elapsed = start_time.elapsed();

    println!("\nSummary:");
    println!("Total count: {}", count);
    println!("Total set: {}", total_set);
    println!("Batch size: {}", batch_size);
    println!("Number of batches: {}", num_batches);
    println!("Total elapsed time: {:?}", total_elapsed);
    println!(
        "Throughput: {:.2} items/sec",
        total_set as f64 / total_elapsed.as_secs_f64()
    );

    Ok(())
}

async fn run_test_batch_get(
    addr: String,
    count: usize,
    batch_size: usize,
    key_range: u32,
    duplicate_rate: f64,
) -> Result<(), Box<dyn std::error::Error>> {
    let num_batches = count.div_ceil(batch_size);
    println!(
        "Batch getting {} keys (range 1..={}, duplicate_rate={:.0}%) in {} batches of {} from {}...",
        count,
        key_range,
        duplicate_rate * 100.0,
        num_batches,
        batch_size,
        addr
    );

    let mut client = KeyValueClient::connect(addr).await?;
    let start_time = Instant::now();
    let mut total_found = 0usize;

    for batch_num in 0..num_batches {
        let keys_in_batch = std::cmp::min(batch_size, count - batch_num * batch_size);
        let mut keys = Vec::with_capacity(keys_in_batch);
        let mut rng = thread_rng();

        // Generate unique keys first
        let unique_count = ((keys_in_batch as f64) * (1.0 - duplicate_rate)).ceil() as usize;
        let unique_count = unique_count.max(1); // At least 1 unique key

        for _ in 0..unique_count {
            let key = rng.gen_range(1..=key_range).to_string();
            keys.push(key);
        }

        // Fill remaining with duplicates from existing keys
        while keys.len() < keys_in_batch {
            let idx = rng.gen_range(0..unique_count);
            keys.push(keys[idx].clone());
        }

        // Shuffle to mix duplicates
        use rand::seq::SliceRandom;
        keys.shuffle(&mut rng);

        let request = tonic::Request::new(BatchGetRequest { keys });
        let response = client.batch_get(request).await?;
        let found = response.into_inner().items.len();
        total_found += found;

        if num_batches <= 10 || batch_num < 3 || batch_num >= num_batches - 3 {
            println!(
                "[Batch {}/{}] Requested {} keys ({} unique), found {}",
                batch_num + 1,
                num_batches,
                keys_in_batch,
                unique_count,
                found
            );
        } else if batch_num == 3 {
            println!("...");
        }
    }

    let total_elapsed = start_time.elapsed();

    println!("\nSummary:");
    println!("Total requested: {}", count);
    println!("Total found: {}", total_found);
    println!("Batch size: {}", batch_size);
    println!("Duplicate rate: {:.0}%", duplicate_rate * 100.0);
    println!("Number of batches: {}", num_batches);
    println!("Total elapsed time: {:?}", total_elapsed);
    println!(
        "Throughput: {:.2} keys/sec",
        count as f64 / total_elapsed.as_secs_f64()
    );

    Ok(())
}

#[derive(Serialize)]
struct MetricsJson {
    operations: OperationsMetrics,
    sstable: SstableMetrics,
    memtable: MemtableMetrics,
    compaction: CompactionMetrics,
    blockcache: BlockcacheMetrics,
}

#[derive(Serialize)]
struct OperationsMetrics {
    get_count: u64,
    set_count: u64,
    delete_count: u64,
    batch_get_count: u64,
    batch_set_count: u64,
    batch_delete_count: u64,
}

#[derive(Serialize)]
struct SstableMetrics {
    searches: u64,
    bloom_filter_hits: u64,
    bloom_filter_false_positives: u64,
    bloom_effectiveness: f64,
}

#[derive(Serialize)]
struct MemtableMetrics {
    flushes: u64,
    flush_bytes: u64,
}

#[derive(Serialize)]
struct CompactionMetrics {
    count: u64,
    bytes_read: u64,
    bytes_written: u64,
}

#[derive(Serialize)]
struct BlockcacheMetrics {
    entries: u64,
    size_bytes: u64,
    max_size_bytes: u64,
    hits: u64,
    misses: u64,
    evictions: u64,
    hit_ratio: f64,
}

async fn run_test_metrics(addr: String, json: bool) -> Result<(), Box<dyn std::error::Error>> {
    let mut client = KeyValueClient::connect(addr).await?;
    let request = tonic::Request::new(GetMetricsRequest {});
    let response = client.get_metrics(request).await?;
    let m = response.into_inner();

    if json {
        let metrics = MetricsJson {
            operations: OperationsMetrics {
                get_count: m.get_count,
                set_count: m.set_count,
                delete_count: m.delete_count,
                batch_get_count: m.batch_get_count,
                batch_set_count: m.batch_set_count,
                batch_delete_count: m.batch_delete_count,
            },
            sstable: SstableMetrics {
                searches: m.sstable_searches,
                bloom_filter_hits: m.bloom_filter_hits,
                bloom_filter_false_positives: m.bloom_filter_false_positives,
                bloom_effectiveness: m.bloom_effectiveness,
            },
            memtable: MemtableMetrics {
                flushes: m.memtable_flushes,
                flush_bytes: m.memtable_flush_bytes,
            },
            compaction: CompactionMetrics {
                count: m.compaction_count,
                bytes_read: m.compaction_bytes_read,
                bytes_written: m.compaction_bytes_written,
            },
            blockcache: BlockcacheMetrics {
                entries: m.blockcache_entries,
                size_bytes: m.blockcache_size_bytes,
                max_size_bytes: m.blockcache_max_size_bytes,
                hits: m.blockcache_hits,
                misses: m.blockcache_misses,
                evictions: m.blockcache_evictions,
                hit_ratio: m.blockcache_hit_ratio,
            },
        };
        println!("{}", serde_json::to_string(&metrics)?);
    } else {
        println!("=== Engine Metrics ===");
        println!();
        println!("Operations:");
        println!("  GET: {}", m.get_count);
        println!("  SET: {}", m.set_count);
        println!("  DELETE: {}", m.delete_count);
        println!("  Batch GET: {} keys", m.batch_get_count);
        println!("  Batch SET: {} keys", m.batch_set_count);
        println!("  Batch DELETE: {} keys", m.batch_delete_count);
        println!();
        println!("SSTable:");
        println!("  Searches: {}", m.sstable_searches);
        println!("  Bloom filter hits: {}", m.bloom_filter_hits);
        println!(
            "  Bloom filter false positives: {}",
            m.bloom_filter_false_positives
        );
        println!(
            "  Bloom effectiveness: {:.1}%",
            m.bloom_effectiveness * 100.0
        );
        println!();
        println!("MemTable:");
        println!("  Flushes: {}", m.memtable_flushes);
        println!(
            "  Flush bytes: {} ({:.2} MB)",
            m.memtable_flush_bytes,
            m.memtable_flush_bytes as f64 / 1024.0 / 1024.0
        );
        println!();
        println!("Compaction:");
        println!("  Count: {}", m.compaction_count);
        println!(
            "  Bytes read: {} ({:.2} MB)",
            m.compaction_bytes_read,
            m.compaction_bytes_read as f64 / 1024.0 / 1024.0
        );
        println!(
            "  Bytes written: {} ({:.2} MB)",
            m.compaction_bytes_written,
            m.compaction_bytes_written as f64 / 1024.0 / 1024.0
        );
        println!();
        println!("Block Cache:");
        println!("  Entries: {}", m.blockcache_entries);
        println!(
            "  Size: {} ({:.2} MB)",
            m.blockcache_size_bytes,
            m.blockcache_size_bytes as f64 / 1024.0 / 1024.0
        );
        println!(
            "  Max size: {} ({:.2} MB)",
            m.blockcache_max_size_bytes,
            m.blockcache_max_size_bytes as f64 / 1024.0 / 1024.0
        );
        println!("  Hits: {}", m.blockcache_hits);
        println!("  Misses: {}", m.blockcache_misses);
        println!("  Evictions: {}", m.blockcache_evictions);
        println!("  Hit ratio: {:.1}%", m.blockcache_hit_ratio * 100.0);
    }

    Ok(())
}

async fn run_test_batch_delete(
    addr: String,
    count: usize,
    batch_size: usize,
    key_range: u32,
) -> Result<(), Box<dyn std::error::Error>> {
    let num_batches = count.div_ceil(batch_size);
    println!(
        "Batch deleting {} keys (range 1..={}) in {} batches of {} from {}...",
        count, key_range, num_batches, batch_size, addr
    );

    let mut client = KeyValueClient::connect(addr).await?;
    let start_time = Instant::now();
    let mut total_deleted = 0usize;

    for batch_num in 0..num_batches {
        let keys_in_batch = std::cmp::min(batch_size, count - batch_num * batch_size);
        let mut keys = Vec::with_capacity(keys_in_batch);

        for _ in 0..keys_in_batch {
            let mut rng = thread_rng();
            let key = rng.gen_range(1..=key_range).to_string();
            keys.push(key);
        }

        let request = tonic::Request::new(BatchDeleteRequest { keys });
        let response = client.batch_delete(request).await?;
        total_deleted += response.into_inner().count as usize;

        if num_batches <= 10 || batch_num < 3 || batch_num >= num_batches - 3 {
            println!(
                "[Batch {}/{}] Deleted {} keys",
                batch_num + 1,
                num_batches,
                keys_in_batch
            );
        } else if batch_num == 3 {
            println!("...");
        }
    }

    let total_elapsed = start_time.elapsed();

    println!("\nSummary:");
    println!("Total requested: {}", count);
    println!("Total deleted: {}", total_deleted);
    println!("Batch size: {}", batch_size);
    println!("Number of batches: {}", num_batches);
    println!("Total elapsed time: {:?}", total_elapsed);
    println!(
        "Throughput: {:.2} keys/sec",
        count as f64 / total_elapsed.as_secs_f64()
    );

    Ok(())
}
