mod engine;
mod server;

use clap::{Parser, Subcommand};
use rand::distributions::Alphanumeric;
use rand::{Rng, thread_rng};
use server::EngineType;
use server::kv::key_value_client::KeyValueClient;
use server::kv::{
    BatchDeleteRequest, BatchGetRequest, BatchSetRequest, DeleteRequest, GetRequest, KeyValuePair,
    SetRequest,
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

        /// Storage engine to use
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
    Set { key: String, value: String },
    /// Get the value of a key
    Get { key: String },
    /// Delete a key
    Delete { key: String },
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
        } => {
            let addr = addr.parse()?;
            server::run_server(
                addr,
                engine.clone(),
                data_dir.clone(),
                *log_capacity_bytes,
                *lsm_memtable_capacity_bytes,
                *lsm_compaction_trigger_file_count,
            )
            .await;
            Ok(())
        }
        Commands::Test { addr, command } => match command {
            TestCommands::RandomSet {
                count,
                parallel,
                key_range,
            } => run_test_set(addr.clone(), *count, *parallel, *key_range).await,
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
            TestCommands::Set { key, value } => {
                let mut client = KeyValueClient::connect(addr.clone()).await?;
                let request = tonic::Request::new(SetRequest {
                    key: key.clone(),
                    value: value.clone(),
                });
                let response = client.set(request).await?;
                println!("RESPONSE={:?}", response);
                Ok(())
            }
            TestCommands::Get { key } => {
                let mut client = KeyValueClient::connect(addr.clone()).await?;
                let request = tonic::Request::new(GetRequest { key: key.clone() });
                match client.get(request).await {
                    Ok(response) => {
                        println!("{}", response.into_inner().value);
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
        },
    }
}

async fn run_test_set(
    addr: String,
    count: usize,
    parallel: usize,
    key_range: u32,
) -> Result<(), Box<dyn std::error::Error>> {
    println!(
        "Generating and setting {} random key-value pairs (range 1..={}) with parallelism {} to {}...",
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
            let (key, value) = {
                let mut rng = thread_rng();
                let key = rng.gen_range(1..=key_range).to_string();
                let value: String = (0..20).map(|_| rng.sample(Alphanumeric) as char).collect();
                (key, value)
            };

            let request = tonic::Request::new(SetRequest {
                key: key.clone(),
                value: value.clone(),
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

            let request = tonic::Request::new(GetRequest { key: key.clone() });

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
