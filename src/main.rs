mod engine;
mod server;

use clap::{Parser, Subcommand};
use rand::distributions::Alphanumeric;
use rand::{Rng, thread_rng};
use server::EngineType;
use server::kv::key_value_client::KeyValueClient;
use server::kv::{GetRequest, SetRequest};
use std::sync::Arc;
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

        /// Data file path for Log engine
        #[arg(long, default_value = "kv_store.data")]
        data_file: String,

        /// Compaction threshold in bytes for Log engine
        #[arg(long, default_value_t = 1024)]
        log_engine_compaction_threshold: u64,
    },
    /// Test commands
    Test {
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
    },
    /// Get multiple random keys
    RandomGet {
        /// Number of keys to generate and get
        count: usize,
        /// Concurrency level
        #[arg(short, long, default_value_t = 1)]
        parallel: usize,
    },
    /// Set a key-value pair
    Set { key: String, value: String },
    /// Get the value of a key
    Get { key: String },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    match &cli.command {
        Commands::Server {
            addr,
            engine,
            data_file,
            log_engine_compaction_threshold,
        } => {
            let addr = addr.parse()?;
            server::run_server(
                addr,
                engine.clone(),
                data_file.clone(),
                *log_engine_compaction_threshold,
            )
            .await;
            Ok(())
        }
        Commands::Test { command } => match command {
            TestCommands::RandomSet { count, parallel } => run_test_set(*count, *parallel).await,
            TestCommands::RandomGet { count, parallel } => run_test_get(*count, *parallel).await,
            TestCommands::Set { key, value } => {
                let mut client = KeyValueClient::connect("http://127.0.0.1:50051").await?;
                let request = tonic::Request::new(SetRequest {
                    key: key.clone(),
                    value: value.clone(),
                });
                let response = client.set(request).await?;
                println!("RESPONSE={:?}", response);
                Ok(())
            }
            TestCommands::Get { key } => {
                let mut client = KeyValueClient::connect("http://127.0.0.1:50051").await?;
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
        },
    }
}

async fn run_test_set(count: usize, parallel: usize) -> Result<(), Box<dyn std::error::Error>> {
    println!(
        "Generating and setting {} random key-value pairs with parallelism {}...",
        count, parallel
    );

    let semaphore = Arc::new(Semaphore::new(parallel));
    let mut handles = Vec::with_capacity(count);

    for i in 0..count {
        let permit = semaphore.clone().acquire_owned().await.unwrap();
        let handle = tokio::spawn(async move {
            let mut client = match KeyValueClient::connect("http://127.0.0.1:50051").await {
                Ok(c) => c,
                Err(e) => {
                    println!("Failed to connect: {}", e);
                    return;
                }
            };

            let (key, value) = {
                let mut rng = thread_rng();
                let key = rng.gen_range(1..=100).to_string();
                let value: String = (0..20).map(|_| rng.sample(Alphanumeric) as char).collect();
                (key, value)
            };

            let request = tonic::Request::new(SetRequest {
                key: key.clone(),
                value: value.clone(),
            });

            if let Err(e) = client.set(request).await {
                println!("Failed to set key {}: {}", key, e);
            } else if count <= 10 || i < 5 || i >= count - 5 {
                println!("[{}/{}] Setting {} = {}", i + 1, count, key, value);
            } else if i == 5 {
                println!("...");
            }
            drop(permit);
        });
        handles.push(handle);
    }

    for handle in handles {
        let _ = handle.await;
    }

    println!("Successfully processed {} random key-value pairs.", count);
    Ok(())
}

async fn run_test_get(count: usize, parallel: usize) -> Result<(), Box<dyn std::error::Error>> {
    println!(
        "Generating and getting {} random keys with parallelism {}...",
        count, parallel
    );

    let semaphore = Arc::new(Semaphore::new(parallel));
    let mut handles = Vec::with_capacity(count);

    for i in 0..count {
        let permit = semaphore.clone().acquire_owned().await.unwrap();
        let handle = tokio::spawn(async move {
            let mut client = match KeyValueClient::connect("http://127.0.0.1:50051").await {
                Ok(c) => c,
                Err(e) => {
                    println!("Failed to connect: {}", e);
                    return;
                }
            };

            let key = {
                let mut rng = thread_rng();
                rng.gen_range(1..=100).to_string()
            };

            let request = tonic::Request::new(GetRequest { key: key.clone() });

            match client.get(request).await {
                Ok(_) => {
                    if count <= 10 || i < 5 || i >= count - 5 {
                        println!("[{}/{}] Get key: {} -> Status: OK", i + 1, count, key);
                    } else if i == 5 {
                        println!("...");
                    }
                }
                Err(e) => println!("Request failed for key {}: {}", key, e),
            }
            drop(permit);
        });
        handles.push(handle);
    }

    for handle in handles {
        let _ = handle.await;
    }

    println!("Successfully processed {} random get operations.", count);
    Ok(())
}
