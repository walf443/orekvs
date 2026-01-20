mod server;

use clap::{Parser, Subcommand};
use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};
use server::kv::key_value_client::KeyValueClient;
use server::kv::{GetRequest, SetRequest};

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
    },
    /// Get multiple random keys
    RandomGet {
        /// Number of keys to generate and get
        count: usize,
    },
    /// Set a key-value pair
    Set {
        key: String,
        value: String,
    },
    /// Get the value of a key
    Get {
        key: String,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    match &cli.command {
        Commands::Server { addr } => {
            let addr = addr.parse()?;
            server::run_server(addr).await;
            Ok(())
        }
        Commands::Test { command } => match command {
            TestCommands::RandomSet { count } => {
                run_test_set(*count).await
            }
            TestCommands::RandomGet { count } => {
                run_test_get(*count).await
            }
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
                let request = tonic::Request::new(GetRequest {
                    key: key.clone(),
                });
                match client.get(request).await {
                    Ok(response) => {
                        println!("{}", response.into_inner().value);
                    },
                    Err(e) => {
                        println!("Error: {}", e);
                    }
                }
                Ok(())
            }
        },
    }
}

async fn run_test_set(count: usize) -> Result<(), Box<dyn std::error::Error>> {
    let mut rng = thread_rng();
    let mut client = KeyValueClient::connect("http://127.0.0.1:50051").await?;
    println!("Generating and setting {} random key-value pairs...", count);

    for i in 0..count {
        let key: String = rng.gen_range(1..=100).to_string();
        let value: String = (0..20)
            .map(|_| rng.sample(Alphanumeric) as char)
            .collect();

        let request = tonic::Request::new(SetRequest {
            key: key.clone(),
            value: value.clone(),
        });

        if let Err(e) = client.set(request).await {
            println!("Failed to set key {}: {}", key, e);
            continue;
        }

        if count <= 10 || i < 5 || i >= count - 5 {
            println!("[{}/{}] Setting {} = {}", i + 1, count, key, value);
        } else if i == 5 {
            println!("...");
        }
    }

    println!("Successfully processed {} random key-value pairs.", count);
    Ok(())
}

async fn run_test_get(count: usize) -> Result<(), Box<dyn std::error::Error>> {
    let mut rng = thread_rng();
    let mut client = KeyValueClient::connect("http://127.0.0.1:50051").await?;
    println!("Generating and getting {} random keys...", count);

    for i in 0..count {
        let key: String = rng.gen_range(1..=100).to_string();

        let request = tonic::Request::new(GetRequest {
            key: key.clone(),
        });

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
    }

    println!("Successfully processed {} random get operations.", count);
    Ok(())
}