mod server;

use clap::{Parser, Subcommand};
use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};
use server::SetRequest;

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
    Server,
    /// Test commands
    Test {
        #[command(subcommand)]
        command: TestCommands,
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

#[derive(Subcommand)]
enum TestCommands {
    /// Set multiple random key-value pairs
    Set {
        /// Number of key-value pairs to generate and set
        count: usize,
    },
    /// Get multiple random keys
    Get {
        /// Number of keys to generate and get
        count: usize,
    },
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    match &cli.command {
        Commands::Server => {
            server::run_server().await;
        }
        Commands::Test { command } => match command {
            TestCommands::Set { count } => {
                run_test_set(*count).await;
            }
            TestCommands::Get { count } => {
                run_test_get(*count).await;
            }
        },
        Commands::Set { key, value } => {
            let client = reqwest::Client::new();
            let res = client
                .post("http://localhost:3000/set")
                .json(&SetRequest {
                    key: key.clone(),
                    value: value.clone(),
                })
                .send()
                .await;

            match res {
                Ok(r) if r.status().is_success() => println!("OK"),
                Ok(r) => println!("Error: {}", r.status()),
                Err(e) => println!("Request failed: {}", e),
            }
        }
        Commands::Get { key } => {
            let res = reqwest::get(format!("http://localhost:3000/get/{}", key)).await;
            match res {
                Ok(r) if r.status().is_success() => {
                    let body: server::GetResponse = r.json().await.unwrap();
                    println!("{}", body.value);
                }
                Ok(r) => println!("Error: {}", r.status()),
                Err(e) => println!("Request failed: {}", e),
            }
        }
    }
}

async fn run_test_set(count: usize) {
    let mut rng = thread_rng();
    let client = reqwest::Client::new();
    println!("Generating and setting {} random key-value pairs...", count);

    for i in 0..count {
        let key: String = (0..10)
            .map(|_| rng.sample(Alphanumeric) as char)
            .collect();
        let value: String = (0..20)
            .map(|_| rng.sample(Alphanumeric) as char)
            .collect();

        let res = client
            .post("http://localhost:3000/set")
            .json(&SetRequest {
                key: key.clone(),
                value: value.clone(),
            })
            .send()
            .await;

        if let Err(e) = res {
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
}

async fn run_test_get(count: usize) {
    let mut rng = thread_rng();
    println!("Generating and getting {} random keys...", count);

    for i in 0..count {
        let key: String = (0..10)
            .map(|_| rng.sample(Alphanumeric) as char)
            .collect();

        let res = reqwest::get(format!("http://localhost:3000/get/{}", key)).await;

        match res {
            Ok(r) => {
                if count <= 10 || i < 5 || i >= count - 5 {
                    println!("[{}/{}] Get key: {} -> Status: {}", i + 1, count, key, r.status());
                } else if i == 5 {
                    println!("...");
                }
            }
            Err(e) => println!("Request failed for key {}: {}", key, e),
        }
    }

    println!("Successfully processed {} random get operations.", count);
}