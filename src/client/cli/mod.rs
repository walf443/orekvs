mod batch_delete;
mod batch_get;
mod batch_set;
mod metrics;
mod random_cas;
mod random_get;
mod random_set;
mod single;

use clap::Subcommand;

pub use crate::server::kv::key_value_client::KeyValueClient;

#[derive(Subcommand)]
pub enum Commands {
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
    /// Compare-and-set multiple random keys (first sets initial values, then CAS updates)
    RandomCas {
        /// Number of CAS operations to perform
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
    /// Count keys matching a prefix
    Count {
        /// Key prefix to count
        prefix: String,
    },
}

pub async fn run(addr: String, command: &Commands) -> Result<(), Box<dyn std::error::Error>> {
    match command {
        Commands::RandomSet {
            count,
            parallel,
            key_range,
            value_size,
            ttl,
        } => random_set::run(addr, *count, *parallel, *key_range, *value_size, *ttl).await,
        Commands::RandomGet {
            count,
            parallel,
            key_range,
        } => random_get::run(addr, *count, *parallel, *key_range).await,
        Commands::RandomCas {
            count,
            parallel,
            key_range,
            value_size,
        } => random_cas::run(addr, *count, *parallel, *key_range, *value_size).await,
        Commands::BatchSet {
            count,
            batch_size,
            key_range,
        } => batch_set::run(addr, *count, *batch_size, *key_range).await,
        Commands::BatchGet {
            count,
            batch_size,
            key_range,
            duplicate_rate,
        } => batch_get::run(addr, *count, *batch_size, *key_range, *duplicate_rate).await,
        Commands::BatchDelete {
            count,
            batch_size,
            key_range,
        } => batch_delete::run(addr, *count, *batch_size, *key_range).await,
        Commands::Set {
            key,
            value,
            ttl,
            if_not_exists,
            if_match,
        } => single::run_set(addr, key, value, *ttl, *if_not_exists, if_match.as_deref()).await,
        Commands::Get {
            key,
            with_expire_at,
        } => single::run_get(addr, key, *with_expire_at).await,
        Commands::GetExpireAt { key } => single::run_get_expire_at(addr, key).await,
        Commands::Delete { key } => single::run_delete(addr, key).await,
        Commands::Metrics { json } => metrics::run(addr, *json).await,
        Commands::Count { prefix } => run_count(addr, prefix).await,
    }
}

async fn run_count(addr: String, prefix: &str) -> Result<(), Box<dyn std::error::Error>> {
    let mut client = KeyValueClient::connect(addr).await?;
    let request = tonic::Request::new(crate::server::kv::CountRequest {
        prefix: prefix.to_string(),
    });
    let response = client.count(request).await?;
    println!("{}", response.into_inner().count);
    Ok(())
}
