use clap::{Parser, Subcommand};
use orelsm::client::cli::Commands as ClientCommands;
use orelsm::engine::lsm_tree::WalArchiveConfig;
use orelsm::server;
use server::EngineType;

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
    /// Client commands for interacting with the server
    Client {
        /// Server address to connect to
        #[arg(long, default_value = "http://127.0.0.1:50051")]
        addr: String,

        #[command(subcommand)]
        command: ClientCommands,
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
        Commands::Client { addr, command } => orelsm::client::cli::run(addr.clone(), command).await,
    }
}
