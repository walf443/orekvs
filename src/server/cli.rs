use clap::Args;

use crate::engine::lsm_tree::{
    DEFAULT_WAL_MAX_SIZE_BYTES, DEFAULT_WAL_RETENTION_SECS, WalArchiveConfig,
};
use crate::server::{EngineType, run_follower, run_server};

#[derive(Args)]
pub struct Command {
    #[arg(long, default_value = "127.0.0.1:50051")]
    pub addr: String,

    /// Storage engine to use (ignored if --leader-addr is set, uses lsm-tree)
    #[arg(long, value_enum, default_value_t = EngineType::Memory)]
    pub engine: EngineType,

    /// Data directory for storage engines
    #[arg(long, default_value = "kv_data")]
    pub data_dir: String,

    /// Max size for log file in bytes for Log engine
    #[arg(long = "log-engine-capacity-bytes", default_value_t = 1024)]
    pub log_capacity_bytes: u64,

    /// MemTable flush trigger size in bytes for LSM-tree engine
    #[arg(long = "lsm-engine-memtable-capacity-bytes", default_value_t = 4194304)]
    pub lsm_memtable_capacity_bytes: u64,

    /// Compaction trigger (number of SSTables) for LSM-tree engine
    #[arg(long = "lsm-engine-compaction-trigger-file-count", default_value_t = 4)]
    pub lsm_compaction_trigger_file_count: usize,

    /// WAL group commit batch interval in microseconds for LSM-tree engine
    /// Higher values increase throughput but also increase latency
    #[arg(long = "lsm-engine-wal-batch-interval-micros", default_value_t = 100)]
    pub lsm_wal_batch_interval_micros: u64,

    /// Enable replication service for followers to connect (leader mode)
    #[arg(long)]
    pub enable_replication: bool,

    /// Port for replication service (default: main port + 1)
    #[arg(long)]
    pub replication_port: Option<u16>,

    /// Leader address to replicate from (follower mode, e.g., http://127.0.0.1:50052)
    /// When set, this server becomes a read-only follower
    #[arg(long)]
    pub leader_addr: Option<String>,

    /// WAL retention period in seconds for LSM-tree engine (default: 604800 = 7 days, 0 = disabled)
    #[arg(long = "lsm-engine-wal-retention-secs")]
    pub lsm_wal_retention_secs: Option<u64>,

    /// Maximum total WAL size in bytes for LSM-tree engine (default: 1073741824 = 1GB, 0 = disabled)
    #[arg(long = "lsm-engine-wal-max-size-bytes")]
    pub lsm_wal_max_size_bytes: Option<u64>,

    /// Disable WAL archiving for LSM-tree engine (keep all WAL files for replication)
    #[arg(long = "lsm-engine-disable-wal-archive")]
    pub lsm_disable_wal_archive: bool,
}

pub async fn run(cmd: &Command) -> Result<(), Box<dyn std::error::Error>> {
    let addr: std::net::SocketAddr = cmd.addr.parse()?;

    // Build WAL archive config
    let wal_archive_config = if cmd.lsm_disable_wal_archive {
        WalArchiveConfig::disabled()
    } else {
        WalArchiveConfig {
            retention_secs: match cmd.lsm_wal_retention_secs {
                Some(0) => None,
                Some(secs) => Some(secs),
                None => Some(DEFAULT_WAL_RETENTION_SECS),
            },
            max_size_bytes: match cmd.lsm_wal_max_size_bytes {
                Some(0) => None,
                Some(bytes) => Some(bytes),
                None => Some(DEFAULT_WAL_MAX_SIZE_BYTES),
            },
        }
    };

    // If leader_addr is set, run as follower
    if let Some(leader) = &cmd.leader_addr {
        run_follower(
            leader.clone(),
            cmd.data_dir.clone(),
            cmd.lsm_memtable_capacity_bytes,
            cmd.lsm_compaction_trigger_file_count,
            cmd.lsm_wal_batch_interval_micros,
            addr,
            wal_archive_config,
        )
        .await;
    } else {
        // Run as leader/standalone
        let repl_addr = if cmd.enable_replication {
            let port = cmd.replication_port.unwrap_or(addr.port() + 1);
            Some(std::net::SocketAddr::new(addr.ip(), port))
        } else {
            None
        };
        run_server(
            addr,
            cmd.engine.clone(),
            cmd.data_dir.clone(),
            cmd.log_capacity_bytes,
            cmd.lsm_memtable_capacity_bytes,
            cmd.lsm_compaction_trigger_file_count,
            cmd.lsm_wal_batch_interval_micros,
            repl_addr,
            wal_archive_config,
        )
        .await;
    }
    Ok(())
}
