use clap::Args;

use crate::engine::lsm_tree::{
    DEFAULT_WAL_MAX_SIZE_BYTES, DEFAULT_WAL_RETENTION_SECS, WalArchiveConfig,
};
use crate::server::{EngineType, run_follower, run_server};

/// Parse a size string with optional suffix (e.g., "4MiB", "1GiB", "512KiB", "1024")
/// Supports:
/// - Binary: KiB (1024), MiB (1024^2), GiB (1024^3), TiB (1024^4)
/// - Decimal: KB (1000), MB (1000^2), GB (1000^3), TB (1000^4)
/// - Plain numbers (bytes)
fn parse_size(s: &str) -> Result<u64, String> {
    let s = s.trim();
    if s.is_empty() {
        return Err("empty string".to_string());
    }

    // Find where the numeric part ends
    let num_end = s
        .find(|c: char| !c.is_ascii_digit() && c != '.')
        .unwrap_or(s.len());

    let (num_str, suffix) = s.split_at(num_end);
    let suffix = suffix.trim();

    let num: f64 = num_str
        .parse()
        .map_err(|_| format!("invalid number: {}", num_str))?;

    let multiplier: u64 = match suffix.to_uppercase().as_str() {
        "" => 1,
        // Binary (IEC)
        "KIB" => 1024,
        "MIB" => 1024 * 1024,
        "GIB" => 1024 * 1024 * 1024,
        "TIB" => 1024 * 1024 * 1024 * 1024,
        // Decimal (SI)
        "KB" | "K" => 1000,
        "MB" | "M" => 1000 * 1000,
        "GB" | "G" => 1000 * 1000 * 1000,
        "TB" | "T" => 1000 * 1000 * 1000 * 1000,
        // Also allow B suffix
        "B" => 1,
        _ => return Err(format!("unknown suffix: {}", suffix)),
    };

    Ok((num * multiplier as f64) as u64)
}

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

    /// Max size for log file in bytes for Log engine (e.g., 1024, 1KiB, 1MiB)
    #[arg(long = "log-engine-capacity-bytes", default_value = "1KiB", value_parser = parse_size)]
    pub log_capacity_bytes: u64,

    /// MemTable flush trigger size in bytes for LSM-tree engine (e.g., 4MiB, 64MiB)
    #[arg(long = "lsm-engine-memtable-capacity-bytes", default_value = "4MiB", value_parser = parse_size)]
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

    /// Maximum total WAL size in bytes for LSM-tree engine (e.g., 1GiB, 100MiB, 0 = disabled)
    #[arg(long = "lsm-engine-wal-max-size-bytes", value_parser = parse_size)]
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_size_plain_numbers() {
        assert_eq!(parse_size("0").unwrap(), 0);
        assert_eq!(parse_size("1").unwrap(), 1);
        assert_eq!(parse_size("1024").unwrap(), 1024);
        assert_eq!(parse_size("4194304").unwrap(), 4194304);
    }

    #[test]
    fn test_parse_size_binary_suffixes() {
        // KiB
        assert_eq!(parse_size("1KiB").unwrap(), 1024);
        assert_eq!(parse_size("20KiB").unwrap(), 20 * 1024);
        // MiB
        assert_eq!(parse_size("1MiB").unwrap(), 1024 * 1024);
        assert_eq!(parse_size("4MiB").unwrap(), 4 * 1024 * 1024);
        assert_eq!(parse_size("64MiB").unwrap(), 64 * 1024 * 1024);
        // GiB
        assert_eq!(parse_size("1GiB").unwrap(), 1024 * 1024 * 1024);
        assert_eq!(parse_size("4GiB").unwrap(), 4 * 1024 * 1024 * 1024);
        // TiB
        assert_eq!(parse_size("1TiB").unwrap(), 1024 * 1024 * 1024 * 1024);
    }

    #[test]
    fn test_parse_size_decimal_suffixes() {
        // KB
        assert_eq!(parse_size("1KB").unwrap(), 1000);
        assert_eq!(parse_size("1K").unwrap(), 1000);
        // MB
        assert_eq!(parse_size("1MB").unwrap(), 1000 * 1000);
        assert_eq!(parse_size("1M").unwrap(), 1000 * 1000);
        // GB
        assert_eq!(parse_size("1GB").unwrap(), 1000 * 1000 * 1000);
        assert_eq!(parse_size("1G").unwrap(), 1000 * 1000 * 1000);
        // TB
        assert_eq!(parse_size("1TB").unwrap(), 1000 * 1000 * 1000 * 1000);
        assert_eq!(parse_size("1T").unwrap(), 1000 * 1000 * 1000 * 1000);
    }

    #[test]
    fn test_parse_size_case_insensitive() {
        assert_eq!(parse_size("1kib").unwrap(), 1024);
        assert_eq!(parse_size("1KIB").unwrap(), 1024);
        assert_eq!(parse_size("1Kib").unwrap(), 1024);
        assert_eq!(parse_size("1mib").unwrap(), 1024 * 1024);
        assert_eq!(parse_size("1gib").unwrap(), 1024 * 1024 * 1024);
    }

    #[test]
    fn test_parse_size_with_spaces() {
        assert_eq!(parse_size("  1024  ").unwrap(), 1024);
        assert_eq!(parse_size("4 MiB").unwrap(), 4 * 1024 * 1024);
        assert_eq!(parse_size("1 GiB").unwrap(), 1024 * 1024 * 1024);
    }

    #[test]
    fn test_parse_size_with_b_suffix() {
        assert_eq!(parse_size("100B").unwrap(), 100);
        assert_eq!(parse_size("1024B").unwrap(), 1024);
    }

    #[test]
    fn test_parse_size_fractional() {
        assert_eq!(parse_size("1.5KiB").unwrap(), 1536); // 1.5 * 1024
        assert_eq!(parse_size("2.5MiB").unwrap(), 2621440); // 2.5 * 1024 * 1024
    }

    #[test]
    fn test_parse_size_errors() {
        assert!(parse_size("").is_err());
        assert!(parse_size("abc").is_err());
        assert!(parse_size("1XiB").is_err());
        assert!(parse_size("1PiB").is_err()); // PiB not supported
    }
}
