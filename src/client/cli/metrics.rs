use crate::server::kv::GetMetricsRequest;
use crate::server::kv::key_value_client::KeyValueClient;
use serde::Serialize;

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

pub async fn run(addr: String, json: bool) -> Result<(), Box<dyn std::error::Error>> {
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
