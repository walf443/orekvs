//! Analyze Chrome tracing JSON to find GET operation bottlenecks
//!
//! Run with: cargo run --example analyze_trace -- ./profile_get_trace.json

use std::collections::HashMap;
use std::env;
use std::fs::File;
use std::io::BufReader;

use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct TraceEvent {
    #[serde(default)]
    ph: String,
    #[serde(default)]
    name: String,
    #[serde(default)]
    tid: u64,
    #[serde(default)]
    ts: f64,
}

#[derive(Debug, Default)]
struct SpanStats {
    count: u64,
    total_us: f64,
    min_us: f64,
    max_us: f64,
}

impl SpanStats {
    fn new() -> Self {
        Self {
            count: 0,
            total_us: 0.0,
            min_us: f64::INFINITY,
            max_us: 0.0,
        }
    }

    fn add(&mut self, duration_us: f64) {
        self.count += 1;
        self.total_us += duration_us;
        self.min_us = self.min_us.min(duration_us);
        self.max_us = self.max_us.max(duration_us);
    }

    fn avg_us(&self) -> f64 {
        if self.count > 0 {
            self.total_us / self.count as f64
        } else {
            0.0
        }
    }
}

fn main() {
    let args: Vec<String> = env::args().collect();
    let filepath = args
        .get(1)
        .map(|s| s.as_str())
        .unwrap_or("./profile_get_trace.json");

    let file = File::open(filepath).expect("Failed to open trace file");
    let reader = BufReader::new(file);
    let events: Vec<TraceEvent> = serde_json::from_reader(reader).expect("Failed to parse JSON");

    // Track span starts (tid, name) -> ts
    let mut span_starts: HashMap<(u64, String), f64> = HashMap::new();
    // Track stats per span name
    let mut stats: HashMap<String, SpanStats> = HashMap::new();
    // Track phase stats
    let phase_names = [
        "memtable_gets",
        "sstable_gets",
        "nonexistent_gets",
        "random_gets",
    ];
    let mut phase_stats: HashMap<String, HashMap<String, SpanStats>> = HashMap::new();
    for phase in &phase_names {
        phase_stats.insert(phase.to_string(), HashMap::new());
    }
    let mut current_phase: Option<String> = None;

    for event in &events {
        let ph = &event.ph;
        let name = &event.name;
        let tid = event.tid;
        let ts = event.ts;

        // Track phase
        if phase_names.contains(&name.as_str()) {
            if ph == "B" {
                current_phase = Some(name.clone());
            } else if ph == "E" {
                current_phase = None;
            }
        }

        if ph == "B" {
            span_starts.insert((tid, name.clone()), ts);
        } else if ph == "E" {
            let key = (tid, name.clone());
            if let Some(start_ts) = span_starts.remove(&key) {
                let duration = ts - start_ts;
                stats
                    .entry(name.clone())
                    .or_insert_with(SpanStats::new)
                    .add(duration);

                if let Some(ref phase) = current_phase {
                    if name != phase {
                        phase_stats
                            .get_mut(phase)
                            .unwrap()
                            .entry(name.clone())
                            .or_insert_with(SpanStats::new)
                            .add(duration);
                    }
                }
            }
        }
    }

    // Print overall stats
    println!("{}", "=".repeat(80));
    println!("OVERALL SPAN STATISTICS (microseconds)");
    println!("{}", "=".repeat(80));
    println!(
        "{:<35} {:>8} {:>12} {:>10} {:>10} {:>10}",
        "Span Name", "Count", "Total", "Avg", "Min", "Max"
    );
    println!("{}", "-".repeat(80));

    // Sort by total time descending
    let mut sorted_stats: Vec<_> = stats.iter().collect();
    sorted_stats.sort_by(|a, b| b.1.total_us.partial_cmp(&a.1.total_us).unwrap());

    for (name, s) in &sorted_stats {
        if s.count > 0 {
            println!(
                "{:<35} {:>8} {:>12.1} {:>10.1} {:>10.1} {:>10.1}",
                name,
                s.count,
                s.total_us,
                s.avg_us(),
                s.min_us,
                s.max_us
            );
        }
    }

    // Print per-phase breakdown for GET operations
    for phase_name in &phase_names {
        let phase_data = phase_stats.get(*phase_name).unwrap();
        if phase_data.is_empty() {
            continue;
        }

        println!();
        println!("{}", "=".repeat(80));
        println!("BREAKDOWN FOR: {}", phase_name);
        println!("{}", "=".repeat(80));
        println!(
            "{:<35} {:>8} {:>12} {:>10} {:>10} {:>10} {:>8}",
            "Span Name", "Count", "Total", "Avg", "Min", "Max", "%Total"
        );
        println!("{}", "-".repeat(80));

        // Calculate total time for this phase
        let get_stats = phase_data.get("get_with_expire_at");
        let total_phase_time = get_stats.map(|s| s.total_us).unwrap_or(1.0);

        let mut sorted_phase: Vec<_> = phase_data.iter().collect();
        sorted_phase.sort_by(|a, b| b.1.total_us.partial_cmp(&a.1.total_us).unwrap());

        for (name, s) in &sorted_phase {
            if s.count > 0 {
                let pct = if total_phase_time > 0.0 {
                    (s.total_us / total_phase_time) * 100.0
                } else {
                    0.0
                };
                println!(
                    "{:<35} {:>8} {:>12.1} {:>10.1} {:>10.1} {:>10.1} {:>7.1}%",
                    name,
                    s.count,
                    s.total_us,
                    s.avg_us(),
                    s.min_us,
                    s.max_us,
                    pct
                );
            }
        }
    }

    // Summary analysis
    println!();
    println!("{}", "=".repeat(80));
    println!("BOTTLENECK ANALYSIS");
    println!("{}", "=".repeat(80));

    // Focus on SSTable gets as that's the most interesting case
    if let Some(sstable_phase) = phase_stats.get("sstable_gets") {
        let get_total = sstable_phase
            .get("get_with_expire_at")
            .map(|s| s.total_us)
            .unwrap_or(0.0);

        let mut bottlenecks: Vec<_> = sstable_phase
            .iter()
            .filter(|(name, s)| *name != "get_with_expire_at" && s.count > 0)
            .map(|(name, s)| {
                let pct = if get_total > 0.0 {
                    (s.total_us / get_total) * 100.0
                } else {
                    0.0
                };
                (name.clone(), pct, s.avg_us())
            })
            .collect();

        bottlenecks.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());

        println!("\nTop time consumers in SSTable GET operations:");
        for (name, pct, avg) in bottlenecks.iter().take(10) {
            println!("  - {}: {:.1}% of total time (avg {:.1}us)", name, pct, avg);
        }

        // Specific recommendations
        println!("\nRecommendations:");
        for (name, pct, _avg) in &bottlenecks {
            if name == "decompress_block" && *pct > 20.0 {
                println!(
                    "  * Block decompression takes {:.1}% - consider adjusting compression level",
                    pct
                );
            }
            if name == "acquire_sstable_lock" && *pct > 10.0 {
                println!(
                    "  * Lock acquisition takes {:.1}% - consider RWLock or lock-free approach",
                    pct
                );
            }
            if name == "load_index_from_mmap" && *pct > 15.0 {
                println!(
                    "  * Index loading takes {:.1}% - increase block cache size",
                    pct
                );
            }
            if name == "parse_block_entries" && *pct > 20.0 {
                println!(
                    "  * Block parsing takes {:.1}% - consider lazy parsing or better format",
                    pct
                );
            }
            if name == "bloom_filter_check" && *pct > 5.0 {
                println!(
                    "  * Bloom filter check takes {:.1}% - check bloom filter size/hash count",
                    pct
                );
            }
        }
    }
}
