use crate::server::kv::SetRequest;
use crate::server::kv::key_value_client::KeyValueClient;
use rand::distributions::Alphanumeric;
use rand::{Rng, thread_rng};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Semaphore;

pub async fn run(
    addr: String,
    count: usize,
    parallel: usize,
    key_range: u32,
    value_size: usize,
    ttl: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    if ttl > 0 {
        println!(
            "Generating and setting {} random key-value pairs (range 1..={}, value_size={}, ttl={}s) with parallelism {} to {}...",
            count, key_range, value_size, ttl, parallel, addr
        );
    } else {
        println!(
            "Generating and setting {} random key-value pairs (range 1..={}, value_size={}) with parallelism {} to {}...",
            count, key_range, value_size, parallel, addr
        );
    }

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
                let value: String = (0..value_size)
                    .map(|_| rng.sample(Alphanumeric) as char)
                    .collect();
                (key, value)
            };

            let request = tonic::Request::new(SetRequest {
                key: key.clone(),
                value: value.clone(),
                ttl_seconds: ttl,
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
