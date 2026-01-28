use crate::server::kv::GetRequest;
use crate::server::kv::key_value_client::KeyValueClient;
use rand::{Rng, thread_rng};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Semaphore;

pub async fn run(
    addr: String,
    count: usize,
    parallel: usize,
    key_range: u32,
) -> Result<(), Box<dyn std::error::Error>> {
    println!(
        "Generating and getting {} random keys (range 1..={}) with parallelism {} from {}...",
        count, key_range, parallel, addr
    );

    let client = KeyValueClient::connect(addr).await?;
    let semaphore = Arc::new(Semaphore::new(parallel));
    let mut handles = Vec::with_capacity(count);
    let start_time = Instant::now();

    for i in 0..count {
        let permit = semaphore.clone().acquire_owned().await.unwrap();
        let mut client_clone = client.clone();
        let handle = tokio::spawn(async move {
            let key = {
                let mut rng = thread_rng();
                rng.gen_range(1..=key_range).to_string()
            };

            let request = tonic::Request::new(GetRequest {
                key: key.clone(),
                include_expire_at: false,
            });

            let req_start = Instant::now();
            let res = client_clone.get(request).await;
            let duration = req_start.elapsed();

            let result = match res {
                Ok(_) => {
                    if count <= 10 || i < 5 || i >= count - 5 {
                        println!("[{}/{}] Get key: {} -> Status: OK", i + 1, count, key);
                    } else if i == 5 {
                        println!("...");
                    }
                    Some(duration)
                }
                Err(e) => {
                    println!("Request failed for key {}: {}", key, e);
                    None
                }
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
