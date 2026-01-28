use crate::server::kv::key_value_client::KeyValueClient;
use crate::server::kv::{CompareAndSetRequest, SetRequest};
use rand::distributions::Alphanumeric;
use rand::{Rng, thread_rng};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::RwLock;
use std::time::Instant;
use tokio::sync::Semaphore;

pub async fn run(
    addr: String,
    count: usize,
    parallel: usize,
    key_range: u32,
    value_size: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    println!(
        "Phase 1: Setting up {} initial key-value pairs (range 1..={}, value_size={})...",
        key_range, key_range, value_size
    );

    // Phase 1: Setup initial values for all keys in the range
    let client = KeyValueClient::connect(addr.clone()).await?;
    let semaphore = Arc::new(Semaphore::new(parallel));
    let mut setup_handles = Vec::with_capacity(key_range as usize);

    for key_num in 1..=key_range {
        let permit = semaphore.clone().acquire_owned().await.unwrap();
        let mut client_clone = client.clone();
        let handle = tokio::spawn(async move {
            let key = key_num.to_string();
            let value: String = {
                let mut rng = thread_rng();
                (0..value_size)
                    .map(|_| rng.sample(Alphanumeric) as char)
                    .collect()
            };

            let request = tonic::Request::new(SetRequest {
                key: key.clone(),
                value: value.clone(),
                ttl_seconds: 0,
            });

            let res = client_clone.set(request).await;
            drop(permit);
            if res.is_ok() {
                Some((key, value))
            } else {
                None
            }
        });
        setup_handles.push(handle);
    }

    // Collect initial values
    let mut key_values: HashMap<String, String> = HashMap::new();
    for handle in setup_handles {
        if let Ok(Some((key, value))) = handle.await {
            key_values.insert(key, value);
        }
    }
    println!("Setup complete: {} keys initialized", key_values.len());

    // Phase 2: Run CAS operations
    println!(
        "\nPhase 2: Running {} CAS operations with parallelism {}...",
        count, parallel
    );

    let key_values = Arc::new(RwLock::new(key_values));
    let mut handles = Vec::with_capacity(count);
    let start_time = Instant::now();

    for i in 0..count {
        let permit = semaphore.clone().acquire_owned().await.unwrap();
        let mut client_clone = client.clone();
        let key_values_clone = key_values.clone();
        let handle = tokio::spawn(async move {
            let (key, expected_value, new_value) = {
                let mut rng = thread_rng();
                let key = rng.gen_range(1..=key_range).to_string();
                let expected_value = {
                    let kv = key_values_clone.read().unwrap();
                    kv.get(&key).cloned().unwrap_or_default()
                };
                let new_value: String = (0..value_size)
                    .map(|_| rng.sample(Alphanumeric) as char)
                    .collect();
                (key, expected_value, new_value)
            };

            let request = tonic::Request::new(CompareAndSetRequest {
                key: key.clone(),
                expected_value: expected_value.clone(),
                new_value: new_value.clone(),
                expect_exists: true,
                ttl_seconds: 0,
            });

            let req_start = Instant::now();
            let res = client_clone.compare_and_set(request).await;
            let duration = req_start.elapsed();

            let result = match res {
                Ok(response) => {
                    let resp = response.into_inner();
                    if resp.success {
                        // Update our local cache with new value
                        let mut kv = key_values_clone.write().unwrap();
                        kv.insert(key.clone(), new_value.clone());
                        if count <= 10 || i < 5 || i >= count - 5 {
                            println!("[{}/{}] CAS {} OK", i + 1, count, key);
                        } else if i == 5 {
                            println!("...");
                        }
                        Some((duration, true))
                    } else {
                        // CAS failed due to race condition, update cache with current value
                        let mut kv = key_values_clone.write().unwrap();
                        if !resp.current_value.is_empty() {
                            kv.insert(key.clone(), resp.current_value);
                        }
                        if count <= 10 || i < 5 || i >= count - 5 {
                            println!("[{}/{}] CAS {} CONFLICT (retryable)", i + 1, count, key);
                        } else if i == 5 {
                            println!("...");
                        }
                        Some((duration, false))
                    }
                }
                Err(e) => {
                    println!("Failed CAS for key {}: {}", key, e);
                    None
                }
            };
            drop(permit);
            result
        });
        handles.push(handle);
    }

    let mut successful_durations = Vec::new();
    let mut cas_success_count = 0;
    let mut cas_conflict_count = 0;
    for handle in handles {
        if let Ok(Some((duration, success))) = handle.await {
            successful_durations.push(duration);
            if success {
                cas_success_count += 1;
            } else {
                cas_conflict_count += 1;
            }
        }
    }

    let total_elapsed = start_time.elapsed();
    let total_completed = successful_durations.len();

    println!("\nSummary:");
    println!("Total CAS operations: {}", count);
    println!("Completed: {}", total_completed);
    println!("  - CAS success: {}", cas_success_count);
    println!(
        "  - CAS conflict (expected value mismatch): {}",
        cas_conflict_count
    );
    println!("Total elapsed time: {:?}", total_elapsed);

    if total_completed > 0 {
        let avg_duration: std::time::Duration =
            successful_durations.iter().sum::<std::time::Duration>() / total_completed as u32;
        println!("Average request time: {:?}", avg_duration);
        println!(
            "Throughput: {:.2} req/sec",
            total_completed as f64 / total_elapsed.as_secs_f64()
        );
    }

    Ok(())
}
