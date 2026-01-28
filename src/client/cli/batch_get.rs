use crate::server::kv::BatchGetRequest;
use crate::server::kv::key_value_client::KeyValueClient;
use rand::seq::SliceRandom;
use rand::{Rng, thread_rng};
use std::time::Instant;

pub async fn run(
    addr: String,
    count: usize,
    batch_size: usize,
    key_range: u32,
    duplicate_rate: f64,
) -> Result<(), Box<dyn std::error::Error>> {
    let num_batches = count.div_ceil(batch_size);
    println!(
        "Batch getting {} keys (range 1..={}, duplicate_rate={:.0}%) in {} batches of {} from {}...",
        count,
        key_range,
        duplicate_rate * 100.0,
        num_batches,
        batch_size,
        addr
    );

    let mut client = KeyValueClient::connect(addr).await?;
    let start_time = Instant::now();
    let mut total_found = 0usize;

    for batch_num in 0..num_batches {
        let keys_in_batch = std::cmp::min(batch_size, count - batch_num * batch_size);
        let mut keys = Vec::with_capacity(keys_in_batch);
        let mut rng = thread_rng();

        // Generate unique keys first
        let unique_count = ((keys_in_batch as f64) * (1.0 - duplicate_rate)).ceil() as usize;
        let unique_count = unique_count.max(1); // At least 1 unique key

        for _ in 0..unique_count {
            let key = rng.gen_range(1..=key_range).to_string();
            keys.push(key);
        }

        // Fill remaining with duplicates from existing keys
        while keys.len() < keys_in_batch {
            let idx = rng.gen_range(0..unique_count);
            keys.push(keys[idx].clone());
        }

        // Shuffle to mix duplicates
        keys.shuffle(&mut rng);

        let request = tonic::Request::new(BatchGetRequest { keys });
        let response = client.batch_get(request).await?;
        let found = response.into_inner().items.len();
        total_found += found;

        if num_batches <= 10 || batch_num < 3 || batch_num >= num_batches - 3 {
            println!(
                "[Batch {}/{}] Requested {} keys ({} unique), found {}",
                batch_num + 1,
                num_batches,
                keys_in_batch,
                unique_count,
                found
            );
        } else if batch_num == 3 {
            println!("...");
        }
    }

    let total_elapsed = start_time.elapsed();

    println!("\nSummary:");
    println!("Total requested: {}", count);
    println!("Total found: {}", total_found);
    println!("Batch size: {}", batch_size);
    println!("Duplicate rate: {:.0}%", duplicate_rate * 100.0);
    println!("Number of batches: {}", num_batches);
    println!("Total elapsed time: {:?}", total_elapsed);
    println!(
        "Throughput: {:.2} keys/sec",
        count as f64 / total_elapsed.as_secs_f64()
    );

    Ok(())
}
