use crate::server::kv::BatchDeleteRequest;
use crate::server::kv::key_value_client::KeyValueClient;
use rand::{Rng, thread_rng};
use std::time::Instant;

pub async fn run(
    addr: String,
    count: usize,
    batch_size: usize,
    key_range: u32,
) -> Result<(), Box<dyn std::error::Error>> {
    let num_batches = count.div_ceil(batch_size);
    println!(
        "Batch deleting {} keys (range 1..={}) in {} batches of {} from {}...",
        count, key_range, num_batches, batch_size, addr
    );

    let mut client = KeyValueClient::connect(addr).await?;
    let start_time = Instant::now();
    let mut total_deleted = 0usize;

    for batch_num in 0..num_batches {
        let keys_in_batch = std::cmp::min(batch_size, count - batch_num * batch_size);
        let mut keys = Vec::with_capacity(keys_in_batch);

        for _ in 0..keys_in_batch {
            let mut rng = thread_rng();
            let key = rng.gen_range(1..=key_range).to_string();
            keys.push(key);
        }

        let request = tonic::Request::new(BatchDeleteRequest { keys });
        let response = client.batch_delete(request).await?;
        total_deleted += response.into_inner().count as usize;

        if num_batches <= 10 || batch_num < 3 || batch_num >= num_batches - 3 {
            println!(
                "[Batch {}/{}] Deleted {} keys",
                batch_num + 1,
                num_batches,
                keys_in_batch
            );
        } else if batch_num == 3 {
            println!("...");
        }
    }

    let total_elapsed = start_time.elapsed();

    println!("\nSummary:");
    println!("Total requested: {}", count);
    println!("Total deleted: {}", total_deleted);
    println!("Batch size: {}", batch_size);
    println!("Number of batches: {}", num_batches);
    println!("Total elapsed time: {:?}", total_elapsed);
    println!(
        "Throughput: {:.2} keys/sec",
        count as f64 / total_elapsed.as_secs_f64()
    );

    Ok(())
}
