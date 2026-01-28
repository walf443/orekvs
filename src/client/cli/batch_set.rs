use crate::server::kv::key_value_client::KeyValueClient;
use crate::server::kv::{BatchSetRequest, KeyValuePair};
use rand::distributions::Alphanumeric;
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
        "Batch setting {} key-value pairs (range 1..={}) in {} batches of {} to {}...",
        count, key_range, num_batches, batch_size, addr
    );

    let mut client = KeyValueClient::connect(addr).await?;
    let start_time = Instant::now();
    let mut total_set = 0usize;

    for batch_num in 0..num_batches {
        let items_in_batch = std::cmp::min(batch_size, count - batch_num * batch_size);
        let mut items = Vec::with_capacity(items_in_batch);

        for _ in 0..items_in_batch {
            let mut rng = thread_rng();
            let key = rng.gen_range(1..=key_range).to_string();
            let value: String = (0..20).map(|_| rng.sample(Alphanumeric) as char).collect();
            items.push(KeyValuePair { key, value });
        }

        let request = tonic::Request::new(BatchSetRequest { items });
        let response = client.batch_set(request).await?;
        total_set += response.into_inner().count as usize;

        if num_batches <= 10 || batch_num < 3 || batch_num >= num_batches - 3 {
            println!(
                "[Batch {}/{}] Set {} items",
                batch_num + 1,
                num_batches,
                items_in_batch
            );
        } else if batch_num == 3 {
            println!("...");
        }
    }

    let total_elapsed = start_time.elapsed();

    println!("\nSummary:");
    println!("Total count: {}", count);
    println!("Total set: {}", total_set);
    println!("Batch size: {}", batch_size);
    println!("Number of batches: {}", num_batches);
    println!("Total elapsed time: {:?}", total_elapsed);
    println!(
        "Throughput: {:.2} items/sec",
        total_set as f64 / total_elapsed.as_secs_f64()
    );

    Ok(())
}
