use crate::server::kv::key_value_client::KeyValueClient;
use crate::server::kv::{
    CompareAndSetRequest, DeleteRequest, GetExpireAtRequest, GetRequest, SetRequest,
};

pub async fn run_set(
    addr: String,
    key: &str,
    value: &str,
    ttl: u64,
    if_not_exists: bool,
    if_match: Option<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut client = KeyValueClient::connect(addr).await?;

    // Use CAS if conditional options are specified
    if if_not_exists || if_match.is_some() {
        let (expected_value, expect_exists) = if if_not_exists {
            (String::new(), false)
        } else {
            (if_match.unwrap().to_string(), true)
        };

        let request = tonic::Request::new(CompareAndSetRequest {
            key: key.to_string(),
            expected_value,
            new_value: value.to_string(),
            expect_exists,
            ttl_seconds: ttl,
        });

        match client.compare_and_set(request).await {
            Ok(response) => {
                let resp = response.into_inner();
                if resp.success {
                    println!("OK");
                } else {
                    println!(
                        "FAILED: current_value={}",
                        if resp.current_value.is_empty() {
                            "(key not found)".to_string()
                        } else {
                            format!("\"{}\"", resp.current_value)
                        }
                    );
                }
            }
            Err(e) => {
                println!("Error: {}", e);
            }
        }
    } else {
        // Normal set
        let request = tonic::Request::new(SetRequest {
            key: key.to_string(),
            value: value.to_string(),
            ttl_seconds: ttl,
        });
        let response = client.set(request).await?;
        if response.into_inner().success {
            println!("OK");
        }
    }
    Ok(())
}

pub async fn run_get(
    addr: String,
    key: &str,
    with_expire_at: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut client = KeyValueClient::connect(addr).await?;
    let request = tonic::Request::new(GetRequest {
        key: key.to_string(),
        include_expire_at: with_expire_at,
    });
    match client.get(request).await {
        Ok(response) => {
            let resp = response.into_inner();
            if with_expire_at {
                println!("value={} expire_at={}", resp.value, resp.expire_at);
            } else {
                println!("{}", resp.value);
            }
        }
        Err(e) => {
            println!("Error: {}", e);
        }
    }
    Ok(())
}

pub async fn run_get_expire_at(addr: String, key: &str) -> Result<(), Box<dyn std::error::Error>> {
    let mut client = KeyValueClient::connect(addr).await?;
    let request = tonic::Request::new(GetExpireAtRequest {
        key: key.to_string(),
    });
    match client.get_expire_at(request).await {
        Ok(response) => {
            let resp = response.into_inner();
            if resp.exists {
                if resp.expire_at == 0 {
                    println!("Key exists, no expiration");
                } else {
                    println!("Key expires at: {} (Unix timestamp)", resp.expire_at);
                }
            } else {
                println!("Key does not exist");
            }
        }
        Err(e) => {
            println!("Error: {}", e);
        }
    }
    Ok(())
}

pub async fn run_delete(addr: String, key: &str) -> Result<(), Box<dyn std::error::Error>> {
    let mut client = KeyValueClient::connect(addr).await?;
    let request = tonic::Request::new(DeleteRequest {
        key: key.to_string(),
    });
    let response = client.delete(request).await?;
    println!("RESPONSE={:?}", response);
    Ok(())
}
