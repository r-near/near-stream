//! Block ingestion from NEAR blockchain via neardata.xyz
//!
//! This module handles:
//! - Discovering the latest finalized block height
//! - Polling for new blocks sequentially
//! - Publishing blocks to subscribers via broadcast channel

use anyhow::Result;
use redis::aio::ConnectionManager;
use reqwest::Client;
use serde_json::Value;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{info, warn};

#[derive(Clone)]
pub struct IngestConfig {
    pub neardata_base: String,
}

/// Creates a configured HTTP client with fast timeouts and no automatic retries
fn create_http_client() -> Client {
    Client::builder()
        .timeout(Duration::from_secs(10))
        .connect_timeout(Duration::from_secs(5))
        .pool_idle_timeout(Duration::from_secs(90))
        .build()
        .expect("Failed to build HTTP client")
}

/// Discovers the latest finalized block height from neardata.xyz.
///
/// Uses the /v0/last_block/final endpoint which redirects to /v0/block/<height>
async fn discover_latest_height(client: &Client, cfg: &IngestConfig) -> Result<u64> {
    let url = format!("{}/v0/last_block/final", cfg.neardata_base);
    let resp = client.get(&url).send().await?;
    let status = resp.status();

    if status.as_u16() == 429 {
        anyhow::bail!("Rate limited (429) when discovering latest height");
    }

    let final_url = resp.url();

    // Parse height from URL path: /v0/block/<height>
    let path = final_url.path();
    let segments: Vec<&str> = path.split('/').collect();

    let height = final_url
        .path_segments()
        .and_then(|mut segments| segments.next_back())
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| {
            anyhow::anyhow!(
                "Failed to parse block height from URL. final_url='{}', path='{}', segments={:?}",
                final_url,
                path,
                segments
            )
        })?;

    info!(height, "Discovered latest finalized block");
    Ok(height)
}

/// Fetches a specific block by height, returns None if not yet available
async fn fetch_block(client: &Client, cfg: &IngestConfig, height: u64) -> Result<Option<Value>> {
    let url = format!("{}/v0/block/{}", cfg.neardata_base, height);
    let resp = client.get(&url).send().await?;
    let status = resp.status();

    if !status.is_success() {
        if status.as_u16() == 429 {
            warn!(height, "Rate limited (429) - backing off for 1 second");
            // Sleep here to backoff before returning
            sleep(Duration::from_millis(1000)).await;
        } else {
            warn!(
                height,
                status_code = status.as_u16(),
                "Non-success status when fetching block"
            );
        }
        return Ok(None);
    }

    let json: Value = resp.json().await?;
    // neardata.xyz returns null for blocks that don't exist yet
    if json.is_null() {
        warn!(height, "Block returned null (not available yet)");
        Ok(None)
    } else {
        Ok(Some(json))
    }
}

/// Main ingestion loop - optimistically polls for new blocks and publishes to Redis
pub async fn run_ingestor(cfg: IngestConfig, mut redis_conn: ConnectionManager) -> Result<()> {
    info!("Starting block ingestor");

    let client = create_http_client();
    let mut next_height = discover_latest_height(&client, &cfg).await?;

    info!(next_height, "Starting optimistic ingestion from block");

    loop {
        // Optimistically fetch next block without checking finality first
        match fetch_block(&client, &cfg, next_height).await {
            Ok(Some(block)) => {
                // Publish to Redis Streams
                if let Err(e) = crate::redis_stream::publish_block(&mut redis_conn, next_height, &block).await {
                    warn!(height = next_height, error = ?e, "Failed to publish block to Redis, retrying");
                    sleep(Duration::from_millis(100)).await;
                    continue;
                }

                info!(height = next_height, "Ingested and published block");
                next_height += 1;

                // Small delay to avoid rate limits
                sleep(Duration::from_millis(150)).await;
            }
            Ok(None) => {
                // Block returned null - could be skipped or not available yet
                // Try to verify by checking next block, but only if we're not being rate limited

                // Add a small delay before checking next block to avoid rapid-fire requests
                sleep(Duration::from_millis(200)).await;

                match fetch_block(&client, &cfg, next_height + 1).await {
                    Ok(Some(next_block)) => {
                        // Check if next block's prev_height skips over current height
                        if let Some(prev_height) = next_block
                            .pointer("/header/prev_height")
                            .and_then(|v| v.as_u64())
                        {
                            if prev_height < next_height {
                                // Confirmed: block was skipped by validator
                                warn!(
                                    height = next_height,
                                    prev_height,
                                    "Block skipped by validator"
                                );
                                next_height += 1;
                                continue;
                            }
                        }
                        // Next block exists but doesn't skip current block
                        // Current block must not be available yet
                        info!(height = next_height, "Block not available yet, waiting");
                        sleep(Duration::from_secs(1)).await;
                    }
                    Ok(None) | Err(_) => {
                        // Either next block is null (at chain head) or we got an error
                        // (rate limit, network issue, etc). In any case, we can't verify
                        // if the current block was skipped, so wait and retry.
                        info!(height = next_height, "At chain head or cannot verify block, waiting");
                        sleep(Duration::from_secs(2)).await;
                    }
                }
            }
            Err(err) => {
                // Check if error indicates we're too far ahead
                let err_str = err.to_string();
                if err_str.contains("BLOCK_DOES_NOT_EXIST") || err_str.contains("too far in the future") {
                    info!(height = next_height, "Ahead of finality, waiting");
                    sleep(Duration::from_secs(1)).await;
                } else {
                    warn!(
                        height = next_height,
                        error = ?err,
                        "Failed to fetch block, retrying"
                    );
                    sleep(Duration::from_millis(500)).await;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    /// Test that we correctly detect skipped blocks using prev_height verification
    /// Based on real NEAR data: block 170797835 was skipped
    #[test]
    fn test_detect_skipped_block() {
        // Block 170797835 returns null (skipped by validator)
        // Block 170797836 exists and points back to 170797834
        let block_170797836 = json!({
            "header": {
                "height": 170797836,
                "prev_height": 170797834  // Skips over 170797835!
            }
        });

        // Verify the detection logic
        let current_height = 170797835;
        let next_block = &block_170797836;

        let prev_height = next_block
            .pointer("/header/prev_height")
            .and_then(|v| v.as_u64());

        assert_eq!(prev_height, Some(170797834));
        assert!(prev_height.unwrap() < current_height,
            "prev_height should be less than current_height, confirming block was skipped");
    }

    /// Test that we don't incorrectly mark sequential blocks as skipped
    #[test]
    fn test_sequential_blocks_not_skipped() {
        // Sequential blocks with no gaps
        let block_101 = json!({
            "header": {
                "height": 101,
                "prev_height": 100  // Sequential, not skipped
            }
        });

        let current_height = 100;
        let next_block = &block_101;

        let prev_height = next_block
            .pointer("/header/prev_height")
            .and_then(|v| v.as_u64());

        assert_eq!(prev_height, Some(100));
        assert!(prev_height.unwrap() >= current_height,
            "prev_height should equal current_height for sequential blocks");
    }
}
