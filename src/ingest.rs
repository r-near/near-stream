//! Block ingestion from NEAR blockchain via neardata.xyz
//!
//! This module handles:
//! - Discovering the latest finalized block height
//! - Polling for new blocks sequentially
//! - Publishing blocks to subscribers via broadcast channel

use anyhow::Result;
use reqwest_middleware::{ClientBuilder, ClientWithMiddleware};
use reqwest_retry::{policies::ExponentialBackoff, RetryTransientMiddleware};
use serde_json::Value;
use std::time::Duration;
use tokio::sync::broadcast;
use tokio::time::sleep;
use tracing::{error, info, warn};

use crate::types::BlockMsg;

#[derive(Clone)]
pub struct IngestConfig {
    pub neardata_base: String,
    pub poll_retry_ms: u64,
}

/// Creates a configured HTTP client with automatic retries and exponential backoff
fn create_http_client() -> ClientWithMiddleware {
    let reqwest_client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .connect_timeout(Duration::from_secs(5))
        .pool_idle_timeout(Duration::from_secs(90))
        .build()
        .expect("Failed to build HTTP client");

    let retry_policy = ExponentialBackoff::builder()
        .build_with_max_retries(5);

    ClientBuilder::new(reqwest_client)
        .with(RetryTransientMiddleware::new_with_policy(retry_policy))
        .build()
}

/// Discovers the latest finalized block height from neardata.xyz.
///
/// Uses the /v0/last_block/final endpoint which redirects to /v0/block/<height>
async fn discover_latest_height(client: &ClientWithMiddleware, cfg: &IngestConfig) -> Result<u64> {
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
async fn fetch_block(client: &ClientWithMiddleware, cfg: &IngestConfig, height: u64) -> Result<Option<Value>> {
    let url = format!("{}/v0/block/{}", cfg.neardata_base, height);
    let resp = client.get(&url).send().await?;
    let status = resp.status();

    if !status.is_success() {
        if status.as_u16() == 429 {
            warn!(
                height,
                "Rate limited (429) - consider increasing POLL_RETRY_MS"
            );
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

/// Main ingestion loop - polls for new blocks and broadcasts them
pub async fn run_ingestor(cfg: IngestConfig, tx: broadcast::Sender<BlockMsg>) -> Result<()> {
    info!("Starting block ingestor");

    let client = create_http_client();
    let mut next_height = discover_latest_height(&client, &cfg).await?;

    loop {
        // Check what's the latest finalized block
        match discover_latest_height(&client, &cfg).await {
            Ok(latest_finalized) => {
                if next_height <= latest_finalized {
                    // We have blocks to catch up on
                    let blocks_behind = latest_finalized - next_height + 1;
                    info!(
                        next_height,
                        latest_finalized,
                        blocks_behind,
                        "Catching up to latest finalized block"
                    );

                    // Fetch all blocks from next_height to latest_finalized
                    while next_height <= latest_finalized {
                        match fetch_block(&client, &cfg, next_height).await {
                            Ok(Some(block)) => {
                                let msg = BlockMsg {
                                    height: next_height,
                                    block,
                                };

                                // Broadcast to all subscribers (ignore if no receivers)
                                let _ = tx.send(msg);

                                info!(height = next_height, "Ingested and broadcast block");
                                next_height += 1;
                            }
                            Ok(None) => {
                                warn!(
                                    height = next_height,
                                    latest_finalized,
                                    "Block not available yet (unexpected - should be finalized)"
                                );
                                break; // Exit inner loop, will re-check finality
                            }
                            Err(err) => {
                                error!(
                                    height = next_height,
                                    error = ?err,
                                    "Failed to fetch block"
                                );
                                break; // Exit inner loop, will retry after sleep
                            }
                        }
                    }
                } else {
                    // We're caught up, wait for new finalized blocks
                    info!(next_height, "Caught up, waiting for new finalized blocks");
                    sleep(Duration::from_millis(cfg.poll_retry_ms)).await;
                }
            }
            Err(err) => {
                error!(error = ?err, "Failed to discover latest finalized height");
                sleep(Duration::from_millis(cfg.poll_retry_ms)).await;
            }
        }
    }
}
