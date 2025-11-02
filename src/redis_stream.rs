//! Redis Streams integration for block distribution
//!
//! This module handles:
//! - Connecting to Redis server
//! - Publishing blocks to Redis Streams
//! - Subscribing to blocks from Redis Streams with automatic trimming

use anyhow::Result;
use redis::{aio::ConnectionManager, AsyncCommands, streams::{StreamReadOptions, StreamReadReply}};
use serde_json::Value;
use tracing::info;

const STREAM_KEY: &str = "blocks";
const MAX_BLOCKS: usize = 256;

/// Setup Redis connection
pub async fn setup_redis(redis_url: &str) -> Result<ConnectionManager> {
    info!(redis_url, "Connecting to Redis");
    let client = redis::Client::open(redis_url)?;
    let conn = ConnectionManager::new(client).await?;
    Ok(conn)
}

/// Publish a block to Redis Streams
pub async fn publish_block(
    conn: &mut ConnectionManager,
    height: u64,
    block: &Value,
) -> Result<String> {
    let block_json = serde_json::to_string(block)?;

    // Use height as entry ID for efficient range queries
    let entry_id = format!("{}-0", height);

    let id: String = conn
        .xadd(
            STREAM_KEY,
            entry_id,
            &[("height", height.to_string()), ("block", block_json)],
        )
        .await?;

    // Trim to keep only latest MAX_BLOCKS
    let len: usize = conn.xlen(STREAM_KEY).await?;
    if len > MAX_BLOCKS {
        let _: () = conn
            .xtrim(STREAM_KEY, redis::streams::StreamMaxlen::Approx(MAX_BLOCKS))
            .await?;
    }

    Ok(id)
}

/// Subscribe to blocks from Redis Streams starting from a specific height or ID
pub async fn read_blocks(
    conn: &mut ConnectionManager,
    last_id: Option<String>,
) -> Result<StreamReadReply> {
    let start_id = last_id.unwrap_or_else(|| "$".to_string());

    let opts = StreamReadOptions::default()
        .block(1000); // Block for 1 second waiting for new messages

    let results: StreamReadReply = conn
        .xread_options(&[STREAM_KEY], &[&start_id], &opts)
        .await?;

    Ok(results)
}


/// Get all blocks for catch-up (from beginning or from specific height)
pub async fn get_catchup_blocks(
    conn: &mut ConnectionManager,
    from_height: Option<u64>,
) -> Result<Vec<(u64, Value, String)>> {
    use redis::streams::StreamRangeReply;

    // If no from_height specified, return empty - client will start from live stream
    let Some(from_height) = from_height else {
        return Ok(Vec::new());
    };

    // Start from next block after from_height
    let start_id = format!("{}-0", from_height + 1);

    // Read only blocks >= from_height using XRANGE with height-based IDs
    let results: StreamRangeReply = conn
        .xrange(STREAM_KEY, &start_id, "+")
        .await?;

    let mut blocks = Vec::new();

    for stream_id in results.ids {
        // Parse height and block from the entry
        let height: u64 = stream_id.map.get("height")
            .and_then(|v| match v {
                redis::Value::BulkString(s) => String::from_utf8(s.clone()).ok(),
                redis::Value::SimpleString(s) => Some(s.clone()),
                _ => None,
            })
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);

        let block_json = stream_id.map.get("block")
            .and_then(|v| match v {
                redis::Value::BulkString(s) => String::from_utf8(s.clone()).ok(),
                redis::Value::SimpleString(s) => Some(s.clone()),
                _ => None,
            })
            .unwrap_or_default();

        if let Ok(block) = serde_json::from_str(&block_json) {
            blocks.push((height, block, stream_id.id.clone()));
        }
    }

    Ok(blocks)
}
