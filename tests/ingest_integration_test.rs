//! Integration tests for block ingestion with mocked API responses
//!
//! Tests various real-world scenarios:
//! - Skipped blocks
//! - Rate limiting
//! - API lag (blocks not available immediately)
//! - Consecutive skipped blocks
//!
//! NOTE: Run with `--test-threads=1` to avoid Redis stream name conflicts:
//! `cargo test --test ingest_integration_test -- --test-threads=1`

use serde_json::json;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// Helper to create a mock block with specified height and prev_height
fn mock_block(height: u64, prev_height: u64) -> serde_json::Value {
    json!({
        "header": {
            "height": height,
            "prev_height": prev_height,
            "timestamp": 1234567890,
        },
        "chunks": []
    })
}

/// Test that skipped blocks are detected via lookahead
#[tokio::test]
async fn test_skipped_block_via_lookahead() {
    let mock_server = MockServer::start().await;

    // Setup: Block 100 exists, 101 is skipped, 102 exists and points to 100
    let block_100 = mock_block(100, 99);
    let block_102 = mock_block(102, 100); // Skips 101!

    // Track how many times we fetched each block
    let block_101_calls = Arc::new(AtomicUsize::new(0));
    let block_102_calls = Arc::new(AtomicUsize::new(0));

    // Mock /v0/last_block/final - redirect to block 100
    let final_url = format!("{}/v0/block/100", mock_server.uri());
    Mock::given(method("GET"))
        .and(path("/v0/last_block/final"))
        .respond_with(
            ResponseTemplate::new(302).insert_header("Location", final_url.as_str()),
        )
        .mount(&mock_server)
        .await;

    // Block 100 - available
    Mock::given(method("GET"))
        .and(path("/v0/block/100"))
        .respond_with(ResponseTemplate::new(200).set_body_json(&block_100))
        .mount(&mock_server)
        .await;

    // Block 101 - returns null (skipped)
    let counter_101 = block_101_calls.clone();
    Mock::given(method("GET"))
        .and(path("/v0/block/101"))
        .respond_with(move |_: &wiremock::Request| {
            counter_101.fetch_add(1, Ordering::SeqCst);
            ResponseTemplate::new(200).set_body_json(serde_json::Value::Null)
        })
        .mount(&mock_server)
        .await;

    // Block 102 - available (lookahead will find this)
    let counter_102 = block_102_calls.clone();
    Mock::given(method("GET"))
        .and(path("/v0/block/102"))
        .respond_with(move |_: &wiremock::Request| {
            counter_102.fetch_add(1, Ordering::SeqCst);
            ResponseTemplate::new(200).set_body_json(&block_102)
        })
        .mount(&mock_server)
        .await;

    // Block 103 - to keep it going
    Mock::given(method("GET"))
        .and(path("/v0/block/103"))
        .respond_with(ResponseTemplate::new(200).set_body_json(mock_block(103, 102)))
        .mount(&mock_server)
        .await;

    // Setup Redis and flush all data
    let redis_client = redis::Client::open("redis://localhost:6379").unwrap();
    let mut redis_conn = redis::aio::ConnectionManager::new(redis_client)
        .await
        .unwrap();

    // Flush entire database to ensure clean state for height-based IDs
    let _: () = redis::cmd("FLUSHDB")
        .query_async(&mut redis_conn)
        .await
        .unwrap();

    // Run ingester in background
    let config = near_stream::ingest::IngestConfig {
        neardata_base: mock_server.uri(),
    };

    let ingest_handle = tokio::spawn(async move {
        near_stream::ingest::run_ingestor(config, redis_conn).await
    });

    // Wait for ingestion to process blocks
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Verify block 101 was fetched (trying to get it)
    assert!(
        block_101_calls.load(Ordering::SeqCst) > 0,
        "Should have attempted to fetch block 101"
    );

    // Verify block 102 was fetched (lookahead found it)
    assert!(
        block_102_calls.load(Ordering::SeqCst) > 0,
        "Should have fetched block 102 via lookahead"
    );

    println!("Block 101 fetch attempts: {}", block_101_calls.load(Ordering::SeqCst));
    println!("Block 102 fetch attempts: {}", block_102_calls.load(Ordering::SeqCst));

    ingest_handle.abort();
}

/// Test that rate limiting causes proper backoff
#[tokio::test]
async fn test_rate_limit_causes_backoff() {
    let mock_server = MockServer::start().await;

    let block_200 = mock_block(200, 199);
    let rate_limit_count = Arc::new(AtomicUsize::new(0));

    // Mock finalized endpoint
    let final_url = format!("{}/v0/block/200", mock_server.uri());
    Mock::given(method("GET"))
        .and(path("/v0/last_block/final"))
        .respond_with(
            ResponseTemplate::new(302).insert_header("Location", final_url.as_str()),
        )
        .mount(&mock_server)
        .await;

    // Block 200 - available
    Mock::given(method("GET"))
        .and(path("/v0/block/200"))
        .respond_with(ResponseTemplate::new(200).set_body_json(&block_200))
        .mount(&mock_server)
        .await;

    // Block 201 - rate limited first 2 times, then available
    let counter = rate_limit_count.clone();
    Mock::given(method("GET"))
        .and(path("/v0/block/201"))
        .respond_with(move |_: &wiremock::Request| {
            let count = counter.fetch_add(1, Ordering::SeqCst);
            if count < 2 {
                ResponseTemplate::new(429) // Rate limited
            } else {
                ResponseTemplate::new(200).set_body_json(mock_block(201, 200))
            }
        })
        .mount(&mock_server)
        .await;

    let redis_client = redis::Client::open("redis://localhost:6379").unwrap();
    let mut redis_conn = redis::aio::ConnectionManager::new(redis_client)
        .await
        .unwrap();

    // Flush entire database to ensure clean state for height-based IDs
    let _: () = redis::cmd("FLUSHDB")
        .query_async(&mut redis_conn)
        .await
        .unwrap();

    let config = near_stream::ingest::IngestConfig {
        neardata_base: mock_server.uri(),
    };

    let ingest_handle = tokio::spawn(async move {
        near_stream::ingest::run_ingestor(config, redis_conn).await
    });

    // Should eventually succeed after backoff
    tokio::time::sleep(Duration::from_secs(10)).await;

    // Verify we hit rate limit at least twice before succeeding
    let attempts = rate_limit_count.load(Ordering::SeqCst);
    assert!(
        attempts >= 3,
        "Should have retried after rate limits (attempts: {})",
        attempts
    );

    println!("Block 201 fetch attempts (including rate limits): {}", attempts);

    ingest_handle.abort();
}

/// Test API lag where block becomes available after several attempts
#[tokio::test]
async fn test_api_lag_eventually_succeeds() {
    let mock_server = MockServer::start().await;

    let block_300 = mock_block(300, 299);
    let lag_attempts = Arc::new(AtomicUsize::new(0));

    // Mock finalized endpoint - starts at 300 then updates to 310
    let finalized_calls = Arc::new(AtomicUsize::new(0));
    let finalized_counter = finalized_calls.clone();

    Mock::given(method("GET"))
        .and(path("/v0/last_block/final"))
        .respond_with(move |_: &wiremock::Request| {
            let count = finalized_counter.fetch_add(1, Ordering::SeqCst);
            let block_height = if count < 2 { 300 } else { 310 };
            let final_url = format!("http://example.com/v0/block/{}", block_height);
            ResponseTemplate::new(302).insert_header("Location", final_url.as_str())
        })
        .mount(&mock_server)
        .await;

    Mock::given(method("GET"))
        .and(path("/v0/block/300"))
        .respond_with(ResponseTemplate::new(200).set_body_json(&block_300))
        .mount(&mock_server)
        .await;

    // Block 301 - returns null first 3 times (API lag), then becomes available
    let counter = lag_attempts.clone();
    Mock::given(method("GET"))
        .and(path("/v0/block/301"))
        .respond_with(move |_: &wiremock::Request| {
            let count = counter.fetch_add(1, Ordering::SeqCst);
            if count < 3 {
                ResponseTemplate::new(200).set_body_json(serde_json::Value::Null)
            } else {
                ResponseTemplate::new(200).set_body_json(mock_block(301, 300))
            }
        })
        .mount(&mock_server)
        .await;

    // Add more blocks so it keeps running
    for height in 302..=310 {
        Mock::given(method("GET"))
            .and(path(format!("/v0/block/{}", height)))
            .respond_with(ResponseTemplate::new(200).set_body_json(mock_block(height, height - 1)))
            .mount(&mock_server)
            .await;
    }

    let redis_client = redis::Client::open("redis://localhost:6379").unwrap();
    let mut redis_conn = redis::aio::ConnectionManager::new(redis_client)
        .await
        .unwrap();

    // Flush entire database to ensure clean state for height-based IDs
    let _: () = redis::cmd("FLUSHDB")
        .query_async(&mut redis_conn)
        .await
        .unwrap();

    let config = near_stream::ingest::IngestConfig {
        neardata_base: mock_server.uri(),
    };

    let ingest_handle = tokio::spawn(async move {
        near_stream::ingest::run_ingestor(config, redis_conn).await
    });

    // Wait for block to eventually be available (longer timeout for test concurrency)
    tokio::time::sleep(Duration::from_secs(15)).await;

    // Should have tried multiple times before succeeding
    let attempts = lag_attempts.load(Ordering::SeqCst);
    assert!(
        attempts >= 4,
        "Should have retried until block became available (attempts: {})",
        attempts
    );

    println!("Block 301 fetch attempts during API lag: {}", attempts);

    ingest_handle.abort();
}

/// Test that finality check happens immediately when lookahead fails
/// This validates the fix where we check finality right away instead of waiting
#[tokio::test]
async fn test_immediate_finality_check_on_unavailable_block() {
    let mock_server = MockServer::start().await;

    let block_500 = mock_block(500, 499);
    let finality_check_count = Arc::new(AtomicUsize::new(0));

    // Mock finalized endpoint - track how many times it's called
    let finality_counter = finality_check_count.clone();
    Mock::given(method("GET"))
        .and(path("/v0/last_block/final"))
        .respond_with(move |_: &wiremock::Request| {
            let count = finality_counter.fetch_add(1, Ordering::SeqCst);
            // First call returns 500, subsequent calls return 520 (well ahead - definitely skipped)
            let block_height = if count == 0 { 500 } else { 520 };
            let final_url = format!("http://example.com/v0/block/{}", block_height);
            ResponseTemplate::new(302).insert_header("Location", final_url.as_str())
        })
        .mount(&mock_server)
        .await;

    Mock::given(method("GET"))
        .and(path("/v0/block/500"))
        .respond_with(ResponseTemplate::new(200).set_body_json(&block_500))
        .mount(&mock_server)
        .await;

    // Block 501 - returns null initially (to simulate it being unavailable/skipped)
    let block_501_calls = Arc::new(AtomicUsize::new(0));
    let counter_501 = block_501_calls.clone();
    Mock::given(method("GET"))
        .and(path("/v0/block/501"))
        .respond_with(move |_: &wiremock::Request| {
            counter_501.fetch_add(1, Ordering::SeqCst);
            // Always return null to simulate skipped block
            ResponseTemplate::new(200).set_body_json(serde_json::Value::Null)
        })
        .mount(&mock_server)
        .await;

    // Lookahead blocks 502, 503 - also return null (can't confirm via lookahead)
    Mock::given(method("GET"))
        .and(path("/v0/block/502"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::Value::Null))
        .mount(&mock_server)
        .await;

    Mock::given(method("GET"))
        .and(path("/v0/block/503"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::Value::Null))
        .mount(&mock_server)
        .await;

    // Block 504 onwards are available - block 504 points to 500 (skipping 501-503)
    Mock::given(method("GET"))
        .and(path("/v0/block/504"))
        .respond_with(ResponseTemplate::new(200).set_body_json(mock_block(504, 500)))
        .mount(&mock_server)
        .await;

    // Remaining blocks
    for height in 505..=525 {
        Mock::given(method("GET"))
            .and(path(format!("/v0/block/{}", height)))
            .respond_with(ResponseTemplate::new(200).set_body_json(mock_block(height, height - 1)))
            .mount(&mock_server)
            .await;
    }

    let redis_client = redis::Client::open("redis://localhost:6379").unwrap();
    let mut redis_conn = redis::aio::ConnectionManager::new(redis_client)
        .await
        .unwrap();

    // Flush entire database to ensure clean state for height-based IDs
    let _: () = redis::cmd("FLUSHDB")
        .query_async(&mut redis_conn)
        .await
        .unwrap();

    let config = near_stream::ingest::IngestConfig {
        neardata_base: mock_server.uri(),
    };

    let ingest_handle = tokio::spawn(async move {
        near_stream::ingest::run_ingestor(config, redis_conn).await
    });

    // Wait for immediate finality check to happen (should be quick with the fix)
    let mut checks_snapshot = 0;
    let detection_start = std::time::Instant::now();

    for _ in 0..30 {  // Check every 100ms for up to 3 seconds
        tokio::time::sleep(Duration::from_millis(100)).await;
        checks_snapshot = finality_check_count.load(Ordering::SeqCst);
        if checks_snapshot >= 2 {
            break;  // Detected!
        }
    }

    let detection_time = detection_start.elapsed();

    ingest_handle.abort();

    println!("Time to detect skip: {:?}", detection_time);
    println!("Finality checks performed: {}", checks_snapshot);

    // With the fix, we should have checked finality at least twice:
    // 1. Initial discovery (~0.2s)
    // 2. Immediate check when block 501 can't be verified via lookahead (~0.8s total)
    assert!(
        checks_snapshot >= 2,
        "Should have performed immediate finality check when lookahead failed (got {} checks)",
        checks_snapshot
    );

    // With the fix, detection should happen within 2 seconds
    // Without the fix, would wait 30+ seconds for periodic finality check
    assert!(
        detection_time < Duration::from_secs(2),
        "Should detect skipped block quickly via immediate finality check (took {:?})",
        detection_time
    );
}

/// Test consecutive skipped blocks
#[tokio::test]
async fn test_consecutive_skipped_blocks() {
    let mock_server = MockServer::start().await;

    let block_400 = mock_block(400, 399);
    // Blocks 401 and 402 are both skipped
    let block_403 = mock_block(403, 400); // Skips 401 and 402!

    // Mock finalized endpoint
    let final_url = format!("{}/v0/block/400", mock_server.uri());
    Mock::given(method("GET"))
        .and(path("/v0/last_block/final"))
        .respond_with(
            ResponseTemplate::new(302).insert_header("Location", final_url.as_str()),
        )
        .mount(&mock_server)
        .await;

    Mock::given(method("GET"))
        .and(path("/v0/block/400"))
        .respond_with(ResponseTemplate::new(200).set_body_json(&block_400))
        .mount(&mock_server)
        .await;

    // Blocks 401 and 402 - both return null
    Mock::given(method("GET"))
        .and(path("/v0/block/401"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::Value::Null))
        .mount(&mock_server)
        .await;

    Mock::given(method("GET"))
        .and(path("/v0/block/402"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::Value::Null))
        .mount(&mock_server)
        .await;

    // Block 403 - available
    let block_403_calls = Arc::new(AtomicUsize::new(0));
    let counter_403 = block_403_calls.clone();
    Mock::given(method("GET"))
        .and(path("/v0/block/403"))
        .respond_with(move |_: &wiremock::Request| {
            counter_403.fetch_add(1, Ordering::SeqCst);
            ResponseTemplate::new(200).set_body_json(&block_403)
        })
        .mount(&mock_server)
        .await;

    Mock::given(method("GET"))
        .and(path("/v0/block/404"))
        .respond_with(ResponseTemplate::new(200).set_body_json(mock_block(404, 403)))
        .mount(&mock_server)
        .await;

    let redis_client = redis::Client::open("redis://localhost:6379").unwrap();
    let mut redis_conn = redis::aio::ConnectionManager::new(redis_client)
        .await
        .unwrap();

    // Flush entire database to ensure clean state for height-based IDs
    let _: () = redis::cmd("FLUSHDB")
        .query_async(&mut redis_conn)
        .await
        .unwrap();

    let config = near_stream::ingest::IngestConfig {
        neardata_base: mock_server.uri(),
    };

    let ingest_handle = tokio::spawn(async move {
        near_stream::ingest::run_ingestor(config, redis_conn).await
    });

    tokio::time::sleep(Duration::from_secs(5)).await;

    // Should have fetched block 403 (finding consecutive skipped blocks via lookahead)
    assert!(
        block_403_calls.load(Ordering::SeqCst) > 0,
        "Should have found block 403 after skipping 401 and 402"
    );

    println!("Successfully detected consecutive skipped blocks 401 and 402");
    println!("Block 403 fetch attempts: {}", block_403_calls.load(Ordering::SeqCst));

    ingest_handle.abort();
}
