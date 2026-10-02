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
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// Helper to create a mock block with specified height and prev_height
fn mock_block(height: u64, prev_height: u64) -> serde_json::Value {
    json!({
        "block": { "header": {
            "height": height,
            "prev_height": prev_height,
            "timestamp": 1234567890,
        },
        "chunks": [] },
        "shards": []
    })
}

/// Explicit latest-head startup skips backlog while preserving cached entries.
#[tokio::test]
async fn test_start_from_latest_preserves_cache_and_skips_persisted_cursor() {
    let server = MockServer::start().await;
    Mock::given(path("/v0/last_block/final"))
        .respond_with(ResponseTemplate::new(302).insert_header("Location", "/v0/block/200"))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(path("/v0/block/200"))
        .respond_with(ResponseTemplate::new(200).set_body_json(mock_block(200, 199)))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(path("/v0/block/201"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(serde_json::Value::Null)
                .set_delay(Duration::from_secs(10)),
        )
        .mount(&server)
        .await;
    let redis_client = redis::Client::open(
        std::env::var("TEST_REDIS_URL").expect("Set TEST_REDIS_URL to a disposable Redis database"),
    )
    .unwrap();
    let mut conn = redis::aio::ConnectionManager::new(redis_client)
        .await
        .unwrap();
    redis::cmd("FLUSHDB")
        .query_async::<()>(&mut conn)
        .await
        .unwrap();
    near_stream::redis_stream::publish_block(&mut conn, 100, &mock_block(100, 99))
        .await
        .unwrap();
    let config = near_stream::ingest::IngestConfig {
        neardata_base: server.uri(),
        request_interval: Duration::from_millis(1),
        poll_retry: Duration::from_millis(100),
        start_from_latest: true,
    };
    let handle = tokio::spawn(near_stream::ingest::run_ingestor(config, conn.clone()));
    tokio::time::timeout(Duration::from_secs(3), async {
        while near_stream::redis_stream::last_published_height(&mut conn)
            .await
            .unwrap()
            != Some(200)
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    handle.abort();
    assert!(handle.await.unwrap_err().is_cancelled());
    let blocks = near_stream::redis_stream::get_catchup_blocks(&mut conn, Some(99))
        .await
        .unwrap();
    assert_eq!(
        blocks
            .iter()
            .map(|(height, _, _)| *height)
            .collect::<Vec<_>>(),
        vec![100, 200]
    );
    assert!(server.received_requests().await.unwrap().iter().all(|r| {
        !r.url.path().starts_with("/v0/block/")
            || matches!(r.url.path(), "/v0/block/200" | "/v0/block/201")
    }));
}

/// Restart against a different endpoint while its head is ahead of our cursor.
/// Catch-up must preserve every available block and still advance a skipped 404.
#[tokio::test]
async fn test_restart_resumes_persisted_cursor_across_endpoint_change() {
    let first = MockServer::start().await;
    Mock::given(path("/v0/last_block/final"))
        .respond_with(ResponseTemplate::new(302).insert_header("Location", "/v0/block/100"))
        .expect(1)
        .mount(&first)
        .await;
    Mock::given(path("/v0/block/100"))
        .respond_with(ResponseTemplate::new(200).set_body_json(mock_block(100, 99)))
        .expect(1)
        .mount(&first)
        .await;

    let redis_client = redis::Client::open(
        std::env::var("TEST_REDIS_URL").expect("Set TEST_REDIS_URL to a disposable Redis database"),
    )
    .unwrap();
    let mut redis_conn = redis::aio::ConnectionManager::new(redis_client)
        .await
        .unwrap();
    redis::cmd("FLUSHDB")
        .query_async::<()>(&mut redis_conn)
        .await
        .unwrap();
    assert_eq!(
        near_stream::redis_stream::last_published_height(&mut redis_conn)
            .await
            .unwrap(),
        None
    );
    let config = near_stream::ingest::IngestConfig {
        neardata_base: first.uri(),
        request_interval: Duration::from_millis(1),
        poll_retry: Duration::from_millis(100),
        start_from_latest: false,
    };
    let handle = tokio::spawn(near_stream::ingest::run_ingestor(
        config,
        redis_conn.clone(),
    ));
    tokio::time::timeout(Duration::from_secs(3), async {
        while near_stream::redis_stream::last_published_height(&mut redis_conn)
            .await
            .unwrap()
            != Some(100)
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    handle.abort();
    assert!(handle.await.unwrap_err().is_cancelled());

    let second = MockServer::start().await;
    Mock::given(path("/v0/last_block/final"))
        .respond_with(ResponseTemplate::new(302).insert_header("Location", "/v0/block/200"))
        .expect(0)
        .mount(&second)
        .await;
    Mock::given(path("/v0/block/101"))
        .respond_with(ResponseTemplate::new(404))
        .expect(1)
        .mount(&second)
        .await;
    for (height, prev_height) in [(102, 100), (103, 102)] {
        Mock::given(path(format!("/v0/block/{height}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(mock_block(height, prev_height)))
            .mount(&second)
            .await;
    }
    let config = near_stream::ingest::IngestConfig {
        neardata_base: second.uri(),
        request_interval: Duration::from_millis(1),
        poll_retry: Duration::from_millis(100),
        start_from_latest: false,
    };
    let handle = tokio::spawn(near_stream::ingest::run_ingestor(
        config,
        redis_conn.clone(),
    ));
    tokio::time::timeout(Duration::from_secs(3), async {
        while near_stream::redis_stream::last_published_height(&mut redis_conn)
            .await
            .unwrap()
            != Some(103)
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    handle.abort();
    assert!(handle.await.unwrap_err().is_cancelled());

    let blocks = near_stream::redis_stream::get_catchup_blocks(&mut redis_conn, Some(99))
        .await
        .unwrap();
    assert_eq!(
        blocks
            .iter()
            .map(|(height, _, _)| *height)
            .collect::<Vec<_>>(),
        vec![100, 102, 103]
    );
    assert!(second
        .received_requests()
        .await
        .unwrap()
        .iter()
        .all(|r| r.url.path() != "/v0/block/200"));
}

/// A malformed durable cursor must fail instead of silently starting at the tip.
#[tokio::test]
async fn test_invalid_persisted_cursor_does_not_jump_to_head() {
    let server = MockServer::start().await;
    let redis_client = redis::Client::open(
        std::env::var("TEST_REDIS_URL").expect("Set TEST_REDIS_URL to a disposable Redis database"),
    )
    .unwrap();
    let mut conn = redis::aio::ConnectionManager::new(redis_client)
        .await
        .unwrap();
    redis::cmd("FLUSHDB")
        .query_async::<()>(&mut conn)
        .await
        .unwrap();
    redis::cmd("XADD")
        .arg("blocks")
        .arg("100-1")
        .arg("height")
        .arg(100)
        .arg("block")
        .arg("{}")
        .query_async::<String>(&mut conn)
        .await
        .unwrap();
    let config = near_stream::ingest::IngestConfig {
        neardata_base: server.uri(),
        request_interval: Duration::from_millis(1),
        poll_retry: Duration::from_millis(100),
        start_from_latest: false,
    };
    let error = near_stream::ingest::run_ingestor(config, conn)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("not a height-based ID"));
    assert!(server.received_requests().await.unwrap().is_empty());
}

/// A successful HTTP response with another height must never enter the stream.
#[tokio::test]
async fn test_wrong_response_height_does_not_advance_persisted_cursor() {
    let server = MockServer::start().await;
    let calls = Arc::new(AtomicUsize::new(0));
    let counter = calls.clone();
    let allow_valid_response = Arc::new(AtomicBool::new(false));
    let response_gate = allow_valid_response.clone();
    Mock::given(path("/v0/block/101"))
        .respond_with(move |_: &wiremock::Request| {
            counter.fetch_add(1, Ordering::SeqCst);
            let height = if response_gate.load(Ordering::SeqCst) {
                101
            } else {
                999
            };
            ResponseTemplate::new(200).set_body_json(mock_block(height, 100))
        })
        .mount(&server)
        .await;
    let redis_client = redis::Client::open(
        std::env::var("TEST_REDIS_URL").expect("Set TEST_REDIS_URL to a disposable Redis database"),
    )
    .unwrap();
    let mut conn = redis::aio::ConnectionManager::new(redis_client)
        .await
        .unwrap();
    redis::cmd("FLUSHDB")
        .query_async::<()>(&mut conn)
        .await
        .unwrap();
    near_stream::redis_stream::publish_block(&mut conn, 100, &mock_block(100, 99))
        .await
        .unwrap();
    let config = near_stream::ingest::IngestConfig {
        neardata_base: server.uri(),
        request_interval: Duration::from_millis(1),
        poll_retry: Duration::from_millis(250),
        start_from_latest: false,
    };
    let handle = tokio::spawn(near_stream::ingest::run_ingestor(config, conn.clone()));
    tokio::time::timeout(Duration::from_secs(3), async {
        while calls.load(Ordering::SeqCst) == 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(25)).await;
    assert_eq!(
        near_stream::redis_stream::last_published_height(&mut conn)
            .await
            .unwrap(),
        Some(100)
    );
    allow_valid_response.store(true, Ordering::SeqCst);
    tokio::time::timeout(Duration::from_secs(3), async {
        while near_stream::redis_stream::last_published_height(&mut conn)
            .await
            .unwrap()
            != Some(101)
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    handle.abort();
    assert!(handle.await.unwrap_err().is_cancelled());
    assert!(calls.load(Ordering::SeqCst) >= 2);
    let blocks = near_stream::redis_stream::get_catchup_blocks(&mut conn, Some(100))
        .await
        .unwrap();
    assert_eq!(blocks.len(), 1);
    assert_eq!(blocks[0].0, 101);
    assert_eq!(
        blocks[0].1.pointer("/block/header/height"),
        Some(&json!(101))
    );
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
        .respond_with(ResponseTemplate::new(302).insert_header("Location", final_url.as_str()))
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
    let redis_client = redis::Client::open(
        std::env::var("TEST_REDIS_URL").expect("Set TEST_REDIS_URL to a disposable Redis database"),
    )
    .unwrap();
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
        request_interval: Duration::from_millis(1),
        poll_retry: Duration::from_millis(100),
        start_from_latest: false,
    };

    let ingestor_conn = redis_conn.clone();
    let ingest_handle =
        tokio::spawn(async move { near_stream::ingest::run_ingestor(config, ingestor_conn).await });

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

    let published = near_stream::redis_stream::get_catchup_blocks(&mut redis_conn, Some(100))
        .await
        .unwrap();
    assert!(published.iter().any(|(height, _, _)| *height == 102));
    assert!(!published.iter().any(|(height, _, _)| *height == 101));

    println!(
        "Block 101 fetch attempts: {}",
        block_101_calls.load(Ordering::SeqCst)
    );
    println!(
        "Block 102 fetch attempts: {}",
        block_102_calls.load(Ordering::SeqCst)
    );

    ingest_handle.abort();
}

/// Test that rate limiting pauses requests rather than retrying every second
#[tokio::test]
async fn test_rate_limit_causes_backoff() {
    let mock_server = MockServer::start().await;

    let block_200 = mock_block(200, 199);
    let rate_limit_count = Arc::new(AtomicUsize::new(0));

    // Mock finalized endpoint
    let final_url = format!("{}/v0/block/200", mock_server.uri());
    Mock::given(method("GET"))
        .and(path("/v0/last_block/final"))
        .respond_with(ResponseTemplate::new(302).insert_header("Location", final_url.as_str()))
        .mount(&mock_server)
        .await;

    // Block 200 - available
    Mock::given(method("GET"))
        .and(path("/v0/block/200"))
        .respond_with(ResponseTemplate::new(200).set_body_json(&block_200))
        .mount(&mock_server)
        .await;

    // Block 201 - rate limited first 2 times, then available after cooldown
    let counter = rate_limit_count.clone();
    Mock::given(method("GET"))
        .and(path("/v0/block/201"))
        .respond_with(move |_: &wiremock::Request| {
            let count = counter.fetch_add(1, Ordering::SeqCst);
            if count < 2 {
                ResponseTemplate::new(429).insert_header("Retry-After", "1") // Rate limited
            } else {
                ResponseTemplate::new(200).set_body_json(mock_block(201, 200))
            }
        })
        .mount(&mock_server)
        .await;

    let redis_client = redis::Client::open(
        std::env::var("TEST_REDIS_URL").expect("Set TEST_REDIS_URL to a disposable Redis database"),
    )
    .unwrap();
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
        request_interval: Duration::from_millis(1),
        poll_retry: Duration::from_millis(100),
        start_from_latest: false,
    };

    let ingest_handle =
        tokio::spawn(async move { near_stream::ingest::run_ingestor(config, redis_conn).await });

    // The shared cooldown must suppress repeated requests during this interval.
    tokio::time::sleep(Duration::from_secs(10)).await;

    // Verify only the initial rate-limited attempt reached NearData.
    let attempts = rate_limit_count.load(Ordering::SeqCst);
    assert!(
        attempts == 1,
        "Should wait for the shared 60s cooldown (attempts: {})",
        attempts
    );

    println!(
        "Block 201 fetch attempts (including rate limits): {}",
        attempts
    );

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

    let redis_client = redis::Client::open(
        std::env::var("TEST_REDIS_URL").expect("Set TEST_REDIS_URL to a disposable Redis database"),
    )
    .unwrap();
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
        request_interval: Duration::from_millis(1),
        poll_retry: Duration::from_millis(100),
        start_from_latest: false,
    };

    let ingest_handle =
        tokio::spawn(async move { near_stream::ingest::run_ingestor(config, redis_conn).await });

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

    let redis_client = redis::Client::open(
        std::env::var("TEST_REDIS_URL").expect("Set TEST_REDIS_URL to a disposable Redis database"),
    )
    .unwrap();
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
        request_interval: Duration::from_millis(1),
        poll_retry: Duration::from_millis(100),
        start_from_latest: false,
    };

    let ingest_handle =
        tokio::spawn(async move { near_stream::ingest::run_ingestor(config, redis_conn).await });

    // Wait for immediate finality check to happen (should be quick with the fix)
    let mut checks_snapshot = 0;
    let detection_start = std::time::Instant::now();

    for _ in 0..30 {
        // Check every 100ms for up to 3 seconds
        tokio::time::sleep(Duration::from_millis(100)).await;
        checks_snapshot = finality_check_count.load(Ordering::SeqCst);
        if checks_snapshot >= 2 {
            break; // Detected!
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

    // With the explicit fast test budget, detection should happen within 2 seconds
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
        .respond_with(ResponseTemplate::new(302).insert_header("Location", final_url.as_str()))
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

    let redis_client = redis::Client::open(
        std::env::var("TEST_REDIS_URL").expect("Set TEST_REDIS_URL to a disposable Redis database"),
    )
    .unwrap();
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
        request_interval: Duration::from_millis(1),
        poll_retry: Duration::from_millis(100),
        start_from_latest: false,
    };

    let ingest_handle =
        tokio::spawn(async move { near_stream::ingest::run_ingestor(config, redis_conn).await });

    tokio::time::sleep(Duration::from_secs(5)).await;

    // Should have fetched block 403 (finding consecutive skipped blocks via lookahead)
    assert!(
        block_403_calls.load(Ordering::SeqCst) > 0,
        "Should have found block 403 after skipping 401 and 402"
    );

    println!("Successfully detected consecutive skipped blocks 401 and 402");
    println!(
        "Block 403 fetch attempts: {}",
        block_403_calls.load(Ordering::SeqCst)
    );

    ingest_handle.abort();
}
