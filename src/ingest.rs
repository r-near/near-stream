//! Block ingestion from NEAR blockchain via neardata.xyz
//!
//! This module handles:
//! - Discovering the latest finalized block height
//! - Polling for new blocks sequentially
//! - Publishing blocks to Redis Streams

use anyhow::{Context, Result};
use redis::aio::ConnectionManager;
use reqwest::{header, Client, Response, StatusCode, Url};
use serde_json::Value;
use std::cmp::Ordering;
use std::time::{Duration, SystemTime};
use tokio::time::{sleep, sleep_until, Instant};
use tracing::{info, warn};

#[derive(Clone)]
pub struct IngestConfig {
    pub neardata_base: String,
    /// Minimum spacing for every HTTP request, including redirects and retries.
    pub request_interval: Duration,
    /// Delay before polling again after unavailable data or an error.
    pub poll_retry: Duration,
    /// Discover the finalized head at every startup instead of resuming Redis.
    pub start_from_latest: bool,
}

/// Result of attempting to fetch a block
enum BlockFetchResult {
    /// Block was successfully fetched
    Found(Value),
    /// Block returned null (not available yet or skipped)
    NotAvailable,
    /// Rate limited by API (429)
    RateLimited,
}

/// Creates a configured HTTP client with fast timeouts and no automatic retries
fn create_http_client() -> Client {
    Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(10))
        .connect_timeout(Duration::from_secs(5))
        .pool_idle_timeout(Duration::from_secs(90))
        .build()
        .expect("Failed to build HTTP client")
}

/// One request budget shared by discovery, blocks, lookahead, redirects and retries.
struct NearDataClient {
    http: Client,
    interval: Duration,
    configured_interval: Duration,
    next_request: Instant,
    rate_limit_streak: u32,
    successful_requests: u32,
    success_window_started: Option<Instant>,
}

impl NearDataClient {
    fn new(interval: Duration) -> Self {
        Self {
            http: create_http_client(),
            interval,
            configured_interval: interval,
            next_request: Instant::now(),
            rate_limit_streak: 0,
            successful_requests: 0,
            success_window_started: None,
        }
    }

    fn reset_success_window(&mut self) {
        self.successful_requests = 0;
        self.success_window_started = None;
    }

    fn record_success(&mut self) {
        if self.interval == self.configured_interval {
            return;
        }
        let now = Instant::now();
        let started = *self.success_window_started.get_or_insert(now);
        self.successful_requests = self.successful_requests.saturating_add(1);
        // Recover one step at a time, after sustained success at the slower rate.
        if self.successful_requests >= 60 && now.duration_since(started) >= Duration::from_secs(60)
        {
            self.interval = (self.interval / 2).max(self.configured_interval);
            self.reset_success_window();
            info!(
                request_interval_ms = self.interval.as_millis(),
                "NearData sustained successful requests; recovering request rate"
            );
        }
    }

    async fn get_once(&mut self, url: Url) -> Result<Response> {
        sleep_until(self.next_request).await;
        self.next_request = Instant::now() + self.interval;
        let resp = match self.http.get(url).send().await {
            Ok(resp) => resp,
            Err(err) => {
                self.reset_success_window();
                return Err(err.into());
            }
        };
        if resp.status() == StatusCode::TOO_MANY_REQUESTS {
            self.reset_success_window();
            // NearData can return Retry-After: 0. Never let that cause a retry storm.
            let fallback = Duration::from_secs(60 * (1_u64 << self.rate_limit_streak.min(3)));
            let delay = retry_after(&resp).unwrap_or_default().max(fallback);
            self.rate_limit_streak = self.rate_limit_streak.saturating_add(1);
            self.interval = self
                .interval
                .saturating_mul(2)
                .min(Duration::from_secs(60).max(self.interval));
            self.next_request = self.next_request.max(Instant::now() + delay);
            warn!(
                cooldown_seconds = delay.as_secs(),
                request_interval_ms = self.interval.as_millis(),
                "NearData rate limited; pausing all requests"
            );
        } else if resp.status().is_success() || resp.status().is_redirection() {
            self.rate_limit_streak = 0;
        } else {
            self.reset_success_window();
        }
        Ok(resp)
    }

    async fn get(&mut self, mut url: Url) -> Result<Response> {
        for _ in 0..=5 {
            let resp = self.get_once(url.clone()).await?;
            if !matches!(resp.status().as_u16(), 301 | 302 | 303 | 307 | 308) {
                return Ok(resp);
            }
            let redirect = (|| -> Result<Url> {
                let location = resp
                    .headers()
                    .get(header::LOCATION)
                    .context("NearData redirect missing Location")?
                    .to_str()?;
                Ok(url.join(location)?)
            })();
            match redirect {
                Ok(next) => {
                    url = next;
                    self.record_success();
                }
                Err(err) => {
                    self.reset_success_window();
                    return Err(err);
                }
            }
        }
        self.reset_success_window();
        anyhow::bail!("Too many NearData redirects")
    }
}

fn retry_after(resp: &Response) -> Option<Duration> {
    let value = resp.headers().get(header::RETRY_AFTER)?.to_str().ok()?;
    value
        .parse::<u64>()
        .ok()
        .map(Duration::from_secs)
        .or_else(|| {
            httpdate::parse_http_date(value)
                .ok()?
                .duration_since(SystemTime::now())
                .ok()
        })
}

/// Discover the head from Location without downloading the redirected block twice.
async fn discover_latest_height(client: &mut NearDataClient, cfg: &IngestConfig) -> Result<u64> {
    let result = async {
        let url = Url::parse(&format!("{}/v0/last_block/final", cfg.neardata_base))?;
        let resp = client.get_once(url.clone()).await?.error_for_status()?;
        let final_url = if resp.status().is_redirection() {
            url.join(
                resp.headers()
                    .get(header::LOCATION)
                    .context("NearData discovery missing Location")?
                    .to_str()?,
            )?
        } else {
            resp.url().clone()
        };
        let height = final_url
            .path_segments()
            .and_then(|mut segments| segments.next_back())
            .and_then(|s| s.parse().ok())
            .context("NearData discovery URL has no block height")?;
        info!(height, "Discovered latest finalized block");
        Ok(height)
    }
    .await;
    if result.is_ok() {
        client.record_success();
    } else {
        client.reset_success_window();
    }
    result
}

/// Fetches a specific block by height
async fn fetch_block(
    client: &mut NearDataClient,
    cfg: &IngestConfig,
    height: u64,
) -> Result<BlockFetchResult> {
    let url = format!("{}/v0/block/{}", cfg.neardata_base, height);
    let resp = client.get(Url::parse(&url)?).await?;
    let status = resp.status();

    if status == StatusCode::TOO_MANY_REQUESTS {
        return Ok(BlockFetchResult::RateLimited);
    }
    // Missing heights can be skipped blocks. Confirm through lookahead/finality;
    // a block 404 must not terminate an otherwise resumable stream.
    if status == StatusCode::NOT_FOUND {
        return Ok(BlockFetchResult::NotAvailable);
    }
    let resp = resp.error_for_status()?;

    let json: Value = match resp.json().await {
        Ok(json) => json,
        Err(err) => {
            client.reset_success_window();
            return Err(err.into());
        }
    };
    // neardata.xyz returns null for blocks that don't exist yet or if skipped
    if json.is_null() {
        client.record_success();
        Ok(BlockFetchResult::NotAvailable)
    } else {
        let actual_height = json.pointer("/block/header/height").and_then(Value::as_u64);
        if actual_height != Some(height) {
            client.reset_success_window();
            anyhow::bail!(
                "NearData block height mismatch: requested {height}, received {actual_height:?}"
            );
        }
        client.record_success();
        Ok(BlockFetchResult::Found(json))
    }
}

/// Main ingestion loop - optimistically polls for new blocks and publishes to Redis
pub async fn run_ingestor(cfg: IngestConfig, mut redis_conn: ConnectionManager) -> Result<()> {
    info!("Starting block ingestor");

    anyhow::ensure!(
        !cfg.request_interval.is_zero(),
        "request_interval must be positive"
    );
    anyhow::ensure!(!cfg.poll_retry.is_zero(), "poll_retry must be positive");
    let mut client = NearDataClient::new(cfg.request_interval);
    let stored_height = crate::redis_stream::last_published_height(&mut redis_conn).await?;
    let resume_height = stored_height
        .map(|height| height.checked_add(1).context("Redis block height overflow"))
        .transpose()?;
    let mut next_height = match resume_height {
        Some(height) if !cfg.start_from_latest => {
            info!(
                height = height - 1,
                "Resuming after the last block stored in Redis"
            );
            height
        }
        resume_height => {
            if cfg.start_from_latest {
                info!("Starting from the provider's finalized head");
            }
            let latest_height = loop {
                match discover_latest_height(&mut client, &cfg).await {
                    Ok(height) => break height,
                    Err(err) => {
                        if is_permanent_http_error(&err) {
                            return Err(err);
                        }
                        warn!(error = ?err, "Failed to discover NearData head, retrying");
                        sleep(cfg.poll_retry).await;
                    }
                }
            };
            latest_height.max(resume_height.unwrap_or(latest_height))
        }
    };

    info!(next_height, "Starting optimistic ingestion from block");

    loop {
        // Optimistically fetch next block
        match fetch_block(&mut client, &cfg, next_height).await {
            Ok(BlockFetchResult::Found(block)) => {
                // Publish to Redis Streams
                if let Err(e) =
                    crate::redis_stream::publish_block(&mut redis_conn, next_height, &block).await
                {
                    warn!(height = next_height, error = ?e, "Failed to publish block to Redis, retrying");
                    sleep(Duration::from_millis(100)).await;
                    continue;
                }

                info!(height = next_height, "Ingested and published block");
                next_height += 1;

                // The shared client paces the next request.
            }
            Ok(BlockFetchResult::NotAvailable) => {
                // Block returned null - could be skipped or not available yet
                // Use lookahead to verify, then check finality if needed
                sleep(Duration::from_millis(200)).await;

                let mut found_confirmation = false;

                // Look ahead up to 2 blocks to detect skipped blocks
                for lookahead in 1..=2 {
                    match fetch_block(&mut client, &cfg, next_height + lookahead).await {
                        Ok(BlockFetchResult::Found(block)) => {
                            // Found a block - check its prev_height
                            if let Some(prev_height) = block
                                .pointer("/block/header/prev_height")
                                .and_then(|v| v.as_u64())
                            {
                                match prev_height.cmp(&next_height) {
                                    Ordering::Less => {
                                        // Confirmed: current block was skipped
                                        warn!(
                                            height = next_height,
                                            prev_height,
                                            lookahead_height = next_height + lookahead,
                                            "Block skipped by validator (detected via lookahead)"
                                        );
                                        next_height += 1;
                                        found_confirmation = true;
                                        break;
                                    }
                                    Ordering::Equal => {
                                        // Next block points to current block, so current block exists
                                        // but just isn't available yet
                                        info!(
                                            height = next_height,
                                            "Block not available yet, waiting"
                                        );
                                        sleep(cfg.poll_retry).await;
                                        found_confirmation = true;
                                        break;
                                    }
                                    Ordering::Greater => {
                                        // prev_height > next_height means there might be multiple skips
                                        // Continue looking ahead
                                    }
                                }
                            }
                        }
                        Ok(BlockFetchResult::NotAvailable) => {
                            // This lookahead block is also null, try next lookahead
                            sleep(Duration::from_millis(100)).await;
                            continue;
                        }
                        Ok(BlockFetchResult::RateLimited) => {
                            // Hit rate limit during lookahead - stop and back off
                            warn!(
                                height = next_height,
                                "Rate limited during lookahead, backing off"
                            );
                            found_confirmation = true;
                            sleep(cfg.poll_retry).await;
                            break;
                        }
                        Err(err) => {
                            if is_permanent_http_error(&err) {
                                return Err(err);
                            }
                            // Error fetching lookahead block, stop trying
                            break;
                        }
                    }
                }

                if !found_confirmation {
                    // Couldn't find any block in lookahead range
                    // Check finalized height to determine if block was skipped
                    match discover_latest_height(&mut client, &cfg).await {
                        Ok(latest_finalized) => {
                            if next_height + 10 < latest_finalized {
                                // Block was definitely skipped - chain moved ahead
                                warn!(
                                    height = next_height,
                                    latest_finalized, "Block skipped (detected via finality check)"
                                );
                                next_height += 1;
                                continue;
                            } else {
                                // Truly at chain head
                                info!(
                                    height = next_height,
                                    latest_finalized, "At chain head, waiting"
                                );
                                sleep(cfg.poll_retry).await;
                            }
                        }
                        Err(e) => {
                            if is_permanent_http_error(&e) {
                                return Err(e);
                            }
                            warn!(error = ?e, "Failed to check finality, waiting");
                            sleep(cfg.poll_retry).await;
                        }
                    }
                }
            }
            Ok(BlockFetchResult::RateLimited) => {
                // Rate limited on the current block - back off
                warn!(
                    height = next_height,
                    "Rate limited (429), waiting for shared cooldown"
                );
                sleep(cfg.poll_retry).await;
            }
            Err(err) => {
                if is_permanent_http_error(&err) {
                    return Err(err);
                }
                // Check if error indicates we're too far ahead
                let err_str = err.to_string();
                if err_str.contains("BLOCK_DOES_NOT_EXIST")
                    || err_str.contains("too far in the future")
                {
                    info!(height = next_height, "Ahead of finality, waiting");
                    sleep(cfg.poll_retry).await;
                } else {
                    warn!(
                        height = next_height,
                        error = ?err,
                        "Failed to fetch block, retrying"
                    );
                    sleep(cfg.poll_retry).await;
                }
            }
        }
    }
}

fn is_permanent_http_error(err: &anyhow::Error) -> bool {
    err.downcast_ref::<reqwest::Error>()
        .and_then(reqwest::Error::status)
        .is_some_and(|status| status.is_client_error() && !matches!(status.as_u16(), 408 | 429))
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn config(base: String) -> IngestConfig {
        IngestConfig {
            neardata_base: base,
            request_interval: Duration::from_millis(100),
            poll_retry: Duration::from_millis(10),
            start_from_latest: false,
        }
    }

    #[tokio::test(start_paused = true)]
    async fn rate_recovery_requires_sustained_success_and_respects_ceiling() {
        let configured = Duration::from_millis(500);
        let mut client = NearDataClient::new(configured);
        client.interval = configured * 4;
        for _ in 0..60 {
            client.record_success();
            tokio::time::advance(Duration::from_millis(500)).await;
        }
        // Enough successes alone cannot recover before the minimum stable window.
        assert_eq!(client.interval, Duration::from_secs(2));
        tokio::time::advance(Duration::from_secs(30)).await;
        client.record_success();
        assert_eq!(client.interval, Duration::from_secs(1));
        for _ in 0..60 {
            client.record_success();
            tokio::time::advance(Duration::from_secs(1)).await;
        }
        client.record_success();
        assert_eq!(client.interval, configured);
        tokio::time::advance(Duration::from_secs(60)).await;
        client.record_success();
        assert_eq!(client.interval, configured);
    }

    #[tokio::test(start_paused = true)]
    async fn failures_restart_the_rate_recovery_window() {
        let mut client = NearDataClient::new(Duration::from_secs(1));
        client.interval = Duration::from_secs(4);
        client.record_success();
        tokio::time::advance(Duration::from_secs(60)).await;
        for _ in 0..58 {
            client.record_success();
        }
        assert_eq!(client.interval, Duration::from_secs(4));
        client.reset_success_window();
        for _ in 0..60 {
            client.record_success();
        }
        assert_eq!(client.interval, Duration::from_secs(4));
        tokio::time::advance(Duration::from_secs(60)).await;
        client.record_success();
        assert_eq!(client.interval, Duration::from_secs(2));
    }

    #[tokio::test]
    async fn discovery_uses_location_without_following_redirect() {
        let server = MockServer::start().await;
        Mock::given(path("/v0/last_block/final"))
            .respond_with(ResponseTemplate::new(302).insert_header("Location", "/v0/block/123"))
            .expect(1)
            .mount(&server)
            .await;
        let cfg = config(server.uri());
        let mut client = NearDataClient::new(cfg.request_interval);
        assert_eq!(
            discover_latest_height(&mut client, &cfg).await.unwrap(),
            123
        );
        assert_eq!(server.received_requests().await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn discovery_blocks_lookahead_and_redirects_share_spacing() {
        let server = MockServer::start().await;
        Mock::given(path("/v0/last_block/final"))
            .respond_with(ResponseTemplate::new(302).insert_header("Location", "/v0/block/123"))
            .mount(&server)
            .await;
        let starts = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let recorded = starts.clone();
        Mock::given(method("GET"))
            .respond_with(move |req: &wiremock::Request| {
                recorded.lock().unwrap().push(std::time::Instant::now());
                if req.url.path() == "/v0/block/124" {
                    ResponseTemplate::new(302).insert_header("Location", "/archive/124")
                } else {
                    let height = if req.url.path() == "/archive/124" {
                        124
                    } else {
                        123
                    };
                    ResponseTemplate::new(200)
                        .set_body_json(json!({"block": {"header": {"height":height}}}))
                }
            })
            .mount(&server)
            .await;
        let cfg = config(server.uri());
        let mut client = NearDataClient::new(cfg.request_interval);
        discover_latest_height(&mut client, &cfg).await.unwrap();
        let first_fetch = std::time::Instant::now();
        fetch_block(&mut client, &cfg, 123).await.unwrap();
        assert!(first_fetch.elapsed() >= Duration::from_millis(90));
        fetch_block(&mut client, &cfg, 124).await.unwrap();
        let starts = starts.lock().unwrap();
        assert_eq!(starts.len(), 3);
        assert!(starts
            .windows(2)
            .all(|w| w[1].duration_since(w[0]) >= Duration::from_millis(90)));
    }

    #[tokio::test]
    async fn zero_retry_after_cools_down_all_paths_and_recovers() {
        let server = MockServer::start().await;
        let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counter = calls.clone();
        Mock::given(path("/v0/block/123"))
            .respond_with(move |_: &wiremock::Request| {
                if counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 {
                    ResponseTemplate::new(429).insert_header("Retry-After", "0")
                } else {
                    ResponseTemplate::new(200)
                        .set_body_json(json!({"block": {"header": {"height":123}}}))
                }
            })
            .mount(&server)
            .await;
        let cfg = config(server.uri());
        let mut client = NearDataClient::new(cfg.request_interval);
        assert!(matches!(
            fetch_block(&mut client, &cfg, 123).await.unwrap(),
            BlockFetchResult::RateLimited
        ));
        assert!(client.next_request.duration_since(Instant::now()) >= Duration::from_secs(59));
        assert!(tokio::time::timeout(
            Duration::from_millis(50),
            discover_latest_height(&mut client, &cfg)
        )
        .await
        .is_err());
        assert_eq!(server.received_requests().await.unwrap().len(), 1);
        // Move the deadline forward explicitly instead of spending a minute in the test.
        client.next_request = Instant::now();
        assert!(matches!(
            fetch_block(&mut client, &cfg, 123).await.unwrap(),
            BlockFetchResult::Found(_)
        ));
        assert_eq!(client.rate_limit_streak, 0);
        assert_eq!(client.interval, Duration::from_millis(200));
    }

    #[tokio::test]
    async fn retry_after_seconds_and_http_dates_extend_cooldown() {
        let server = MockServer::start().await;
        for (height, value) in [
            (123, "120".to_string()),
            (
                124,
                httpdate::fmt_http_date(SystemTime::now() + Duration::from_secs(180)),
            ),
        ] {
            Mock::given(path(format!("/v0/block/{height}")))
                .respond_with(
                    ResponseTemplate::new(429).insert_header("Retry-After", value.as_str()),
                )
                .mount(&server)
                .await;
        }
        let cfg = config(server.uri());
        let mut client = NearDataClient::new(cfg.request_interval);
        fetch_block(&mut client, &cfg, 123).await.unwrap();
        assert!(client.next_request.duration_since(Instant::now()) >= Duration::from_secs(119));
        client.next_request = Instant::now();
        fetch_block(&mut client, &cfg, 124).await.unwrap();
        assert!(client.next_request.duration_since(Instant::now()) >= Duration::from_secs(178));
    }

    #[tokio::test]
    async fn server_errors_are_not_unavailable_blocks() {
        let server = MockServer::start().await;
        Mock::given(path("/v0/block/123"))
            .respond_with(ResponseTemplate::new(503))
            .mount(&server)
            .await;
        Mock::given(path("/v0/block/124"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::Value::Null))
            .mount(&server)
            .await;
        let cfg = config(server.uri());
        let mut client = NearDataClient::new(cfg.request_interval);
        let err = fetch_block(&mut client, &cfg, 123).await.err().unwrap();
        assert!(!is_permanent_http_error(&err));
        assert!(matches!(
            fetch_block(&mut client, &cfg, 124).await.unwrap(),
            BlockFetchResult::NotAvailable
        ));
    }

    #[tokio::test]
    async fn rejected_authentication_fails_instead_of_retrying() {
        let server = MockServer::start().await;
        Mock::given(path("/v0/last_block/final"))
            .respond_with(ResponseTemplate::new(401))
            .mount(&server)
            .await;
        let cfg = config(server.uri());
        let mut client = NearDataClient::new(cfg.request_interval);
        assert!(is_permanent_http_error(
            &discover_latest_height(&mut client, &cfg).await.unwrap_err()
        ));
    }

    #[tokio::test]
    async fn malformed_or_wrong_height_blocks_reset_recovery_without_counting_success() {
        let server = MockServer::start().await;
        for (height, body) in [
            (123, json!({})),
            (124, json!({"block":{"header":{"height":999}}})),
        ] {
            Mock::given(path(format!("/v0/block/{height}")))
                .respond_with(ResponseTemplate::new(200).set_body_json(body))
                .mount(&server)
                .await;
        }
        let cfg = config(server.uri());
        let mut client = NearDataClient::new(cfg.request_interval);
        client.interval *= 2;
        for height in [123, 124] {
            client.successful_requests = 59;
            client.success_window_started = Some(Instant::now() - Duration::from_secs(60));
            assert!(fetch_block(&mut client, &cfg, height).await.is_err());
            assert_eq!(client.interval, cfg.request_interval * 2);
            assert_eq!(client.successful_requests, 0);
            assert!(client.success_window_started.is_none());
        }
    }

    /// Test that we correctly detect skipped blocks using prev_height verification
    /// Based on real NEAR data: block 170797835 was skipped
    #[test]
    fn test_detect_skipped_block() {
        // Block 170797835 returns null (skipped by validator)
        // Block 170797836 exists and points back to 170797834
        let block_170797836 = json!({
            "block": { "header": {
                "height": 170797836,
                "prev_height": 170797834  // Skips over 170797835!
            }}
        });

        // Verify the detection logic
        let current_height = 170797835;
        let next_block = &block_170797836;

        let prev_height = next_block
            .pointer("/block/header/prev_height")
            .and_then(|v| v.as_u64());

        assert_eq!(prev_height, Some(170797834));
        assert!(
            prev_height.unwrap() < current_height,
            "prev_height should be less than current_height, confirming block was skipped"
        );
    }

    /// Test that we don't incorrectly mark sequential blocks as skipped
    #[test]
    fn test_sequential_blocks_not_skipped() {
        // Sequential blocks with no gaps
        let block_101 = json!({
            "block": { "header": {
                "height": 101,
                "prev_height": 100  // Sequential, not skipped
            }}
        });

        let current_height = 100;
        let next_block = &block_101;

        let prev_height = next_block
            .pointer("/block/header/prev_height")
            .and_then(|v| v.as_u64());

        assert_eq!(prev_height, Some(100));
        assert!(
            prev_height.unwrap() >= current_height,
            "prev_height should equal current_height for sequential blocks"
        );
    }

    /// Test consecutive skipped blocks (like 170866966 and 170866967)
    /// Both blocks return null, but block 170866968 exists
    #[test]
    fn test_consecutive_skipped_blocks() {
        // Blocks 170866966 and 170866967 both return null (skipped)
        // Block 170866968 exists and points back to 170866965
        let block_170866968 = json!({
            "block": { "header": {
                "height": 170866968,
                "prev_height": 170866965  // Skips over 170866966 and 170866967!
            }}
        });

        // Test detecting first skipped block (170866966)
        let current_height = 170866966;
        let lookahead_block = &block_170866968;

        let prev_height = lookahead_block
            .pointer("/block/header/prev_height")
            .and_then(|v| v.as_u64());

        assert_eq!(prev_height, Some(170866965));
        assert!(
            prev_height.unwrap() < current_height,
            "prev_height should be less than current_height when blocks are skipped"
        );

        // Test detecting second skipped block (170866967)
        let current_height = 170866967;
        assert!(
            prev_height.unwrap() < current_height,
            "prev_height should also be less than second skipped block height"
        );
    }
}
