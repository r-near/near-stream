//! SSE streaming from Redis Streams
//!
//! This module handles:
//! - SSE stream creation with catch-up + live functionality from Redis
//! - HTTP handler for /stream endpoint

use axum::{
    extract::{Query, State},
    http::{header, HeaderMap},
    response::{
        sse::{Event, KeepAlive, Sse},
        Html, IntoResponse, Response,
    },
    Json,
};
use futures::stream::{Stream, StreamExt};
use redis::aio::ConnectionManager;
use serde::Deserialize;
use std::{convert::Infallible, sync::Arc, time::Duration};
use tokio::sync::Mutex;
use tracing::{error, info};

/// Shared state for SSE streaming
#[derive(Clone)]
pub struct StreamState {
    redis_conn: Arc<Mutex<ConnectionManager>>,
}

impl StreamState {
    pub fn new(redis_conn: ConnectionManager) -> Self {
        Self {
            redis_conn: Arc::new(Mutex::new(redis_conn)),
        }
    }

    /// Get Redis connection for subscribing
    pub fn redis_conn(&self) -> Arc<Mutex<ConnectionManager>> {
        self.redis_conn.clone()
    }
}

/// Creates an SSE stream from Redis Streams
async fn create_sse_stream(
    redis_conn: Arc<Mutex<ConnectionManager>>,
    resume_from: Option<u64>,
) -> impl Stream<Item = Result<Event, Infallible>> {
    // Get catchup blocks first
    let catchup_blocks = {
        let mut conn = redis_conn.lock().await;
        match crate::redis_stream::get_catchup_blocks(&mut conn, resume_from).await {
            Ok(blocks) => blocks,
            Err(e) => {
                error!(error = ?e, "Failed to get catchup blocks from Redis");
                Vec::new()
            }
        }
    };

    // Create catchup stream
    let catchup_stream =
        futures::stream::iter(catchup_blocks.into_iter().map(|(height, block, _)| {
            let block_json = serde_json::to_string(&block).unwrap_or_else(|_| "null".to_string());
            Ok::<_, Infallible>(
                Event::default()
                    .event("block")
                    .id(height.to_string())
                    .data(block_json),
            )
        }));

    // Create live stream
    let live_stream = futures::stream::unfold(
        (redis_conn, None),
        |(redis_conn, mut last_id): (Arc<Mutex<ConnectionManager>>, Option<String>)| async move {
            loop {
                let mut conn = redis_conn.lock().await;

                match crate::redis_stream::read_blocks(&mut conn, last_id.clone()).await {
                    Ok(reply) => {
                        drop(conn); // Release lock

                        for stream_key in reply.keys {
                            if let Some(entry) = stream_key.ids.into_iter().next() {
                                // Parse height and block
                                let height: u64 = entry
                                    .map
                                    .get("height")
                                    .and_then(|v| match v {
                                        redis::Value::BulkString(s) => {
                                            String::from_utf8(s.clone()).ok()
                                        }
                                        redis::Value::SimpleString(s) => Some(s.clone()),
                                        _ => None,
                                    })
                                    .and_then(|s| s.parse().ok())
                                    .unwrap_or(0);

                                let block_json = entry
                                    .map
                                    .get("block")
                                    .and_then(|v| match v {
                                        redis::Value::BulkString(s) => {
                                            String::from_utf8(s.clone()).ok()
                                        }
                                        redis::Value::SimpleString(s) => Some(s.clone()),
                                        _ => None,
                                    })
                                    .unwrap_or_default();

                                last_id = Some(entry.id);

                                let event = Ok::<_, Infallible>(
                                    Event::default()
                                        .event("block")
                                        .id(height.to_string())
                                        .data(block_json),
                                );

                                return Some((event, (redis_conn, last_id)));
                            }
                        }

                        // No messages, continue loop
                        continue;
                    }
                    Err(e) => {
                        drop(conn); // Release lock
                        error!(error = ?e, "Error reading from Redis");
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        continue;
                    }
                }
            }
        },
    );

    // Chain catchup and live streams
    catchup_stream.chain(live_stream).boxed()
}

#[derive(Deserialize)]
pub struct StreamQuery {
    from_height: Option<u64>,
}

/// Check if request is from a browser based on User-Agent and Accept headers
fn is_browser_request(headers: &HeaderMap) -> bool {
    // First check: If Accept header explicitly requests text/event-stream, serve SSE
    // (This handles EventSource requests from the browser)
    if let Some(accept) = headers.get(header::ACCEPT) {
        if let Ok(accept_str) = accept.to_str() {
            if accept_str.contains("text/event-stream") {
                return false; // Not a browser page request, it's an EventSource request
            }
        }
    }

    // Second check: If Accept header prefers text/html, serve HTML
    if let Some(accept) = headers.get(header::ACCEPT) {
        if let Ok(accept_str) = accept.to_str() {
            if accept_str.contains("text/html") {
                return true;
            }
        }
    }

    // Fallback: Check User-Agent for common browsers
    if let Some(ua) = headers.get(header::USER_AGENT) {
        if let Ok(ua_str) = ua.to_str() {
            if ua_str.contains("Mozilla")
                || ua_str.contains("Chrome")
                || ua_str.contains("Safari")
                || ua_str.contains("Edge")
            {
                // Only serve HTML if it's not curl (which also has Mozilla in some versions)
                return !ua_str.starts_with("curl");
            }
        }
    }

    false
}

/// SSE endpoint handler that serves HTML for browsers and SSE for API clients
pub async fn stream_handler(
    headers: HeaderMap,
    State(state): State<StreamState>,
    Query(query): Query<StreamQuery>,
) -> Response {
    // If request is from a browser, serve HTML homepage
    if is_browser_request(&headers) {
        info!("Serving HTML homepage to browser");
        return Html(include_str!("index.html")).into_response();
    }

    // Otherwise, serve SSE stream
    let resume_from = query.from_height;
    info!(?resume_from, "New SSE client connected");

    let redis_conn = state.redis_conn();
    let event_stream = create_sse_stream(redis_conn, resume_from).await;

    Sse::new(event_stream)
        .keep_alive(
            KeepAlive::new()
                .interval(Duration::from_secs(30))
                .text("keep-alive"),
        )
        .into_response()
}

/// Health check endpoint
pub async fn health_handler() -> Json<&'static str> {
    Json("ok")
}
