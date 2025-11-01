//! SSE streaming and ring buffer management
//!
//! This module handles:
//! - Maintaining a ring buffer of recent blocks for catch-up
//! - SSE stream creation with catch-up + live functionality
//! - HTTP handler for /stream endpoint

use axum::{
    extract::{Query, State},
    http::{header, HeaderMap},
    response::{sse::{Event, KeepAlive, Sse}, Html, IntoResponse, Response},
    Json,
};
use futures::stream::{self, Stream, StreamExt};
use serde::Deserialize;
use std::{
    collections::VecDeque,
    convert::Infallible,
    sync::{Arc, RwLock},
    time::Duration,
};
use tokio::sync::broadcast;
use tracing::{info, warn};

use crate::types::BlockMsg;

/// Shared state for SSE streaming
#[derive(Clone)]
pub struct StreamState {
    ring: Arc<RwLock<VecDeque<BlockMsg>>>,
    tx: broadcast::Sender<BlockMsg>,
    ring_size: usize,
}

impl StreamState {
    pub fn new(ring_size: usize) -> Self {
        let (tx, _rx) = broadcast::channel(1024);
        Self {
            ring: Arc::new(RwLock::new(VecDeque::with_capacity(ring_size))),
            tx,
            ring_size,
        }
    }

    /// Get broadcast sender for ingestor to publish to
    pub fn broadcaster(&self) -> broadcast::Sender<BlockMsg> {
        self.tx.clone()
    }

    /// Add a block to the ring buffer (called by a background task)
    pub fn push_block(&self, msg: BlockMsg) {
        let mut ring = self.ring.write().unwrap();
        if ring.len() == self.ring_size {
            ring.pop_front();
        }
        ring.push_back(msg);
    }

    /// Subscribe to new blocks and receive them via broadcast channel
    pub fn subscribe(&self) -> broadcast::Receiver<BlockMsg> {
        self.tx.subscribe()
    }

    /// Get catchup blocks from ring buffer, filtering by resume_from height
    fn get_catchup(&self, resume_from: Option<u64>) -> Vec<BlockMsg> {
        let ring = self.ring.read().unwrap();
        ring.iter()
            .filter(|msg| resume_from.is_none_or(|height| msg.height > height))
            .cloned()
            .collect()
    }
}

/// Creates an SSE stream with catch-up from ring buffer + live updates
fn create_sse_stream(
    state: StreamState,
    resume_from: Option<u64>,
) -> impl Stream<Item = Result<Event, Infallible>> {
    // Step 1: Get catch-up blocks from ring buffer
    let catchup_msgs = state.get_catchup(resume_from);
    let catchup_stream = stream::iter(catchup_msgs.into_iter().map(|msg| {
        let json = serde_json::to_string(&msg.block).unwrap_or_else(|_| "null".to_string());
        Ok::<_, Infallible>(
            Event::default()
                .event("block")
                .id(msg.height.to_string())
                .data(json),
        )
    }));

    // Step 2: Subscribe to live broadcast
    let rx = state.subscribe();
    let live_stream = stream::unfold(rx, |mut rx| async move {
        match rx.recv().await {
            Ok(msg) => {
                let json =
                    serde_json::to_string(&msg.block).unwrap_or_else(|_| "null".to_string());
                let event = Event::default()
                    .event("block")
                    .id(msg.height.to_string())
                    .data(json);
                Some((Ok::<_, Infallible>(event), rx))
            }
            Err(broadcast::error::RecvError::Lagged(skipped)) => {
                warn!(skipped, "Client lagged behind, sending resync event");
                let event = Event::default().event("ping").data("resync");
                Some((Ok::<_, Infallible>(event), rx))
            }
            Err(_) => None, // Channel closed
        }
    });

    // Chain catch-up and live streams
    catchup_stream.chain(live_stream)
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
            if ua_str.contains("Mozilla") || ua_str.contains("Chrome") || ua_str.contains("Safari") || ua_str.contains("Edge") {
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

    let event_stream = create_sse_stream(state, resume_from);

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
