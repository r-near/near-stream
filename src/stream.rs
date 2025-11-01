//! SSE streaming and ring buffer management
//!
//! This module handles:
//! - Maintaining a ring buffer of recent blocks for catch-up
//! - SSE stream creation with catch-up + live functionality
//! - HTTP handler for /stream endpoint

use axum::{
    extract::{Query, State},
    response::sse::{Event, KeepAlive, Sse},
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

/// SSE endpoint handler
pub async fn stream_handler(
    State(state): State<StreamState>,
    Query(query): Query<StreamQuery>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let resume_from = query.from_height;

    info!(?resume_from, "New SSE client connected");

    let event_stream = create_sse_stream(state, resume_from);

    Sse::new(event_stream).keep_alive(
        KeepAlive::new()
            .interval(Duration::from_secs(30))
            .text("keep-alive"),
    )
}

/// Health check endpoint
pub async fn health_handler() -> Json<&'static str> {
    Json("ok")
}
