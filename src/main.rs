//! NEAR Stream - Server-Sent Events stream for NEAR blockchain blocks
//!
//! A lightweight service that ingests NEAR blocks from neardata.xyz and streams
//! them to clients via SSE with automatic catch-up support.

mod ingest;
mod stream;
mod types;

use axum::{routing::get, Router};
use std::env;
use tower_http::trace::TraceLayer;
use tracing::{error, info};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use ingest::{run_ingestor, IngestConfig};
use stream::{health_handler, stream_handler, StreamState};

/// Application configuration loaded from environment variables
#[derive(Clone)]
struct Config {
    neardata_base: String,
    ring_size: usize,
    poll_retry_ms: u64,
    bind_addr: String,
    bind_port: u16,
}

impl Config {
    fn from_env() -> Self {
        Self {
            neardata_base: env::var("NEARDATA_BASE")
                .unwrap_or_else(|_| "https://mainnet.neardata.xyz".to_string()),
            ring_size: env::var("RING_SIZE")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(256),
            poll_retry_ms: env::var("POLL_RETRY_MS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(1000),
            bind_addr: env::var("BIND_ADDR").unwrap_or_else(|_| "0.0.0.0".to_string()),
            bind_port: env::var("BIND_PORT")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(8080),
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize tracing
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "near_stream=info,tower_http=info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let config = Config::from_env();

    info!(
        neardata_base = %config.neardata_base,
        ring_size = config.ring_size,
        poll_retry_ms = config.poll_retry_ms,
        "Starting NEAR Stream server"
    );

    // Create shared stream state
    let stream_state = StreamState::new(config.ring_size);

    // Spawn ingestor task
    {
        let ingest_cfg = IngestConfig {
            neardata_base: config.neardata_base.clone(),
            poll_retry_ms: config.poll_retry_ms,
        };
        let tx = stream_state.broadcaster();

        tokio::spawn(async move {
            if let Err(e) = run_ingestor(ingest_cfg, tx).await {
                error!(error = ?e, "Ingestor task failed");
            }
        });
    }

    // Spawn ring buffer updater task (subscribes to broadcasts and updates ring)
    {
        let state = stream_state.clone();
        let mut rx = state.subscribe();

        tokio::spawn(async move {
            loop {
                match rx.recv().await {
                    Ok(msg) => state.push_block(msg),
                    Err(e) => {
                        error!(error = ?e, "Ring buffer updater error");
                        break;
                    }
                }
            }
        });
    }

    // Build router
    let app = Router::new()
        .route("/blocks", get(stream_handler))
        .route("/healthz", get(health_handler))
        .layer(TraceLayer::new_for_http())
        .with_state(stream_state);

    // Start HTTP server
    let bind = format!("{}:{}", config.bind_addr, config.bind_port);
    let listener = tokio::net::TcpListener::bind(&bind).await?;

    info!(addr = %bind, "Starting HTTP server");

    axum::serve(listener, app).await?;

    Ok(())
}
