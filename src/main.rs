//! NEAR Stream - Server-Sent Events stream for NEAR blockchain blocks
//!
//! A lightweight service that ingests NEAR blocks from neardata.xyz and streams
//! them to clients via SSE with automatic catch-up support.

mod ingest;
mod redis_stream;
mod stream;

use axum::{routing::get, Router};
use std::env;
use tower_http::trace::TraceLayer;
use tracing::info;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use ingest::{run_ingestor, IngestConfig};
use stream::{health_handler, stream_handler, StreamState};

/// Runtime mode for the application
#[derive(Clone, Debug, PartialEq)]
enum Mode {
    /// Run only the ingester
    Ingester,
    /// Run only the SSE server
    Server,
}

impl Mode {
    fn from_env() -> Self {
        match env::var("MODE").as_deref() {
            Ok("ingester") => Self::Ingester,
            Ok("server") => Self::Server,
            _ => {
                eprintln!("ERROR: MODE environment variable must be set to 'ingester' or 'server'");
                std::process::exit(1);
            }
        }
    }
}

/// Application configuration loaded from environment variables
#[derive(Clone)]
struct Config {
    mode: Mode,
    neardata_base: String,
    redis_url: String,
    poll_retry_ms: u64,
    bind_addr: String,
    bind_port: u16,
}

impl Config {
    fn from_env() -> Self {
        Self {
            mode: Mode::from_env(),
            neardata_base: env::var("NEARDATA_BASE")
                .unwrap_or_else(|_| "https://mainnet.neardata.xyz".to_string()),
            redis_url: env::var("REDIS_URL")
                .unwrap_or_else(|_| "redis://localhost:6379".to_string()),
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
        mode = ?config.mode,
        neardata_base = %config.neardata_base,
        redis_url = %config.redis_url,
        poll_retry_ms = config.poll_retry_ms,
        "Starting NEAR Stream"
    );

    // Setup Redis connection
    let redis_conn = redis_stream::setup_redis(&config.redis_url).await?;

    match config.mode {
        Mode::Ingester => {
            // Run only ingester
            info!("Running in INGESTER mode");

            let ingest_cfg = IngestConfig {
                neardata_base: config.neardata_base,
            };

            run_ingestor(ingest_cfg, redis_conn).await?;
        }
        Mode::Server => {
            // Run only server
            info!("Running in SERVER mode");
            run_server(config, redis_conn).await?;
        }
    }

    Ok(())
}

async fn run_server(
    config: Config,
    redis_conn: redis::aio::ConnectionManager,
) -> anyhow::Result<()> {
    // Create stream state
    let stream_state = StreamState::new(redis_conn);

    // Build router
    let app = Router::new()
        .route("/", get(stream_handler))
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
