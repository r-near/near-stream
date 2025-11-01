use actix_web::http::header;
use actix_web::{
    get,
    web::{Data, Query},
    App, HttpRequest, HttpResponse, HttpServer, Responder,
};
use futures_util::Stream;
use log::{error, info, warn};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::VecDeque,
    sync::{Arc, RwLock},
    time::Duration,
};
use tokio::sync::broadcast;
use tokio::time::sleep;

// -------------------------------
// Config
// -------------------------------

#[derive(Clone)]
struct AppConfig {
    neardata_base: String, // e.g. https://mainnet.neardata.xyz
    ring_size: usize,      // e.g. 256
    poll_retry_ms: u64,    // wait between polls when no new block yet
}

// -------------------------------
// Data model we broadcast
// -------------------------------

#[derive(Clone, Debug, Serialize, Deserialize)]
struct BlockMsg {
    height: u64,
    block: Value, // full JSON from neardata for that block
}

// -------------------------------
// Shared state
// -------------------------------

#[derive(Clone)]
struct SharedState {
    // ring buffer of recent blocks
    ring: Arc<RwLock<VecDeque<BlockMsg>>>,
    // broadcast channel for live subscribers
    tx: broadcast::Sender<BlockMsg>,
    // config
    cfg: AppConfig,
}

// Helper: push into ring buffer w/ max capacity
fn push_ring(ring: &mut VecDeque<BlockMsg>, msg: BlockMsg, cap: usize) {
    if ring.len() == cap {
        ring.pop_front();
    }
    ring.push_back(msg);
}

// -------------------------------
// Block ingestion task
// -------------------------------
//
// Loop:
//  1. discover the latest finalized block height from /v0/last_block/final
//     (that endpoint redirects to /v0/block/<height>)
//  2. walk forward 1-by-1 with /v0/block/<height>
//  3. for each new block, store + broadcast
//

async fn discover_latest_final_height(cfg: &AppConfig) -> anyhow::Result<u64> {
    info!(
        "Discovering latest finalized block height from {}",
        cfg.neardata_base
    );
    // We'll GET /v0/last_block/final and rely on the redirect URL to infer height.
    // FastNEAR docs say it redirects to a concrete /v0/block/<block_height>.
    let url = format!("{}/v0/last_block/final", cfg.neardata_base);
    let resp = reqwest::get(&url).await?;
    let final_url = resp.url().clone(); // after redirects
                                        // final_url should end with /v0/block/<height>
                                        // we'll parse the last path segment
    let segments = final_url.path().split('/').collect::<Vec<_>>();
    let last_seg = segments
        .last()
        .ok_or_else(|| anyhow::anyhow!("bad redirect url"))?;
    let h: u64 = last_seg.parse()?;
    info!("Latest finalized height: {}", h);
    Ok(h)
}

async fn fetch_block(cfg: &AppConfig, height: u64) -> anyhow::Result<Option<Value>> {
    let url = format!("{}/v0/block/{}", cfg.neardata_base, height);
    let resp = reqwest::get(&url).await?;

    if resp.status().is_success() {
        let v: Value = resp.json().await?;
        // FastNEAR contract: if block doesn't exist => `null`
        if v.is_null() {
            Ok(None)
        } else {
            Ok(Some(v))
        }
    } else {
        // Non-200 could be transient
        Ok(None)
    }
}

async fn run_ingestor(state: SharedState) -> anyhow::Result<()> {
    info!("Starting block ingestor");
    // 1. Get last finalized height to seed the loop.
    let mut next_height = discover_latest_final_height(&state.cfg).await?;

    loop {
        info!("Attempting to fetch block {}", next_height);
        match fetch_block(&state.cfg, next_height).await {
            Ok(Some(block_json)) => {
                // Try to read height from URL rather than trusting JSON layout.
                // But we already know `next_height`.
                let msg = BlockMsg {
                    height: next_height,
                    block: block_json,
                };

                // Save to ring buffer
                {
                    let mut ring = state.ring.write().unwrap();
                    push_ring(&mut ring, msg.clone(), state.cfg.ring_size);
                }

                // Broadcast to all subscribers
                let _ = state.tx.send(msg);

                info!("Fetched and broadcasted block {}", next_height);
                // advance
                next_height += 1;
                // no sleep: try immediate next block
            }
            Ok(None) => {
                // Block not ready yet, chill a bit
                warn!(
                    "Block {} not ready yet, retrying in {}ms",
                    next_height, state.cfg.poll_retry_ms
                );
                sleep(Duration::from_millis(state.cfg.poll_retry_ms)).await;
            }
            Err(err) => {
                error!("Error fetching block {}: {:?}", next_height, err);
                // On error we don't want to spin at 100% CPU.
                sleep(Duration::from_millis(state.cfg.poll_retry_ms)).await;
            }
        }
    }
}

// -------------------------------
// SSE response builder
// -------------------------------

fn format_sse_event(msg: &BlockMsg) -> String {
    // We emit one SSE event per block:
    // event: block
    // id: <height>
    // data: <json...>
    // \n
    let json_str = serde_json::to_string(&msg.block).unwrap_or("null".to_string());
    format!("event: block\nid: {}\ndata: {}\n\n", msg.height, json_str)
}

// We'll stream using an async Stream of Bytes
fn sse_stream(
    state: SharedState,
    resume_from: Option<u64>,
) -> impl Stream<Item = Result<actix_web::web::Bytes, actix_web::Error>> {
    use futures_util::stream::{self, StreamExt};

    // Step 1. Snapshot ring buffer for catch-up.
    let catchup: Vec<String> = {
        let ring = state.ring.read().unwrap();
        ring.iter()
            .filter(|m| {
                if let Some(h) = resume_from {
                    m.height > h
                } else {
                    true
                }
            })
            .map(|m| format_sse_event(m))
            .collect()
    };

    // Step 2. Subscribe to broadcast for live.
    let rx = state.tx.subscribe();

    // Convert catchup Vec<String> into a stream first,
    // then chain a live stream that waits on rx.recv().
    let catchup_stream = stream::iter(
        catchup
            .into_iter()
            .map(|s| Ok::<_, actix_web::Error>(actix_web::web::Bytes::from(s))),
    );

    let live_stream = stream::unfold(rx, move |mut rx| async move {
        match rx.recv().await {
            Ok(msg) => {
                let frame = format_sse_event(&msg);
                Some((
                    Ok::<_, actix_web::Error>(actix_web::web::Bytes::from(frame)),
                    rx,
                ))
            }
            Err(broadcast::error::RecvError::Lagged(skipped)) => {
                warn!("Subscriber lagged, skipped {} msgs", skipped);
                // even if lagged, continue trying
                Some((
                    Ok::<_, actix_web::Error>(actix_web::web::Bytes::from(
                        "event: ping\ndata: \"resync\"\n\n",
                    )),
                    rx,
                ))
            }
            Err(_) => {
                // Sender closed. In practice this should never happen unless shutdown.
                None
            }
        }
    });

    catchup_stream.chain(live_stream)
}

// -------------------------------
// HTTP handlers
// -------------------------------

#[derive(Deserialize)]
struct StreamQuery {
    from_height: Option<u64>,
}

#[get("/stream")]
async fn stream_handler(
    req: HttpRequest,
    q: Query<StreamQuery>,
    state: Data<SharedState>,
) -> impl Responder {
    // 1. figure out resume cursor
    // Priority: query.from_height > Last-Event-ID header
    let mut resume_from = q.from_height;

    if resume_from.is_none() {
        if let Some(last_id_hdr) = req.headers().get("Last-Event-ID") {
            if let Ok(s) = last_id_hdr.to_str() {
                if let Ok(h) = s.parse::<u64>() {
                    resume_from = Some(h);
                }
            }
        }
    }

    info!("New SSE stream connection, resume_from={:?}", resume_from);
    // 2. build streaming body
    let event_stream = sse_stream(state.get_ref().clone(), resume_from);

    // 3. build response
    HttpResponse::Ok()
        .insert_header((header::CONTENT_TYPE, "text/event-stream"))
        .insert_header((header::CACHE_CONTROL, "no-cache"))
        .insert_header((header::CONNECTION, "keep-alive"))
        .streaming(event_stream)
}

#[get("/healthz")]
async fn health() -> impl Responder {
    info!("Health check requested");
    HttpResponse::Ok().body("ok")
}

// -------------------------------
// main
// -------------------------------

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    env_logger::init();
    // read env
    let neardata_base = std::env::var("NEARDATA_BASE")
        .unwrap_or_else(|_| "https://mainnet.neardata.xyz".to_string());
    let ring_size: usize = std::env::var("RING_SIZE")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(256);
    let poll_retry_ms: u64 = std::env::var("POLL_RETRY_MS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(500);

    let cfg = AppConfig {
        neardata_base,
        ring_size,
        poll_retry_ms,
    };

    info!(
        "Starting NEAR SSE server with config: neardata_base={}, ring_size={}, poll_retry_ms={}",
        cfg.neardata_base, cfg.ring_size, cfg.poll_retry_ms
    );

    // broadcast channel
    let (tx, _rx) = broadcast::channel::<BlockMsg>(1024);

    let shared_state = SharedState {
        ring: Arc::new(RwLock::new(VecDeque::with_capacity(cfg.ring_size))),
        tx,
        cfg: cfg.clone(),
    };

    // spawn ingestor task
    {
        let st = shared_state.clone();
        tokio::spawn(async move {
            if let Err(e) = run_ingestor(st).await {
                error!("Ingestor crashed: {:?}", e);
            }
        });
    }

    info!("Starting HTTP server on 0.0.0.0:8080");
    // start HTTP server
    HttpServer::new(move || {
        App::new()
            .app_data(Data::new(shared_state.clone()))
            .service(stream_handler)
            .service(health)
    })
    .bind(("0.0.0.0", 8080))?
    .run()
    .await
}
