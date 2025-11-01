# NEAR Stream

Real-time Server-Sent Events (SSE) stream for NEAR blockchain blocks. Powered by [neardata.xyz](https://neardata.xyz).

## Hosted Service

Use the free hosted service at **[live.near.tools](https://live.near.tools)**

### Quick Start

**Stream from latest block:**

```bash
curl -N https://live.near.tools
```

**JavaScript/TypeScript:**

```javascript
const eventSource = new EventSource("https://live.near.tools")

eventSource.addEventListener("block", (event) => {
  const block = JSON.parse(event.data)
  console.log("Block", event.lastEventId, block)
})

eventSource.addEventListener("ping", (event) => {
  console.log("Keep-alive ping")
})

// Automatic reconnection with resume support
eventSource.onerror = () => {
  console.log("Connection lost, reconnecting...")
  // Browser automatically sends Last-Event-ID header to resume
}
```

**Resume from specific block:**

```bash
curl -N "https://live.near.tools?from_height=170727400"
```

### API Endpoints

#### `GET /`

Server-Sent Events endpoint for real-time block streaming.

**Query Parameters:**

- `from_height` (optional) - Resume stream from block height (exclusive)

**Response Headers:**

- `Content-Type: text/event-stream`
- `Cache-Control: no-cache`

**Event Format:**

```
event: block
id: 170727400
data: {"block_height":170727400,"block_hash":"...","prev_block_hash":"...",...}

event: block
id: 170727401
data: {"block_height":170727401,...}
```

#### `GET /healthz`

Health check endpoint. Returns `"ok"` as JSON.

## Features

- **Real-time streaming** - SSE endpoint for live NEAR block data
- **Automatic catch-up** - Clients can resume from any block height
- **Finality guarantee** - Only streams finalized blocks
- **Batch processing** - Efficiently handles multiple new blocks per poll
- **Ring buffer** - In-memory cache of recent blocks for fast catch-up
- **Exponential backoff** - Automatic retries with backoff on rate limits and errors
- **Production-ready** - Built for reliability and self-hosting

## Self-Hosting

Want to run your own instance? Deploy with Docker or build from source.

### Docker

```bash
docker pull ghcr.io/r-near/near-stream:latest

docker run -d \
  -p 8080:8080 \
  -e RUST_LOG=info \
  ghcr.io/r-near/near-stream:latest
```

Server starts on `http://localhost:8080`

### Build from Source

```bash
# Build
cargo build --release

# Run with defaults (mainnet)
RUST_LOG=info cargo run --release

# Run with custom config
NEARDATA_BASE=https://mainnet.neardata.xyz \
RING_SIZE=512 \
POLL_RETRY_MS=1000 \
cargo run --release
```

### Configuration

All configuration via environment variables:

| Variable        | Description                          | Default                            |
| --------------- | ------------------------------------ | ---------------------------------- |
| `NEARDATA_BASE` | neardata.xyz API base URL            | `https://mainnet.neardata.xyz`     |
| `RING_SIZE`     | Number of recent blocks to cache     | `256`                              |
| `POLL_RETRY_MS` | Milliseconds between finality checks | `1000`                             |
| `BIND_ADDR`     | Server bind address                  | `0.0.0.0`                          |
| `BIND_PORT`     | Server bind port                     | `8080`                             |
| `RUST_LOG`      | Log level (tracing filter)           | `near_stream=info,tower_http=info` |

### Retry Behavior

The service automatically retries failed requests with exponential backoff:

- **Max retries**: 5 attempts
- **Backoff**: Exponentially increases (1s, 2s, 4s, 8s, 16s)
- **Triggers**: 429 (rate limit), 5xx (server errors), network failures
- **Jitter**: Prevents thundering herd on retry

### Tuning Recommendations

**For testnet:**

```bash
NEARDATA_BASE=https://testnet.neardata.xyz
```

**For high-traffic (many SSE clients):**

```bash
RING_SIZE=1024  # More catch-up history
```

**If consistently hitting rate limits:**

```bash
POLL_RETRY_MS=2000  # Poll less frequently
```

**For verbose debugging:**

```bash
RUST_LOG=near_stream=debug,tower_http=debug
```

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                   NEAR Stream Service                   │
├─────────────────────────────────────────────────────────┤
│                                                         │
│  ┌──────────────┐         ┌─────────────────┐           │
│  │   Ingestor   │───────▶│ Broadcast Chan   │           │
│  │              │         │  (tokio::mpsc)  │           │
│  │ - Polls      │         └────────┬────────┘           │
│  │   /v0/last_  │                  │                    │
│  │   block/final│                  │                    │
│  │ - Fetches    │         ┌────────▼────────┐           │
│  │   blocks     │         │  Ring Buffer    │           │
│  │ - Batch      │         │   Updater       │           │
│  │   catch-up   │         │                 │           │
│  └──────────────┘         └─────────────────┘           │
│                                                         │
│                          ┌─────────────────┐            │
│  ┌──────────────┐        │  Ring Buffer    │            │
│  │ SSE Handler  │◀──────│  (VecDeque)      │           │
│  │              │        │                 │            │
│  │ - Catch-up   │        │  - 256 blocks   │            │
│  │ - Live       │        │  - Fast resume  │            │
│  │   stream     │        └─────────────────┘            │
│  └──────────────┘                                       │
│         │                                               │
└─────────┼───────────────────────────────────────────────┘
          │
          ▼
    SSE Clients
```

### Data Flow

1. **Ingestor** polls neardata.xyz every 1s for latest finalized block
2. **Batch catch-up**: If multiple blocks finalized, fetches all sequentially
3. **Broadcast**: Each block published to tokio broadcast channel
4. **Ring buffer updater**: Subscribes to broadcast, maintains recent blocks
5. **SSE handler**:
   - New client connects
   - Serves catch-up blocks from ring buffer
   - Subscribes to broadcast for live blocks
   - Streams both via SSE

## Limitations

- **No persistence**: Blocks only stored in-memory (ring buffer)
  - Service restart = clients must reconnect from current finalized block
  - Not an issue for live streaming use case
- **Single chain**: Configure for mainnet OR testnet, not both
- **Rate limits**: Respects neardata.xyz API limits (1s poll interval)

## Contributing

This is a public good project. Contributions welcome!

## License

MIT

## Credits

Built with:

- [axum](https://github.com/tokio-rs/axum) - Web framework
- [tokio](https://tokio.rs/) - Async runtime
- [tracing](https://github.com/tokio-rs/tracing) - Structured logging
- [reqwest-retry](https://github.com/TrueLayer/reqwest-middleware) - Automatic retry with exponential backoff
- [neardata.xyz](https://neardata.xyz) - NEAR block data API
