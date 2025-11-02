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
- **Horizontally scalable** - Scale SSE servers independently with Redis
- **Distributed architecture** - Separate ingester and server components
- **Batch processing** - Efficiently handles multiple new blocks per poll
- **Redis-backed buffer** - 256 recent blocks cached for fast catch-up
- **Production-ready** - Built for reliability, self-hosting, and Kubernetes

## Self-Hosting

Want to run your own instance? Deploy with Docker Compose or build from source.

### Docker Compose (Recommended)

The service uses a distributed architecture with Redis for horizontal scaling:

```bash
# Start all services (Redis + Ingester + Server)
docker compose up -d

# Scale SSE servers for high traffic
docker compose up -d --scale server=3
```

This starts:
- **Redis** - Block storage and streaming (port 6380)
- **Ingester** - Fetches blocks from neardata.xyz and publishes to Redis
- **Server** - Serves SSE streams to clients (port 8080)

Server available at `http://localhost:8080`

### Build from Source

```bash
# Build
cargo build --release

# Run ingester (fetches blocks, publishes to Redis)
MODE=ingester \
REDIS_URL=redis://localhost:6379 \
NEARDATA_BASE=https://mainnet.neardata.xyz \
POLL_RETRY_MS=1000 \
cargo run --release

# Run server (serves SSE streams from Redis)
MODE=server \
REDIS_URL=redis://localhost:6379 \
BIND_ADDR=0.0.0.0 \
BIND_PORT=8080 \
cargo run --release
```

### Configuration

All configuration via environment variables:

| Variable        | Description                          | Default                            | Used By         |
| --------------- | ------------------------------------ | ---------------------------------- | --------------- |
| `MODE`          | Runtime mode: `ingester` or `server` | **Required**                       | Both            |
| `REDIS_URL`     | Redis connection URL                 | `redis://localhost:6379`           | Both            |
| `NEARDATA_BASE` | neardata.xyz API base URL            | `https://mainnet.neardata.xyz`     | Ingester        |
| `POLL_RETRY_MS` | Milliseconds between finality checks | `1000`                             | Ingester        |
| `BIND_ADDR`     | Server bind address                  | `0.0.0.0`                          | Server          |
| `BIND_PORT`     | Server bind port                     | `8080`                             | Server          |
| `RUST_LOG`      | Log level (tracing filter)           | `near_stream=info,tower_http=info` | Both            |

### Retry Behavior

The service automatically retries failed requests with exponential backoff:

- **Max retries**: 5 attempts
- **Backoff**: Exponentially increases (1s, 2s, 4s, 8s, 16s)
- **Triggers**: 429 (rate limit), 5xx (server errors), network failures
- **Jitter**: Prevents thundering herd on retry

### Tuning Recommendations

**For testnet:**

```bash
# In docker-compose.yml, update ingester environment:
NEARDATA_BASE=https://testnet.neardata.xyz
```

**For high-traffic (many SSE clients):**

```bash
# Scale SSE servers horizontally
docker compose up -d --scale server=5
```

**If consistently hitting rate limits:**

```bash
# In docker-compose.yml, update ingester environment:
POLL_RETRY_MS=2000  # Poll less frequently
```

**For verbose debugging:**

```bash
RUST_LOG=near_stream=debug,tower_http=debug
```

## Architecture

Distributed architecture with Redis for horizontal scaling:

```
┌──────────────────────────────────────────────────────────────────┐
│                      NEAR Stream (Split)                         │
└──────────────────────────────────────────────────────────────────┘
                               │
              ┌────────────────┴────────────────┐
              │                                 │
              ▼                                 ▼
    ┌─────────────────┐              ┌─────────────────┐
    │  Ingester (1x)  │              │  Server (Nx)    │
    │                 │              │                 │
    │ - Polls         │              │ - Catch-up      │
    │   neardata.xyz  │              │   from Redis    │
    │ - Fetches       │              │ - Subscribe to  │
    │   blocks        │              │   new blocks    │
    │ - Publishes to  │              │ - Serve SSE     │
    │   Redis Streams │              │   to clients    │
    └────────┬────────┘              └────────┬────────┘
             │                                │
             │      ┌──────────────┐          │
             └─────▶│    Redis     │◀─────────┘
                    │   Streams    │
                    │              │
                    │ - 256 blocks │
                    │ - Auto-trim  │
                    │ - Pub/sub    │
                    └──────────────┘
                           │
                           ▼
                    SSE Clients
```

### Components

1. **Ingester** (single instance)
   - Polls neardata.xyz for finalized blocks
   - Handles skipped blocks and rate limiting
   - Publishes to Redis Streams
   - Auto-trims to maintain 256 block buffer

2. **Server** (horizontally scalable)
   - Reads catch-up blocks from Redis
   - Subscribes to new blocks via Redis Streams
   - Serves SSE streams to clients
   - Scale independently: `docker compose up --scale server=N`

3. **Redis Streams**
   - Persistent block buffer (256 blocks)
   - Pub/sub for real-time distribution
   - Automatic trimming (LRU eviction)
   - Single source of truth

### Data Flow

1. **Ingester** polls neardata.xyz every ~1s for latest finalized block
2. **Batch catch-up**: If multiple blocks finalized, fetches all sequentially
3. **Publish**: Each block published to Redis Streams
4. **Auto-trim**: Redis maintains last 256 blocks
5. **SSE Servers**:
   - New client connects
   - Server reads catch-up blocks from Redis
   - Server subscribes to Redis for new blocks
   - Streams both via SSE to client

## Limitations

- **Limited history**: Only 256 most recent blocks cached in Redis
  - Older blocks require fetching from neardata.xyz directly
  - Sufficient for reconnection and catch-up scenarios
- **Single chain**: Configure for mainnet OR testnet, not both
- **Rate limits**: Respects neardata.xyz API limits (~1s poll interval)
- **Redis dependency**: Both ingester and servers require Redis connection

## Contributing

This is a public good project. Contributions welcome!

## License

MIT

## Credits

Built with:

- [axum](https://github.com/tokio-rs/axum) - Web framework
- [tokio](https://tokio.rs/) - Async runtime
- [redis-rs](https://github.com/redis-rs/redis-rs) - Redis client with Streams support
- [tracing](https://github.com/tokio-rs/tracing) - Structured logging
- [reqwest-retry](https://github.com/TrueLayer/reqwest-middleware) - Automatic retry with exponential backoff
- [neardata.xyz](https://neardata.xyz) - NEAR block data API
