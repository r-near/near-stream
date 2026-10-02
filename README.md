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
| `POLL_RETRY_MS` | Delay after unavailable data or errors | `1000`                             | Ingester        |
| `NEARDATA_REQUESTS_PER_MINUTE` | Shared maximum request rate | `15` | Ingester |
| `BIND_ADDR`     | Server bind address                  | `0.0.0.0`                          | Server          |
| `BIND_PORT`     | Server bind port                     | `8080`                             | Server          |
| `RUST_LOG`      | Log level (tracing filter)           | `near_stream=info,tower_http=info` | Both            |

### Retry Behavior

All NearData requests share a pacing budget, including discovery, lookahead,
redirects, and retries. The default `NEARDATA_REQUESTS_PER_MINUTE=15` spaces
request starts by 4.1 seconds (4 seconds plus a 100ms margin). Catch-up uses the
same budget. `POLL_RETRY_MS` adds a delay after unavailable data or errors; it
does not cap the request rate by itself.

HTTP 429 pauses the entire NearData client for at least 60 seconds. Longer
`Retry-After` values are respected, including HTTP dates. Consecutive 429s
increase the fallback cooldown to 120, 240, then 480 seconds. A zero or missing
`Retry-After` never causes an immediate retry. Each 429 also doubles request
spacing, up to 60 seconds (or the initial spacing if already longer). After at
least 60 consecutive successful requests spanning at least 60 seconds, spacing
recovers by one halving step, never faster than the
configured ceiling. Failures restart this recovery window. Success resets the
cooldown escalation.
Startup discovery also retries transient failures. Network failures, rate limits,
and server errors are never treated as missing blocks. Permanent client errors
such as 401/403 fail instead of looping indefinitely.

The head is read from the `/v0/last_block/final` redirect without following it,
which avoids downloading the first block twice.

On restart, ingestion resumes after the newest height already stored in the Redis
`blocks` stream. An empty stream starts at the provider's finalized head. Changing
`NEARDATA_BASE` therefore preserves the stored cursor and catches up sequentially
through an endpoint with the same network and NearData response format. Keep one
ingester per Redis stream. Missing block responses (null or HTTP 404) use the same
lookahead and finality checks to advance skipped heights.
Non-null block responses must contain the requested `/block/header/height`;
malformed or mismatched responses are retried without advancing the cursor.

**Capacity:** an unauthenticated 30 requests/minute budget cannot keep a complete
stream live when the chain produces more than 30 blocks/minute. Ingestion
preserves sequential blocks and will fall behind in that case. A higher provider
allowance is necessary for a complete live stream. Do not raise the configured
rate above the allowance for your IP; other indexers on the same IP share it.

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
NEARDATA_REQUESTS_PER_MINUTE=10  # Leave room for other requests on the same IP
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

1. **Ingester** resumes its Redis cursor (or discovers the finalized head for an empty stream) and fetches successive heights within its request budget
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
- **Rate limits**: Shared request pacing and global 429 cooldown; lower budgets can accumulate lag
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
- [reqwest](https://github.com/seanmonstar/reqwest) - HTTP transport with shared request pacing
- [neardata.xyz](https://neardata.xyz) - NEAR block data API

## Local validation

Use a disposable Redis database; the integration suite clears it:

```bash
docker run -d --rm --name near-stream-test-redis -p 127.0.0.1:16379:6379 redis:8-alpine
TEST_REDIS_URL=redis://127.0.0.1:16379 cargo test -- --test-threads=1
cargo fmt --check
cargo clippy --all-targets -- -D warnings
docker stop near-stream-test-redis
```
