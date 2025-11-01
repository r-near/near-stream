# Build stage
FROM rust:1.91-bookworm AS builder

WORKDIR /app

# Copy manifests
COPY Cargo.toml Cargo.lock ./

# Copy source code
COPY src ./src

# Build release binary
RUN cargo build --release

# Runtime stage
FROM debian:bookworm-slim

# Install CA certificates for HTTPS requests
RUN apt-get update && \
    apt-get install -y ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# Copy binary from builder
COPY --from=builder /app/target/release/near-stream /usr/local/bin/near-stream

# Expose port
EXPOSE 8080

# Set default environment variables
ENV RUST_LOG=info
ENV BIND_ADDR=0.0.0.0
ENV BIND_PORT=8080

# Run the binary
CMD ["near-stream"]
