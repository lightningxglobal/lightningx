# Stage 1: build
FROM rust:1.78-slim AS builder
WORKDIR /app

# Install build dependencies (needed for sqlx native-tls and openssl)
RUN apt-get update && apt-get install -y pkg-config libssl-dev && rm -rf /var/lib/apt/lists/*

# Cache dependencies: copy manifests first, compile a dummy binary
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo "fn main(){}" > src/main.rs && cargo build --release

# Copy real source and rebuild (touch main.rs to force re-link)
COPY src ./src
COPY migrations ./migrations
RUN touch src/main.rs && cargo build --release

# Stage 2: minimal runtime image
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates libssl3 && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/exchange-server /usr/local/bin/exchange-server
COPY --from=builder /app/migrations /migrations

EXPOSE 3000
CMD ["exchange-server"]
