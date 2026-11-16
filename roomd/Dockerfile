# Build stage. Debian trixie matches the runtime image below.
FROM rust:1.99-slim-trixie AS builder

WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY build.rs ./
COPY proto ./proto
COPY src ./src
RUN cargo build --release --locked

# Runtime stage.
FROM debian:trixie-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*

RUN useradd --system --uid 10001 --no-create-home roomd

COPY --from=builder /src/target/release/roomd /usr/local/bin/roomd

USER roomd

ENV ROOMD_GRPC_ADDR=0.0.0.0:50051 \
    ROOMD_HTTP_ADDR=0.0.0.0:8080 \
    ROOMD_LOG_FORMAT=json

EXPOSE 50051 8080

HEALTHCHECK --interval=15s --timeout=3s --start-period=5s --retries=3 \
    CMD ["curl", "-fsS", "http://127.0.0.1:8080/healthz"]

ENTRYPOINT ["roomd"]
