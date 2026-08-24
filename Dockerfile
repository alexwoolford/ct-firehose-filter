# syntax=docker/dockerfile:1

FROM rust:1.91.1-bookworm AS builder
WORKDIR /app
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY src ./src
COPY keywords.txt ./keywords.txt
RUN cargo build --release --locked --bin ct-firehose-filter \
    && strip target/release/ct-firehose-filter

FROM debian:bookworm-slim AS runtime
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --create-home --uid 10001 ctfilter

WORKDIR /app
COPY --from=builder /app/target/release/ct-firehose-filter /usr/local/bin/ct-firehose-filter
# Tiny demo watchlist; mount a real domains.txt at runtime.
COPY keywords.txt /app/keywords.txt

USER ctfilter
ENV RUST_LOG=info \
    EGRESS=stdout \
    WATCHLIST_FILE=/app/keywords.txt \
    CERTSTREAM_URL=ws://certstream:8080/

ENTRYPOINT ["/usr/local/bin/ct-firehose-filter"]
