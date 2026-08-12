# syntax=docker/dockerfile:1

FROM rust:1.91.1-bookworm AS builder
WORKDIR /app
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY src ./src
COPY keywords.txt ./keywords.txt
COPY suppress.txt ./suppress.txt
COPY glue.txt ./glue.txt
RUN cargo build --release --locked --bin ct-firehose-filter --bin ct-novelty-consumer \
    && strip target/release/ct-firehose-filter target/release/ct-novelty-consumer

FROM debian:bookworm-slim AS runtime
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --create-home --uid 10001 ctfilter

WORKDIR /app
COPY --from=builder /app/target/release/ct-firehose-filter /usr/local/bin/ct-firehose-filter
COPY --from=builder /app/target/release/ct-novelty-consumer /usr/local/bin/ct-novelty-consumer
# Tiny demo watchlist + default suppress/glue; mount real lists at runtime.
COPY keywords.txt /app/keywords.txt
COPY suppress.txt /app/suppress.txt
COPY glue.txt /app/glue.txt

USER ctfilter
ENV RUST_LOG=info \
    EGRESS=stdout \
    WATCHLIST_FILE=/app/keywords.txt \
    SUPPRESS_FILE=/app/suppress.txt \
    GLUE_FILE=/app/glue.txt \
    CERTSTREAM_URL=ws://certstream:8080/

ENTRYPOINT ["/usr/local/bin/ct-firehose-filter"]
