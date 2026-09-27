# syntax=docker/dockerfile:1

# --- Build stage ------------------------------------------------------------
# mediasoup compiles its C++ worker the first time this crate builds, which needs Python,
# a C/C++ toolchain, and pkg-config. Expect this stage to take several minutes on a clean cache.
FROM rust:1-bookworm AS build

RUN rustup component add rustfmt && apt-get update && apt-get install -y --no-install-recommends \
    python3 \
    python3-pip \
    ninja-build \
    cmake \
    pkg-config \
    ca-certificates \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY migrations ./migrations
COPY src ./src

RUN cargo build --release && strip target/release/server

# --- Runtime stage -----------------------------------------------------------
FROM debian:bookworm-slim AS runtime

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    ffmpeg \
  && rm -rf /var/lib/apt/lists/* \
  && useradd --create-home --shell /usr/sbin/nologin app

WORKDIR /app
COPY --from=build /app/target/release/server /usr/local/bin/server
COPY migrations ./migrations

RUN mkdir -p /app/recordings && chown -R app:app /app
USER app

EXPOSE 10000
# RTP/RTCP port range must match MEDIASOUP_MIN_PORT/MEDIASOUP_MAX_PORT and be published with
# `docker run -p 40000-49999:40000-49999/udp` (or the equivalent in docker-compose.yml).

ENTRYPOINT ["/usr/local/bin/server"]
