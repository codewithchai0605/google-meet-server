# syntax=docker/dockerfile:1

# --- Build stage ------------------------------------------------------------
FROM rust:1-bookworm AS build

RUN rustup component add rustfmt \
    && apt-get update \
    && apt-get install -y --no-install-recommends \
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

# Prevent Render's limited build environment from spawning
# too many simultaneous Rust/C++ compilation jobs.
ENV CARGO_BUILD_JOBS=1
ENV CMAKE_BUILD_PARALLEL_LEVEL=1
ENV NINJAFLAGS=-j1

RUN cargo build --release -j1 && \
    strip target/release/server

# --- Runtime stage ----------------------------------------------------------
FROM debian:bookworm-slim AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        ffmpeg \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --create-home --shell /usr/sbin/nologin app

WORKDIR /app

COPY --from=build /app/target/release/server /usr/local/bin/server
COPY migrations ./migrations

RUN mkdir -p /app/recordings \
    && chown -R app:app /app

USER app

EXPOSE 10000

ENTRYPOINT ["/usr/local/bin/server"]