# syntax=docker/dockerfile:1

# --- Build stage --------------------------------------------------------------
# rodio/cpal links against ALSA (alsa-sys via pkg-config), so libasound2-dev is
# required to build. librespot is built with rustls, so no OpenSSL is needed.
FROM rust:1-slim-bookworm AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
        pkg-config \
        libasound2-dev \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Cache dependency compilation: build against a stub source first, then the real
# tree. Any change to src/ or web/ invalidates only the final layer.
COPY Cargo.toml Cargo.lock ./
RUN mkdir -p src/bin web \
    && echo 'fn main() {}' > src/bin/server.rs \
    && echo 'fn main() {}' > src/bin/client.rs \
    && echo '// stub' > src/lib.rs \
    && : > web/index.html \
    && cargo build --release --locked --bins \
    && rm -rf src web

COPY . .
# `touch` so cargo notices the real sources are newer than the cached stub build.
RUN find src web -type f -exec touch {} + \
    && cargo build --release --locked --bins

# --- Runtime stage ------------------------------------------------------------
# libasound2: cpal at runtime. ca-certificates: rustls native roots for librespot.
# pulseaudio-utils: parec/pactl for the PulseAudio/PipeWire capture source.
FROM debian:bookworm-slim AS runtime

RUN apt-get update && apt-get install -y --no-install-recommends \
        libasound2 \
        ca-certificates \
        pulseaudio-utils \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/target/release/server /usr/local/bin/server
COPY --from=builder /build/target/release/client /usr/local/bin/client

# Config (rustcast.yaml) and per-client state (clients.json) are read/written
# relative to the working directory; mount a volume here to persist them.
WORKDIR /data
VOLUME /data

# HTTP control UI + API.
EXPOSE 8080/tcp
# Audio, time-sync, telemetry, catalog, control, and inter-server stats.
# Multicast/unicast streaming generally needs host networking (--network host).
EXPOSE 5004-5011/udp
EXPOSE 5006/tcp

ENTRYPOINT ["server"]
