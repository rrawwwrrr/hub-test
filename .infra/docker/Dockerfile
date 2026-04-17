# syntax=docker/dockerfile:1

# ── Stage 1: build ────────────────────────────────────────────────────────────
FROM golang:1.24 AS builder

WORKDIR /app
COPY . .
# CGO_ENABLED=1 required for modernc.org/sqlite (uses C shims)
RUN CGO_ENABLED=1 GOOS=linux go build -mod=vendor -ldflags="-X main.version=${VERSION:-dev}" -o hub-test .

# ── Stage 2: runtime ──────────────────────────────────────────────────────────
FROM ubuntu:24.04

RUN apt-get update -o "Acquire::https::Verify-Peer=false" \
 && apt-get install -o "Acquire::https::Verify-Peer=false" -y --no-install-recommends \
      adb \
      ca-certificates \
 && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/hub-test /usr/local/bin/hub-test

# Writable dirs for reports, APK cache, and SQLite DB
RUN mkdir -p /data/reports /data/apk
WORKDIR /data

RUN echo 5 > /build_version
ENTRYPOINT ["hub-test"]
