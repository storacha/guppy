# ============================================
# Build base stage
# ============================================
FROM golang:1.26.1-trixie AS build-base

# Docker sets TARGETARCH automatically during multi-platform builds
ARG TARGETARCH

WORKDIR /go/src/guppy

COPY go.* .
RUN go mod download
COPY --parents cmd internal pkg Makefile main.go version.json ./

# ============================================
# Production build stage
# ============================================
FROM build-base AS build-prod

# Allow the Makefile to look up the git commit, at the expense of busting the
# cache whenever `.git` changes.
COPY --parents .git ./

# Production build - with symbol stripping
RUN --mount=type=cache,target=/root/.cache/go-build CGO_ENABLED=0 GOOS=linux GOARCH=${TARGETARCH} make guppy-prod

# ============================================
# Debug build stage
# ============================================
FROM build-base AS build-debug

# Debug build - no optimizations, no inlining
RUN --mount=type=cache,target=/root/.cache/go-build CGO_ENABLED=0 GOOS=linux GOARCH=${TARGETARCH} make guppy-debug

# ============================================
# Debug tools
# ============================================
FROM golang:1.26.1-trixie AS build-debug-tools

ARG TARGETARCH

# Install delve debugger and randdir tool for target architecture
RUN GOARCH=${TARGETARCH} go install github.com/go-delve/delve/cmd/dlv@latest && \
    GOARCH=${TARGETARCH} go install github.com/storacha/randdir@latest

# ============================================
# Production image
# ============================================
FROM debian:bookworm-slim AS prod

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY --from=build-prod /go/src/guppy/guppy /usr/bin/guppy

ENTRYPOINT ["/usr/bin/guppy"]

# ============================================
# Development image
# ============================================
FROM debian:bookworm-slim AS dev

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    # Shell experience
    bash-completion \
    less \
    vim-tiny \
    # Process debugging
    procps \
    htop \
    strace \
    # Network debugging
    iputils-ping \
    dnsutils \
    net-tools \
    tcpdump \
    # Data tools
    jq \
    && rm -rf /var/lib/apt/lists/*

# Delve debugger and randdir tool
COPY --from=build-debug-tools /go/bin/dlv /usr/bin/dlv
COPY --from=build-debug-tools /go/bin/randdir /usr/bin/randdir

# Debug binary (with symbols, no optimizations)
COPY --from=build-debug /go/src/guppy/guppy /usr/bin/guppy

# Create data directories
RUN mkdir -p /root/.storacha/guppy /root/.config/guppy

WORKDIR /root

# Shell niceties
ENV TERM=xterm-256color
RUN echo 'alias ll="ls -la"' >> /etc/bash.bashrc && \
    echo 'PS1="\[\e[32m\][guppy-dev]\[\e[m\] \w# "' >> /etc/bash.bashrc

SHELL ["/bin/bash", "-c"]
ENTRYPOINT ["/usr/bin/guppy"]

# ============================================
# Test image (without interactive tools)
# ============================================
FROM debian:bookworm-slim AS test

# Debug binary (with symbols, no optimizations)
COPY --from=build-debug /go/src/guppy/guppy /usr/bin/guppy

# Create data directories
RUN mkdir -p /root/.storacha/guppy /root/.config/guppy

WORKDIR /root

SHELL ["/bin/bash", "-c"]
ENTRYPOINT ["/usr/bin/guppy"]
