# ============================================
# Build stage (shared)
# ============================================
FROM golang:1.25.3-trixie AS build

# Docker sets TARGETARCH automatically during multi-platform builds
ARG TARGETARCH

WORKDIR /go/src/guppy

COPY go.* .
RUN go mod download
COPY . .

# Production build - with symbol stripping
RUN CGO_ENABLED=0 GOOS=linux GOARCH=${TARGETARCH} make guppy-prod

# ============================================
# Debug build stage
# ============================================
FROM build AS build-debug

ARG TARGETARCH

# Install delve debugger and randdir tool for target architecture
RUN GOARCH=${TARGETARCH} go install github.com/go-delve/delve/cmd/dlv@latest && \
    GOARCH=${TARGETARCH} go install github.com/storacha/randdir@latest

# Debug build - no optimizations, no inlining
RUN CGO_ENABLED=0 GOOS=linux GOARCH=${TARGETARCH} make guppy-debug

# ============================================
# Production image
# ============================================
FROM debian:bookworm-slim AS prod

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY --from=build /go/src/guppy/guppy /usr/bin/guppy

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
COPY --from=build-debug /go/bin/dlv /usr/bin/dlv
COPY --from=build-debug /go/bin/randdir /usr/bin/randdir

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
