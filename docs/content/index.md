# Getting Started with Guppy

Guppy is a Go client and CLI for the Storacha network, built for enterprise-scale, resumable uploads with parallel processing.

## Quick Start

### 1. Install

```bash
go install github.com/storacha/guppy
```

### 2. Log In

Authenticate with your Storacha account:

```bash
guppy login you@example.com
```

This sends a confirmation email. Click the link in the email to authorize Guppy to act on your behalf.

### 3. Create a Space

```bash
guppy space generate --name "my data"
```

### 4. Upload Data

Add a source directory and upload it:

```bash
guppy upload source add <space> /path/to/data
guppy upload <space>
```

### 5. Retrieve Data

```bash
guppy retrieve <space> <cid> ./output
```

---

## Key Concepts

**Spaces** are where your data lives on the Storacha network. Each space has a unique `did:key:` identifier and can be referred to by name.

**Sources** are local filesystem paths associated with a space. Guppy scans sources, breaks the data into UnixFS nodes, packs them into shards, and uploads the shards to the network.

**Resumable uploads** are built in. If an upload is interrupted, run the same `guppy upload` command again and it picks up where it left off.

## Configuration

Guppy stores identity, proofs, and upload state in `~/.storacha/guppy/` by default. Override with `--data-dir`.

Optional config file at `~/.config/guppy/config.toml` or `./config.toml`. Environment variables use the `GUPPY_` prefix.

## What's Next

- [CLI Reference](cli/index.md) - Complete command documentation
