# Getting Started with Guppy

Guppy is a Go client and CLI for the [Storacha network](https://storacha.network), built for enterprise-scale, resumable uploads with parallel processing.

Unlike simple upload tools, Guppy is designed for large datasets that may take hours or days to upload. It tracks progress locally, so interrupted uploads resume exactly where they left off. Data is prepared and uploaded in parallel, which maximizes throughput and allows the data to start uploading early, before the full scan is complete.

## Install

```bash
go install github.com/storacha/guppy@latest
```

Verify the installation:

```bash
guppy version
```

## Step 1: Log In

Guppy automatically generates an agent identity on first use. To authorize that identity with your Storacha account:

```bash
guppy login you@example.com
```

Storacha will send a confirmation email. Click the link in the email to authorize Guppy to act on your behalf.

Your identity, proofs, and other state are stored in `~/.storacha/guppy/` by default. You can set a different directory with `--data-dir`.

## Step 2: Create a Space

A **space** is a container for your data on the Storacha network. Each space has a unique `did:key:` identifier and can also have a human-readable name.

```bash
guppy space generate --name my-data
```

This creates a new space, provisions it to your account for billing, and grants your account full access. Your space is identified by a DID, which looks like `did:key:…`. Any command which refers to space can take either the DID or the name you gave it. You can list the spaces you have access to at any time:

```bash
guppy space list
```

## Step 3: Add a Source

A **source** is a local filesystem path associated with a space. Guppy scans sources to discover files to upload.

```bash
guppy upload source add my-data /path/to/dataset
```

You can add multiple sources to a space. Each one will become a separate upload in the space, each with its own root CID. List a space's sources with:

```bash
guppy upload source list my-data
```

The association of sources to spaces is purely local. Adding a source to a space doesn't affect the space itself on the network, and doesn't affect other instances of `guppy` on other machines pointing to the same space.

## Step 4: Upload

Run `upload` for your space:

```bash
guppy upload my-data
# Or, for easier visuals for humans
guppy upload my-data --ui
```

Guppy processes the upload in stages:

1. **Scan** — Walks the source paths to discover files
2. **DAG** — Breaks files into a DAG of [UnixFS nodes](https://docs.ipfs.tech/concepts/file-systems/#unix-file-system-unixfs)
3. **Shard & Index** — Packs nodes into shards for storage, and creates indexes into the shards
4. **Upload** — Uploads shards and indexes to the Storacha network

These stages form a pipeline, so as soon as a file is discovered, it will be broken into UnixFS nodes, and then those nodes will be packed into shards. As shards are filled, they will be indexed and then uploaded. Shard uploads happen in parallel.

All progress is tracked in a local SQLite database. If the process is interrupted at any stage, just run the `upload` command on the space again to resume.

### Catching changed sources

By default, when resuming, `upload` will scan the entire source again, in case any files have changed. The result will be a complete, consistent snapshot of the source. Any work that was in progress in later stages will continue, but the upload will not complete until the re-scan has confirmed that nothing has changed.

If you know the source data hasn't changed, you can use `--assume-unchanged-sources`. This will skip scanning files and directories which have been scanned before, so interrupted filesystem scans will resume rather than restart, and completed filesystem scans will not be repeated.

```bash
guppy upload my-data --assume-unchanged-sources
```

Guppy will also verify during upload that the data it's sending is what was scanned and hashed originally. If something's become inconsistent, those files will be marked as un-scanned, and resuming the upload will perform a new scan on them. You can also use `--retry` to automatically retry the upload after hitting such an issue.

### Repairing bad states

If the database does get into an incorrect state for some reason, or if you're ever concerned it has, you can check the local upload records to make sure they're consistent:

```bash
guppy upload check
```

## Step 5: Verify

After uploading, you can verify your data is complete and correctly stored:

```bash
guppy verify <root-cid>
```

Note that this requires reading the data from your space, which will incur egress charges. `<root-cid>` is the CID of the root of the DAG to verify, which can be the root of the entire upload or of some file or directory within the upload, which will verify from there down.

You can list your uploads to see their root CIDs:

```bash
guppy ls my-data
```

## Step 6: Retrieve

Retrieve content from the network:

```bash
guppy retrieve my-data <cid> ./output
```

The content path supports several formats:

- `<cid>` — Retrieve a file/directory with the given root CID
- `<cid>/path/to/file` — Retrieve a file/directory by path below a given root CID
- `/ipfs/<cid>` or `ipfs://<cid>` — IPFS-style paths

### Running a Gateway

To provide HTTP access, you can run a local IPFS Gateway:

```bash
guppy gateway serve --port 3000
```

This starts an [IPFS Gateway](https://docs.ipfs.tech/concepts/ipfs-gateway/) that serves content from your spaces. Access it at `http://localhost:3000/ipfs/<cid>`. Provide a space name or DID to limit the gateway to that space.

## Configuration

Guppy can be configured through three layers (in order of precedence):

1. CLI flags
2. Environment variables (e.g., `GUPPY_GATEWAY_PORT=8080`)
3. TOML Config file

Config files are loaded from these locations (in order of precedence):

1. Path specified with `--config`
2. `~/.config/guppy/config.toml`
3. `./config.toml` (current directory)

Available networks: `forge`, `hot`, `warm-staging`.

## What's Next

- [CLI Reference](cli/index.md) — Complete documentation for all commands and flags
