# Configuration

Guppy can be configured through three layers, which override each other in order of precedence (highest to lowest):

1. **CLI flags** — Override everything (e.g., `--port 8080`)
2. **Environment variables** — Prefixed with `GUPPY_` (e.g., `GUPPY_GATEWAY_PORT=8080`)
3. **Config file** — TOML format

## Config File

Guppy looks for a TOML config file in these locations, reading only the first file found:

1. Path specified with `--config`
2. `~/.config/guppy/config.toml`
3. `./config.toml` (current directory)

### Complete Reference

```toml
# Repository configuration — where Guppy stores its data.
[repo]
  # Directory for the SQLite database and agent store.
  data_dir = "~/.storacha/guppy"

  # PostgreSQL connection URL. If set, Guppy uses PostgreSQL instead of SQLite.
  # Format: postgres://user:password@host:port/dbname
  # database_url = "postgres://user:password@localhost:5432/guppy"

# Network configuration — which Storacha network to use.
[network]
  # Preset network name. Sets all the service URLs and DIDs below.
  # Available presets: forge, forge-test, hot, warm-staging
  name = "forge"

  # Override individual network endpoints (optional).
  # These take precedence over the preset values.
  # upload_id = "did:web:up.forge.storacha.network"
  # upload_url = "https://up.forge.storacha.network"
  # receipts_url = "https://up.forge.storacha.network/receipt/"
  # indexer_id = "did:web:indexer.forge.storacha.network"
  # indexer_url = "https://indexer.forge.storacha.network"
  # authorized_retrievals = true

# Gateway configuration — settings for `guppy gateway serve`.
[gateway]
  # Port to listen on.
  port = 3000

  # Number of blocks to cache in memory. Blocks are typically <1MB due to IPFS
  # chunking, so 1000 blocks ≈ up to 1GB of memory.
  block_cache_capacity = 1000

  # Enable trusted gateway mode, allowing deserialized responses.
  # See: https://docs.ipfs.tech/reference/http/gateway/#trusted-vs-trustless
  trusted = true

  # Logging level for the gateway server.
  # Options: debug, info, warn, error
  log_level = "warn"

  # External HTTPS URL where the gateway is publicly accessible.
  # Required for Kubo retrieval integration. Enables /routing/v1/ endpoints.
  # advertise_url = "https://gateway.example.com"

  # Subdomain gateway configuration.
  # When enabled, content is served at https://<cid>.ipfs.<host>/ and
  # path-style requests (/ipfs/<cid>) are redirected to the subdomain form.
  # See: https://specs.ipfs.tech/http-gateways/subdomain-gateway/
  [gateway.subdomain]
    enabled = false
    # Public host(s) for the gateway. Required if subdomain mode is enabled.
    # hosts = ["gateway.example.com"]

[upload]
  # Number of replicas to request per shard. Cannot be greater than 3. Not
  # recommended to set less than 3 except for testing purposes.
  replicas = 3
```

## Environment Variables

Environment variables use the `GUPPY_` prefix. Dots in config keys become underscores.

### Examples

| Variable | Config Key |
|----------|-----------|
| `GUPPY_REPO_DATA_DIR` | `repo.data_dir` |
| `GUPPY_NETWORK_NAME` | `network.name` |
| `GUPPY_GATEWAY_PORT` | `gateway.port` |
| (and so on.) | |


### Legacy Variables

These `STORACHA_*` environment variables are still supported for backward compatibility. They are only used if the corresponding `GUPPY_*` variable is not set.

| Variable | Equivalent |
|----------|-----------|
| `STORACHA_NETWORK` | `GUPPY_NETWORK_NAME` |
| `STORACHA_SERVICE_DID` | `GUPPY_NETWORK_UPLOAD_ID` |
| `STORACHA_SERVICE_URL` | `GUPPY_NETWORK_UPLOAD_URL` |
| `STORACHA_RECEIPTS_URL` | `GUPPY_NETWORK_RECEIPTS_URL` |
| `STORACHA_INDEXING_SERVICE_DID` | `GUPPY_NETWORK_INDEXER_ID` |
| `STORACHA_INDEXING_SERVICE_URL` | `GUPPY_NETWORK_INDEXER_URL` |
| `STORACHA_AUTHORIZED_RETRIEVALS` | `GUPPY_NETWORK_AUTHORIZED_RETRIEVALS` |

## Data Directory

The data directory (`~/.storacha/guppy/` by default) contains:

- **SQLite database** — Tracks upload progress (scans, DAGs, shards, blobs)
- **Agent store** — Your agent identity and UCAN delegations/proofs

Override with `--data-dir` or `GUPPY_REPO_DATA_DIR`.

## Test Network

To use Guppy with the **Forge Test network**, use the CLI flag `--network=forge-test`, or configure the network in your `config.toml` like so:

```toml
[network]
  name = "forge-test"
```

The test network is intended to be used while integrating Guppy with your application. Data stored on the test network is not permanent.

!!! warning

    The test network is reset every month. This includes all data, indexes and accounts. All delegations issued by the service expire at the end of the month.
