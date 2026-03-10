# gateway serve

Start an IPFS Gateway that serves content from the Storacha network.

This is a **long-running process** that operates an [IPFS Gateway](https://docs.ipfs.tech/concepts/ipfs-gateway/) for your data. By default it serves all spaces you are authorized to access. You can restrict it to specific spaces by passing them as arguments.

The gateway can be configured via command line flags, environment variables (prefix `GUPPY_GATEWAY_`), or a TOML config file.

## Usage

```
guppy gateway serve [space...] [flags]
```

## Arguments

| Argument | Description |
|----------|-------------|
| `space` | Optional space DIDs or names to restrict content served (serves all authorized spaces if omitted) |

## Flags

| Flag | Description | Default |
|------|-------------|---------|
| `--port, -p <port>` | HTTP port | `3000` |
| `--block-cache-capacity, -c <n>` | Number of blocks to cache in memory | `1000` |
| `--advertise-url <url>` | External HTTPS URL for delegated routing responses (required for Kubo retrieval) | |
| `--subdomain, -s` | Enable subdomain gateway mode (e.g., `<cid>.ipfs.<host>`) | |
| `--host <host>` | Gateway host(s) for subdomain mode (required if subdomain enabled) | |
| `--trusted, -t` | Enable trusted gateway mode allowing deserialized responses | `true` |
| `--log-level <level>` | Logging level (`debug`, `info`, `warn`, `error`) | |

## Example

Start a gateway on port 8080:

```bash
guppy gateway serve --port 8080
```

Serve a specific space:

```bash
guppy gateway serve "my data" --port 3000
```

Start with subdomain mode:

```bash
guppy gateway serve --subdomain --host gateway.example.com --port 3000
```

## Configuration File

The gateway can also be configured via TOML:

```toml
[gateway]
  block_cache_capacity = 1000
  log_level = "warn"
  port = 3000
  trusted = true

[gateway.subdomain]
  enabled = true
  hosts = ["gateway.example.com"]
```

Place the config file at `~/.config/guppy/config.toml` for automatic loading, or specify a path with `--config`.

## Stopping the Gateway

Use `Ctrl+C` to gracefully shut down the gateway.
