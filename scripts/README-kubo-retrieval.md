# Retrieving content with Kubo

This guide explains how to run a local [Kubo](https://github.com/ipfs/kubo) node that retrieves IPFS content through a guppy gateway.

## Prerequisites

- [Kubo](https://docs.ipfs.tech/install/command-line/) (the `ipfs` CLI)
- [jq](https://jqlang.org/)
- A running guppy gateway (`guppy gateway serve`), either local or remote
- A TLS proxy in front of the gateway (Kubo requires HTTPS for HTTP retrieval)

## Architecture

<!-- This should be better -->
```
ipfs get /ipfs/<cid>
       |
       v
  Kubo daemon
       |
       |  1. Delegated routing (HTTP)
       |     GET /routing/v1/providers/<cid>
       v
  Guppy gateway (:3000)
       |
       |  2. Content retrieval (HTTPS)
       |     GET /ipfs/<cid>?format=raw
       v
  TLS proxy (:3443) --> Guppy gateway (:3000)
```

Kubo discovers content providers by querying the gateway's delegated routing endpoint over plain HTTP. The routing response directs Kubo to fetch blocks from the gateway's TLS address. Kubo requires HTTPS (with HTTP/2) for HTTP block retrieval, so a TLS proxy is required in front of the gateway, even if it uses a self-signed certificate (enabled with `--insecure`) for local use. With a valid certificate with a real CA root, this setup is suitable to use as a public Bitswap gateway.

## Quick start (for a local gateway)

Start the gateway (in one terminal):

```sh
# The gateway needs to know the TLS port, as it will advertise that port to Kubo
# through delegated routing.
guppy gateway serve --port 3000 --tls-port 3443
```

Start the TLS proxy (in another terminal):

```sh
./scripts/nginx-tls-proxy.sh --listen 3443 --upstream 3000
```

Start Kubo and retrieve content (in another terminal):

```sh
# Start the Kubo daemon
./scripts/kubo-gateway.sh \
  --init \
  --ipfs-path ./my-ipfs \
  --insecure \
  --gateway-url http://localhost:3000

# In a separate terminal, retrieve content
export IPFS_PATH=./my-ipfs
ipfs get /ipfs/<cid>
```

## TLS setup

Kubo requires HTTPS with HTTP/2 for gateway retrieval. You can provide TLS in
two ways:

### Option 1: Use the included nginx script (local-only)

Generate a self-signed certificate and run the proxy:

```sh
openssl req -x509 -newkey rsa:2048 -keyout key.pem -out cert.pem \
  -days 365 -nodes -subj "/CN=localhost" \
  -addext "subjectAltName=DNS:localhost,IP:127.0.0.1"

./scripts/nginx-tls-proxy.sh
```

When using a self-signed certificate, pass `--insecure` to `kubo-gateway.sh` to
skip certificate verification.

### Option 2: Use your own TLS proxy (local or remote gateway)

Any TLS-terminating reverse proxy will work (nginx, caddy, etc.) as long as it:

- Serves HTTP/2 over TLS
- Proxies to the gateway's HTTP port
- Passes through the `Host` header

`./scripts/nginx-tls-proxy.sh` may point to a remote Guppy gateway, in which case it is generally recommended to use a real TLS certificate and not use `--insecure`. Note, however, that this will proxy all retrieval traffic through that gateway.

## Script reference

### `kubo-gateway.sh`

Configures and runs a Kubo node for gateway-based retrieval.

| Flag | Required | Description |
|------|----------|-------------|
| `--gateway-url <url>` | yes | Gateway HTTP URL for delegated routing |
| `--ipfs-path <path>` | yes | IPFS repo directory |
| `--init` | no | Run `ipfs init` first (for fresh nodes) |
| `--insecure` | no | Allow self-signed TLS certificates |
| `--api-port <port>` | no | Kubo API port (default: 5001) |

### `nginx-tls-proxy.sh`

Runs nginx as an HTTP/2 TLS-terminating reverse proxy, to easily connect a local Kubo to a local Guppy gateway.

| Flag | Required | Default | Description |
|------|----------|---------|-------------|
| `--listen <port>` | no | 3443 | HTTPS listen port |
| `--upstream <port>` | no | 3000 | Upstream HTTP port |

Environment variables `NGINX_LISTEN`, and
`NGINX_UPSTREAM` can be used instead of flags.
