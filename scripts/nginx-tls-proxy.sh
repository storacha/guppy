#!/usr/bin/env bash
#
# Run nginx as a TLS-terminating reverse proxy in front of the guppy gateway.
#
# Usage:
#   ./scripts/nginx-tls-proxy.sh --cert cert.pem --key key.pem
#
# Options (also settable via environment variables):
#   --cert, NGINX_TLS_CERT     Path to TLS certificate file (required)
#   --key,  NGINX_TLS_KEY      Path to TLS key file (required)
#   --listen, NGINX_LISTEN     HTTPS listen port (default: 3443)
#   --upstream, NGINX_UPSTREAM Upstream HTTP port (default: 3000)

set -euo pipefail

LISTEN="${NGINX_LISTEN:-3443}"
UPSTREAM="${NGINX_UPSTREAM:-3000}"
CERT="${NGINX_TLS_CERT:-}"
KEY="${NGINX_TLS_KEY:-}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --cert) CERT="$2"; shift 2 ;;
    --key) KEY="$2"; shift 2 ;;
    --listen) LISTEN="$2"; shift 2 ;;
    --upstream) UPSTREAM="$2"; shift 2 ;;
    *) echo "Unknown option: $1" >&2; exit 1 ;;
  esac
done

if [[ -z "$CERT" || -z "$KEY" ]]; then
  echo "Error: --cert and --key are required" >&2
  exit 1
fi

CERT="$(cd "$(dirname "$CERT")" && pwd)/$(basename "$CERT")"
KEY="$(cd "$(dirname "$KEY")" && pwd)/$(basename "$KEY")"

TMPDIR="$(mktemp -d)"

trap 'rm -rf "$TMPDIR"' EXIT

cat > "$TMPDIR/nginx.conf" <<EOF
daemon off;
worker_processes 1;
error_log /dev/stderr;
pid $TMPDIR/nginx.pid;

events {
  worker_connections 1024;
}

http {
  access_log /dev/stdout;

  server {
    listen $LISTEN ssl;
		http2 on;

    ssl_certificate     $CERT;
    ssl_certificate_key $KEY;

    location / {
      proxy_pass http://127.0.0.1:$UPSTREAM;
      proxy_http_version 1.1;

      proxy_set_header Host              \$host;
      proxy_set_header X-Real-IP         \$remote_addr;
      proxy_set_header X-Forwarded-For   \$proxy_add_x_forwarded_for;
      proxy_set_header X-Forwarded-Host  \$host;
      proxy_set_header X-Forwarded-Proto https;
    }
  }
}
EOF

echo "nginx TLS proxy: https://localhost:$LISTEN -> http://localhost:$UPSTREAM"
exec nginx -c "$TMPDIR/nginx.conf"
