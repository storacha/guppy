#!/usr/bin/env bash
#
# Configure and run a Kubo node to retrieve content via a guppy gateway
# using delegated routing.
#
# Usage:
#   ./scripts/kubo-gateway.sh --gateway-url http://localhost:3000 --ipfs-path ./my-ipfs
#
# Required:
#   --gateway-url   Gateway HTTP URL for delegated routing
#   --ipfs-path     Path to IPFS repo directory (sets IPFS_PATH)
#
# Options:
#   --api-port      Kubo API port (default: 5001)
#   --insecure      Allow TLS connections to gateways with self-signed certs
#   --init          Run ipfs init before configuring (for fresh nodes)

set -euo pipefail

GATEWAY_URL=""
IPFS_PATH_ARG=""
API_PORT="5001"
INSECURE=false
DO_INIT=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --gateway-url) GATEWAY_URL="$2"; shift 2 ;;
    --ipfs-path) IPFS_PATH_ARG="$2"; shift 2 ;;
    --api-port) API_PORT="$2"; shift 2 ;;
    --insecure) INSECURE=true; shift ;;
    --init) DO_INIT=true; shift ;;
    *) echo "Unknown option: $1" >&2; exit 1 ;;
  esac
done

if [[ -z "$GATEWAY_URL" ]]; then
  echo "Error: --gateway-url is required" >&2
  exit 1
fi

if [[ -z "$IPFS_PATH_ARG" ]]; then
  echo "Error: --ipfs-path is required" >&2
  exit 1
fi

export IPFS_PATH="$IPFS_PATH_ARG"

if [[ "$DO_INIT" == true ]]; then
  ipfs init
fi

ipfs config --json Addresses "$(jq -n \
  --arg apiPort "$API_PORT" \
  '{
    "API": "/ip4/127.0.0.1/tcp/\($apiPort)",
    "Gateway": [],
    "Swarm": [],
    "Announce": [],
    "NoAnnounce": [],
    "AppendAnnounce": []
  }'
)"

ipfs config --json Routing "$(jq -n \
  --arg gatewayURL "$GATEWAY_URL" \
  '{"Type": "delegated", "DelegatedRouters": [$gatewayURL]}'
)"

ipfs config --bool Provide.Enabled false
ipfs config --bool AutoTLS.Enabled false
ipfs config --bool Discovery.MDNS.Enabled false

if [[ "$INSECURE" == true ]]; then
  ipfs config --bool HTTPRetrieval.TLSInsecureSkipVerify true
fi

exec ipfs daemon
