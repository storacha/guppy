#!/bin/sh
set -eu

: "${COMPOSE_PROJECT:?COMPOSE_PROJECT must be set}"

until id=$(docker ps -q \
    --filter "label=com.docker.compose.project=$COMPOSE_PROJECT" \
    --filter "label=com.docker.compose.service=upload") && [ -n "$id" ]; do
  sleep 0.2
done

echo "Watching upload container $id for login URLs..."

docker logs -f --since 0s "$id" 2>&1 | while IFS= read -r line; do
  url=$(printf '%s' "$line" | grep -oE '"url"[[:space:]]*:[[:space:]]*"[^"]+"' | sed -E 's/.*"url"[^"]*"([^"]+)".*/\1/' || true)
  [ -n "$url" ] || continue
  echo "clicking $url"
  if ! wget -O /dev/null --post-data='' "$url"; then
    echo "verification fetch failed: $url" >&2
  fi
done
