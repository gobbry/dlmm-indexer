#!/usr/bin/env bash
# Serves the hardcoded Old Faithful epochs in archive/epochs/ as a Solana JSON-RPC endpoint on
# :8899, for the indexer's archive lane (ARCHIVE_RPC_URL). Downloads faithful-cli on first run.
set -euo pipefail

FAITHFUL_VERSION="${FAITHFUL_VERSION:-v0.7.28}"
LISTEN="${ARCHIVE_LISTEN:-:8899}"
# files.old-faithful.net 429s to a burst of index requests, rate limit to stay under the limit.
EPOCH_LOAD_CONCURRENCY="${ARCHIVE_EPOCH_LOAD_CONCURRENCY:-2}"
ARCHIVE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BIN_DIR="$ARCHIVE_DIR/bin"
BINARY="$BIN_DIR/faithful-cli-$FAITHFUL_VERSION"

case "$(uname -s)-$(uname -m)" in
  Darwin-arm64) ASSET=faithful-cli_darwin_arm64 ;;
  Darwin-x86_64) ASSET=faithful-cli_darwin_amd64 ;;
  Linux-x86_64) ASSET=faithful-cli_linux_amd64 ;;
  *)
    echo "no faithful-cli release for $(uname -s) $(uname -m); build it from github.com/rpcpool/yellowstone-faithful" >&2
    exit 1
    ;;
esac

if [ ! -x "$BINARY" ]; then
  mkdir -p "$BIN_DIR"
  URL="https://github.com/rpcpool/yellowstone-faithful/releases/download/$FAITHFUL_VERSION/$ASSET"
  echo "downloading $URL" >&2
  curl --fail --location --silent --show-error --output "$BINARY.partial" "$URL"
  chmod +x "$BINARY.partial"
  mv "$BINARY.partial" "$BINARY"
fi

# Refuse when starting on the same port
PORT="${LISTEN##*:}"
if lsof -nP -iTCP:"$PORT" -sTCP:LISTEN >/dev/null 2>&1; then
  echo "an archive server is already listening on :$PORT (lsof -nP -iTCP:$PORT -sTCP:LISTEN); stop it first" >&2
  exit 1
fi

exec "$BINARY" rpc --listen "$LISTEN" --epoch-load-concurrency "$EPOCH_LOAD_CONCURRENCY" "$ARCHIVE_DIR/epochs"
