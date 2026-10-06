#!/bin/sh
# files.old-faithful.net refuses a burst of index opens with 429, and the server does not
# retry an epoch that failed to load. So: copy the configs to a writable directory, run with
# --watch, and every five minutes rewrite the config of every epoch the server does not list,
# which the watcher treats as a change and loads again.
set -eu
SRC=/epochs
RUN=/run/epochs
mkdir -p "$RUN"
cp "$SRC"/*.yml "$RUN"/
faithful-cli rpc --listen :8899 --epoch-load-concurrency "${EPOCH_LOAD_CONCURRENCY:-2}" --watch "$RUN" &
SERVER=$!
loaded_epochs() {
  curl -s --max-time 10 http://127.0.0.1:8899 -H 'content-type: application/json' \
    -d '{"jsonrpc":"2.0","id":1,"method":"getVersion"}' \
    | sed -n 's/.*"epochs":\[\([0-9,]*\)\].*/\1/p' | tr ',' '\n'
}
(
  while sleep "${EPOCH_RECONCILE_SECONDS:-300}"; do
    kill -0 "$SERVER" 2>/dev/null || exit 0
    loaded="$(loaded_epochs)"
    for config in "$SRC"/*.yml; do
      epoch="$(basename "$config" .yml)"
      if ! printf '%s\n' "$loaded" | grep -qx "$epoch"; then
        echo "epoch $epoch not loaded; triggering a reload" >&2
        cp "$config" "$RUN/$epoch.yml.tmp" && mv "$RUN/$epoch.yml.tmp" "$RUN/$epoch.yml"
        sleep 5
      fi
    done
  done
) &
wait "$SERVER"
