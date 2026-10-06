# Old Faithful archive

This directory is the indexer's deep-history endpoint. `run.sh` downloads the `faithful-cli`
release binary for this host into the gitignored `bin/`. Supported hosts are darwin arm64,
darwin amd64 and linux amd64. `run.sh` pins the version to `v0.7.28`. Set `FAITHFUL_VERSION` to
override it. Then `run.sh` serves the epochs in `epochs/` as a Solana JSON-RPC endpoint on
`:8899`. Set `ARCHIVE_LISTEN` to override the address.

The server reads blocks with HTTP range requests against `files.old-faithful.net`, so it
downloads nothing up front. Each block takes tens of seconds. With 12 requests in flight, the
rate is 0.5 to 0.7 blocks/s.

```bash
./archive/run.sh
curl -s localhost:8899 -H 'content-type: application/json' \
  -d '{"jsonrpc":"2.0","id":1,"method":"getFirstAvailableBlock"}'
```

Set `ARCHIVE_RPC_URL` for the indexer (see the main README).

## In compose

`docker compose up` starts the `archive` service from `Dockerfile` (the linux/amd64 release
binary. Apple Silicon runs it under emulation). The service copies the configs to a writable
directory and runs the server with `--watch`. `files.old-faithful.net` refuses a burst of
index opens with `429`, and the server does not retry an epoch that failed to load. So every
five minutes `entrypoint.sh` rewrites the config of each epoch the server does not list. The
watcher then loads that epoch again. A rate limit at start therefore heals itself. The indexer
reaches the service as `http://archive:8899`. `run.sh` is for a host run with `cargo run`. The
two cannot run at once because both bind `:8899`.

## Epochs served

The author hardcoded the epoch list on 2026-10-05. It covers six months of history: every published
epoch from 951 (first block 2026-04-03) to 1047. Epoch 1047 was the newest published epoch on
that day. Epochs 1048 and 1049 were not out yet. That is 97 one-file configs. At start, the
server reads the index headers of each epoch. This takes about two minutes for the set. The
server stores nothing. Epoch 1046 holds the fixture block 452139025.

## Adding an epoch by hand

```bash
N=1048
CID=$(curl -s https://files.old-faithful.net/$N/epoch-$N.cid)
```

1. Copy `epochs/1047.yml` to `epochs/$N.yml`.
2. Replace the epoch number and the CID.
3. Check that the six URLs answer `200` to `curl -I`.
4. Restart `run.sh`.

Old Faithful publishes an epoch some days after the epoch ends. For this reason, the indexer
sends only holes at least a week old to this archive. Automatic discovery of new epochs is
future work: a sidecar that polls for the CID and writes the config.
