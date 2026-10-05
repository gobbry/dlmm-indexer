# Old Faithful archive

The indexer's deep-history endpoint. `run.sh` downloads the `faithful-cli` release binary
for this host (darwin arm64 or amd64, linux amd64; pinned to `v0.7.28`, override with
`FAITHFUL_VERSION`) into the gitignored `bin/`, then serves the epochs in `epochs/` as a
Solana JSON-RPC endpoint on `:8899` (override with `ARCHIVE_LISTEN`). Blocks are read by
HTTP range requests against `files.old-faithful.net`, so nothing is downloaded up front;
each block takes tens of seconds, 0.5 to 0.7 blocks/s with 12 in flight.

```bash
./archive/run.sh
curl -s localhost:8899 -H 'content-type: application/json' \
  -d '{"jsonrpc":"2.0","id":1,"method":"getFirstAvailableBlock"}'
```

Set `ARCHIVE_RPC_URL` for the indexer (see the main README).

## Epochs served

Hardcoded on 2026-10-05: six months of history, every published epoch from 951 (first block
2026-04-03) to 1047 (the newest published that day; 1048 and 1049 were not out yet). That is
97 one-file configs; the server reads each epoch's index headers at start, about two minutes
for the set, and stores nothing. Epoch 1046 holds the fixture block 452139025.

## Adding an epoch by hand

```bash
N=1048
CID=$(curl -s https://files.old-faithful.net/$N/epoch-$N.cid)
```

Copy `epochs/1047.yml` to `epochs/$N.yml`, replace the epoch number and the CID, check that
the six URLs answer `200` to `curl -I`, and restart `run.sh`. An epoch is published some
days after it ends, which is why the indexer only sends holes at least a week old here.
Discovering new epochs automatically (a sidecar that polls for the CID and writes the
config) is future work.
