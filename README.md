# Meteora DLMM swap indexer

Indexes every swap of Meteora's DLMM program on Solana mainnet, stores each one exactly
once in TimescaleDB, and serves hourly and daily volume per pool, in tokens and USD, over a REST API. Three parts: **core** (Rust `indexer`: Geyser stream or RPC tail as the live source, a coverage reconciler and range filler, Binance price feed, one writer), **api** (Rust/Axum, read-only database role), and **cli** (`metclanker`, a Bun CLI that doubles as an agent skill in `.agents/skills/metclanker/`). The program indexed is `LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo`.

Design and trade-offs: [DESIGN.md](DESIGN.md). Full design and build plan:
[docs/plans/](docs/plans/).

## Prerequisites

- Docker with Compose v2 (tested on Docker 29.7, Compose 5.5).
- [Bun](https://bun.sh) 1.3+ for the CLI.
- A Solana mainnet RPC URL. Intended: a free [Helius](https://dashboard.helius.dev) key
  (10 requests per second) or a free QuickNode endpoint.

## Quick start

**1. Configure** (1 min).

```bash
cp .env.example .env
# edit .env: set RPC_URL (and RPC_REQUESTS_PER_SECOND_MAX=4 if you use the public endpoint)
```

**2. Start**

```bash
docker compose up -d --build
docker compose ps
```

**3. Check health**

```bash
curl localhost:8080/v1/health
```

```json
{"status":"ok","cursor_slot":452758149,"last_block_time":"2026-10-02T23:28:50Z","lag_seconds":24,"open_job_count":1,"blocked_job_count":0}
```

`status`, in order of precedence:

| status | meaning |
|---|---|
| `starting` | no cursor yet: the first live block has not committed |
| `lagging` | `lag_seconds > 120` (newest indexed block, the top of `slot_coverage`, versus now) |
| `blocked` | `blocked_job_count > 0`: a fill job needs an operator; `slot_range_job.blocked_reason` says why (`missing_in_storage`, `unmappable`, `end_unproven`, `archive_unavailable`, each with its slot) |
| `backfilling` | `open_job_count > 0`: holes in coverage still being filled |
| `ok` | none of the above |

A `rebuilding` array (for example `["pool_volume_1d"]`) appears only while an offline
`rebuild-projection` has left a projection in the `building` state; `status` is unaffected.
Using a public endpoint processes less blocks a second than the chain produces, which will cause `lagging` and a growing `open_job_count`. This will cause backpressure issues and cause solana RPC to be the primary driver.

**4. List pools**

```bash
cd cli && bun install
bun run metclanker pools --limit 10
```

Prints a table of pools ranked by 24-hour USD volume (from what has been indexed so far)
with a bar column, then the equivalent `curl` command.

**5. Volume beside Meteora's own numbers**

```bash
bun run metclanker volume --pool <address from step 4> --range 24h --bucket hour --compare
```

```
│ 23 │ 2026-10-02T22:00:00Z │ 0     │           0 │           0 │           n/a │    0.15     │   n/a      │
│ 24 │ 2026-10-02T23:00:00Z │ 1     │ 3.055721053 │ 7.505417576 │ 889.391982756 │  889.43     │ -0.04      │ ███
```

Columns: swaps, `volume_x`, `volume_y` (human units), our `volume_usd`, Meteora's, and the difference. **How to read `--compare`:** buckets that start before our first indexed swap (`first_swap_at`) show `n/a`, because our zero there means "not indexed", not "no volume".
The bucket that *contains* `first_swap_at` is partial: we only hold the minutes after the
indexer started, so it is comparable only by luck (as above) and usually reads low. Only
buckets entirely after `first_swap_at` are a fair comparison. Meteora's in-progress bucket also lags, so the newest bucket usually differs. Meteora's `volume` field is undocumented; we infer it is USD from the numbers.
Total from an empty checkout to step 5: about 5 minutes with a cold image cache, 2 minutes warm.

**6. Kill and restart (optional).** `docker compose stop indexer`, wait a minute,
`docker compose start indexer`. The live source resumes after the top of `slot_coverage`, but the first live block it sees can sit past it when the gap is long; every block write records its `(parent_slot, slot]` in `slot_coverage`, so the missed minute is a hole between two ranges. Within 10 s the reconciler opens one `slot_range_job` for it (`coverage_reconciled` in the logs) and the filler works it off; the job gets `completed_at` once coverage contains its range:

```bash
docker compose exec db psql -U postgres -d dlmm \
  -c "select id, start_slot, end_slot, next_slot, blocked_reason, completed_at from slot_range_job"
docker compose exec db psql -U postgres -d dlmm \
  -c "select * from slot_coverage order by start_slot"
docker compose exec db psql -U postgres -d dlmm \
  -c "select signature, swap_ordinal, block_time, count(*) from swap group by 1,2,3 having count(*) > 1"
```

Observed (before the reconciler, when the first live block opened the job itself): stopped at 23:29:37, started at 23:30:39, and the first live block created job 2 for slots 452758157 to 452758523 (367 slots, the minute away). The filler finished the
earlier job, then started job 2 (`next_slot` 452758157, then 452758172 two minutes later:
about 7 slots a minute on the public endpoint, so a one-minute gap takes most of an hour
there and a few minutes on Helius). The duplicate query returned zero rows, with 249 swaps stored live and 282 by fills.

## CLI

Run from `cli/` with `bun run metclanker <command>` (or `bun run build` for a single
binary in `cli/dist/metclanker`). Interactive prompts appear for missing flags on a TTY.

| command | flags |
|---|---|
| `health` | |
| `pools` | `--limit N` |
| `volume` | `--pool <base58>` `--bucket hour\|day` `--range 24h\|7d\|30d` or `--from/--to <rfc3339>`, `--compare` |
| `swaps` | `--pool <base58>` `--limit N` |

Global: `--base-url` (default `http://127.0.0.1:8080`), `--output table|json|raw`,
`--no-input`, `--timeout-ms`, `--no-color`. `--output json` prints one JSON envelope
(`meta`, `request`, `response`, `data`, `summary`, `compare`, `error`) and nothing else on
stdout, which is what agents use: `bun run metclanker volume --pool <p> --range 7d --bucket
day --output json --no-input`. Exit codes: 0 ok, 1 HTTP or JSON error, 2 usage, 3 network
or timeout.

## API

All times UTC, buckets `[start, end)` labelled by start, amounts as strings (raw integer
units and human decimals), `volume_usd` null for empty or wholly unpriced buckets.

```sh
curl localhost:8080/v1/health
curl 'localhost:8080/v1/pools?limit=20'
curl 'localhost:8080/v1/pools/<pool>/volume?bucket=hour&from=2026-10-02T00:00:00Z&to=2026-10-03T00:00:00Z'
curl 'localhost:8080/v1/pools/<pool>/volume?bucket=day&from=2026-09-01T00:00:00Z&to=2026-10-01T00:00:00Z'
curl 'localhost:8080/v1/pools/<pool>/swaps?limit=20'
```

`from` is floored and `to` ceiled to the bucket and echoed back. Caps: 744 hourly or 366
daily buckets. Hourly buckets read `pool_volume_1h`, daily ones read `pool_volume_1d`, a
separate fold over the same swaps (not a sum of hours at query time). Errors are
`{"error":{"code":"invalid_range","message":"..."}}` with 400 or 404, and 503
`projection_rebuilding` when the table that bucket reads is mid-rebuild. `/v1/pools` reads
`pool_volume_1h` and `pool_stats`, so it answers 503 while either rebuilds; volume responses
keep answering while `pool_stats` rebuilds, with `first_swap_at` null.

`/swaps` returns, per swap, the `Swap` event (`amount_in`, `amount_out`, `fee`,
`protocol_fee`, `host_fee`, `fee_rate_1e9`) and, when the transaction carried a decoded
`Swap2Evt`, its fee split: `mm_fee` (the liquidity providers' share), `limit_order_fee`,
`amount_left`, `fee_side` (`input`/`output`) and `fee_token` (`x`/`y`); these are `null`
for swaps without one. `source` says which path stored the row: `live_geyser`, `live_rpc`
(the RPC tail) or `fill` (a fill job, named by `fill_job_id`).

## Live sources: Geyser and the RPC tail

Without `GEYSER_ENDPOINT` the RPC tail is the live source (`live_source_rpc_tail` at
startup): every 2 s it lists the blocks past the cursor with `getBlocks` and fetches only
those with `getBlock`, the same two calls the range filler uses.

With `GEYSER_ENDPOINT` (and `GEYSER_X_TOKEN` if the provider wants one) the indexer logs
`live_source_geyser` and subscribes to a Yellowstone `blocks` stream at `finalized`,
filtered to the DLMM program, resuming with `from_slot = cursor + 1`. Fallback and handback
are automatic: when the stream fails or is silent for 30 s, the supervisor starts the RPC
tail from the cursor (`rpc_tail_started`); when the stream is healthy again it stops the
tail and Geyser resubscribes from the cursor. Overlap around a handback dedupes on the
unique key (a non-zero `duplicate_swap_count` in the logs there is expected); a block
neither source delivered leaves a hole in `slot_coverage`, which the reconciler hands to the filler. Rows record
which source stored them (`source = 'live_geyser'` or `'live_rpc'`).

Status: built and tested against an in-process Yellowstone server (serves blocks, goes
silent, the tail takes over within 30 s with a continuous cursor); **not yet verified
against a live provider**, since no credential was available.

## Backfill from a timestamp

```bash
# .env
BACKFILL_FROM=2026-10-04T12:00:00Z
```

On every boot the indexer bisects over `getBlocks` and `getBlockTime` to the first slot at
or after that instant that holds a block (`coverage_floor_resolved`, with the slot), then
starts the live source. That slot is the reconciler's floor: once the first live block has
committed (and, with an archive configured, once its window is read), the next tick (every
10 s) opens one `slot_range_job` from the floor up to the lowest `slot_coverage` range, and the filler works it alongside live ingestion; its rows
carry `source = 'fill'` and the job's `fill_job_id`. A restart resolves the same floor and
finds the job already open (or the range already covered), so nothing is opened twice.
Progress:

```bash
docker compose exec db psql -U postgres -d dlmm -c \
  "select id, start_slot, end_slot, next_slot, blocked_reason, completed_at from slot_range_job"
docker compose exec db psql -U postgres -d dlmm -c \
  "select source, count(*) from swap group by 1"
```

Observed on 2026-10-04 (fresh database, public RPC at 4 requests per second, no Geyser):
`BACKFILL_FROM` 15 s in the past resolved to slot 453149692, and the first `fill` row is in
that very slot; job 1 ran from 453149692 to 453149791 (that run predates the reconciler, so
the first live block opened it; today the first reconcile tick opens it). In
7 minutes the filler covered 35 of its 100 slots (the public endpoint throttles hard), with
128 `fill` rows, every one carrying `fill_job_id`, beside 453 `live_rpc` rows; no swap was
stored twice and the fee identity below returned 0 over all 581 rows. A day of history is
about 320,000 blocks at the 0.27 s slots measured in October 2026; that needs a paid RPC
tier, not a free one.

After the same run, with the indexer stopped, `rebuild-projection pool_volume_1d` replayed
581 swaps in one page and reproduced all 155 `pool_volume_1d` rows exactly
(`select * from pool_volume_1d order by pool, bucket` before and after, diffed); started
while the indexer ran, it exited with "another indexer holds the lock".

## Deep history from an Old Faithful archive

History more than a week old can come from an [Old Faithful](https://old-faithful.net)
archive instead of the RPC provider: `faithful-cli rpc` serves the published epoch files
(CAR plus indexes on `files.old-faithful.net`) as a Solana JSON-RPC endpoint, read by range
requests, so nothing is downloaded up front. The indexer talks to it with the same
`getBlock` call and decoder as the provider. There is no official container image, so it
runs beside compose, on the host:

```bash
./archive/run.sh        # downloads faithful-cli into archive/bin/ once, listens on :8899
```

Then point the indexer at it in `.env`:

```bash
ARCHIVE_RPC_URL=http://host.docker.internal:8899   # indexer in compose
# ARCHIVE_RPC_URL=http://127.0.0.1:8899            # indexer run with cargo on the host
```

Routing: the archive's window runs from its first available slot up to the week line, the
first block at or after now minus 604,800 s (one week, in time, found by `getBlockTime`
bisection on the archive itself; `archive_window_resolved` in the logs, with `bottom` and
`top`). The window is read beside live at boot, because the archive's `getSlot` and
`getFirstAvailableBlock` take 10 to 17 s each, and refreshed every 10 minutes; until it is
read no job is opened or walked. The reconciler cuts a hole at both edges of the window so no
job is half and half, and a job goes to the archive when the window holds all of it.
Archive and provider jobs are walked concurrently. With `ARCHIVE_RPC_URL` unset everything
goes to the provider, as before; when it is set but the archive does not answer at boot, live
ingest and pricing carry on while filling waits: the indexer logs `archive_window_unreadable`
and asks again every 20 s, and opens or walks no job until the window is read, so old holes
are never quietly sent to the provider. An archive job with a block the archive lacks (it
answers `-32009` and the next block's parent names it, or it is the job's end) asks for that
block again on each pass and blocks as `missing_in_storage` after 30 passes without progress,
so a read that fails once never blocks a job; one whose slot the archive keeps answering with a
retryable error (an epoch the server did not load) blocks after about twenty minutes as
`archive_unavailable`; an archive that does not answer at all (restarting, loading epochs) is
an outage, which never blocks a job. See `archive/README.md` for the epochs served and how to add one.

Observed on 2026-10-04 UTC (fresh database, public RPC at 4 requests per second through a
counting proxy, archive with epochs 1043 to 1047): `BACKFILL_FROM` two minutes below the week
line resolved to slot 451076871 (`coverage_floor_resolved`, 21 s into the boot, slowed by the
public endpoint's 429s); live started then, and `archive_window_resolved` followed 19 s later
with `bottom` 450576000 and `top` 451077403. The first reconcile opened archive job 2 for
451076871 to 451077403 (533 slots) and provider job 1 from 451077404 up to the live range.
The run was stopped at its 12-minute limit with the archive job at slot 451077327 and
restarted with the same `BACKFILL_FROM`; the restart resolved the same floor, opened nothing
for the covered range, resumed job 2 from the table, and the reconciler completed it 110 s
later, merging it with the provider job's progress into one range from the floor. About 14
minutes of archive fetching in all (533 slots in about 840 s, about 0.6 slots/s; skipped
slots hold no block, so blocks/s is at most that); 1,847 `fill` rows came from the
archive job, the provider received no `getBlock` for those slots (only the floor search's
`getBlocks` and `getBlockTime`, by design), and no swap was stored twice.

## Rebuilding a projection

```bash
docker compose stop indexer
docker compose run --rm indexer indexer rebuild-projection pool_volume_1d
# on the host: cargo run --bin indexer -- rebuild-projection pool_volume_1d
docker compose start indexer
```

Names: `pool_volume_1h`, `pool_volume_1d`, `pool_stats`. Offline only: the subcommand takes
the same advisory lock as `run` and exits with "another indexer holds the lock" while the
indexer runs. It marks the projection `building` (the API then answers 503
`projection_rebuilding` for that bucket width), truncates its table, and replays the swap
log in pages of 5,000 rows in `(slot, transaction_index, swap_ordinal)` order, each page in
one transaction that also advances the projection's cursor; a killed rebuild resumes from
that cursor. It ends by marking the projection `live` and logging `projection_rebuilt`
with row and page counts. USD comes from each row's stored `volume_usd`, so the rebuilt
table reflects prices as they stand now. After a restart the time the indexer was stopped
is a hole in `slot_coverage`; the reconciler opens a job for it and the filler closes it.

## Schema notes

`migrations/0001_schema.sql` is the whole schema. Values are guarded by SQL domains:
`solana_address` and `solana_signature` (base58 text of the right length),
`u64` (`NUMERIC(20,0)` within the `u64` range; the unit is in the column name, as in
`fee_rate_1e9`). Enums cover direction, quote asset, fee side and token, source, price
source and projection state.
`slot_coverage` holds the contiguous slot ranges fully indexed (an exclusion constraint
keeps them disjoint); its top range's end is the cursor every live source resumes after.
`slot_range_job` rows are derived from its holes and may not overlap either. A
`CHECK` ties `source = 'fill'` to a non-null `fill_job_id`. Projections: `pool_volume_1h`,
`pool_volume_1d` (UTC days) and `pool_stats`, with `projection` holding each one's version,
replay cursor and state.

## Verification queries

```bash
# fee identity: the Swap2Evt split adds up to the Swap event's fee (expect 0)
docker compose exec db psql -U postgres -d dlmm -c \
  "select count(*) from swap where mm_fee is not null and fee <> mm_fee + protocol_fee + limit_order_fee + host_fee"
# no swap stored twice (expect 0 rows)
docker compose exec db psql -U postgres -d dlmm -c \
  "select signature, swap_ordinal, block_time, count(*) from swap group by 1,2,3 having count(*) > 1"
```

## Tests

```sh
docker compose up -d db
set -a; source .env; set +a
cargo test --workspace          # decoder fixtures, idempotency and reprice against the db, API smoke
ARCHIVE_RPC_URL=http://127.0.0.1:8899 cargo test -p dlmm_core --test archive -- --include-ignored   # with ./archive/run.sh up
cargo clippy --all-targets -- -D warnings && cargo fmt --check
cd cli && bun test
```

Database tests use `#[sqlx::test]`, which creates a throwaway database per test on the
compose server. `core/tests/archive.rs` is skipped unless `ARCHIVE_RPC_URL` is set: it fetches
block 452139025 from the archive and checks that the fixtures in it map and decode exactly as
their mainnet `getTransaction` copies. Decoder fixtures are real mainnet `getTransaction` results in
`core/tests/fixtures/` (direct swap, Jupiter route, two swaps in one transaction, deep
nesting, and a failed transaction that still carries a `Swap` event).

## Configuration

Set in `.env`; compose overrides the two database URLs and `API_LISTEN_ADDRESS` inside the
containers.

| variable | default | meaning |
|---|---|---|
| `RPC_URL` | required | Solana mainnet RPC endpoint |
| `RPC_REQUESTS_PER_SECOND_MAX` | `10` | client-side limit, 60 percent of it for live blocks; minimum 2, practical minimum 7 (the tail needs 5 a second at the 0.27 s slots measured in October 2026; 10 for Helius free is fine); 4 on the public endpoint is a lagging demo |
| `RPC_TRANSACTION_VERSION_MAX` | `1` | `maxSupportedTransactionVersion`; raise only with a mapper for the new version |
| `POSTGRES_PASSWORD` | `postgres` | superuser password of the `db` service |
| `DATABASE_URL` | localhost | read-write URL (indexer, `cargo test`) |
| `DATABASE_URL_READONLY` | localhost | read-only URL for the API as `api_reader`; falls back to `DATABASE_URL` |
| `API_READER_PASSWORD` | `api_reader` | password of the `api_reader` role, created on first db start |
| `API_LISTEN_ADDRESS` | `0.0.0.0:8080` | socket address the API binds to |
| `PRICE_API_BASE_URL` | `https://data-api.binance.vision` | Binance market-data host (`api.binance.com` returns 451 to US egress) |
| `PRICE_SOURCE` | `binance` | market price source a swap is priced from (a `price_source` value; only `binance` today); the feed's own `peg` and `carried_forward` rows stay eligible |
| `GEYSER_ENDPOINT` | unset | Yellowstone gRPC endpoint (`http://` or `https://`); when set, Geyser is the primary live source and the RPC tail the automatic fallback |
| `GEYSER_X_TOKEN` | unset | sent as the `x-token` header; leave empty for providers that authenticate by URL or IP |
| `GEYSER_TRANSACTION_VERSION_MAX` | `1` | highest transaction version the Geyser mapper accepts; a block above it stops the indexer |
| `BACKFILL_FROM` | unset | RFC 3339 instant; the reconciler's floor: history from that time up to the lowest indexed range is filled |
| `ARCHIVE_RPC_URL` | unset | Old Faithful `faithful-cli rpc` endpoint (`archive/run.sh`); holes at least a week old (by block time) that it serves are filled from it instead of `RPC_URL` |
| `ARCHIVE_REQUESTS_PER_SECOND_MAX` | `20` | client-side limit for the archive; minimum 2. Latency, not this, paces it: 12 blocks in flight |
| `LOG_FORMAT` | `json` | `json` or `pretty` |
| `RUST_LOG` | `info` | tracing filter |

## Layout

```
core/            dlmm_core: indexer binary (decode, enrich, projection, store, gateway, actor)
  tests/fixtures/  real mainnet getTransaction JSON
api/             dlmm_api: Axum API binary
cli/             metclanker (Bun + TypeScript)
migrations/      0001_schema.sql (TimescaleDB, SQL domains, projections)
docker/db-init/  creates the api_reader role
archive/         Old Faithful epoch configs and run.sh for the deep-history archive
.agents/skills/  agent skills, including metclanker
docs/plans/      system design and implementation plan
```
