# Meteora DLMM swap indexer

This system indexes every swap of Meteora's DLMM program on Solana mainnet. It stores each swap exactly once in TimescaleDB and serves hourly and daily volume per pool, in tokens and in USD, over a REST API. 

The system has three parts.
- **core** is the Rust `indexer`
- **api** is a Rust/Axum REST API
- **cli** is `metclanker`, a Bun CLI that is also an agent skill.

The indexed program is `LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo`.

## Prerequisites

- Docker with Compose v2
- [Bun](https://bun.sh) for the CLI.
- A Solana RPC URL. A public endpoint will lag as the RPS is insufficient.
- A Yellowstone gRPC endpoint and token (Triton is used)
- Old Faithful archive for history older than one week. Compose runs it for you (see [archive/README.md](archive/README.md)).

## Configure

```bash
cp .env.example .env
```

Then edit `.env`. These variables matter:

| variable | what it does |
|---|---|
| `DB_DSN` | Database DSN for the indexer and the API. The default is correct. Compose sets it inside the containers. |
| `RPC_URL` | Solana mainnet RPC endpoint. The indexer requires it. The API uses it as well for some endpoints. |
| `RPC_RPS_MAX` | Client-side request limit based on RPC tier limits. |
| `TRANSACTION_VERSION_MAX` | Highest transaction version that the indexer accepts (default `1`). |
| `GEYSER_URL`, `GEYSER_X_TOKEN` | Yellowstone gRPC endpoint and its `x-token`. When `GEYSER_URL` is set, Geyser is the primary live source and fallbacks to RPC. |
| `BINANCE_DATA_API_URL` | Binance market-data host (default `https://data-api.binance.vision`). |
| `ARCHIVE_RPC_URL` | Old Faithful endpoint. When it is set, the archive lane is on and compose points the indexer at its `archive` service. Empty sends all historical requests to `RPC_URL`. |
| `ARCHIVE_RPS_MAX` | Client-side request limit for the archive (default `20`). |
| `LOG_FORMAT` | `json` or `pretty`. |
| `RUST_LOG` | Tracing filter (default `info`). |

## Start

```bash
docker compose up -d --build
docker compose ps
```

Compose starts four services: `db`, `archive`, `indexer` and `api`. Then check health:

```bash
curl localhost:8080/v1/health
```

```json
{"status":"ok","cursor_slot":452758149,"last_block_time":"2026-10-02T23:28:50Z","lag_seconds":24,"open_job_count":1,"blocked_job_count":0}
```

List the pools with the CLI:

```bash
cd cli && bun install
bun run metclanker pools --limit 10
```

Start backfilling:

```bash
# provider backfilling
bun run metclanker backfill --from "$(date -u -v-1H +%Y-%m-%dT%H:%M:%SZ)" 
#archive backfilling
bun run metclanker backfill --from "$(date -u -v-7d -v-10M +%Y-%m-%dT%H:%M:%SZ)"
bun run metclanker backfill --list
```

For a terminal, run `bun run metclanker --interactive`.

## How it works

**Live blocks.** The indexer reads finalized blocks only. With `GEYSER_URL`, a Yellowstone `blocks` stream filtered to the DLMM program is the live source. If the stream fails or stays silent, the RPC tail (`getBlocks` and `getBlock`) continues from the same cursor. When the stream recovers, the tail stops. Without `GEYSER_URL`, the RPC tail is the only live source.

**One writer per block.** Each block goes through one decoder and one writer. The writer commits each block in one database transaction. That transaction holds the block's swaps, its slot coverage and the projection updates. The unique swap key makes a replay of a block change nothing.

**Coverage and the reconciler.** Each block write records the slot range that the block covers. The reconciler finds the holes between these ranges and opens one `slot_range_job` for each hole. A filler walks each job with the same `getBlock` call and the same decoder. A job is complete only when coverage contains its full range. A block that the node cannot supply blocks the job with a reason, and health reports `blocked`.

**Backfill.** Ask for history with `POST /v1/backfills` or `metclanker backfill --from <rfc3339>`. The API finds the first slot at or after that instant and inserts a job up to the indexed range. The reconciler splits a request older than one week at the week line. The archive fills the older part and the RPC provider fills the newer part. `backfill --list` shows the jobs, and `backfill --cancel <job_id>` cancels one.

**Pricing.** The price feed stores Binance 1-minute close prices for SOL, USDC and USDT. A swap gets its USD value from its quote leg in one of these assets. A pool with no such quote asset stays unpriced, and the API counts its unpriced swaps.

**Projections.** The indexer keeps three projections of the swap log: hourly volume per pool, daily volume per pool, and per-pool stats.

**API.** The API does not change data, except for backfill requests and cancels. The full contract is `api/openapi.json`, and a running instance also serves it at `GET /openapi.json`.

**CLI.** `metclanker` has the subcommands `health`, `pools`, `volume`, `swaps` and `backfill`. Agents pass `--output json --no-input` and get one JSON envelope on stdout. Humans use `--interactive`. `volume --compare` puts Meteora's own Data API numbers next to each bucket.

## Check

These two queries must return 0. The first query checks that the database holds no swap twice. The second query checks that the `Swap2Evt` fee split adds up to the `Swap` event's fee.

```bash
docker compose exec db psql -U postgres -d dlmm -c \
  "select signature, swap_ordinal, block_time, count(*) from swap group by 1,2,3 having count(*) > 1"
docker compose exec db psql -U postgres -d dlmm -c \
  "select count(*) from swap where mm_fee is not null and fee <> mm_fee + protocol_fee + limit_order_fee + host_fee"
```

Run the tests:

```sh
docker compose up -d db
set -a; source .env; set +a
# #[sqlx::test] reads DATABASE_URL by name, so hand it the DSN under that name
DATABASE_URL="$DB_DSN" cargo test --workspace   # decoder fixtures, idempotency and reprice against the db, API smoke
ARCHIVE_RPC_URL=http://127.0.0.1:8899 cargo test -p dlmm_core --test archive -- --include-ignored   # the compose archive service answers on this port
cargo clippy --all-targets -- -D warnings && cargo fmt --check
cd cli && bun test
```

The decoder fixtures are real mainnet `getTransaction` results in `core/tests/fixtures/`. The archive test runs only when `ARCHIVE_RPC_URL` is set.

## Read more

- [DESIGN.md](DESIGN.md): the two-page design (data source, redundancy, schema, idempotency, decoding, API, pricing, scaling, next steps).
- [docs/plans/2026-10-01-system-design.md](docs/plans/2026-10-01-system-design.md): the full reasoning. Ingestion and the reconciler are in §2. Decoding is in §3 and pricing in §5. The data schema is in §6 and the API in §8. Operations, testing and scaling are in §10, §11 and §12.
- [SCALE.md](SCALE.md): choke points, the message queue and the scaling plan.
- [archive/README.md](archive/README.md): the Old Faithful archive, the epochs it serves and how to add one.
- [.agents/skills/metclanker/SKILL.md](.agents/skills/metclanker/SKILL.md): every CLI command, flag and JSON field.
- [AGENTS.md](AGENTS.md): engineering conventions for this repository.
- Code: `core/` (indexer), `api/` (API), `cli/` (metclanker), `migrations/` (schema).
