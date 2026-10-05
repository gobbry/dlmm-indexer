# Design

Program `LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo`.
Full reasoning: `docs/plans/2026-10-01-system-design.md`.

## 1. Data source

For efficiency, protocol often average far more than 10 DLMM transactions per slot, making it expensive to index via `getTransaction`. All indexing is done by decoding blocks instead.
For simplicity, ingestion is by finalized block: this makes it easy to detect gaps via a block's `parent_slot`.
For checkpointing, store a cursor and query `from_slot = cursor + 1`. DB writes and event processing should always be idempotent by default as indexing may duplicate.

Realtime:
> Goal is to continue indexing in realtime despite gaps or historical backfilling required
1. Yellowstone gRPC (Primary), with `blocks` filter on `finalized`. Uses an alternative polling when `GEYSER_ENDPOINT` is not set.
2. Alternative via RPC polling every x seconds, with `getBlocks(cursor+1,cursor+max(64,x/0.2))` and `getBlock`. `getSlot(finalized)` is used to seed cursor at start or to measure indexing lag. Notably, indexing lag can occur in which it snapshots back to the latest finalized slot instead of trying to catch up, relying on historical data source to fill gaps.

Historical (or gaps found):
> Goal is to fill gaps and then backfill historically
1. Gaps are found by reconciliation when a block's `parent_slot` does not match the predecessor.
2. Backfilling is done via indexing from a block at a historical point of time to a cached live block. Job is not completed until it has fully indexed till the cached live block.
3. Gaps or backfills older than 1 week ago go to the Old Faithful archive to reduce RPC cost.

## 2. Redundancy

Completeness is a fact in the database, not an event an actor must notice. `slot_coverage` holds the contiguous slot ranges fully indexed; every block write, live or fill, covers `(parent_slot, slot]` in the same transaction as its swaps, so skipped slots are covered by the chain's own word and a crash anywhere leaves coverage exact.

A reconciler tick derives the holes between `BACKFILL_FROM` (or the first live slot) and the top range and opens one `slot_range_job` per hole that has none; an exclusion constraint forbids overlapping jobs. A Geyser drop, a tail more than 150 slots behind jumping to the tip, a restart and a historical backfill are all just holes, nearest the tip first.

A job is complete only when coverage contains its range; a block the node has lost marks it `blocked_reason`, never a silent completion. Live and fill draw from separate shares of the request budget (60/40), under a GCRA limiter and jittered backoff honouring `Retry-After`. One instance runs under `pg_try_advisory_lock`; the unique swap key would make a second writer safe but wasteful.

When the stream disconnects or is silent for 30 s, the RPC tail takes over from the cursor and stops once the stream delivers again; both write through the same cursor, so the handover is a cursor read, not a reconciliation.

## 3. Database and schema

Database: TimescaleDB with exact `NUMERIC`, unique-key idempotency and hypertable chunks.
SQL Domains: `solana_address`, `solana_signature`, `u64` (`NUMERIC(20,0)`)
Schema:
- `swap`: append-only event log (1-day chunks) containing `Swap` event, `Swap2Evt` fee split, the quote leg, `volume_usd` and `source` (`live_geyser`, `live_rpc`, `fill`)
- `pool` and `token`: created upon detection of pool or token
- `price`: `(asset, ts, source)` for prices of assets 

- `slot_coverage`: contiguous covered slot ranges (to prevent re-indexing), the latest end (top of `slot_coverage`) is the cursor
- `decode_failure`: one row per `(signature, reason)` as a dead letter and debugging for transactions the decoder fails
- `slot_range_job`: queue for gaps and backfilling

- `pool_volume_1h` and `pool_volume_1d`: `(pool, bucket)`, swap count, volume in each
  token and in USD, unpriced count
- `pool_stats`: per pool, lifetime totals and first and last swap time.
- `projection`: one row per projection with its version, log cursor and state
  (`live` or `building`)

## 4. Idempotency

A swap is `(signature, swap_ordinal)` but unique key is `(signature, swap_ordinal, block_time)`: the ordinal is its position among DLMM swap instructions in execution order, deterministic for a given transaction. `block_time` is required because a hypertable key must include the partition column.

Inserts are `ON CONFLICT DO NOTHING RETURNING`, so a replayed block projects nothing. Job progress (next_slot) moves with `GREATEST` and the cursor is the top of `slot_coverage`

## 5. Decoding

A DLMM swap is any instruction, top-level or inner, whose program is DLMM and whose first
eight bytes are one of six discriminators (`swap`, `swap2`, `swap_exact_out`,
`swap_exact_out2`, `swap_with_price_impact`, `swap_with_price_impact2`). All six instructions share the same account layout (0 as pool, 6 & 7 as mints, 10 as user) regardless of direct swaps or external routers.

DLMM emits events by `emit_cpi!`: a self-CPI whose data is sha256("anchor:event")[:8], the event discriminator, then Borsh. A swap's events are the self-CPIs at height `h + 1` after it, up to the next instruction at `<= h`.
- `Swap` gives amounts and fees;
- `Swap2Evt` gives the fee split (`mm_fee`, `limit_order_fee`, `amount_left`, `fee_side`, `fee_token`)
- Disagreements can happen with `Swap`, marking it as an `EventMismatch`, and `fee = mm_fee + protocol_fee + limit_order_fee + host_fee` must hold.

Failed transactions are dropped before decoding; a fixture proves why (a failed route still carries an executed `Swap` event). Malformed input does not panic and is always persisted as a `Result`in `decode_failure`.

## 6. API design

`GET /v1/pools/{pool}/volume?bucket=hour|day&from&to` (RFC 3339, any offset, converted to
UTC); `from` floored and `to` ceiled to the bucket and echoed; buckets `[start, end)`
labelled by start; caps 744 hourly or 366 daily. Every bucket appears; empty ones have
`volume_usd: null`. Amounts are strings in raw and human units (scaled by
`token.decimals`); the response carries mints, decimals and `first_swap_at`. Also
`/v1/health` (`starting`, `lagging`, `blocked`, `backfilling` or `ok`; never 503 on lag),
`/v1/pools`, `/v1/pools/{pool}/swaps`. Errors: `{error: {code, message}}`. The API is
read-only (`api_reader`).

## 7. Pricing

Only SOL, USDC and USDT is currently used to price the swap's quote leg in USD. However, there exists cases where these 3 do not sufficiently cover them (<3%). In such cases, simply do not price them yet.

For binance as source, prices are Binance 1m close prices. A swap can take the latest price at or before its block time, within a minute, from the configured source, or stays unpriced until the feed's sweep fills it.

In future, using Birdeye (which requires a key) or a path-finding algo using DLMM pools such that it routes to one of the majors should be implemented.

## 8. Scaling

Choke points, where each sits here, and what replaces it:

| choke point | this design | at scale |
|---|---|---|
| ingress: 8 MB per block over RPC at <5 blocks/s (0.2 s slots); free tiers answer 429 | Geyser primary (one message per block, no polling), RPC tail as fallback, two-lane GCRA limiter | Geyser producer writes finalized blocks to a Kafka/Redpanda topic keyed by slot; the topic is the buffer, the replay and the fan-out |
| decode CPU | pure, allocation-light, per block | stateless decoder consumers scaled horizontally; safe because identity is `(signature, swap_ordinal)` |
| the single writer: one transaction per block, about 7 round trips, 3 ms | 80x headroom at 400 ms slots | writers partitioned by pool over the same unique key; a block fans out by pool; the cursor becomes per-partition offsets |
| hot projection rows | one upsert per (pool, hour) per block, HOT updates | projections as consumers with their own cursors (the `projection` table already holds them), deltas batched per N blocks |
| price lookups | `price_at`: latest eligible row in `(t − 60 s, t]` by index scan on immutable rows; partial index for the sweep | the price feed as its own topic consumer |
| the event log and reads | hypertable chunks, covering indexes, read-only role | compression after 7 days, read replicas, ClickHouse (`ReplacingMergeTree` plus `AggregatingMergeTree`) for analytics |
| gap-fill backlog: 9,000 `getBlock` per hour of gap; DLMM is ~70 of each block's transactions, so blocks move ~15x the bytes needed but cost ~50x fewer metered calls than per-transaction fetching | fill lane, parent-chain verification, blocked jobs | deep replay (LaserStream 48 h); signature paging plus `getTransaction` where bandwidth is metered rather than calls; Old Faithful for bulk history |

The queue version is a refactor: components already talk in `FinalizedBlock` and
`(signature, swap_ordinal)`, and exactly-once stays the cursor plus the unique key.

## 9. Next steps

Not done: Geyser verified against a live provider (tested only against an in-process
server, for want of a credential); one-hop pool pricing for the unquoted 2 percent; online
projection rebuild (correct because projections commute); a coverage-aware compare against
Meteora's Data API with drift alerts. Then a redecode command for `decode_failure`, and
metrics.
