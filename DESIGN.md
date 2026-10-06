# Design

Program `LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo`.
Full reasoning: `docs/plans/2026-10-01-system-design.md`.

## 1. Data source

- DLMM often averages far more than 10 transactions per slot, so `getTransaction` per swap is expensive.
- Ingestion is by finalized blocks. This means gap detection is simple via `parent_slot`.
- Checkpointing is done via a virtual `cursor`

Live:
1. Yellowstone gRPC (primary), with a `blocks` filter on `finalized`. Without `GEYSER_URL`, the indexer uses the RPC tail.
2. RPC polling: `getBlocks(cursor+1, cursor+max(64,x/0.2))` and `getBlock` every x seconds. `getSlot(finalized)` seeds the cursor and measures lag. A tail far behind jumps to the tip instead of trying to catch up.

Historical:
1. A backfill indexes from a historical block to a cached live block.
2. Gaps or backfills older than 1 week go to the Old Faithful archive to reduce RPC cost.

Ingestion is source-agnostic - every source (Geyser stream, RPC tail, archive filler) produces the same `FinalizedBlock`. The design is event-driven and kappa-style: one stream-processing path, and reprocessing replays blocks through it.

## 2. Redundancy

`slot_coverage` are contiguous indexed slot ranges. A reconciler loop computes the difference to the desired state (of no gaps). It opens one `slot_range_job` per gap, nearest the tip first.

The loop is level-triggered. It reads the state every tick and does not react to gap events. Thus a crash, a dropped stream message, a tail jump and a cancelled backfill are all the same. A missed event can never become a lost gap. Every block write covers `(parent_slot, slot]` in the same transaction as its swaps.

A backfill is a `POST /v1/backfills` request. The API inserts the job, and the reconciler splits it at the week line. A job is complete only when coverage contains its range. A block that the node lost sets `blocked_reason`.

Live and fill draw from separate shares of the request budget (60/40). A GCRA limiter meters both. Backoff adds jitter and obeys `Retry-After`. One instance runs under `pg_try_advisory_lock`.

When the stream is silent for 30 s, the RPC tail starts from the cursor. It stops when the stream delivers again.

## 3. Database and schema

Database: TimescaleDB to store time-series data. Relying on hypertable chunks, `NUMERIC` and unique-key idempotency.
QL Domains: `solana_address`, `solana_signature`, `u64` (`NUMERIC(20,0)`)
Schema:
- `swap`: append-only event store (1-day chunks). It holds the `Swap` event, the `Swap2Evt` fee split, the quote leg, `volume_usd` and `source` (`live_geyser`, `live_rpc`, `fill`)
- `pool`, `token`: created on first sight. `price`: `(asset, ts, source)`
- `slot_coverage`, `slot_range_job`, `decode_failure` (one row per `(signature, reason)`)
- `pool_volume_1h`, `pool_volume_1d`: per `(pool, bucket)` counts and volumes
- `pool_stats`: lifetime totals per pool. `projection`: state per projection

Strictly no ORM used. The domain schema (Rust) and data schema (SQL) are distinct. `convert.rs` maps them once at the boundary to prevent object-relational impedance mismatch. Rust uses newtypes, sum types and a typestate while SQL uses domains and constraints.

The design uses revent sourcing. The `swap` table is the core log of on-chain swap events. `volume_usd` and `price_ts` are late-bound stream enrichment.

`pool_volume_1h`, `pool_volume_1d` and `pool_stats` are projections: materialised read models.

## 4. Idempotency

Delivery is at least once from every source. The writes make it effectively once. The natural key `(signature, swap_ordinal)` is the identity. The ordinal is the swap's position among DLMM swaps in execution order.

Inserts are `ON CONFLICT DO NOTHING RETURNING`. The writer folds only the returned rows into projections. Thus a replayed block is a no-op at every layer. Coverage merge and job progress (`next_slot` by `GREATEST`) are monotone. The cursor derives from coverage and is not stored. The filler verifies the parent chain. The design assumes exactly-once only of the effect, never of the transport.

## 5. Decoding

A DLMM swap is any DLMM instruction, top-level or inner, with one of six discriminators (`swap`, `swap2`, `swap_exact_out`, `swap_exact_out2`, `swap_with_price_impact`, `swap_with_price_impact2`). All six share one account layout (0 pool, 6 and 7 mints, 10 user). This is the same even for direct swaps and for routers.

DLMM emits events by `emit_cpi!`: a self-CPI whose data is sha256("anchor:event")[:8], the event discriminator, then Borsh. A swap's events are the self-CPIs at height `h + 1` after it, up to the next instruction at `<= h`. `Swap` gives amounts and fees. `Swap2Evt` gives the fee split. When the two disagree, the decoder marks an `EventMismatch`. `fee = mm_fee + protocol_fee + limit_order_fee + host_fee` must hold.

The indexer drops failed transactions before decoding. A fixture proves why: a failed route still carries an executed `Swap` event. Malformed input never panics. It goes to `decode_failure`, a dead-letter table. The indexer keeps the failures for a redecode and does not consume them.

## 6. API design

The services follow Command Query Responsibility Segration (CQRS). The indexer is the command side and owns every write. The API is the query side over denormalised projections. CQRS separates models and responsibilities, allowing both the indexer and API to scale differently.

`GET /v1/pools/{pool}/volume?bucket=hour|day&from&to` takes RFC 3339 times with any offset and converts them to UTC. The API floors `from`, ceils `to` and echoes both. Buckets are `[start, end)`, labelled by start, at most 744 hourly or 366 daily. Empty buckets appear with `volume_usd: null`. Amounts are strings in raw and human units. Health reports `starting`, `lagging`, `blocked`, `backfilling` or `ok`, and never answers 503 on lag. Errors: `{error: {code, message}}`. `api/openapi.json` (served at `GET /openapi.json`) lists every route. The query routes are read-only by construction (SELECT only). The DSN decides the database user.

## 7. Pricing

The indexer prices a swap's quote leg in USD only through SOL, USDC and USDT. These three assets miss about <3% of swaps, which stay unpriced for now.

Prices are Binance 1m close prices. A swap takes the latest price at or before its block time, within one minute. Otherwise it stays unpriced until the feed's sweep fills it. A future version should use Birdeye (which requires a key) or a path search over DLMM pools to one of the majors.

## 8. Scaling

The in-process version is an actor system. A supervisor, sources, filler, processor and price feed exchange `FinalizedBlock` messages over bounded channels, so backpressure holds by construction. The queue version replaces the channels with topics partitioned by slot. The writer becomes a consumer group, and the block transaction fences its offsets. The model does not change, because identity, coverage and commutative projections already make every stage idempotent. `SCALE.md` has the measurements.

| choke point | this design | at scale |
|---|---|---|
| ingress: 1 to 4 MB decoded per block, 80 to 180 KB on the wire with zstd, at <5 blocks/s (0.2 s slots). Free tiers answer 429 | Geyser primary (one message per block, no polling), RPC tail as fallback, two-lane GCRA limiter | Geyser producer writes finalized blocks to a Kafka/Redpanda topic keyed by slot. The topic is the buffer, the replay and the fan-out |
| decode CPU | pure, allocation-light, per block | stateless decoder consumers scale horizontally. Safe because identity is `(signature, swap_ordinal)` |
| the single writer: one transaction per block, about 7 round trips, 3 ms | 80x headroom at 400 ms slots | writers as a consumer group partitioned by slot hash. One transaction per block with its coverage, offsets fenced in that transaction. Measured 1.66x with two writers and 2.55x with four (`SCALE.md`) |
| hot projection rows | one upsert per (pool, hour) per block, HOT updates | projections as consumers with their own cursors (the `projection` table already holds them), deltas batched per N blocks |
| price lookups | `price_at`: latest eligible row in `(t − 60 s, t]` by index scan on immutable rows. Partial index for the sweep | the price feed as its own topic consumer |
| the event log and reads | hypertable chunks, covering indexes, SELECT-only reads | compression after 7 days, read replicas, ClickHouse (`ReplacingMergeTree` plus `AggregatingMergeTree`) for analytics |
| gap-fill backlog: 9,000 `getBlock` per hour of gap. DLMM is ~70 of each block's transactions. Blocks thus move ~15x the bytes needed, but cost ~50x fewer metered calls than per-transaction fetching | fill lane, parent-chain verification, blocked jobs | deep replay (LaserStream 48 h). Signature paging plus `getTransaction` where bandwidth is metered rather than calls. Old Faithful for bulk history |

## 9. Next steps

- One-hop pool pricing for the unpriced (about 2 percent).
- Online projection rebuild.
- A coverage-aware compare against Meteora's Data API with drift alerts.
- A redecode command for `decode_failure`, and metrics.
- Future scaling
