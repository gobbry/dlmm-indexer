# Scale

## Purpose

Today the indexer runs as one process, and one task reads, maps, decodes and writes each block. This document gives the measurements that show where that process binds, and the rule that splits a stage into its own consumer. It also gives the message-queue design that the rule leads to.

## What the measurements say

The sample is 300 consecutive finalized blocks from Triton's Geyser stream, on a release build on an Apple M4 Pro.

| stage | p50 per block | fills one core at |
|---|---|---|
| zstd and prost decode | ~80 µs + 501 µs | ~1,900 blocks/s |
| `map_geyser_block` | 150 µs | ~6,400 blocks/s |
| `decode_block` | 1.96 µs | ~495,000 blocks/s |
| `enrich` (warm caches) | 0.21 µs | ~3.8M blocks/s |
| write (one transaction per block) | 2,219 µs | ~435 blocks/s |

- **Chain rate.** The chain makes 3.7 to 3.8 blocks/s. At that rate the whole CPU path uses about 0.28 % of one core.
- **Writer.** The writer is busy about 0.85 % of its time. It waits on database round trips, so Postgres is not the limit.
- **Fetch.** A provider `getBlock` takes 342 ms at p50. Geyser pushes each block, so live ingest moves at the chain rate.
- **Archive.** On the idle archive with loaded epochs, 119 of 120 `getBlock` calls succeeded. The p50 was 27 s at 4 and at 12 in flight, so the rate was 0.42 blocks/s at 12 in flight. The archive is slow but reliable, and backfill from it is bound by fetch.
- **Parallel decode.** Map and decode run 4.9 times faster on 8 threads, but that stage uses 0.06 % of one core.
- **Writer scaling.** Writers on disjoint slot ranges give 1.66 times the rate at two writers and 2.55 times at four, with zero retries.

## The rule for a separate consumer

A stage becomes its own consumer when at least one condition is true.

- **Capacity.** The stage's p95 latency multiplied by the target block rate is more than its share of one core.
- **Isolation.** A slow period or a failure in the stage must not stop the other stages.
- **Fan-out.** More than one consumer needs the stage's output, for example analytics or a ClickHouse sink.
- **Replay.** The system must derive the stage's output again without the database.

Today no stage meets a condition. Live ingest waits on the chain and backfill waits on fetch, so a split of fetch from decode changes no bound. The writer is first in line, because one writer fills its time at about 400 blocks/s. The fix is concurrent writers, and they need a queue only when a second condition also applies.

When the load changes, measure again.

1. Collect N consecutive finalized blocks from the live stream and save the raw messages.
2. Time each stage alone on the saved blocks. Use 7 passes and take the median of each percentile.
3. Run the pure stages (map, decode, enrich) on 1, 2, 4 and 8 threads.
4. Run 1, 2 and 4 writers on disjoint slot ranges and record blocks/s, p95 and retries.

## The queue design

```text
 ingest (one instance, advisory lock)          decoder group (N)         writer group (up to the partition count)
 +----------------------------+  blocks.finalized  +-----------------+  swaps.decoded  +--------------------------+
 | Geyser source              | -----------------> | decode_block    | --------------> | one transaction per msg  |
 | RPC tail (fallback)        |  key = slot        | enrich          |  key = slot     |   swap rows (unique key) |
 | filler: provider, archive  |  value =           | decimals cache  |  value =        |   pool and token upserts |
 | reconciler                 |  FinalizedBlock    | stateless       |  DecodedBlock   |   cover(parent, slot]    |
 | cursor = top of coverage   |                    +-----------------+                 |   projection deltas      |
 +----------------------------+                                                        |   consumer_offset row    |
                                                                                       +-----------+--------------+
 price service (one instance): Binance prices, reprice sweep                                       |
 api: reads and backfill requests, no change                                        swaps.deadletter
```

- **Partition by slot.** The swaps of a block and its coverage must commit together. A key by slot gives one writer per block, and a duplicate block arrives on the same partition as its original. A key by pool spreads one block over several writers.
- **"Raw" is the filtered `FinalizedBlock`.** The mapper is the only code that knows both Geyser protobuf and RPC JSON. The topic carries the mapped block, so the topic has one message format.
- **The offset fence.** The writer stores `consumer_offset(topic, partition, offset)` in the block transaction. At start or at a rebalance, it seeks to the stored offset plus one, so a redelivered message is a no-op. A block that an ingestor sends again inserts nothing, because of the unique key.
- **Projections stay in the writer first.** Projection deltas are not idempotent. The writer folds deltas only from the rows that `RETURNING` inserted, in the same transaction. An outbox and a `swaps.inserted` topic can move projections to their own consumers later.
- **The database stays the system of record.** Postgres keeps swaps, coverage, jobs, prices and projections. Redpanda holds transport, buffers, fan-out and replay.
- **Broker or database down.** Ingestors buffer a bounded number of blocks and then stop, and the reconciler fills the holes. When the database stops, writers stop and lag grows, with no loss while the 7-day retention holds. The queue adds no new way to lose data.
- **Poison message or writer crash.** A poison message goes to `swaps.deadletter`, and its slot stays uncovered, so the hole stays visible. A crash rolls back the transaction, the fence does not move, and the broker delivers the message again.
- **Health.** `/v1/health` adds the lag of each consumer group and `deadletter_count`. `lagging` still compares coverage with the current time.

## Phases and decisions

| phase | content | effort |
|---|---|---|
| 2a | topics, producers, the decoder group, the writer group with the offset fence, lag in health | 5 days |
| 2b | dead letter, rebalance tests, a chaos test (kill a writer in a block, stop the broker for 2 minutes) | 2 days |
| 3 | `swaps.inserted` through an outbox, projections as consumers, a ClickHouse sink | 3 days |

Four decisions come before the build.

1. Keep projections in the writer in phase 2 (recommended), or make them consumers with an outbox from the start.
2. Accept `rdkafka` as a dependency (a C library and a longer build), or wait for a pure-Rust client with consumer groups.
3. Set the retention of `blocks.finalized` to 7 days, and decide on tiered storage.
4. Use one mode after the switch (recommended), or keep the in-process path behind a flag. A flag gives two code paths to test.

## What does not change

- **One decoder.** `decode_block` and `enrich` stay pure functions, and a decoder consumer calls the same code.
- **One identity.** `(signature, swap_ordinal)` identifies a swap in every topic and every table.
- **Coverage is the completeness fact.** A slot is complete when `slot_coverage` covers it. Offsets only say how far a consumer read.

The choke-point table in [DESIGN.md §8](DESIGN.md#8-scaling) stays the map of where the system binds.
