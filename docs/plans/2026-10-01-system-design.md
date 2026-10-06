# System design: Meteora DLMM swap indexer

Status: revised draft for the author's review, 2026-10-01. Amended 2026-10-03 (gateways,
rate limiting, event-sourced projections). This document is a design only. It plans no
code. Facts were verified against primary sources on 2026-10-01, except facts marked
*likely* or *to confirm*. The draft went through three adversarial reviews (correctness,
facts, simplicity). The draft includes the accepted findings, and §15 lists the rejected ones.

## 1. System overview

Three deployables, one database, one direction of data flow.

```
   Yellowstone gRPC (finalized blocks)      Solana RPC (getBlocks/getBlock)     Binance 1m klines
                │                                        │                             │
                ▼                                        ▼                             ▼
   ┌────────────────────────────── CORE (indexer binary) ──────────────────────────────────┐
   │  GeyserSource ──on_block(Live)──▶                                                      │
   │                                   BlockProcessor ──writes──▶ TimescaleDB              │
   │  RangeFiller ──on_block(Fill)──▶  (sole writer of swap tables)      ▲                  │
   │   ▲  reads slot_range_job ◀───── job rows written here ─────────────┘                  │
   │   └─ also the LIVE source when no Geyser endpoint is configured (RPC tail)             │
   │  PriceFeed ──writes price──▶ TimescaleDB; ──on_prices_filled──▶ BlockProcessor         │
   └────────────────────────────────────────────────────────────────────────────────────────┘
                                                      │ reads only (SELECT)        
                                                      ▼
                                   ┌──────────── API (axum binary) ────────────┐
                                   │ /v1/health /v1/pools                      │
                                   │ /v1/pools/{p}/volume /v1/pools/{p}/swaps  │
                                   └───────────────────────────────────────────┘
                                                      │ HTTP
                                                      ▼
                                   ┌──────────── CLI (bun, metclanker) ────────┐
                                   │ interactive demo, --output json proof,    │
                                   │ --compare against Meteora's Data API,     │
                                   │ doubles as .agents/skills/metclanker      │
                                   └───────────────────────────────────────────┘
```
**CQRS split.** CORE owns every write to swap data. API connects as a read-only database
role and reads the hourly aggregate. The compiler also enforces the split. The write path is
`pub(crate)` inside the CORE crate, and the read queries are `pub`. The API crate depends on
CORE for domain types and read queries only.

**Consistency boundary.** CORE indexes only finalized blocks, so there is no reorg handling.
The unit of work is one finalized block. One database transaction inserts the block's swaps
and advances the checkpoint. A unique key makes duplicates impossible by construction, not
by discipline.

**Event-driven, actor model.** Four tokio tasks exchange typed messages over bounded
channels. Every handler is an `on_*` function over the actor's own state. There is no shared
mutable state and there are no locks. An event store is out of scope. The post-commit log
line `block_indexed` carries slot, swap count and signatures. A later producer would be
called at that line. No trait is reserved for it.

**Program ID.** The task PDF prints `LBUZ…Pd8ZqK3m`. That ID is a typo with no account on
mainnet. The author confirmed the correct program with the task's creator on 2026-10-04. The
real DLMM program is `LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo`. `getAccountInfo`
verified it (executable, owner BPFLoaderUpgradeable), and Meteora's developer guide confirms
it. The write-up records this as a verification step.

## 2. Ingestion

### 2.1 Live source: Yellowstone `blocks` filter (primary)

CORE subscribes once with `commitment = FINALIZED` and one `blocks` filter `{ account_include:
[DLMM_PROGRAM_ID], include_transactions: true, include_accounts: false, include_entries:
false }`. Once a cursor exists, every (re)connect sets `from_slot = cursor_slot + 1`.

Verified semantics (geyser.proto, filter.rs, grpc.rs at master):

- The server sends exactly one `SubscribeUpdateBlock` per finalized block per filter. It
  carries `slot`, `parent_slot`, `block_time`, `blockhash` and the transactions that touch
  the program. Each transaction comes with full meta (err, inner instructions, loaded
  addresses, logs). A block with zero matching transactions still arrives, with an empty
  list. So CORE observes every finalized block, and the cursor advances through quiet periods.
- The block message comes after all of its content and before the slot status. So a block
  is complete when it arrives. Finalized blocks are never corrected.
- The server does no chunking. The client raises tonic's `max_decoding_message_size`
  (default 4 MiB, and the upstream example uses 1 GiB).
- The server pings every 10 s. The client answers with a `SubscribeRequest { ping }`.
  Pings prove only the connection, not the feed. So the stall timer runs on blocks alone,
  because every finalized block arrives, empty ones included. 30 s without a block is a
  stall, even while pings continue to arrive.
- `from_slot` replays from an in-memory ring of about 100 to 150 slots on most providers.
  For `blocks` filters, replay works only on server builds after 2026-07-08. A `from_slot`
  too far back returns `out_of_range`. The client crate's reconnect helper is
  processed-only, so CORE owns its reconnect loop. `out_of_range` is the *expected* outcome
  after any outage longer than a minute. The coverage reconciler (§2.5) handles it. Replay
  is a shortcut, not a dependency.

The `blocks` filter has no `failed` flag. CORE drops failed transactions when it normalises
the block.

Measured on mainnet (2026-10-06, Triton Dragon's Mouth): the server honours
`account_include`. Every transaction in a block message references DLMM. About half
reference it only through a lookup table, and the others in static keys. But a finalized
block carries 100 to 450 such transactions, and only 5 to 25 of them invoke DLMM. The rest is
bot traffic, and a third of it failed. Its pre/post token balances are 60% of the bytes. A
decoded block is 1 to 4 MiB. Uncompressed, the stream is up to 2.6 MB/block and about
850 GB/day. No filter field separates "references" from "invokes".
(`cuckoo_account_include` is the same match, compressed.) The client therefore sends
`grpc-accept-encoding: zstd` (tonic `accept_compressed`). Over 280 dense blocks, the wire carried
177 KB per block against 1.6 MB decoded. A quieter sample of 300 blocks two days later
carried 76 KB per block against 0.5 MB decoded. The ratio is 7x to 9x, so the stream is
30 to 60 GB/day. gzip measured 3.6x.

Rejected alternative, measured: `transactions { account_include: [DLMM], failed: false,
vote: false }` plus `blocks_meta`, assembled per slot. This alternative drops the failed
third. Over 256 finalized slots, every transaction update arrived before its slot's
`block_meta` (0 violations). Within a slot, updates arrive out of `index` order (791
inversions). So the assembly would be sound. But each transaction is its own message and
compresses alone. With zstd, a slot is 426 KB on the wire, against 177 KB for the compressed
block. So the alternative costs 2.4x the bandwidth and adds a per-slot buffer. The `blocks`
filter stays.

### 2.2 Live source: RPC tail (automatic fallback, and the grader's path)

The task's README must let a grader run `docker compose up` and see live swaps in under ten
minutes on a free RPC. The design cannot assume a Geyser credential. `RangeFiller` runs in
tail mode when `GEYSER_URL` is unset, or whenever the Geyser stream is disconnected or
stalled. Every 2 s, the tail lists `getBlocks(cursor + 1, cursor + window)`. The window is
`max(64, tick_seconds / 0.2)` slots, which is 64 at the 2 s tick. 0.2 s is faster than any
measured pace: mainnet slots ran 0.27 s in epoch 1045 (2026-10), against the protocol's
nominal 0.4 s. So the window outruns a faster chain. The tail calls `getBlock` only on the
listed slots, exactly as the filler walks a job page, and tags the blocks
`BlockOrigin::Live`. The node clamps the listing at its finalized root, so a caught-up tail
sees a short page and a skipped slot costs no call. The tail calls `getSlot(finalized)` only
on first boot, to find the tip, and when a listing returns full. A full listing includes
the window's last slot, so the tail may be far behind. Past 150 slots of lag (about 40 s),
the tail jumps to the tip. The stretch it jumps over is a coverage hole, and the reconciler
turns that hole into a job. There is one RPC fetch mechanism, `getBlocks` then `getBlock`,
for live fallback, gap fill and backfill (author, 2026-10-04). The tail uses the same
decoder, the same writer and the same cursor. Nothing else changes. Geyser is a core part
of the system, not an upgrade (author, 2026-10-04). When configured, Geyser is primary and
the tail is the fallback. The cursor is the handover in either direction. Coverage
reconciles the two sources. A block that one source never delivered leaves a hole in
`slot_coverage`, and the reconciler hands that hole to the RPC filler. Overlap dedupes on
the unique key. An optional reconciler also samples recent slots over RPC and compares swap
counts.

Cost on Helius free tier: 10 requests per second, 1M credits a month, and `getBlock` costs
one credit. At the measured 0.27 s slots, the tail makes about 3.7 `getBlock` calls per second
and moves roughly 30 MB/s before gzip. One constant, `SLOT_MILLISECONDS_OBSERVED = 270`,
gives the live-share minimum: 5 live requests a second, so `RPC_RPS_MAX` of 7 or more. This
load is sustainable for a demo. It exhausts the monthly credits in about 3 days of
continuous tailing. Tick-to-data latency is finalization (about 13 s) plus the poll
interval. This is the concrete "what a free tier can and cannot do" answer. Geyser eliminates
the poll and the per-block RPC cost.

### 2.3 Unit of work: `FinalizedBlock`

Both sources map into one type: `FinalizedBlock { slot, parent_slot, block_time,
transactions }`. Two mapping functions do this, each about fifty lines. One maps the
Yellowstone protobuf, and one maps `getBlock` / `getTransaction` JSON. The design vendors no
upstream conversion code. Mapping drops failed transactions (`meta.err` set) and vote
transactions. It flattens instructions into execution order with `stack_height`. It builds
the key table `static keys ++ loaded writable ++ loaded readonly`. CORE rejects and
refetches a block whose `block_time` is null, and never stores it. `block_time` is part of
the unique key and must agree across sources.

### 2.4 Checkpoint: `slot_coverage`

CORE derives completeness. No component signals it. `slot_coverage(start_slot, end_slot,
end_block_time)` holds the contiguous slot ranges fully indexed. An exclusion constraint
(`int8range(start_slot, end_slot, '[]') WITH &&`) keeps them disjoint. Every block write,
live or fill, covers `(parent_slot, slot]` inside the block's own transaction. One
statement, `cover`, does this. It deletes every range that overlaps or is adjacent to that
interval (`end_slot >= parent_slot AND start_slot <= slot + 1`) and inserts their union. The
union's `end_block_time` is the time of whichever block is the new end. A replayed block
rewrites the same row. Skipped slots need no handling. A block after a skipped slot names the
previous block as its parent, so the skipped slots fall inside its interval. A range always
ends on a block and starts right after a block's parent, so both ends of every hole are
blocks too.

The cursor is the top range: `ORDER BY end_slot DESC LIMIT 1`, its `end_slot` and
`end_block_time`. Geyser resumes with `from_slot = end_slot + 1`. The tail lists from there,
and health reports the cursor. No other checkpoint exists, so the three can never disagree.
A crash at any point leaves the table describing exactly what is indexed.

### 2.5 Reconciler and `RangeFiller`

`slot_range_job { id, start_slot, end_slot, next_slot, blocked_reason, completed_at }`
describes outstanding history. Jobs carry the same exclusion constraint as coverage, so two
jobs never claim one slot. Only the reconciler opens jobs, except a backfill job, which the
API inserts (below). Only the reconciler completes jobs. The reconciler is a 10 s tick
inside the `BlockProcessor` loop (`tokio::select!` over the tick and the two inboxes, tick
first). So the processor stays the single writer of swaps and coverage. One statement,
`reconcile(archive_window)`, does this work:

- Holes are the gaps between consecutive coverage ranges (a `lag` window over `start_slot`),
  and nothing else. There is no floor. So history below the lowest range is owed only once
  a backfill job for it exists (below).
- The statement cuts a hole that straddles an edge of the archive window before its bottom
  and after its top (below). So every job lies wholly on one side.
- A job with `completed_at IS NULL` (open or blocked) already owns any hole it overlaps,
  and the statement skips that hole. A blocked job's hole waits on the operator, not on a
  second job. An unmappable block is the one exception, handled where the job blocks. In
  the same transaction, the job is cut to end on that block, and the slots after it become
  a fresh job. So one bad block loses one slot, not the rest of the job.
- The statement inserts one job per remaining hole with `next_slot = start_slot`, nearest
  the tip first.
- Every uncompleted job whose `[start_slot, end_slot]` lies inside a single coverage range
  receives `completed_at = now()`, blocked ones included (another source filled it). The
  reconciler verifies completion against coverage. No writer claims it. The overlap and
  containment tests use the exclusion constraints' own `int8range` expressions (`&&`, `@>`),
  so they probe the GiST indexes. The remaining scans (the `lag` window, the top range for
  the cursor) read a table of a few rows while healthy.

The statement returns the counts it opened and completed, for the `coverage_reconciled` log
line. When it opened any job, it nudges the filler (`FillerMessage::OnJobsOpened`). The
reconciler catches holes from any cause the same way. Examples are a tail jump past 150
slots of lag, a block neither live source delivered, a restart, and a source that was down.

Backfill is a job the API inserts, not configuration. `POST /v1/backfills` with
`{"from": "<RFC 3339>"}` (§8) resolves the instant to the first slot at or after it that
holds a block. It bisects over `getBlocks` pages on the API's own `RpcGateway` (`RPC_URL`,
`RPC_RPS_MAX`). Then it inserts one job `[from_slot, lowest.start_slot - 1]` with
`next_slot = from_slot` and `end_kind = 'block'`. One `INSERT … SELECT` does this and reads
the lowest coverage range itself. The end is a block, because a range's start minus one is
the parent of its first block. From then on, it is an ordinary job. The filler walks it, its
blocks join coverage to the range above, and the reconciler completes it. The exclusion
constraint makes a second backfill over the same slots a 409, not a duplicate. The API
inserts the job uncut, because it does not read the archive window. The reconciler's next
tick cuts it.

That tick also cuts some open jobs, in the same statement and through the same cut CTE as a
hole. These are jobs with no committed block (`next_slot = start_slot`, not blocked) that
straddle `bottom - 1` or `top`. The job keeps its id and shrinks in place to its last piece,
which keeps the job's own `end_kind`. The statement inserts the other pieces as new jobs.
The insert reads the update's output, so the exclusion constraint sees the job shrunk first.
The piece ending at `bottom - 1` receives `archive_lower_cut`, and the piece ending at `top`
ends on a block. The reconciler logs the split as `backfill_job_split`. So the id that the
API answered with still reads and cancels the newest part of the request.
`GET /v1/backfills` lists the older pieces under their own ids. So the reconciler splits a
request older than a week at the week line, and its old part goes to the archive.

Until that tick, neither lane reads such a job, so the provider never starts the archive's
share. A job already walking keeps its lane, and its parent chain vouches for the rest. A
window that arrives or moves later splits an unstarted job the same way, on the first tick
that knows the window.

The filler reads the table as its work queue. At boot and every 10 s, it loads open jobs
(`completed_at IS NULL AND blocked_reason IS NULL AND next_slot <= end_slot`). It orders them
by `end_slot DESC`, nearest the tip first, up to 100 per pass. Each lane's route is part of
that query, before the limit. So a backlog on one lane never hides the other lane's jobs
(gauntlet, increment 3). The nudge only shortens the wait. A pass walks one page of every
open job. Passes repeat at once while pages remain, so a day-long backfill never delays a
fresh hole (gauntlet, increment 2). The filler creates no jobs. A job's end is a block: the
parent of the block that starts the range above, or the archive window's top, which is a
block. Its start may be a skipped slot, and the first-block rule below accepts that. So a
walk that exhausts its pages without fetching `end_slot` met a node that lacks it. The one
exception is a provider job cut just below the archive window's bottom. That job may end on
a skipped slot (below).

Per job, the filler loops in pages. `getBlocks(next_slot, min(end_slot, next_slot +
page_size_max - 1))` lists the slots that have blocks. Skipped slots are absent, and the
range max is 500,000. Then the filler calls `getBlock(slot, { encoding: json,
transactionDetails: full, rewards: false, maxSupportedTransactionVersion: 1 })` per listed
slot. It filters each block client-side to transactions whose key table contains the DLMM
program. It maps the block to `FinalizedBlock` and sends it as
`BlockOrigin::Fill { job_id, next_slot_after }`. `next_slot_after` is the next fetched
block's slot, or `end_slot + 1` once the filler fetches the end block. So the processor
consumes skipped slots atomically with the swaps.
Progress rides on the blocks themselves. The processor advances `next_slot` to the block's
`next_slot_after` in the block's own transaction. A page that ends in skipped slots needs no
message of its own. The filler sends its last block claiming only its own slot. The next
page's first block carries the job across the skipped slots, because its parent chain proves
them skipped. `next_slot` passing `end_slot` only ends the walk. The job is complete when the
reconciler finds its range covered.

**Absence is verified, never assumed.** Within a job, the filler checks the parent chain.
Each fetched block's `parent_slot` must equal the previously fetched slot. A job's first
block (at its start or on resume) must name a parent below `next_slot`. A parent at or after
`next_slot` is a block that the listing omitted. A parent below it is the chain's own proof
that the slots between are skipped. The cases are the end block of the range below and a
skipped slot before a backfill's first block. A job cut at an archive window edge is the
third case (below). A mismatch means a slot `getBlocks` omitted is missing from this node's
storage, not skipped. The job then receives `blocked_reason = 'missing_in_storage:<slot>'`,
and the filler continues to the next job. Health reports `blocked_job_count`. The operator
points `RPC_URL` at a node with history (or sets `ARCHIVE_RPC_URL`) and clears the column.
The other reasons are `unmappable:<slot>`, `end_unproven:<slot>` and
`archive_unavailable:<slot>` (below). A job never silently completes with unfilled slots.

The filler first checks an end block that is not yet listed against the node's own
`getSlot(finalized)`. `getBlocks` clamps silently at that root, and a Geyser tip runs ahead
of it. So above the root, the job stays open, and the filler retries it on the next scan
(`fill_job_waiting_for_rpc_finalized`). Only an end at or below the root that is still
unlisted blocks the job.

RPC error classes (agave `custom_error.rs`):

- The filler retries `-32004` block not available and `-32019` long-term storage busy.
- `-32007` skipped is terminal and advances.
- `-32009` skipped-or-missing reads as missing on a provider, which listed the slot. It
  reads as skipped on the archive, which lists nothing. The parent-chain test above is the
  guard.
- `-32001` block cleaned up and `-32011` history unavailable are `MissingInStorage`
  (non-archival node) and block the job.
- `-32015` unsupported transaction version is a configuration bug and stops the process.

**Archive routing (increment 3, squashed after gauntlet round 1).** With `ARCHIVE_RPC_URL`
set, a second `RpcGateway` talks to an Old Faithful `faithful-cli rpc` server (§2.8). Its
**window** is `[bottom, top]`. `bottom` is `getFirstAvailableBlock`. `top` is the block on
the week line, the first block at or after `now - ARCHIVE_SAFE_LAG_SECONDS` (604,800 s, one
week). The design defines the lag in time, because the slot rate drifts. The same
`getBlockTime` bisection as a backfill's start finds `top`, but it runs on the archive and
never probes below `bottom`. The archive answers each probe in under a millisecond, and
answers a slot outside its epochs with a retried `-32004`. When even the archive's newest
block is older than a week, `top` is that block. `top` is therefore always a block. The lag
is a week because an epoch is published only after it ends, and recent history is cheap on
the provider.

The archive's own `getFirstAvailableBlock` and `getSlot` take 9 to 17 s each. So the indexer
reads the window once at boot, beside live rather than before it (the two calls in
parallel). It refreshes the window every 10 minutes, off every hot path. Until the first
read, the reconciler opens nothing and neither lane walks. So no hole is cut wrong or handed
to the metered provider. An archive that does not answer at boot never stops the indexer.
Live and the processor continue to run, and the indexer retries the read every 20 s
(`archive_window_unreadable`). Only filling waits. A later failed refresh keeps the window
already published. When even the archive's first block is younger than a week, there is no
window (`archive_holds_nothing_a_week_old`). Then the provider fills everything, rather than
cutting holes around a window of one block.

The reconciler cuts every hole that straddles the window into the piece below `bottom`, the
piece inside, and the piece above `top`. So no job is half archive, half provider. Each lane
selects its own jobs in SQL. The archive takes `start_slot >= bottom AND end_slot <= top`,
and the provider takes the negation. With no window, the provider takes all. The code
asserts the pure `route` rule against what the query returned. The filler runs one loop per
endpoint, so a slow archive page never delays provider jobs. A job can only move from
provider to archive between passes, because the top rises with time. The new lane resumes it
from the table.

The archive cannot list. Its `getBlocks` answers null, because published epochs carry no
slot list. So on the archive, a page is every slot of the window (a pure function, no
request), and `getBlock` resolves each slot. The archive answers a skipped slot with
`-32009`, so on the archive `-32009` reads as skipped. The archive answers a block that it
lacks the same way, and the next block's parent names it. A read that fails transiently
could answer the same way. So that answer is a counted retry, not a verdict. The pass claims
what it fetched and stops. The next pass resumes from memory and asks for the block again.
Only after 30 consecutive passes without progress does the job block there as
`missing_in_storage`. That is the same per-job budget as a retryable error. Any block sent
clears the count.

Every archive job ends on a block. So the lane retries an end block that continues to answer
`-32009` the same way. Then the job blocks as `missing_in_storage:<end>`. It does not wait
on a block above, because that block cannot prove a slot the archive never served. The
archive lane never calls its slow `getSlot`.

The provider piece below `bottom` may end on a skipped slot that nothing inside it can
prove. The reconciler records that on the job (`end_kind = 'archive_lower_cut'`) when it
cuts the job. So the rule never depends on where the archive's bottom is later. An operator
who loads or drops an epoch moves the bottom. That job claims its blocks. Then it waits for
the archive's first block above, without a relist and on whichever lane it now routes to. It
reads that block from coverage alone (no RPC per waiting pass). That block's parent is the
real last block below the cut. If the walk never returned that parent, the job blocks on it
as `missing_in_storage`. The job that would fetch the block above can itself be blocked.
Then the block cannot arrive, and the job blocks as `end_unproven:<slot>`. A slow archive is
only a longer wait. The rule reads the tables, so a restart does not reset it. Either way,
the reconciler still completes the job once coverage spans it.

The archive can answer a job's slot with a retryable error pass after pass. Examples are an
epoch the server did not load, or a CAR read that continues to fail. Such a job blocks after
30 passes, about twenty minutes, as `archive_unavailable:<slot>`. Only an answer about the
slot counts (a JSON-RPC error or an empty body). So a connection refused or a timeout, from
an archive that is down, never blocks a job. On the provider, a retryable error is its rate
limit, and the filler retries it for as long as the limit lasts.

Measured against faithful-cli v0.7.28: 0.5 to 0.7 blocks/s with 12 in flight. The server
assembles each ~6 MB block from range requests. So the archive's window is
`ARCHIVE_IN_FLIGHT_MAX = 12`, and its limiter is moot. Block 452139025, fetched from the
archive, maps to the same `FinalizedTransaction`s as the mainnet fixtures and decodes to the
same swaps. The test is `core/tests/archive.rs`, `#[ignore]`d unless run with `--ignored`
against the server.

`maxSupportedTransactionVersion` is a ceiling, not a filter: 1 returns legacy, v0 and v1
transactions alike. Transaction V1 is live on mainnet since 2026-09-15. Re-verified: on
current blocks, `getBlock` with 0 fails the whole call with `-32015`. With 1, a sample block
decoded 213 v1, 370 v0 and 829 legacy transactions. The value is the named constant
`TRANSACTION_VERSION_MAX_SUPPORTED = 1`. It is bumped deliberately when a new version ships
and the Solana crates parse it. A block refused with `-32015` stops the process, so the
indexer never guesses at it. The pinned crates must be confirmed to decode v1 messages
(*to confirm* when pinning versions).

Fetch shape, in order of the author's priorities (correct, then fast, then cheap):

- `encoding: json`. The message arrives pre-parsed, so the indexer needs no Solana
  wire-format crates. `base64` is 20 percent smaller, but would require parsing legacy, v0
  and v1 message layouts ourselves.
- `Accept-Encoding: gzip` (70 to 90 percent smaller on the wire).
- `rewards: false`.
- `transactionDetails: full`. The `accounts` level omits inner instructions.

One `getBlocks` per page. Then the filler keeps `getBlock` calls in flight up to the
limiter's rate, rather than one at a time.

Cost: a mainnet block is about 8 MB of JSON (6.3 MB base64) before gzip. At the 0.27 s slots
measured in 2026-10, a one-hour gap is about 13,300 `getBlock` calls. That is about 55
minutes on Helius free tier, whose fill lane receives 4 of its 10 requests a second. A full-day
backfill (about 320k calls) needs a paid RPC tier or a Geyser provider with deep replay.
The design keeps `getSignaturesForAddress` plus batched `getTransaction` out of the
assignment for correctness, not cost. It pages by signature, and intra-slot order differs
between nodes. The lookup-table question is settled (2026-10-04). The Jupiter-routed fixture
reaches DLMM only through an address lookup table. `getSignaturesForAddress` lists it under
the DLMM program, so the address index does cover loaded program IDs. The same page
corrected the volume estimate. 1,000 DLMM-touching signatures spanned 14 slots. That is
about 70 per slot, and roughly 15 million a day including the quarter that fail. The cost
comparison depends on what the provider meters. A day of history is about 320,000 `getBlock`
calls moving about 2.6 TB. The alternative is about 15,000 signature pages plus roughly 11
million `getTransaction` fetches (failed transactions are skipped from the listing), moving
about 100 GB. On a credit-metered provider such as Helius, blocks are about 50 times cheaper.
There, a batch of 100 costs 100 credits and bandwidth is free. On a call-plus-bandwidth
provider such as Triton, the two costs are within a few percent of each other. Bandwidth
dominates the block cost, and calls dominate the signature cost. Exactly-once favours blocks
in both cases. A slot is a checkpoint and a `parent_slot` is a completeness proof. A
signature page trusts the node's address index. The signature path is therefore the
alternative for bandwidth-priced providers and for very selective programs, not the default
(see §12).

**Cancel (2026-10-05).** A cancelled job is a job with `blocked_reason = 'cancelled'`.
`DELETE /v1/backfills/{job_id}` (§8) sets it in one statement. That statement touches only an
uncompleted, unblocked row. Deleting the row is not a cancel. A live run showed this. A walk in
flight continued to write for a few seconds and left a coverage stub below the range above.
Within ten seconds, the reconciler derived the space between stub and range as a hole. It
then reopened a job for all of it.

**A blocked job owns its hole**. The reconciler opens nothing over any uncompleted job,
blocked ones included. The filler reads only unblocked jobs. Health counts the job under
`blocked_job_count`, so `status` reads `blocked`. That is honest, because the history below
that point is deliberately missing. `block_job` leaves an already blocked row alone. So a
walk that read the job open just before the cancel cannot overwrite `cancelled` with a
reason of its own. A walk already under way re-reads its job (one primary-key probe) every
50 fetched blocks. It stops when it finds the job blocked, completed or gone, and drops the
block it held back. So a cancel costs at most 50 more `getBlock` calls per lane, not the
rest of a 1,000-slot page. Blocks already queued for the processor still commit, which is
harmless.

To resume, delete the cancelled row. The reconciler then reopens the space between the stub
and the range above. The filler continues from where coverage ends. If the walk stored
nothing before the cancel, there is no stub, and nothing below the lowest range is owed.
Then deleting the row simply withdraws the request.

### 2.6 Reconnect, stall, backoff

- Stall: no block for 30 s is a dead stream, whatever pings arrive (§2.1).
- Reconnect: exponential backoff from 500 ms to a 60 s cap with full jitter. Attempts are
  unbounded, and CORE logs each one. On reconnect, CORE sets `from_slot = cursor.slot + 1`.
  On `out_of_range`, it resubscribes without `from_slot`. The missed stretch is a coverage
  hole, and the reconciler opens a job for it.
- Rate limiting is `governor` (GCRA, 0.10.x): smooth admission, weighted permits, jitter.
  Retries are `backon` (1.6.x). It gives exponential backoff with jitter and a `when`
  predicate over our own error enum. So the predicate classifies a JSON-RPC error inside an
  HTTP 200. Its `adjust` honours `Retry-After`. Both crates were verified as maintained on
  2026-10-03. The design rejected `tower`'s limiter (fixed window), `backoff` (unmaintained
  since 2021) and `reqwest-retry` (ignores `Retry-After`, cannot see JSON-RPC bodies).
- RPC: two governor limiters from one `RPC_RPS_MAX` (minimum 2). The live tail receives
  ceil(60 percent), and fills plus decimals fetches receive the rest. So a long fill never
  starves live blocks (found in gauntlet round 1). Burst is 2, because governor's default
  burst equals the rate and would spike a provider's window counter. The caller acquires the
  permit inside the retried closure, so every attempt pays. Backoff: base 1 s, cap 30 s, six
  attempts. `Retry-After` goes through `adjust` and a shared pause-until across in-flight
  calls.
- Binance: `Quota::per_minute(6000)` with `until_n_ready(2)` per klines call. Every
  response's `X-MBX-USED-WEIGHT-1m` sets a pause-until instant that all callers await. `429`
  and `418` set that instant from `Retry-After` and retry.
- Geyser: no limiter. An unbounded `backon` loop, capped at 60 s, resets after a healthy
  stream.
- Tests inject governor's `FakeRelativeClock`. Its default clock ignores tokio's paused time.
- Channels: the processor's `select!` is biased to the reconcile tick first, so a busy
  stream never starves the tick. Live comes next, so a heavy fill never starves the stream
  into missing pings.

### 2.7 One instance

One indexer runs. At boot, it takes `pg_try_advisory_lock` on a constant. A second instance
exits with a clear message. The unique key would make a second writer safe, but two writers
would fetch the same holes twice and break the single-writer reconciler. Availability comes
from the restart policy plus a resume that costs one subscription. A second Geyser provider,
if ever added, feeds the same processor as a second origin, and the same key dedupes it.

### 2.8 Gateways

Every outbound dependency is a gateway: the shell adapter an actor owns, bundling the
client, its limiter, its retry policy and its environment config. There are three:

- `GeyserGateway`, owned by `GeyserSource`.
- `RpcGateway`. The RPC tail, the filler's provider lane and the processor's decimals fetch
  share the provider instance. The archive instance belongs to the filler's archive lane
  alone.
- `BinanceGateway`, owned by `PriceFeed`.

CORE builds one `RpcGateway` per endpoint, of kind `Endpoint { Provider, Archive }`. It
builds the provider from `RPC_URL` and `RPC_RPS_MAX`. When `ARCHIVE_RPC_URL` is set, it
builds the archive from that URL and `ARCHIVE_RPS_MAX` (default 20). The kind changes four
behaviours:

- listing (every slot, no request, on the archive)
- the `-32009` class
- the fetch window (12 in flight on the archive)
- what a job does when its end block never arrives (§2.5)

Both kinds share the `getBlock` parameters, timeouts, `backon` retry, `governor` limiter and
decoder. The archive itself is `archive/run.sh`. It runs the `faithful-cli` release binary
for the host with hardcoded epoch configs (`archive/epochs/N.yml`, CID and five index URLs
each). It runs beside compose, because no official image exists. The supported transaction
version is one ceiling for every ingestion gateway: `TRANSACTION_VERSION_MAX` from the
environment. A version is supported across every source or not at all. RPC passes the
ceiling through as `maxSupportedTransactionVersion`. The Geyser mapper enforces it, because
the stream has no such parameter. The proto carries no version number, only `versioned` and
an optional `config`. So the mapper reads legacy for `!versioned`, v0 for `versioned`
without `config`, and v1 with `config` (*to confirm* against agave's v1 encoding). A
version above the ceiling stops the process. A new version needs a new mapper and a
re-index, so the operator raises the variable deliberately. `GeyserGateway` accepts zstd
responses (§2.1). A provider without zstd answers uncompressed, so a missing zstd is never a
connection failure.

## 3. Decoding

Verified against the lb_clmm v0.12.0 IDL (`ts-client/src/dlmm/idl/idl.json`), Meteora's
program events page, and real mainnet transactions.

### 3.1 Finding swap instructions

An instruction is a DLMM swap when its program is the DLMM program and its first eight data
bytes are one of six discriminators:

| instruction | discriminator |
|---|---|
| `swap` | `f8c69e91e17587c8` |
| `swap2` | `414b3f4ceb5b5b88` |
| `swap_exact_out` | `fa49652126cf4bb8` |
| `swap_exact_out2` | `2bd7f784893cf351` |
| `swap_with_price_impact` | `38ade6d0ade49ccd` |
| `swap_with_price_impact2` | `4a62c0d6b1334b33` |

Accounts 0 to 12 are identical across all six: `0 lb_pair`, `6 token_x_mint`,
`7 token_y_mint`, `10 user`. Optional accounts always occupy their slot, so the indexes are
stable. A Jupiter route puts the DLMM swap at `stack_height 2`. Deeper nesting exists (swap
at 3, events at 4). Direct top-level swaps are under one percent of DLMM transactions. One
rule covers all of them.

### 3.2 Finding and decoding the event

DLMM emits events with Anchor `emit_cpi!`, as it did from its first release. Each event is a
self-CPI of the DLMM program with exactly one account, the event authority PDA
`D1ZN9Wj1fRSUQfCjhvnu1hqDMT7hzjzBBpi12nVniYD6` (seed `__event_authority`). Its data is
`e445a52e51cb9a1d`, then the 8-byte event discriminator, then the Borsh payload. DLMM writes
no `Program data:` logs, so log truncation on long routes cannot lose a swap.

The events of a swap instruction at `stack_height h` are the DLMM self-CPIs at `h + 1` that
follow it in the flattened sequence. The window ends at the next instruction at
`stack_height <= h`. Token transfers interleave, so the decoder does not assume adjacency.

Every swap instruction currently emits two events for the same fill:

- `Swap`, discriminator `516ce3becdd00ac4`, 129 bytes: `lb_pair, from, start_bin_id i32,
  end_bin_id i32, amount_in u64, amount_out u64, swap_for_y bool, fee u64, protocol_fee u64,
  fee_bps u128, host_fee u64`. Unchanged since the January 2024 IDL.
- `Swap2Evt`, discriminator `2e7452d7941b544d`, 147 bytes, added in v0.11 (December 2025).
  Its field order differs (it is not a suffix of `Swap`). It separates market-maker,
  protocol, limit-order and host fees, and carries `fees_on_input`. Meteora's events page
  says new indexers should prefer it for analytics.

Decision (revised 2026-10-04): the decoder decodes `Swap`, and `Swap` is the canonical source
of identity, amounts and `fee`. The decoder also decodes `Swap2Evt` when its payload is the
147-byte 0.12.0 layout. `Swap2Evt` then supplies the fee split: `mm_fee`, `limit_order_fee`,
`amount_left`, `fee_side` (input or output, from `fees_on_input`) and `fee_token` (x or y,
from `fees_on_token_x`). Its `protocol_fee` and `host_fee` duplicate `Swap`'s, so CORE does
not store them twice. A payload of any other length (pre-May-2026 history) leaves those
columns null. CORE keeps its raw bytes in `swap2_event_payload`, so a later layout can still
be replayed without a re-index. A swap with no `Swap2Evt` at all (pre-v0.11) also leaves
them null.

`Swap2Evt` byte offsets use the 0.12.0 layout, little-endian, verified on all six fixture
events on 2026-10-04. They are `lb_pair` 0, `from` 32, `start_bin_id` i32 at 64,
`end_bin_id` i32 at 68, `swap_for_y` u8 at 72, `fee_bps` u128 at 73. Then come `amount_in`
u64 at 89, `amount_left` u64 at 97, `amount_out` u64 at 105, `mm_fee` u64 at 113,
`protocol_fee` u64 at 121. Last come `limit_order_fee` u64 at 129, `host_fee` u64 at 137,
`fees_on_input` u8 at 145, `fees_on_token_x` u8 at 146. The total is 147.

Invariant on every fixture: `Swap.fee == mm_fee + protocol_fee + limit_order_fee +
host_fee`. Also, `amount_in`, `amount_out`, `swap_for_y`, `protocol_fee` and `host_fee` agree
between the two events. A `Swap2Evt` whose amounts disagree with its `Swap` is a
`DecodeError::EventMismatch` for that swap (chain input, never asserted).

The decoder checks chain input with `Result` and never asserts it. Each of these cases is
a `DecodeError` carrying slot, signature and reason:

- a payload length other than 129
- an unknown event discriminator under a swap
- an event at the wrong stack height
- a `Swap` self-CPI whose parent is not a recognised swap instruction (an orphan, which is
  how a future `swap3` would surface)

A transaction that cannot even be mapped (a missing `stackHeight`, an index out of range) is
`DecodeError::Unmappable`, carrying the `MapError` text. The block still maps. The processor
persists all of these in `decode_failure`, within the block's transaction. So a later
redecode can replay them by signature. The cursor still advances, because losing the cursor
would lose far more. A swap instruction with no `Swap` event in its window is also a
`DecodeError`.

Amounts and fees come from `Swap`. Mints come from the parent instruction's accounts 6 and
7. `user` comes from account 10, which equals the event's `from` in every fixture. `fee` is
in raw units of the fee token. The pool's collect-fee mode decides the fee token. The API
returns `fee` raw and never scales it.

### 3.3 Identity

`swap_ordinal` is the zero-based position of the swap instruction among all qualifying swap
instructions of the transaction in execution order. The identity of a swap is
`(signature, swap_ordinal)`. CORE stores nothing else for identity.

### 3.4 Skips

Mapping drops transactions with `meta.err` set, before the decoder examines any instruction.
One in four transactions touching DLMM fails. A failed route can carry executed `Swap`
events before the failing instruction (fixture below). Vote transactions never reference the
program and so never pass the filter.

### 3.5 Decimals

The transaction does not carry decimals. On first sight of a mint, the processor schedules a
`getMultipleAccounts` fetch, up to 100 per call. `dataSlice { offset: 44, length: 1 }` works
for SPL Token and Token-2022 mints. The processor persists the result in `token`. Decoding
never waits on this fetch. CORE stores raw amounts regardless. The API scales them with
`token.decimals` at read time, and returns null for the human field until the decimals are
known. USD pricing needs only the three quote assets' decimals, which are constants (SOL 9,
USDC 6, USDT 6).

## 4. Program design

### 4.1 Actors

| actor | owns | handles | sends |
|---|---|---|---|
| `GeyserSource` | `GeyserGateway` (stream typestate, backoff), stall timer | stream messages, `Shutdown` | `on_block(Live)` |
| `RangeFiller` | `RpcGateway` (client, governor limiter, backon retry, version ceiling), open job list, current page. In tail mode, the per-tick `getBlocks` listing | job scan tick, `OnJobsOpened` nudge, `Shutdown` | `on_block(Fill)` or `on_block(Live)` |
| `BlockProcessor` | the DB connection (sole writer of swap, projection and coverage tables, and of every job but the API's backfill insert). Also pool and token caches, quote allowlist, popped block awaiting retry | `on_block`, `on_prices_filled`, reconcile tick (10 s), `Shutdown` | `OnJobsOpened` nudge, decimals fetch requests |
| `PriceFeed` | `BinanceGateway` (client, weighted limiter, pause-until, retry), sweep cursor | tick (15 s), sweep tick (60 s), `Shutdown` (its own control channel) | `on_prices_filled(MinuteRange)` on the fill channel, `try_send` |

Channel capacities are explicit newtypes: live blocks 64, fill blocks 16, nudges 16,
price notifications 16. A full channel applies backpressure to its producer. That is
correct, because a source that outruns the database must slow, not drop.

### 4.2 `BlockProcessor` transaction

For every `on_block`:

1. `decode_block(&block) -> DecodedBlock` (pure): swaps plus decode failures.
2. `enrich(decoded, &pool_cache, &token_cache, &allowlist) -> EnrichedBlock` (pure). It
   resolves `(mint_in, mint_out)` from direction and picks the quote leg.
3. One database transaction does these steps, in order:
   - Upsert new `pool` rows.
   - Upsert new `token` rows (decimals null).
   - Insert swaps with `ON CONFLICT DO NOTHING ... RETURNING`. SQL computes `volume_usd`
     from the latest eligible `price` row in `(block_time - 60 s, block_time]`.
   - Run `project(&inserted) -> ProjectionDeltas` (pure) over the rows actually inserted.
     Then run one upsert per projection table that adds the deltas.
   - Insert `decode_failure` rows.
   - For every origin, `cover(parent_slot, slot, block_time)` merges the block into
     `slot_coverage` (§2.4).
   - For `Fill`, also set `slot_range_job.next_slot = GREATEST(next_slot, next_slot_after)`.

   There is no cursor row. CORE derives the cursor from coverage's top range, so it can never
   disagree with what is stored. A replayed block leaves coverage as it was.
4. After commit: log `block_indexed`, and queue decimals fetches for unknown mints.

Between blocks, the processor's loop also runs the reconcile tick (§2.5), so job rows have
the same single writer as everything else.

**Event sourcing.** The `swap` table is the event log. It is append-only and immutable,
except for the late-bound `volume_usd`. `LogPosition (slot, transaction_index,
swap_ordinal)` orders it totally. Everything the API serves is a projection: a pure fold over
events producing deltas for keys, applied with `value = value + delta`. Every projection is
a commutative monoid (sums, counts, min, max). So a gap fill that arrives hours late adds to
a historical bucket, and order does not matter. Only rows that the insert actually returned
feed `project`. So a replayed block projects nothing, and "ingest twice changes nothing"
holds for the projections too. Repricing is the second event type. The `UPDATE` that sets
`volume_usd` returns its rows and projects `volume_usd + v, unpriced_swap_count - 1`.

Projections:

- `pool_volume_1h`, per pool per hour.
- `pool_volume_1d`, per pool per UTC day. It is its own fold over the same inserted rows
  (decided 2026-10-04).
- `pool_stats`, per pool, all time.

Every swap row also records its `source` (`live_geyser`, `live_rpc`, `fill`), so the
reconciliation between sources is auditable. `fill_job_id` names the job.

**Back-processing.** `projection` holds one row per projection: `name`, `version`, cursor
(`LogPosition`), `state`. To add or change a projection, run the subcommand
`indexer rebuild-projection <name>`. It does these steps:

1. Truncate the projection's table.
2. Zero the cursor.
3. Walk the log in pages ordered by `LogPosition`.
4. Apply the same `project` function.
5. Advance the cursor per page in the same transaction (a kill resumes).
6. Mark the projection live.

For this assignment, it runs offline with the indexer stopped. On restart, the stopped time
is a coverage hole, and the reconciler and filler close it. The subcommand ships in
increment 2 (decided 2026-10-04). An online variant is correct, because projections commute,
and it is a next step. That variant applies a new swap immediately to every projection whose
cursor is past the swap's position. Otherwise, it leaves the swap for the replay. This puts
one requirement on today's design. The log stores the whole event (`start_bin_id`,
`end_bin_id`, `transaction_index`, the raw `Swap2Evt` bytes). Projections store only what the
API serves.

On a database error, the processor keeps the popped block and retries it with backoff. It
exits after a bounded number of consecutive failures, and the restart policy then restarts
it. The processor never commits anything partial.

The processor is the only writer to `swap`, `pool`, `token`, `decode_failure`,
`slot_coverage`, `slot_range_job` (the filler only sets `blocked_reason`). `PriceFeed` is the
only writer to `price`. The one cross-table operation, repricing swaps after prices arrive,
runs in the processor on `on_prices_filled`.

### 4.3 Functional core

Pure, mockless, fixture-testable: `map_rpc_block`, `map_geyser_block`, `decode_block`,
`decode_transaction`, `find_swap_instructions`, `pair_events`, `decode_swap_event`,
`account_key_table`, `quote_leg`, `next_page`, `walk_start`, `verify_parent_chain`,
`classify_rpc_error`, `align_range`, `project` (and its per-projection folds
`project_pool_volume_1h`, `project_pool_stats`).

The shell is `main.rs` wiring, the four actor loops, the gRPC, RPC and HTTP clients, and
`store`. The code uses no trait objects. Tests feed `FinalizedBlock` values built from
fixtures straight into the pure functions and the store.

### 4.4 Typestates

- `GeyserStream<Disconnected> -> connect() -> GeyserStream<Connected> ->
  subscribe(filter, from_slot) -> GeyserStream<Subscribed> -> next_block()`. Only the
  subscribed state yields blocks. Reconnect consumes the stream back to `Disconnected`.
- Block lifecycle: `FinalizedBlock -> DecodedBlock -> EnrichedBlock -> StoredBlock`. Each
  transition consumes its input. `StoredBlock` is the post-commit receipt that the log line
  and the aggregate refresh take. So nothing downstream runs before commit.
- Swap lifecycle: `DecodedSwap -> EnrichedSwap` (adds `mint_in`, `mint_out`, `quote_leg`).

Rejected as ceremony: a typestate over job progress (`next_slot` is the state) and a
priced/unpriced swap state (pricing is SQL).

### 4.5 Newtypes and enums

`Signature([u8; 64])`, `Slot(u64)`, `UnixSeconds(i64)`, `PoolAddress([u8; 32])`,
`MintAddress([u8; 32])`, `UserAddress([u8; 32])`, `AccountAddress([u8; 32])`,
`TokenAmountRaw(u64)`, `Decimals(u8)`, `SwapOrdinal(u16)`, `StackHeight(u8)`,
`SlotRange { start, end_inclusive }`, `MinuteRange`, `JobId(i64)`, `PriceUsd(Decimal)`,
`ChannelCapacity(usize)`, `RpsMax(u32)`, `TransactionIndex(u16)`,
`TransactionVersionMax(u8)`, `LogPosition { slot, transaction_index, swap_ordinal }`,
`BinId(i32)`, `HourBucket(UnixSeconds)`.

Enums instead of booleans: `SwapDirection { XToY, YToX }`, `QuoteAsset { Sol, Usdc, Usdt }`,
`BlockOrigin { Live, Fill { job_id, next_slot_after } }`, `PriceSource { Binance, Peg,
CarriedForward }`, `RpcErrorClass { Retry, SkippedSlot, MissingInStorage, ConfigurationBug }`,
`LiveSource { Geyser, RpcTail }`, `ProjectionName { PoolVolume1h, PoolStats }`,
`ProjectionState { Building, Live }`, `OutputFormat { Table, Json, Raw }` (CLI).

### 4.6 Error handling per actor

- `GeyserSource`: any stream error or stall is a reconnect, never a crash.
- `RangeFiller`: per the error classes. It reports a blocked job and never completes it.
- `BlockProcessor`: described in §4.2. Decode errors are data, not failures.
- `PriceFeed`: failures delay prices. Swaps stay unpriced until the sweep fills them.

### 4.7 Shutdown

On `SIGTERM`, sources stop producing. The processor drains both block channels, commits and
exits. Compose `stop_grace_period` is 30 s.

### 4.8 Assertions

`debug_assert!` only, for internal invariants that the code itself establishes:

- `swap_ordinal` strictly increasing within a transaction
- `next_slot_after > slot`
- a covered block's `parent_slot < slot`
- open jobs listed in strictly descending `end_slot`
- channel capacities non-zero
- the allowlist has three distinct mints
- a `StoredBlock` is produced at most once per block

Properties of chain input (payload lengths, stack heights, `parent_slot < slot`, monotone
`block_time`) are `Result`s.

## 5. Pricing

**Model.** USD volume is the quote-side leg of the swap times the quote asset's latest USD
price at the swap's block time. Dune's `add_amount_usd_dex_trades` prices the trusted side
first. DefiLlama counts a swap once on whichever side has a price. Verified on 2026-10-01
over the top 2,491 DLMM pools by 24-hour volume ($317M, effectively all volume of 132,999
pools). In that volume, 53.9 percent is quoted in USDC and 41.7 percent in SOL. 2.3 percent
has SOL or USDC on the x side. 2.1 percent has no SOL, USDC or USDT side. Three series price
about 98 percent.

**Quote leg** (pure): the allowlist is three mints, never symbols (a fake "USDT" pool
exists). If exactly one side is allowlisted, that side is the quote. If both are
(SOL-USDC), the rule picks the stable. If neither is, `quote_asset_symbol` stays null and
the swap stays unpriced.

**Deterministic price** (squashed 2026-10-05). A `price` row is keyed `(asset_symbol, ts,
source)`. `ts` is the instant the price was observed. For a Binance one-minute candle, `ts`
is its close time (open plus 60 s). The price of a swap at time `t` is the latest row for
its quote asset and an eligible source. That row must lie in the half-open window
`(t - PRICE_AGE_MAX_SECONDS, t]` (60 s). The rule is a shared SQL fragment, not a function:
`price_lookup_sql!` in `core/src/store/price.rs`. It is one `LATERAL` subquery. At compile
time, the macro splices it into both the swap insert and the reprice sweep. Both use the same
bind positions (`$1` the eligible sources, `$2` the age). So the two paths cannot drift
apart. The plan is the same index scan on `price_pkey` that a planner-inlined SQL function
gave. The rule sits next to the Rust that binds it, not behind a migration (squashed
2026-10-05 from a `price_at` function). The USD arithmetic is the second such fragment,
`usd_value_sql!` (`quote_amount * close_usd / 10^decimals`, exact `NUMERIC`). The lookup
orders `ts DESC, (source = sources[1]) DESC, source`. Ties on `ts` go to the configured
market by name, not by enum order. So a market appended to the enum later still wins them.
On the one-minute grid, this is exactly the previous rule: the close of the last candle
closed at `t`. The lookup never uses a future price.

Eligible sources are the market plus `peg` and `carried_forward`. The market is
`PriceSource::Binance`, a code constant. Configurable sources were misconfigurable, so
onboarding a market is a code change. The feed derives `peg` and `carried_forward` itself:
USDT's 1, and the previous close repeated over an exchange gap. They are part of the
market's series, not rival sources. Rows are immutable. `PriceFeed` stores only closed
candles. Binance can have no candle for a minute. Once that minute is five minutes settled,
the sweep writes a `carried_forward` row at that `ts`. A swap misses only when no eligible
row lies within the bound. It stays unpriced until the sweep writes one. The sweep can find
a minute unseeded: once settled, it has no candle and none in the hour before it. The sweep
remembers that minute in memory and leaves it out of the span that later sweeps read. So a
permanent exchange gap stops widening every sweep window. That minute is still repriced if a
later window happens to carry a price into it. The lookup is a pure function of immutable
rows. So a swap's `volume_usd` is the same whichever path stores it and whenever it is
computed. "Ingest twice changes nothing" holds for USD. `swap.price_ts` records the `ts` of
the row used. The Binance fetch never runs inside a block's transaction.

**Sources.** SOL comes from Binance `SOLUSDT` 1-minute klines (public, no key, weight 2 per
call, decimal strings, up to 1000 candles per call). USDC comes from `USDCUSDT`. USDT is 1
with `source = 'peg'`. No API key: Binance limits public market data by IP (6000 weight per
minute), and a key changes nothing for klines. Our load is one call every 15 s, plus 44
calls for a month of backfill. `BINANCE_DATA_API_URL` defaults to
`https://data-api.binance.vision`, the host that Binance's docs name for key-less market
data. The reason is that `api.binance.com` returns HTTP 451 to US egress. `api.binance.us`
serves the same shape (*likely*). The `data.binance.vision` daily CSV zips are the bulk
fallback. Jupiter's price API has no history and Birdeye needs a key, so neither serves
backfill. Considered and deferred to §13:

- deriving SOL/USD from our own indexed SOL-USDC pools. This is self-contained, but a
  minute's price is final only once every block of that minute is indexed.
- one-hop pool pricing for the 2 percent of volume in pools with no quote asset.

**Filling.** Every 60 s and at boot, the sweep asks for the oldest and newest `block_time`
of `swap WHERE volume_usd IS NULL AND quote_asset_symbol IS NOT NULL`. Each end is an
ordered scan of the partial `swap_unpriced` index that stops at its first row. The scan
skips the minutes already found unpriceable, which the feed keeps for a week per quote
asset. The sweep fetches the missing minutes in chunks of 1000, writes them, and sends
`on_prices_filled(range)`. The processor runs one `UPDATE ... RETURNING` over unpriced swaps
in that range, with the same SQL fragments. In the same transaction, it projects the
returned rows as `volume_usd + v, unpriced_swap_count - 1`. Null-only is sufficient because a
priced row can never change. This one mechanism covers live stalls, gap fills, backfills and
crashes.

**Precision.** Raw amounts are `u64` in Rust and the `u64` domain, `NUMERIC(20,0)` checked
to the u64 range, in the database (u64 exceeds `BIGINT`). Prices are decimal strings parsed
into `rust_decimal::Decimal` and stored as `NUMERIC(18,8)`. No `f64` touches a stored value.
Sums use `NUMERIC`, and the API returns them as strings.

**Unpriced.** The API reports `unpriced_swap_count` per bucket. It reports `volume_usd` as
null when a bucket is empty or wholly unpriced. It never shows a zero for an unknown.

## 6. Data schema (TimescaleDB 2.20 or newer, pg17)

Image `timescale/timescaledb:latest-pg17` (the `WITH (tsdb.*)` form needs 2.20+). It is
always the latest TimescaleDB on Postgres 17. The per-major tag keeps an existing data volume
readable (author, 2026-10-05). TimescaleDB has one job here: chunking and later compressing
the event log. Projections are plain tables that the processor maintains. So there are no
continuous aggregates, refresh policies or real-time unions. Two verified rules shaped the
DDL. A unique index on a hypertable must contain the partition column.
`ON CONFLICT DO NOTHING ... RETURNING` works on hypertables.

```sql
-- migrations/0001_core.sql
CREATE EXTENSION IF NOT EXISTS timescaledb;
-- Exclusion constraints over slot ranges.
CREATE EXTENSION IF NOT EXISTS btree_gist;

-- Domains: the invariants the Rust newtypes enforce, visible in the schema (2026-10-04).
CREATE DOMAIN solana_address   AS TEXT CHECK (VALUE ~ '^[1-9A-HJ-NP-Za-km-z]{32,44}$');
CREATE DOMAIN solana_signature AS TEXT CHECK (VALUE ~ '^[1-9A-HJ-NP-Za-km-z]{86,88}$');
-- A domain names a range, never a unit: the unit (base units, a rate x 1e9) is in the column.
CREATE DOMAIN u64              AS NUMERIC(20,0) CHECK (VALUE >= 0 AND VALUE <= 18446744073709551615);
-- An upper-case ticker, not an enum: onboarding a quote asset is a Rust variant, never a migration.
CREATE DOMAIN asset_symbol     AS TEXT CHECK (VALUE ~ '^[A-Z0-9]{2,16}$');

CREATE TYPE swap_direction   AS ENUM ('x_to_y', 'y_to_x');
CREATE TYPE fee_side         AS ENUM ('input', 'output');
CREATE TYPE fee_token        AS ENUM ('x', 'y');
CREATE TYPE swap_source      AS ENUM ('live_geyser', 'live_rpc', 'fill');
CREATE TYPE price_source     AS ENUM ('binance', 'peg', 'carried_forward');
CREATE TYPE projection_state AS ENUM ('building', 'live');
CREATE TYPE job_end_kind     AS ENUM ('block', 'archive_lower_cut');

CREATE TABLE pool (
    address          solana_address PRIMARY KEY,
    mint_x           solana_address NOT NULL,
    mint_y           solana_address NOT NULL,
    first_seen_slot  BIGINT NOT NULL,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE token (
    mint             solana_address PRIMARY KEY,
    decimals         SMALLINT,                    -- null until fetched
    fetched_at       TIMESTAMPTZ
);

-- The event log. One row per Swap event; immutable except volume_usd (late-bound price).
CREATE TABLE swap (
    block_time           TIMESTAMPTZ      NOT NULL,
    slot                 BIGINT           NOT NULL,
    transaction_index    SMALLINT         NOT NULL,  -- position in block; log order
    signature            solana_signature NOT NULL,
    swap_ordinal         SMALLINT         NOT NULL,
    pool                 solana_address   NOT NULL REFERENCES pool(address),
    user_address         solana_address   NOT NULL,
    direction            swap_direction   NOT NULL,
    mint_in              solana_address   NOT NULL,
    mint_out             solana_address   NOT NULL,
    amount_in            u64              NOT NULL,
    amount_out           u64              NOT NULL,
    fee                  u64              NOT NULL,  -- Swap.fee, base units of the fee token
    protocol_fee         u64              NOT NULL,
    host_fee             u64              NOT NULL,
    fee_rate_1e9         u64              NOT NULL,  -- the IDL's fee_bps: a rate x 1e9
    start_bin_id         INTEGER          NOT NULL,
    end_bin_id           INTEGER          NOT NULL,
    mm_fee               u64,                        -- Swap2Evt (0.12.0 layout) or null
    limit_order_fee      u64,
    amount_left          u64,
    fee_side             fee_side,
    fee_token            fee_token,
    swap2_event_payload  BYTEA,                      -- raw Swap2Evt for layouts not decoded
    quote_asset_symbol   asset_symbol,               -- null = unpriceable pool
    quote_amount         u64,                        -- base units of the quote leg
    price_ts             TIMESTAMPTZ,                -- ts of the price row that priced it
    volume_usd           NUMERIC(38,18),             -- null = unpriced (yet)
    source               swap_source      NOT NULL,
    fill_job_id          BIGINT,
    CHECK ((source = 'fill') = (fill_job_id IS NOT NULL)),
    UNIQUE (signature, swap_ordinal, block_time)
) WITH (
    tsdb.hypertable,
    tsdb.partition_column = 'block_time',
    tsdb.chunk_interval   = '1 day',
    tsdb.segmentby        = 'pool',
    tsdb.orderby          = 'block_time DESC'
);
CREATE INDEX swap_pool_time  ON swap (pool, block_time DESC);
-- Unique, so "strictly after a position" paging can never skip a tie; block_time is in it
-- because a hypertable's unique index must hold the partition column.
CREATE UNIQUE INDEX swap_log_order ON swap (slot, transaction_index, swap_ordinal, block_time);
CREATE INDEX swap_unpriced   ON swap (block_time) WHERE volume_usd IS NULL AND quote_asset_symbol IS NOT NULL;

CREATE TABLE decode_failure (
    block_time   TIMESTAMPTZ NOT NULL,
    slot         BIGINT NOT NULL,
    signature    solana_signature NOT NULL,
    reason       TEXT NOT NULL,                      -- 'unmappable: …' for mapping failures
    PRIMARY KEY (signature, reason)
);

-- ts is the instant the price was observed (a one-minute candle's close time), so a swap
-- takes the latest row at or before its block_time and never a price from its future.
CREATE TABLE price (
    asset_symbol  asset_symbol  NOT NULL,
    ts            TIMESTAMPTZ   NOT NULL,
    source        price_source  NOT NULL,
    close_usd     NUMERIC(18,8) NOT NULL,
    PRIMARY KEY (asset_symbol, ts, source)
);

-- The slot ranges fully indexed: every block write covers (parent_slot, slot]. The top range's
-- end is the cursor; a hole between ranges is history owed, derived by the reconciler.
CREATE TABLE slot_coverage (
    start_slot      BIGINT NOT NULL,
    end_slot        BIGINT NOT NULL,              -- inclusive, always a block's slot
    end_block_time  TIMESTAMPTZ NOT NULL,         -- block_time of the block at end_slot
    CHECK (start_slot <= end_slot),
    EXCLUDE USING gist (int8range(start_slot, end_slot, '[]') WITH &&)
);

CREATE TABLE slot_range_job (
    id              BIGSERIAL PRIMARY KEY,
    start_slot      BIGINT NOT NULL,
    end_slot        BIGINT NOT NULL,              -- inclusive
    next_slot       BIGINT NOT NULL,              -- first slot not yet stored
    end_kind        job_end_kind NOT NULL DEFAULT 'block', -- 'archive_lower_cut': cut below the archive
    blocked_reason  TEXT,                         -- set when the node lacks a slot or a block is unmappable
    completed_at    TIMESTAMPTZ,                  -- set by the reconciler once coverage contains the range
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (start_slot <= end_slot),
    CHECK (next_slot >= start_slot),
    EXCLUDE USING gist (int8range(start_slot, end_slot, '[]') WITH &&)
);
CREATE INDEX slot_range_job_open ON slot_range_job (end_slot DESC)
    WHERE completed_at IS NULL AND blocked_reason IS NULL AND next_slot <= end_slot;

-- No SQL functions: the price lookup and the USD arithmetic are Rust-side SQL fragments (§5).


-- migrations/0002_projections.sql: derived from the swap log and rebuildable from it, so the
-- projections get their own migration.
-- Projections: read models maintained by the processor with value = value + delta.
CREATE TABLE pool_volume_1h (
    bucket               TIMESTAMPTZ NOT NULL,    -- hour start, UTC
    pool                 solana_address NOT NULL,
    swap_count           BIGINT NOT NULL,
    volume_x             NUMERIC(39,0) NOT NULL,  -- base units of mint_x traded either way
    volume_y             NUMERIC(39,0) NOT NULL,
    volume_usd           NUMERIC(38,18) NOT NULL, -- sum over priced swaps only
    unpriced_swap_count  BIGINT NOT NULL,
    PRIMARY KEY (pool, bucket)
);
CREATE INDEX pool_volume_1h_bucket ON pool_volume_1h (bucket, pool)
    INCLUDE (swap_count, unpriced_swap_count, volume_usd);

CREATE TABLE pool_volume_1d (LIKE pool_volume_1h INCLUDING ALL);   -- bucket = UTC day start

CREATE TABLE pool_stats (
    pool                 solana_address PRIMARY KEY,
    swap_count           BIGINT NOT NULL,
    volume_x             NUMERIC(39,0) NOT NULL,
    volume_y             NUMERIC(39,0) NOT NULL,
    volume_usd           NUMERIC(38,18) NOT NULL,
    unpriced_swap_count  BIGINT NOT NULL,
    first_swap_at        TIMESTAMPTZ NOT NULL,    -- min
    last_swap_at         TIMESTAMPTZ NOT NULL     -- max
);

CREATE TABLE projection (
    name                      TEXT PRIMARY KEY,
    version                   INTEGER NOT NULL,
    cursor_slot               BIGINT NOT NULL DEFAULT 0,
    cursor_transaction_index  SMALLINT NOT NULL DEFAULT 0,
    cursor_swap_ordinal       SMALLINT NOT NULL DEFAULT 0,
    state                     projection_state NOT NULL DEFAULT 'live',
    updated_at                TIMESTAMPTZ NOT NULL DEFAULT now()
);
INSERT INTO projection (name, version)
VALUES ('pool_volume_1h', 1), ('pool_volume_1d', 1), ('pool_stats', 1);
```


Reasons for non-obvious choices:

- `TEXT` base58 keys rather than `BYTEA`: rows must be readable in a live interview and in
  `metclanker`. The scaling section notes the size cost.
- Unique key leads with `signature`, so it also serves the swaps endpoint's lookups. The
  `swap_log_order` index serves projection rebuilds.
- `volume_x` and `volume_y` are per-side totals regardless of direction. That is what
  "volume in tokens" means for a pool. The API converts with `token.decimals`.
- `fee_rate_1e9` is the event's `fee_bps`, which is really the fee rate scaled by 1e9
  (fixtures show 108039 and 100000000). The column uses the `u64` domain, like the amounts.
  The event field is wider, and a rate past u64 is refused as a misdecode. The column is
  named by its scale because a rate is dimensionless. Lamports and wei are units of SOL and
  ETH, not scales for ratios.
- Units: every amount column holds the token's base units (u64). Sums stay in base units.
  Only the API divides by `token.decimals`. There are no human-readable twin columns. They
  double storage and create two sources of truth. Also, decimals are often unknown at
  insert (author, 2026-10-04, after comparing with ft-backend's `_hmr` columns).
- SQL domains: `solana_address` and `solana_signature` are base58-checked `TEXT`. `u64` is
  `NUMERIC(20,0)` checked to the u64 range, for the nine amount columns and `fee_rate_1e9`.
  A domain carries the range, and the column name carries the unit (squashed 2026-10-05
  from two domains with the same range). They put the invariants the Rust newtypes already
  enforce into the schema a reviewer reads, at no runtime cost.
- `price` keeps `source` in the key. So a second market can be stored beside the first and
  chosen by changing the market constant, without rewriting rows. The primary key serves
  the lookup's range scan on `(asset_symbol, ts)`.
- Quote assets are an `asset_symbol` ticker (`SOL`, `USDC`, `USDT`), not an enum (author,
  2026-10-05). Rust's `QuoteAsset` is already the allowlist the writer checks (mint,
  decimals, Binance symbol). So a SQL enum was a second copy, and it made onboarding an
  asset a migration. The domain keeps only the shape check.
- No SQL functions (2026-10-05): `price_at` and `usd_value` became the shared Rust SQL
  fragments `price_lookup_sql!` and `usd_value_sql!`. The plan is unchanged (both inlined
  before). The arithmetic is the same exact `NUMERIC`. A reader finds every rule that
  prices a swap in one Rust file.
- Update granularity: the processor upserts projection rows once per block, for the keys
  that block touched, never per swap. It never writes an idle pool. A pool active in every
  block receives a HOT update of its `pool_stats` row per block, which autovacuum absorbs. If
  that cost ever shows, batching deltas across blocks is the lever.
- `fill_job_id` proves live and fill overlap safely: a swap reached by both paths keeps the
  first origin.
- Projection sums are `NUMERIC(39,0)` because a sum of u64 values exceeds u64.

**Insert, in prose.** One statement per block inserts all swaps via `unnest` arrays with
`ON CONFLICT (signature, swap_ordinal, block_time) DO NOTHING RETURNING`. It computes
`volume_usd` as `quote_amount * close_usd / 10^quote_decimals` through a
`LEFT JOIN LATERAL`. The join takes the latest eligible `price` row with `ts` in
`(block_time - 60 s, block_time]` (§5). The lookup and the arithmetic are the two shared
fragments of §5. The reprice sweep uses the same lookup in a CTE, since an `UPDATE` target
cannot be referenced from a `LATERAL` in its own `FROM`. The returned rows feed `project`.
One `INSERT ... ON CONFLICT (pool, bucket) DO UPDATE SET swap_count = pool_volume_1h.swap_count
+ EXCLUDED.swap_count, ...` per projection table applies the deltas.

**The API volume query**, hourly, with explicit empty buckets:

```sql
SELECT g.bucket,
       coalesce(v.swap_count, 0)          AS swap_count,
       coalesce(v.volume_x, 0)            AS volume_x,
       coalesce(v.volume_y, 0)            AS volume_y,
       CASE WHEN v.swap_count > v.unpriced_swap_count THEN v.volume_usd END AS volume_usd,
       coalesce(v.unpriced_swap_count, 0) AS unpriced_swap_count
FROM generate_series($from, $to - INTERVAL '1 hour', INTERVAL '1 hour') AS g(bucket)
LEFT JOIN pool_volume_1h v ON v.bucket = g.bucket AND v.pool = $pool
ORDER BY g.bucket;
```

Daily runs the same shape against `pool_volume_1d` with a one-day series (its own fold,
decided 2026-10-04). `generate_series` is plain SQL and makes "empty bucket" explicit.

## 7. Domain schema (Rust, signatures only)

Workspace of two crates plus the Bun CLI. Directory names follow the author's three
services. The package name avoids the reserved `core`.

```
core/        package dlmm_core   lib + bin "indexer"
api/         package dlmm_api    bin "api", depends on dlmm_core
cli/         bun project "metclanker"
```

```rust
// core/src/domain/ids.rs
pub struct Signature([u8; 64]);
pub struct Slot(u64);
pub struct UnixSeconds(i64);
pub struct AccountAddress([u8; 32]);
pub struct PoolAddress([u8; 32]);
pub struct MintAddress([u8; 32]);
pub struct UserAddress([u8; 32]);
pub struct SwapOrdinal(u16);
pub struct StackHeight(u8);
pub struct SlotRange { pub start: Slot, pub end_inclusive: Slot }
pub struct MinuteRange { pub start: UnixSeconds, pub end_inclusive: UnixSeconds }
pub struct JobId(i64);

// core/src/domain/amounts.rs
pub struct TokenAmountRaw(u64);
pub struct Decimals(u8);
pub struct PriceUsd(rust_decimal::Decimal);
pub enum QuoteAsset { Sol, Usdc, Usdt }
pub struct QuoteAllowlist { sol: MintAddress, usdc: MintAddress, usdt: MintAddress }
pub struct QuoteLeg { pub asset: QuoteAsset, pub amount: TokenAmountRaw, pub decimals: Decimals }

// core/src/domain/block.rs
pub enum BlockOrigin { Live(LiveSource), Fill { job_id: JobId, next_slot_after: Slot } }
pub enum LiveSource { Geyser, RpcTail }                 // lives in domain; messages re-exports it
pub struct FinalizedBlock {
    pub slot: Slot, pub parent_slot: Slot, pub block_time: UnixSeconds,
    pub transactions: Vec<FinalizedTransaction>,      // failed and vote transactions already dropped
    pub mapping_failures: Vec<DecodeFailure>,         // Unmappable(text) per transaction that would not map
}
pub struct FinalizedTransaction {
    pub signature: Signature,
    pub transaction_index: TransactionIndex,          // position in the block; log order
    pub account_keys: Vec<AccountAddress>,            // static ++ loaded writable ++ loaded readonly
    pub instructions: Vec<FlatInstruction>,           // execution order
}
pub struct FlatInstruction {
    pub stack_height: StackHeight, pub program_index: u8,
    pub account_indexes: Vec<u8>, pub data: Vec<u8>,
}
pub struct DecodedBlock {
    pub slot: Slot, pub parent_slot: Slot, pub block_time: UnixSeconds,
    pub swaps: Vec<DecodedSwap>, pub failures: Vec<DecodeFailure>,
}
pub struct EnrichedBlock {
    pub slot: Slot, pub parent_slot: Slot, pub block_time: UnixSeconds,
    pub swaps: Vec<EnrichedSwap>, pub failures: Vec<DecodeFailure>,
    pub new_pools: Vec<PoolRecord>, pub unknown_mints: Vec<MintAddress>,
}
pub struct StoredBlock {
    pub slot: Slot, pub block_time: UnixSeconds, pub origin: BlockOrigin,
    pub inserted_swap_count: u32, pub duplicate_swap_count: u32,
}
pub struct Cursor { pub slot: Slot, pub block_time: UnixSeconds }   // the top slot_coverage range's end

// core/src/domain/swap.rs
pub enum SwapDirection { XToY, YToX }
pub enum FeeSide { Input, Output }                     // Swap2Evt.fees_on_input
pub enum FeeToken { X, Y }                             // Swap2Evt.fees_on_token_x
pub struct Swap2Event {                                // Borsh layout of `Swap2Evt`, 147 bytes (0.12.0)
    pub amount_left: TokenAmountRaw, pub mm_fee: TokenAmountRaw, pub limit_order_fee: TokenAmountRaw,
    pub fee_side: FeeSide, pub fee_token: FeeToken,    // protocol_fee and host_fee duplicate Swap's
}
pub enum SwapSource { LiveGeyser, LiveRpc, Fill }
pub struct SwapEvent {                                 // Borsh layout of `Swap`, 129 bytes
    pub lb_pair: PoolAddress, pub from: UserAddress,
    pub start_bin_id: i32, pub end_bin_id: i32,
    pub amount_in: TokenAmountRaw, pub amount_out: TokenAmountRaw,
    pub direction: SwapDirection,
    pub fee: TokenAmountRaw, pub protocol_fee: TokenAmountRaw,
    pub fee_rate_1e9: FeeRate1e9, pub host_fee: TokenAmountRaw,   // the IDL's fee_bps is a rate scaled by 1e9; stored
}
pub struct DecodedSwap {
    pub signature: Signature, pub transaction_index: TransactionIndex, pub ordinal: SwapOrdinal,
    pub pool: PoolAddress, pub mint_x: MintAddress, pub mint_y: MintAddress,
    pub user: UserAddress, pub event: SwapEvent,
    pub event2: Option<Swap2Event>,                    // None when absent or not the 0.12.0 layout
    pub swap2_event_payload: Option<Vec<u8>>,
}
pub struct EnrichedSwap { pub decoded: DecodedSwap, pub mint_in: MintAddress, pub mint_out: MintAddress, pub quote_leg: Option<QuoteLeg> }
pub struct DecodeFailure { pub signature: Signature, pub reason: DecodeError }
pub enum DecodeError {
    Unmappable(String),                                // MapError text; the block still maps
    EventMismatch,                                     // Swap2Evt amounts disagree with Swap
    PayloadLength { expected: usize, actual: usize }, UnknownEventDiscriminator([u8; 8]),
    EventStackHeight { expected: StackHeight, actual: StackHeight }, OrphanSwapEvent,
    SwapWithoutEvent, AccountIndexOutOfRange(u8),
}

// core/src/domain/registry.rs
pub struct PoolRecord { pub address: PoolAddress, pub mint_x: MintAddress, pub mint_y: MintAddress, pub first_seen_slot: Slot }
pub struct TokenRecord { pub mint: MintAddress, pub decimals: Option<Decimals> }

// core/src/domain/job.rs
pub struct SlotRangeJob { pub id: JobId, pub range: SlotRange, pub next_slot: Slot }   // an open job, as the filler reads it
pub struct ReconcileSummary { pub opened_job_count: u32, pub completed_job_count: u32, pub splits: Vec<JobSplit> }
pub struct JobSplit { pub job_id: JobId, pub pieces: Vec<JobPiece> }   // an unstarted job re-cut at the window's edges
pub struct JobPiece { pub id: JobId, pub range: SlotRange, pub end_kind: JobEndKind }

// core/src/domain/price.rs
pub enum PriceSource { Binance, Peg, CarriedForward }
pub struct PricePoint { pub asset: QuoteAsset, pub ts: UnixSeconds, pub close: PriceUsd, pub source: PriceSource }

// core/src/actor/messages.rs
pub enum ProcessorMessage { OnBlock(FinalizedBlock, BlockOrigin), OnPricesFilled(MinuteRange), Shutdown }   // fill progress rides on the block
pub enum FillerMessage { OnJobsOpened, Shutdown }   // a nudge; the job table is the queue
pub enum PriceFeedMessage { Tick, SweepTick, Shutdown }
pub enum LiveSource { Geyser, RpcTail }

// core/src/ingest/geyser.rs — typestate
pub struct GeyserStream<S> { /* endpoint, token, state */ }
pub struct Disconnected; pub struct Connected; pub struct Subscribed;
impl GeyserStream<Disconnected> { pub async fn connect(self) -> Result<GeyserStream<Connected>, GeyserError>; }
impl GeyserStream<Connected>    { pub async fn subscribe(self, from_slot: Option<Slot>) -> Result<GeyserStream<Subscribed>, GeyserError>; }
impl GeyserStream<Subscribed>   { pub async fn next_block(&mut self) -> Result<FinalizedBlock, GeyserError>; pub fn disconnect(self) -> GeyserStream<Disconnected>; }

// core/src/ingest/rpc.rs
pub enum RpcErrorClass { Retry, SkippedSlot, MissingInStorage, ConfigurationBug }
pub struct RpsMax(u32);
pub struct TokenBucket { capacity: u32, refill_count_per_second: u32 }

// core/src/store/ — write path pub(crate); read path pub
pub(crate) async fn write_block(/* conn, EnrichedBlock, BlockOrigin */) -> Result<StoredBlock, StoreError>;   // covers (parent_slot, slot] in the block's transaction
pub(super) async fn cover(/* tx, parent_slot: Slot, slot: Slot, block_time: UnixSeconds */) -> Result<(), StoreError>;
pub(crate) async fn read_cursor(/* executor */) -> Result<Option<Cursor>, StoreError>;
pub async fn reconcile(/* conn, Option<ArchiveWindow> */) -> Result<ReconcileSummary, StoreError>;   // pub for core/tests/processor.rs
pub async fn insert_backfill_job(/* executor, from_slot: Slot */) -> Result<BackfillInsert, StoreError>;   // pub for the API's POST /v1/backfills
pub(crate) async fn read_open_jobs(/* pool, RowCountMax, JobSelection { All, InsideArchiveWindow, OutsideArchiveWindow } */) -> Result<Vec<SlotRangeJob>, StoreError>;   // ORDER BY end_slot DESC, route before LIMIT
pub(crate) async fn block_job(/* pool, JobId, JobBlock */) -> Result<(), StoreError>;   // Unmappable(x) mid-job: end at x, reopen x+1..end in one transaction
pub async fn read_swap_log_page(/* after: LogPosition, RowCountMax */) -> Result<Vec<InsertedSwap>, StoreError>;
pub(crate) async fn reprice_unpriced(/* tx, MinuteRange */) -> Result<Vec<RepricedSwap>, StoreError>;
pub(crate) async fn apply_deltas(/* tx, ProjectionDeltas */) -> Result<(), StoreError>;
pub(crate) async fn rebuild_projection(/* conn, ProjectionName */) -> Result<(), StoreError>;
pub async fn read_pool_volume(/* pool, AlignedRange */) -> Result<Vec<VolumeBucket>, StoreError>;
pub async fn read_recent_swaps(/* pool, limit */) -> Result<Vec<SwapRow>, StoreError>;
pub async fn read_health() -> Result<IndexerHealth, StoreError>;

// core/src/gateway/ — shell adapters, one per outbound dependency
pub struct TransactionVersionMax(u8);
pub struct GeyserGateway  { /* endpoint, x_token, transaction_version_max, max_decoding_message_size_bytes, backoff: backon::ExponentialBuilder */ }
pub struct RpcGateway     { /* url, limiter: governor::RateLimiter (direct, GCRA), retry: backon::ExponentialBuilder, transaction_version_max */ }
pub struct BinanceGateway { /* base_url, limiter: governor::RateLimiter (weighted), pause_until: Option<Instant>, retry */ }

// core/src/projection/ — pure folds over inserted or repriced swaps
pub struct LogPosition { pub slot: Slot, pub transaction_index: TransactionIndex, pub swap_ordinal: SwapOrdinal }
pub enum ProjectionName { PoolVolume1h, PoolVolume1d, PoolStats }
pub enum ProjectionState { Building, Live }
pub struct HourBucket(UnixSeconds);
pub struct VolumeDelta { pub swap_count: i64, pub volume_x: u128, pub volume_y: u128, pub volume_usd: Decimal, pub unpriced_swap_count: i64 }
pub struct PoolStatsDelta { pub volume: VolumeDelta, pub first_swap_at: UnixSeconds, pub last_swap_at: UnixSeconds }
pub struct ProjectionDeltas { pub pool_volume_1h: Vec<((PoolAddress, HourBucket), VolumeDelta)>, pub pool_stats: Vec<(PoolAddress, PoolStatsDelta)> }
pub fn project(inserted: &[InsertedSwap]) -> ProjectionDeltas;
pub fn project_repriced(repriced: &[RepricedSwap]) -> ProjectionDeltas;

// core/src/domain/query.rs — read-side types live in core so store::read_* can use them
pub enum Bucket { Hour, Day }
pub struct AlignedRange { pub from: UnixSeconds, pub to_exclusive: UnixSeconds, pub bucket: Bucket }
pub struct VolumeBucket { pub start: UnixSeconds, pub swap_count: u64, pub volume_x: Decimal, pub volume_y: Decimal, pub volume_usd: Option<Decimal>, pub unpriced_swap_count: u64 }
pub struct IndexerHealth { pub cursor_slot: Option<Slot>, pub last_block_time: Option<UnixSeconds>, pub open_job_count: u32, pub blocked_job_count: u32, pub rebuilding_projections: Vec<ProjectionName> }   // cursor and time: the top coverage range
```

Left to the build phase: derives per newtype, `sqlx` encode/decode at the store boundary
(`TokenAmountRaw -> Decimal`, `Slot -> i64`, `SwapOrdinal -> i16`), and `Display` as base58
for the address newtypes.

## 8. API

Axum, JSON, UTC everywhere, base path `/v1`.

| method and path | purpose |
|---|---|
| `GET /v1/health` | always 200 while the database answers: `{ status, cursor_slot, last_block_time, lag_seconds, open_job_count, blocked_job_count }`. `cursor_slot` and `last_block_time` are the top `slot_coverage` range's `end_slot` and `end_block_time` (the newest indexed block, swaps or not). `lag_seconds` is now minus that time. Open counts jobs with `completed_at IS NULL AND blocked_reason IS NULL AND next_slot <= end_slot`. Blocked counts those with `completed_at IS NULL AND blocked_reason IS NOT NULL`. `status` precedence: `starting` (no cursor), `lagging` (`lag_seconds > 120`), `blocked` (`blocked_job_count > 0`), `backfilling` (`open_job_count > 0`), else `ok` |
| `GET /v1/pools?limit=20&offset=0` | pools ordered by 24-hour USD volume from `pool_volume_1h`, then 24-hour swap count, then address. Adds `page: { limit, offset, total }` |
| `GET /v1/pools/{pool}` | one entry of that list on its own |
| `GET /v1/pools/{pool}/volume?bucket=hour\|day&from=<rfc3339>&to=<rfc3339>` | the required endpoint |
| `GET /v1/pools/{pool}/swaps?limit=20&before=<cursor>` | most recent swaps with signature, ordinal, amounts, fee raw, `fill_job_id`, newest first by keyset. Adds `page: { limit, next_cursor }` |
| `POST /v1/backfills` with `{"from": "<rfc3339>"}` | inserts one backfill job (§2.5) and answers 202 `{ job_id, start_slot, end_slot }` |
| `GET /v1/backfills` | the 50 newest jobs, newest first: `{ jobs: [{ job_id, state, start_slot, end_slot, next_slot, end_kind, blocked_reason, completed_at, created_at }] }`. It lists every job, not only requested ones, since the filler walks the reconciler's holes the same way. `state` is `completed`, `cancelled` (blocked with reason `cancelled`), `blocked` or `open` |
| `GET /v1/backfills/{job_id}` | one job in the same shape. 404 `job_not_found`, 400 `invalid_job_id` |
| `DELETE /v1/backfills/{job_id}` | cancels: sets `blocked_reason = 'cancelled'` (§2.5) and answers 200 `{ job_id, start_slot, end_slot, next_slot, state: "cancelled" }`. 404 `job_not_found`. 409 `job_already_completed`, or `job_already_blocked` with the existing reason in the message |

**Backfill**, one of the API's two writes (the other is its cancel, below). Checks, in order:

1. The body is JSON with an RFC 3339 `from` (400 `invalid_backfill_body`).
2. `from` is not in the future (400 `backfill_from_in_future`).
3. The API has `RPC_URL` (503 `backfill_unavailable`).
4. Something is indexed (409 `nothing_indexed_yet`).
5. The bisection succeeds (502 `rpc_unavailable`) and lands below the lowest range (400
   `backfill_from_after_coverage`).
6. The insert does not overlap a job (409 `backfill_overlaps_job`).

The cheap checks run before the bisection, which costs tens of RPC calls. The insert
re-reads the lowest range itself, so the checks are early answers, not the guarantee.
Writing here is acceptable because it writes a job row, never a swap. The processor stays
the only writer of swaps, coverage and projections. The same exclusion constraint as the
reconciler's checks the job. The filler and reconciler cannot tell it from a hole's job. It
needs no advisory lock. The API's RPC limiter is its own, so a backfill request adds its
bisection to the indexer's load on the same key.

**Cancel**, the API's other write: one `UPDATE … RETURNING` behind a `SELECT … FOR UPDATE`
of the target. So the refusal it reports describes the row that the update saw. It writes
only `blocked_reason` and `updated_at` of a job row. Resuming has no endpoint: the operator
deletes the cancelled row (README, Backfill).

**Paging.** `limit` is 1 to 100 (default 20) on both lists. One page stays a few hundred KB.
`limit` and `offset` take canonical decimal digits only (no sign, no leading zero, no
exponent). The cursor likewise takes only the spelling the API issued. So one page has one
URL. `/v1/pools` pages by `offset` over a ranking that moves as blocks land. The CLI caps
`pools --limit` at 100 as a usage error, and `--offset` reads past it. The CLI's JSON
envelope carries the API's `page`. `/swaps` pages by an opaque keyset cursor, the last row's
`(block_time, slot, transaction_index, swap_ordinal)`. So newer swaps never shift a page.
While `pool_stats` is live, the pool's `first_swap_at` bounds the scan below. So a quiet
pool or the last page does not probe every older chunk.

**Validation for volume**, in order:

1. `pool` parses as base58 and exists (404).
2. `bucket` is present.
3. `from` and `to` parse as RFC 3339 (any offset, converted to UTC).
4. The API floors `from` and ceils `to` to the bucket boundary, and echoes the effective
   range.
5. `from < to` after alignment.
6. The cap: 744 hourly buckets (31 days) or 366 daily.

Buckets are `[start, end)` labelled by `start`, aligned to UTC midnight and the top of the
hour.

**Response.**

```json
{
  "pool": "…", "mint_x": "So111…", "mint_y": "EPjF…", "decimals_x": 9, "decimals_y": 6,
  "bucket": "hour", "from": "2026-10-01T00:00:00Z", "to": "2026-10-01T06:00:00Z",
  "buckets": [
    { "start": "2026-10-01T00:00:00Z", "swap_count": 128,
      "volume_x": "1532.123456789", "volume_x_raw": "1532123456789",
      "volume_y": "181204.561234",  "volume_y_raw": "181204561234",
      "volume_usd": "181170.02", "unpriced_swap_count": 0 },
    { "start": "2026-10-01T01:00:00Z", "swap_count": 0,
      "volume_x": "0", "volume_x_raw": "0", "volume_y": "0", "volume_y_raw": "0",
      "volume_usd": null, "unpriced_swap_count": 0 }
  ]
}
```

Amounts are strings: raw integer units plus a human decimal scaled by `token.decimals`
(null until decimals are fetched). `volume_usd` is null for empty or wholly unpriced
buckets. Errors: `{ "error": { "code": "invalid_range", "message": "…" } }` with 400 or 404.

## 9. Metclanker (CLI and agent skill)

Bun plus TypeScript, two runtime dependencies: `commander` (subcommands, help) and
`@clack/prompts` (select, confirm, spinner, note). Everything else is Bun: `fetch`,
`Bun.color`, `Bun.inspect.table`, `Bun.stringWidth`, `bun test`, `bun build --compile`.
The design rejected Ink and React. They add fifteen transitive dependencies, and their
retained-mode rendering fights verbatim proof output. Bars are hand-rolled block
characters, about 25 lines.

Commands: `metclanker health`, `metclanker pools [--limit]`, `metclanker volume [--pool]
[--bucket hour|day] [--from] [--to | --range 24h|7d|30d] [--compare] [--base-url]
[--timeout-ms] [--output table|json|raw] [--no-input] [--no-color]`, `metclanker swaps
[--pool] [--limit]`.

Interactive mode runs only when stdin and stdout are TTYs and neither `--no-input` nor a
non-table `--output` is set. Each missing flag triggers only its own prompt. The spinner
shows the request line while it fetches, and stops with `200 OK · 143 ms · 8.2 KB`. The
table has a right-aligned bar column scaled to the largest bucket. A `note` with the
equivalent `curl` command follows the table.

**Proof.** `--output json` prints one JSON object and nothing else to stdout: `{ meta: {
tool, version, generated_at }, request: { method, url, params, headers }, response: {
status, latency_ms, headers, body_bytes }, data: [buckets], summary: { total_usd,
max_bucket } }`, stable key order, no ANSI. `--output raw` prints the request line, status,
headers and verbatim body. `--compare` also fetches Meteora's own numbers from
`https://dlmm.datapi.meteora.ag/pools/{address}/volume/history` (timeframe `1h` or `24h`,
`start_time`, `end_time`, 30 requests per second). It shows them beside ours per bucket,
with the difference. This is the strongest "the system works" evidence a reviewer can collect
in one command. Diagnostics go to stderr. Exit codes: 0 ok, 1 non-2xx or invalid JSON, 2
usage, 3 network or timeout, 130 cancelled. Non-TTY with missing flags exits 2 and never
hangs.

**Skill.** `.agents/skills/metclanker/SKILL.md` per the agentskills.io format (`name`
equal to the directory, `description` with triggers, `compatibility` naming Bun and the API
URL). Body:

- "always use `--output json --no-input`"
- the command and flag table
- the output envelope contract
- exit codes
- two examples
- failure handling (retry once on 3, surface status and body on 1)

It is symlinked under `.claude/skills/` like the other skills.

## 10. Operations

`docker-compose.yml`:

- `db`: `timescale/timescaledb:latest-pg17`, named volume, `healthcheck:
  pg_isready`, `TS_TUNE_MEMORY` set to the compose memory limit.
- `indexer`: `depends_on: db: condition: service_healthy`. It runs `sqlx migrate run`, then
  the actors. `restart: unless-stopped`. `stop_grace_period: 30s`. The healthcheck tests
  that `/tmp/healthy` is younger than two minutes. The processor touches that file on every
  commit, and the tail poll on every tick.
- `api`: `depends_on: indexer: condition: service_healthy` (so migrations exist). Port
  8080. `healthcheck: curl -f /v1/health`. `restart: unless-stopped`.
- `metclanker` is not a service. The README runs it with `bun run` or the compiled binary.

Environment (`.env.example` committed, `.env` ignored):

- `DB_DSN`
- `RPC_URL`, `RPC_RPS_MAX`
- `TRANSACTION_VERSION_MAX` (default 1, both live sources and the filler)
- `GEYSER_URL` and `GEYSER_X_TOKEN` (optional, absent means RPC tail)
- `BINANCE_DATA_API_URL` (default `https://data-api.binance.vision`)
- `ARCHIVE_RPC_URL` and `ARCHIVE_RPS_MAX` (optional)
- `LOG_FORMAT`

`#[sqlx::test]` reads `DATABASE_URL` by name, so tests run as
`DATABASE_URL="$DB_DSN" cargo test --workspace`. Backfill is not configured. It is a job
that the API inserts on `POST /v1/backfills` (§2.5). The API reads `RPC_URL` and
`RPC_RPS_MAX` for it, and answers 503 without `RPC_URL`.

Logging: `tracing` JSON lines with `slot`, `signature`, `job_id`. One INFO `block_indexed`
per block, with swap and duplicate counts. One WARN per decode failure. Counters go through
`/v1/health` only. One multi-stage Dockerfile builds both binaries, and compose `command`
selects one.

**README path** (the ten-minute test), in order, each with expected output:

1. `cp .env.example .env`, then paste a free Helius or QuickNode RPC URL. (1 min)
2. `docker compose up -d`. Then `docker compose logs -f indexer` shows
   `migrations_applied`, `rpc_tail_started`, then one `block_indexed` line about every
   270 ms. (3 min)
3. `curl localhost:8080/v1/health` returns `status: ok` with a recent `last_block_time`.
4. `bun install && bun run metclanker pools` lists pools with volume in the last minutes.
5. `bun run metclanker volume --pool <top pool> --range 24h --bucket hour --compare`
   renders the table and Meteora's figures beside it.
6. Optional: set `GEYSER_URL` and restart to switch the live source. Submit
   `metclanker backfill --from <instant>` to see a backfill job progress in `/v1/health`.

**Later, out of scope.** ECS Fargate: one `indexer` service (desired count 1) and one `api`
service (count 2 behind an ALB). The database runs on Timescale Cloud or a self-managed
instance. Managed PostgreSQL offerings generally lack the Timescale extension (*to confirm*
per provider). Secrets go in Secrets Manager. Geyser comes from the provider's nearest
regional endpoint. GCP equivalents: Cloud Run for the API, one Compute Engine or GKE pod for
the indexer.

## 11. Testing strategy

Decoder fixtures saved from `getTransaction` with `encoding: json` and
`maxSupportedTransactionVersion: 1`, one file per case under `core/tests/fixtures/`:

| case | signature |
|---|---|
| direct top-level `swap2` | `i5A32BcCTCHfhpiDHJXncUxfDPCCLvsxXsuMGcGcCEcjsvr2E7qRwWjZ1GydbV4VPFeHtvGvHKXXYRDfAXKES77` |
| Jupiter route, one DLMM `swap` | `3Pn3nn4pWAdYiqrUubC6jS29p1CTgSSfsEjiFBsWCE4Bji7v37HyUyHQTQTWmCzqEGVYFiBWVujkRAqEXapY5bY7` |
| aggregator, two swaps (`swap` then `swap2`) | `yiaAsmFhSzHqCnSbLzWKaAjVaYqvYDkfnvNfvimEna8BrbQYEYvkJV2Yn5LCYSXA6rMgmJZvQfpHvTmG3P5BvXw` |
| deep nesting, two `swap2` at stack height 3 | `3kytARpRou4qM9w7UMSwqpbCpHjvr4cXpU3fFrJZ83pNXhgD9tJMLcH2ZVTCUVRxZ97BrmFaJQesjRtKVU36WA56` |
| failed transaction with one executed `Swap` event, slot 452146301 | `3FpiEUCx7KDjLT2jKFsiVhy5xoUtresc6TwpJtg3AzCoqvqdje2KTj7CvheHXnvkVYX56eEPUoRT7o22DJ9Ad6XY` |
The failed fixture is the trap. Its `meta.innerInstructions` contains a `Swap` event CPI,
although the transaction failed at instruction 2. So a decoder that does not check
`meta.err` first counts a swap that never settled. Each decoder test asserts the exact swap
count, ordinals, pool, mints, user, direction, amounts and fee. The expected values come from an
explorer, not from the decoder. A fixture whose token-balance deltas differ from the event
amounts (a Token-2022 transfer-fee mint) would prove "amounts from the event" directly. It
is worth adding if one is found.

Idempotency test against the real database (compose `db`):

1. Build a `FinalizedBlock` from two fixtures with a seeded `price` row.
2. Run `write_block` twice as `Live`, then once as `Fill`.
3. After each pass, assert that these are identical: `count(*)`, `sum(amount_in)`,
   `sum(volume_usd)`, every `price_ts`, every `pool_volume_1h` and `pool_stats` row, and
   every `slot_coverage` row.

The projection rows are the strongest assertion: increment-based read models are where
double counting hides. One test covers duplicates, live-plus-fill overlap, restart replay
and USD determinism.

Coverage tests against the database:

- A fake chain with skipped slots, covered in a seeded random order, ends as one range with
  the last block's time. Covering every block again changes nothing.
- `reconcile` opens one job per unowned hole between ranges, nearest the tip first. It skips
  holes that an open or blocked job owns, and completes a job that coverage contains.
- Processor-level tests drive a live jump, one reconcile, the fill and the completion. They
  do the same for a backfill job inserted below the lowest range.
- The backfill insert ends at the slot below the lowest range. It refuses an empty
  coverage, a start at or above it, and an overlap.
- The exclusion constraint rejects an overlapping job.

Pure tests:

- `verify_parent_chain` with a missing slot
- `quote_leg` for the four allowlist cases
- `align_range` at month and day boundaries and the cap order
- `classify_rpc_error`
- `swap_ordinal` determinism (decode a fixture twice, compare)
- `map_rpc_block` drops failed and vote transactions

Not tested:

- Geyser connectivity (needs a credential, and is a manual runbook step)
- Binance HTTP (a recorded response feeds the parser)
- Axum routing beyond one end-to-end smoke test through the real API against the compose
  database

## 12. Scaling: choke points and the queue architecture

The task's creator said the grading emphasis is designing for scale: identify the choke
points of an indexer and address them, with message queues (relayed 2026-10-03). This
section is that answer. Correctness and idempotency stay first. Scale is what the same
invariants look like across more machines.

### 12.1 Choke points

| # | choke point | symptom | this design | at scale |
|---|---|---|---|---|
| 1 | ingress bandwidth | 8 MB JSON per block, about 3.7 blocks/s (0.27 s slots measured in 2026-10). Free RPC tiers answer 429 with 10 s `Retry-After`. A day of history is about 320k `getBlock` calls | Geyser is primary (one protobuf message per finalized block, no polling, `from_slot` replay). The RPC tail is the fallback. Both sit behind one two-lane GCRA limiter, so fills never starve live. The DLMM-filtered block stream is 1.2 to 2.6 MB a block decoded (bot transactions that reference but never invoke DLMM). zstd compresses it to about 177 KB on the wire, 40 to 60 GB/day (§2.1) | a Geyser producer appends every finalized block to a Kafka or Redpanda topic keyed by slot. The topic absorbs bursts, replays history and fans out to consumers. Multiple Geyser providers can produce the same topic and dedupe on slot |
| 2 | decode CPU | one 8 MB block parsed and walked per 270 ms | pure functions, parsed once, no clones on the hot path | stateless decoder consumers, one per topic partition, scaled horizontally. Identity `(signature, swap_ordinal)` makes redelivery harmless |
| 3 | the single writer | one transaction per block, about 7 round trips, 3 ms. The cursor is one row | 80 times headroom at today's rate. The hot path never waits on RPC | writers as a consumer group partitioned by slot hash (SCALE.md). Each writer still commits one transaction per block with its coverage, and stores its `(partition, offset)` in that transaction. Coverage stays the completeness fact, so no global cursor is needed. Measured: 1.66x with two writers, 2.55x with four |
| 4 | hot projection rows | a pool active in every block updates its `pool_stats` row about 3.7 times a second | one upsert per touched key per block. HOT updates. Autovacuum absorbs it | projections become consumers of the swap topic with their own cursors (the `projection` table already models them). Deltas are batched per N blocks or per second. Rebuild is a replay from offset zero |
| 5 | price lookups | one lateral lookup per block, the latest row in `(t - 60 s, t]`. The sweep scans unpriced rows | immutable price rows, partial index `swap_unpriced`, bounded sweeps | the price feed as its own consumer writing a prices topic. Repricing as a compaction job |
| 6 | the event log | tens of millions of rows a day at full DLMM volume. `TEXT` keys cost 88 bytes a signature | hypertable, 1-day chunks, covering indexes, `TEXT` kept for debuggability | columnstore compression after 7 days, `BYTEA` keys, then ClickHouse. ClickHouse uses `ReplacingMergeTree ORDER BY (pool, block_time, signature, swap_ordinal)` with dedupe at merge, so the block cursor remains the exactly-once guard and reads use `FINAL`. Buckets are `AggregatingMergeTree`, fed by incremental materialized views |
| 7 | reads | every request hits the primary | a read-only role, projection tables, bounded ranges | read replicas. The API reads projections only, so it never touches the log |
| 8 | gap-fill backlog | about 13,300 `getBlock` per hour of gap, about 55 minutes on Helius free (its fill lane is 4 rps). A day needs a paid tier. DLMM is about 70 transactions per slot (about 23M a day), a few percent of each block. So block fetching moves about 15 times the bytes it needs. But it makes 50 times fewer metered calls than fetching those transactions one by one | fill lane, parent-chain verification, blocked jobs that never complete silently. An Old Faithful archive is the deep-history endpoint (§2.5). Holes a week or more old cost zero provider calls, at 0.5 to 0.7 blocks/s per server | the topic itself is the replay for anything inside retention. Beyond it, deep replay (LaserStream 48 h). Signature paging plus batched `getTransaction` where bandwidth, not calls, is what the provider meters (lookup-table coverage verified). For bulk history, several archive servers or local CAR files. Also a sidecar that discovers newly published epochs and writes their configs (today they are hardcoded) |

The pool ranking is a known read cost that grows with the pool count, not the page size.
Every `/v1/pools` page aggregates the 24-hour window of `pool_volume_1h` for every pool, then
sorts. Each CLI `pools` call and each `n`/`p` in the interactive Pools screen is such a
page. Measured on a scratch database (2026-10-06): about 3 ms at 300 pools. With 20,000
pools all active in the window (480,000 hourly rows), a page took 138 ms at offset 0 and
150 ms at offset 19,990. The design accepts that while the indexed set is hundreds of pools.
Past that, two options make a page cost its size again. One is a per-pool rolling 24-hour
row that the processor keeps (another projection with its own cursor). The other is the
ranked list, cached in the API for a few seconds and sliced per page.

### 12.2 The queue architecture

```
Geyser A ─┐                                   ┌─ decoder ─┐        ┌─ writer p0 ─┐
Geyser B ─┼─▶ topic: finalized_blocks (slot) ─┼─ decoder ─┼─▶ topic: swaps (pool) ─┼─ writer p1 ─┼─▶ TimescaleDB / ClickHouse
RPC tail ─┘        (retention = replay)       └─ decoder ─┘        └─ writer pN ─┘
                                                                        │
                                              projections ◀────────────┘  (consumers with own cursors)
                                              price feed  ─▶ topic: prices ─▶ repricer
                                              API ─▶ read replicas (projections only)
```

Ordering and exactly-once under partitioning: slot orders blocks totally inside one
partition of `finalized_blocks`. The `swaps` topic keys swaps by pool, so each writer sees
one pool's swaps in log order. Every insert is still `ON CONFLICT DO NOTHING RETURNING`. So
a consumer that replays its partition from its last committed offset inserts nothing twice
and projects nothing twice. Gap detection moves from `parent_slot` on one cursor to a
per-partition offset plus a slot-completeness table. The semantics are unchanged.

What each current module becomes:

- `GeyserSource` and the RPC tail become producers.
- `decode_block` and `enrich` become the decoder consumer, unchanged.
- `write_block` becomes the writer consumer.
- `project` and `apply_deltas` become the projection consumer.
- `PriceFeed` becomes a producer.

Nothing in the functional core changes, which is the point of having one.

### 12.3 Why not now

A queue adds a broker to run, a second serialisation boundary, and consumer-offset
management. The assignment's rate (a few blocks a second, tens of swaps each) is two orders
of magnitude below where any of the choke points bind. The design keeps the queue out and
keeps the seams in. `FinalizedBlock` is the one message type. A producer attaches at the
post-commit `block_indexed` point. The `projection` table already carries per-projection
cursors.

## 13. Next steps (another week)

1. Decode the pre-May-2026 `Swap2Evt` layout from the stored raw payloads, as a replay with
   no chain access. The decoder decodes the 0.12.0 layout at ingest since 2026-10-04.
2. Geyser as the default live source with `from_slot` replay measured per provider.
3. Price the long tail by one hop. A token in an exotic pair takes its own SOL or USDC
   pool's bin price in the same minute (`(1 + bin_step / 10000) ^ end_bin_id`, scaled by
   decimals). A liquidity floor and a staleness cutoff apply. This eliminates most of the 2
   percent unpriced share. Optionally derive SOL/USD from the SOL-USDC pools the same way and
   drop Binance.
4. `metclanker compare` as a scheduled check against Meteora's Data API with alerting on
   drift.
5. Online projection rebuild alongside live indexing. Prometheus metrics. A redecode
   command that replays `decode_failure` rows.
6. If projections ever leave the block transaction (an async fold worker, as ft-backend's
   aggregate worker does), they receive their own monotonic cursor. Health then shows a
   measured lag. Never use triggers. Their cascades capped ft-backend's worker at about 240
   rows a minute before they went statement-level.
6. Per-pool fee volume and per-user endpoints on the same aggregate.
7. Coverage-aware `compare`: expose each job's time range from the API. Then a comparison
   skips (or flags) hours whose slots are still owed by an open or blocked job.
8. (withdrawn 2026-10-04: both ends of every job are real blocks, so no progress-only
   message is needed.)

## 14. Decision log

| decision | alternative rejected | why |
|---|---|---|
| Geyser `blocks` filter at finalized, one block per message | `transactions` filter | a block is an atomic unit with `parent_slot` for gap detection. Transactions carry no completeness signal |
| RPC tail as the fallback live source | require Geyser | the grader's ten-minute path on a free RPC. Zero new mechanism |
| Finalized only | confirmed plus reorg handling | allowed by the task. It eliminates a class of code |
| One `RangeFiller` over `getBlocks` and `getBlock` | `getSignaturesForAddress` | slot-anchored and exactly-once friendly. The alternative pages by signature with cross-node ordering differences |
| One RPC fetch shape, `getBlocks` then `getBlock`, for tail, gap fill and backfill | `getSlot` + per-slot `getBlock` in the tail, or signature paging | one mechanism means one decoder, one writer and one failure mode. Blocks cost ~50x fewer metered calls than per-transaction fetching for DLMM's ~70 transactions a slot |
| Job table is the filler's queue. `OnJobsOpened` is a nudge | message as the only trigger | a crash between commit and send loses nothing |
| Completeness derived from `slot_coverage` by a reconciler | write-time gap verdict + cursor table | derived facts can be re-derived. It catches gaps from any cause. Completion is verified, not claimed |
| `BlockProcessor` is the sole writer | per-actor writes | eliminates every cross-actor consistency question |
| USD as a pre-priced column, last closed price at the block time, computed in SQL | join at query time, compute in Rust, or a lookback longer than one price interval | aggregates cannot join. One formula. Path-independent value |
| Quote-leg pricing from three mints | per-token prices from Jupiter or Birdeye | 98 percent coverage. Jupiter has no history. Birdeye needs a key |
| Binance 1-minute klines | CoinGecko | minute granularity is enterprise-only there |
| TimescaleDB, hypertable, one hourly aggregate | plain PostgreSQL, ClickHouse, or hourly plus daily aggregates | relational, exact `NUMERIC`, unique-key idempotency. A day is 24 hourly rows |
| `TEXT` base58 keys | `BYTEA` | readability in a live interview |
| Two Rust crates, write path `pub(crate)` | one crate or three crates | the compiler enforces CQRS. A third crate is ceremony |
| `decode_failure` table | log line only | "never silently lose swaps" needs a durable record |
| Decode `Swap`, ignore `Swap2Evt` | prefer `Swap2Evt` as Meteora suggests | `Swap` is always emitted and stable since 2024. `Swap2Evt` changed layout in 0.12.0 |
| `generate_series` for empty buckets | `time_bucket_gapfill` | plain SQL, explicit |
| `commander` plus `@clack/prompts` | Ink | two deps versus fifteen. Verbatim proof output |
| No `EventSink` trait | trait with a logging impl | one implementation is not a seam. The log line is the hook |
| Event-sourced projections maintained by the processor | TimescaleDB continuous aggregates | one pure `project` function defines each read model, tested with fixtures, rebuildable from the log. It eliminates refresh policies, real-time unions and the no-join rule |
| Offline projection rebuild | online rebuild with a position rule | zero new concurrency logic for the assignment. Projections commute, so online is a next step |
| Store raw `Swap2Evt` bytes | decode now or discard | 147 bytes a row buys "decode later" as a rebuild instead of a re-index |
| `governor` plus `backon` | `tower` limiter, `backoff`, `reqwest-retry` | GCRA with weighted permits. Maintained. `Retry-After` and JSON-RPC body classification |
| One transaction version ceiling from the environment for every gateway (2026-10-05) | a ceiling per source, or one global constant | a new transaction version means a new mapper and a re-index. A version is supported across every source or not at all. So the operator raises it once, deliberately |
| `encoding: json` for RPC blocks | `base64` | no Solana wire-format crates. 20 percent more bytes |
| Store `fee_rate_1e9` | drop it | the task's fee semantics may need the rate. One `u64` column |
| Base units only in the database | raw plus human-readable twins (ft-backend `_hmr`) | one source of truth. Conversion at the API edge. Decimals may be unknown at insert |
| SQL domains for addresses, signatures and base amounts | plain TEXT and NUMERIC | invariants visible in the schema at no runtime cost (ft-backend's `uint256`, `eth_address`) |
| Projections synchronous in the block transaction | async fold worker with its own checkpoint | strongly consistent, no lag to measure. The `projection` cursor table reserves the async path |
| `price(asset, ts, source)`: latest eligible row in `(block_time - 60 s, block_time]`, source in the key (2026-10-05) | `price_minute` keyed by asset and minute with an exact-minute match | `ts` is when the price was observed. So the rule reads as "the last price known at `t`" and works for any source cadence. On the one-minute grid it equals the old rule. `source` in the key lets a second market sit beside the first behind the market constant |
| One `u64` SQL domain for every u64 column. The domain names the range, the column name the unit (2026-10-05) | `amount_base` and `fee_rate_1e9` domains | a domain is a value range, so two domains with one range were one domain twice. Units already live in column names (`fee_rate_1e9`, base units of the fee token) |
| Backfill is a job the API inserts. The reconciler has no floor (2026-10-05) | `BACKFILL_FROM` resolved on every boot into a reconciler floor, or a request table the indexer resolves | the job is the backfill: one row, visible in SQL and `/v1/health`, submittable while running, refused on overlap by the constraint every job already has. The API writes a job row, never a swap |

ADR: exactly one, for the pricing model (pre-priced USD column from a three-mint quote-leg
allowlist at the last closed price at block time). It is hard to reverse, because every row carries it.
It is surprising, because a reader expects per-token prices or a query-time join. It is a
real trade-off: 98 percent coverage against completeness. "Block as unit of work" is the obvious design
and receives no ADR.

## 15. Review outcome and open questions

Applied from the three reviews:

- RPC tail fallback and README path
- chain-input checks as `Result`s
- `EventSink` dropped
- no vendored conversion code
- failed transactions dropped at mapping
- deterministic closed-minute pricing with immutable price rows and the sweep as the single
  fill mechanism
- daily aggregate dropped
- job state column dropped
- `AlreadySeen` verdict and `GREATEST` cursor
- parent-chain verification for missing slots and the two extra RPC error codes
- durable `decode_failure` with orphan events as errors
- `block_time` null rejected
- advisory lock
- biased `select!`
- backfill job bounds and single creation
- `maxSupportedTransactionVersion` fixed at 1
- `fee_bps` not stored
- API health never 503 on lag
- compose start order and reader password
- all naming and enum nits
- sample caveat on quote shares
- Dune attribution
- ClickHouse moved to §12
- next steps added
- failed fixture found

Rejected:

- renaming `core/` to `indexer/` (the author named the service CORE, and the package name
  already avoids the reserved word)
- dropping `/v1/pools` (the CLI picker and the README step need it)
- dropping `protocol_fee` and `host_fee` (same event, same cost, useful in the swaps
  endpoint)

Resolved on 2026-10-03:

- event-sourced projections replace the continuous aggregate
- rebuild runs offline
- raw `Swap2Evt` bytes are stored
- initial projections are `pool_volume_1h` and `pool_stats`

Resolved on 2026-10-04 (author):

- keys stay `TEXT` base58 for debuggability
- the `Swap2Evt` fee split is decoded at ingest
- SQL domains are adopted
- `pool_volume_1d` is a materialised projection
- per-transaction mapping failures are stored in `decode_failure` before submission
- the `rebuild-projection` subcommand ships
- `swap.source` records `live_geyser`, `live_rpc` or `fill`

Geyser is a core source with RPC fallback, and backfill runs from a timestamp until caught
up.

Open for the author:

1. **Geyser provider.** Chainstack add-on ($49, two streams, about 100-slot replay), Triton
   pay-as-you-go ($125 deposit), or Helius LaserStream ($499 per month, 48-hour replay).
   The RPC tail makes this a quality upgrade rather than a blocker.
2. **Backfill default.** None by default (designed), or a fixed one-hour window? With the
   window, the grader sees history at once, at about 13,300 RPC calls (0.27 s slots).
3. **Range caps.** 744 hourly and 366 daily buckets per request acceptable?
4. **`--compare` in scope for the assignment** or a next step? It is small and it is the
   best proof the CLI can offer.
