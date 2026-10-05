# System design: Meteora DLMM swap indexer

Status: revised draft for the author's review, 2026-10-01; amended 2026-10-03 (gateways,
rate limiting, event-sourced projections). Design only; no code is planned
here. Facts were verified against primary sources on 2026-10-01 unless marked *likely* or
*to confirm*. The draft went through three adversarial reviews (correctness, facts,
simplicity); the accepted findings are folded in and the rejected ones listed in §15.

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
                                                      │ reads only (api_reader role)
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
role and reads the hourly aggregate. The compiler enforces the split too: the write path is
`pub(crate)` inside the CORE crate, the read queries are `pub`, and the API crate depends on
CORE for domain types and read queries only.

**Consistency boundary.** Only finalized blocks are indexed, so there is no reorg handling.
The unit of work is one finalized block: its swaps are inserted and the checkpoint advanced
in one database transaction. Duplicates are impossible by construction (unique key), not by
discipline.

**Event-driven, actor model.** Four tokio tasks exchange typed messages over bounded
channels; every handler is an `on_*` function over the actor's own state. No shared mutable
state, no locks. An event store is out of scope; the post-commit log line
`block_indexed` carrying slot, swap count and signatures is where a producer would be
called later. No trait is reserved for it.

**Program ID.** The task PDF prints `LBUZ…Pd8ZqK3m`, a typo with no account on mainnet;
the author confirmed the correct program with the task's creator on 2026-10-04. The
real DLMM program is `LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo` (verified via
`getAccountInfo`: executable, owner BPFLoaderUpgradeable; confirmed by Meteora's developer
guide). The write-up records this as a verification step.

## 2. Ingestion

### 2.1 Live source: Yellowstone `blocks` filter (primary)

Subscribe once with `commitment = FINALIZED`, one `blocks` filter `{ account_include:
[DLMM_PROGRAM_ID], include_transactions: true, include_accounts: false, include_entries:
false }`, and `from_slot = cursor_slot + 1` on every (re)connect once a cursor exists.

Verified semantics (geyser.proto, filter.rs, grpc.rs at master):

- Exactly one `SubscribeUpdateBlock` per finalized block per filter, carrying `slot`,
  `parent_slot`, `block_time`, `blockhash` and the transactions that touch the program with
  full meta (err, inner instructions, loaded addresses, logs). A block with zero matching
  transactions still arrives with an empty list, so every finalized block is observed and
  the cursor advances through quiet periods.
- The block message follows all of its content and precedes the slot status, so a block is
  complete when it arrives. Finalized blocks are never corrected.
- No server-side chunking; the client raises tonic's `max_decoding_message_size` (default
  4 MiB; the upstream example uses 1 GiB).
- The server pings every 10 s; the client answers with a `SubscribeRequest { ping }`.
  Pings prove only the connection, not the feed, so the stall timer runs on blocks alone
  (every finalized block arrives, empty ones included): 30 s without a block is a stall even
  while pings keep coming.
- `from_slot` replays from an in-memory ring, about 100 to 150 slots on most providers, and
  only on server builds after 2026-07-08 for `blocks` filters. Too far back returns
  `out_of_range`. The client crate's reconnect helper is processed-only, so CORE owns its
  reconnect loop. `out_of_range` is the *expected* outcome after any outage longer than a
  minute, and the coverage reconciler (§2.5) handles it; replay is a shortcut, not a dependency.

The `blocks` filter has no `failed` flag; failed transactions are dropped when the block is
normalised.

### 2.2 Live source: RPC tail (automatic fallback, and the grader's path)

The task's README must let a grader run `docker compose up` and see live swaps in under ten
minutes on a free RPC. A Geyser credential cannot be assumed. When `GEYSER_ENDPOINT` is
unset, or whenever the Geyser stream is disconnected or stalled, `RangeFiller` runs in
tail mode: every 2 s it lists `getBlocks(cursor + 1, cursor + window)`, with window
`max(64, tick_seconds / 0.2)` slots (0.2 s is faster than any pace measured: mainnet slots ran
0.27 s in epoch 1045, 2026-10, against the protocol's nominal 0.4 s, so the window outruns a
faster chain; 64 at the 2 s tick), and calls `getBlock`
only on the listed slots, exactly as the filler walks a job page, tagging the blocks
`BlockOrigin::Live`. The node clamps the listing at its finalized root, so a caught-up tail
sees a short page and a skipped slot costs no call. `getSlot(finalized)` is called only on
first boot (to find the tip) and when a listing comes back full (the window's last slot was
listed, so the tail may be far behind): past 150 slots of lag (about 40 s) the tail jumps to the tip and
the stretch is a coverage hole the reconciler turns into a job. There is one RPC fetch mechanism, `getBlocks`
then `getBlock`, for live fallback, gap fill and backfill (author, 2026-10-04). Same
decoder, same writer, same cursor. Nothing else changes. Geyser is a core part of the
system, not an upgrade (author, 2026-10-04): when configured it is primary, the tail is
the fallback, and the handover in either direction is the cursor. Reconciliation between
the two sources is coverage (a block one source never delivered leaves a hole in
`slot_coverage` that the reconciler hands to the RPC filler; overlap dedupes on the unique key),
plus an optional reconciler that samples recent slots over RPC and compares swap counts.

Cost on Helius free tier (10 requests per second, 1M credits a month, `getBlock` one
credit): about 3.7 `getBlock` calls per second at the measured 0.27 s slots (one constant,
`SLOT_MILLISECONDS_OBSERVED = 270`, from which the live-share minimum is derived: 5 live
requests a second, so `RPC_REQUESTS_PER_SECOND_MAX` of 7 or more) and roughly 30 MB/s before
gzip, which is sustainable for a demo and exhausts the monthly credits in about 3 days of
continuous tailing. Tick-to-data latency is finalization (about 13 s) plus the poll interval. This is
the concrete "what a free tier can and cannot do" answer; Geyser removes the poll and the
per-block RPC cost.

### 2.3 Unit of work: `FinalizedBlock`

Both sources map into one type: `FinalizedBlock { slot, parent_slot, block_time,
transactions }`. Two mapping functions, each about fifty lines: one from the Yellowstone
protobuf, one from `getBlock` / `getTransaction` JSON. No upstream conversion code is
vendored. Mapping drops failed transactions (`meta.err` set) and vote transactions, flattens
instructions into execution order with `stack_height`, and builds the key table `static
keys ++ loaded writable ++ loaded readonly`. A block whose `block_time` is null is rejected
and refetched, never stored; `block_time` is part of the unique key and must agree across
sources.

### 2.4 Checkpoint: `slot_coverage`

Completeness is derived, not signalled. `slot_coverage(start_slot, end_slot,
end_block_time)` holds the contiguous slot ranges fully indexed; an exclusion constraint
(`int8range(start_slot, end_slot, '[]') WITH &&`) keeps them disjoint. Every block write,
live or fill, covers `(parent_slot, slot]` inside the block's own transaction with one
statement, `cover`: it deletes every range overlapping or adjacent to that interval
(`end_slot >= parent_slot AND start_slot <= slot + 1`) and inserts their union, whose
`end_block_time` is the time of whichever block is the new end. A replayed block rewrites
the same row. Skipped slots need no handling: a block after a skipped slot names the
previous block as its parent, so the skipped slots fall inside its interval. A range always
ends on a block and starts right after a block's parent, so both ends of every hole are
blocks too.

The cursor is the top range: `ORDER BY end_slot DESC LIMIT 1`, its `end_slot` and
`end_block_time`. Geyser resumes with `from_slot = end_slot + 1`, the tail lists from there,
and health reports it. No other checkpoint exists, so the three can never disagree, and a
crash at any point leaves the table describing exactly what is indexed.

### 2.5 Reconciler and `RangeFiller`

`slot_range_job { id, start_slot, end_slot, next_slot, blocked_reason, completed_at }`
describes outstanding history; jobs carry the same exclusion constraint as coverage, so two
jobs never claim one slot. Jobs are opened and completed only by the reconciler, a 10 s tick
inside the `BlockProcessor` loop (`tokio::select!` over the tick and the two inboxes, tick
first), so the processor stays the single writer. One statement, `reconcile(floor,
archive_window)`:

- Holes are the gaps between consecutive coverage ranges (a `lag` window over `start_slot`)
  plus, when `floor` is set and below the lowest range, `[floor, lowest.start_slot - 1]`.
- A hole that straddles an edge of the archive window is cut before its bottom and after its
  top (below), so every job lies wholly on one side.
- A hole overlapped by a job with `completed_at IS NULL` (open or blocked) is already owned
  and skipped; a blocked job's hole waits on the operator, not on a second job. An
  unmappable block is the one exception, handled where the job blocks: the job is cut to end
  on that block and the slots after it become a fresh job in the same transaction, so one
  bad block loses one slot, not the rest of the job.
- One job is inserted per remaining hole with `next_slot = start_slot`, nearest the tip
  first.
- Every uncompleted job whose `[start_slot, end_slot]` lies inside a single coverage range
  gets `completed_at = now()`, blocked ones included (another source filled it). Completion
  is verified against coverage, never claimed by a writer. The overlap and containment tests
  are written as the exclusion constraints' own `int8range` expressions (`&&`, `@>`), so they
  probe the GiST indexes; the remaining scans (the `lag` window, the top range for the
  cursor) read a table of a few rows while healthy.

It returns the counts it opened and completed for the `coverage_reconciled` log line and
nudges the filler (`FillerMessage::OnJobsOpened`) when it opened any. Holes from any cause
are caught the same way: a tail jump past 150 slots of lag, a block neither live source
delivered, a restart, a source that was down. `floor` is `BACKFILL_FROM` (RFC 3339)
resolved at every boot, on the idle live lane before the live sources start, by bisection
over `getBlocks` pages to the first slot at or after the timestamp that holds a block. The
floor job is opened at the first tick after the first block commits; a restart resolves the
same floor and finds the job open or its range covered, so there is never a second one.

The filler reads the table as its work queue: at boot and every 10 s it loads open jobs
(`completed_at IS NULL AND blocked_reason IS NULL AND next_slot <= end_slot`) ordered by
`end_slot DESC`, nearest the tip first, up to 100 per pass; each lane's route is part of that
query, before the limit, so a backlog on one lane never hides the other lane's jobs (gauntlet,
increment 3). The nudge only shortens the wait. A pass walks one page of every open job, and
passes repeat at once while pages remain, so a day-long backfill never holds back a fresh
hole (gauntlet, increment 2). It creates no jobs. A job's end is a block (the parent of the
block that starts the range above, or the archive window's top, which is a block); its start
may be a skipped slot, which the first-block rule below accepts. So a walk that runs out of
pages without fetching `end_slot` met a node that lacks it. The one exception is a provider
job cut just below the archive window's bottom, which may end on a skipped slot (below).

Per job the filler loops in pages: `getBlocks(next_slot, min(end_slot, next_slot +
page_size_max - 1))` lists the slots that have blocks (skipped slots are absent; range max
500,000), then `getBlock(slot, { encoding: json, transactionDetails: full, rewards: false,
maxSupportedTransactionVersion: 1 })` per listed slot. Each block is filtered client-side to
transactions whose key table contains the DLMM program, mapped to `FinalizedBlock`, and
sent as `BlockOrigin::Fill { job_id, next_slot_after }`, where `next_slot_after` is the next
fetched block's slot (or `end_slot + 1` once the end block is fetched) so skipped slots are
consumed atomically with the swaps.
Progress rides on the blocks themselves: the processor advances `next_slot` to the block's
`next_slot_after` in the block's own transaction. A page that ends in skipped slots needs no
message of its own: its last block is sent claiming only its own slot, and the next page's
first block, whose parent chain proves the slots between them skipped, carries the job
across them. `next_slot` passing `end_slot` only ends the walk; the job is complete when the
reconciler finds its range covered.

**Absence is verified, never assumed.** Within a job the filler checks the parent chain:
each fetched block's `parent_slot` must equal the previously fetched slot, and a job's first
block (at its start or on resume) must name a parent below `next_slot`: a parent at or after
it is a block the listing omitted, while a parent below it is the chain's own proof that the
slots between are skipped (the end block of the range below, a skipped slot before the
floor's block, or a job cut at an archive window edge, below). A mismatch
means a slot `getBlocks` omitted is missing from this node's storage, not skipped. The job
is then marked `blocked_reason = 'missing_in_storage:<slot>'`, the filler moves on, health
reports `blocked_job_count`, and the operator points `RPC_URL` at a node with history (or
sets `ARCHIVE_RPC_URL`) and clears the column. The other reasons are `unmappable:<slot>`,
`end_unproven:<slot>` and `archive_unavailable:<slot>` (below). A job never silently completes with unfilled slots. An end block not yet
listed is checked against the node's own `getSlot(finalized)` first: `getBlocks` clamps
silently at that root and a Geyser tip runs ahead of it, so above the root the job stays
open and is retried on the next scan (`fill_job_waiting_for_rpc_finalized`), and only an
end at or below the root that is still unlisted blocks it.

RPC error classes (agave `custom_error.rs`): `-32004` block not available and `-32019`
long-term storage busy are retried; `-32007` skipped is terminal and advances; `-32009`
skipped-or-missing reads as missing on a provider (which listed the slot) and as skipped on
the archive (which lists nothing), with the parent-chain test above as the guard; `-32001` block cleaned up and
`-32011` history unavailable are `MissingInStorage` (non-archival node) and block the job;
`-32015` unsupported transaction version is a configuration bug and stops the process.

**Archive routing (increment 3, squashed after gauntlet round 1).** With `ARCHIVE_RPC_URL`
set, a second `RpcGateway` talks to an Old Faithful `faithful-cli rpc` server (§2.8). Its
**window** is `[bottom, top]`: `bottom` is `getFirstAvailableBlock`, and `top` is the block
on the week line, the first block at or after `now - ARCHIVE_SAFE_LAG_SECONDS` (604,800 s,
one week, defined in time because the slot rate drifts), found by the same `getBlockTime`
bisection as the floor but run on the archive and never probing below `bottom` (the archive
answers each probe in under a millisecond, and a slot outside its epochs with a retried
`-32004`); when even the archive's newest block is older than a week, `top` is that block.
`top` is therefore always a block. A week, because an epoch is published only after it ends
and recent history is cheap on the provider. The archive's own `getFirstAvailableBlock` and
`getSlot` take 9 to 17 s each, so the window is read once at boot, beside live rather than
before it (the two calls in parallel), and refreshed every 10 minutes off every hot path;
until it is first read the reconciler opens nothing and neither lane walks, so no hole is cut
wrong or handed to the metered provider. An archive that does not answer at boot never stops
the indexer: live and the processor run on, the read is retried every 20 s
(`archive_window_unreadable`), and only filling waits. A later failed refresh keeps the
window already published. When even the archive's first block is younger than a week there
is no window (`archive_holds_nothing_a_week_old`) and the provider fills everything, rather
than cutting holes around a window of one block. The reconciler cuts
every hole that straddles the window into the piece below `bottom`, the piece inside, and
the piece above `top`, so no job is half archive, half provider. Each lane selects its own
jobs in SQL (archive: `start_slot >= bottom AND end_slot <= top`; provider: the negation; no
window: the provider takes all), and the pure `route` rule is asserted against what the query
returned. The filler runs one loop per endpoint, so a slow archive page never holds up
provider jobs; a job can only move from provider to archive between passes (the top rises
with time), and the new lane resumes it from the table. The archive cannot list: its
`getBlocks` answers null because published epochs carry no slot list, so on the archive a
page is every slot of the window (a pure function, no request) and `getBlock` sorts them
out. It answers a skipped slot with `-32009`, so on the archive `-32009` reads as skipped; a
block it lacks answers the same, and the next block's parent names it. Because a read that
fails transiently could answer the same way, that is a counted retry, not a verdict: the pass
claims what it fetched and stops, the next pass resumes from memory and asks for the block
again, and only after 30 consecutive passes without progress (the same per-job budget as a
retryable error) does the job block there as `missing_in_storage`; any block sent clears the
count. Every archive job ends on a block, so an end block that keeps answering `-32009` is
retried the same way and then blocks as `missing_in_storage:<end>` rather than waiting on a
block above that cannot prove a slot the archive never served; the archive lane never calls its
slow `getSlot`. The provider piece below `bottom` may end on a skipped slot that nothing
inside it can prove; the reconciler records that on the job (`end_kind =
'archive_lower_cut'`) when it cuts it, so the rule never depends on where the archive's
bottom is later (an operator loading or dropping an epoch moves it). That job claims its
blocks and waits, without a relist and on whichever lane it now routes to, for the
archive's first block above, read from coverage alone (no RPC per waiting pass). That
block's parent is the real last block below the cut: if it is one the walk never returned,
the job blocks as `missing_in_storage` on it; if the job that would fetch the block above is
itself blocked, the block cannot arrive and the job blocks as `end_unproven:<slot>` (a slow
archive is only a longer wait, and the rule reads the tables, so a restart does not reset
it); either way the reconciler still completes it once coverage spans it. An archive job
whose slot the archive answers with a retryable error pass after pass (an epoch the server
did not load, a CAR read that keeps failing) blocks after 30 passes, about twenty minutes, as
`archive_unavailable:<slot>`; only an answer about the slot counts (a JSON-RPC error or an
empty body), so a connection refused or a timeout, an archive that is down, never blocks a
job. On the provider a retryable error is its rate limit and is retried for as long as it
lasts. Measured against
faithful-cli v0.7.28: 0.5 to 0.7 blocks/s with 12 in flight (each ~6 MB block is assembled
from range requests), so the archive's window is `ARCHIVE_IN_FLIGHT_MAX = 12` and its
limiter is moot. Block 452139025 fetched from the archive maps to the same
`FinalizedTransaction`s as the mainnet fixtures and decodes to the same swaps
(`core/tests/archive.rs`, `#[ignore]`d unless run with `--ignored` against the server).

`maxSupportedTransactionVersion` is a ceiling, not a filter: 1 returns legacy, v0 and v1
transactions alike. Transaction V1 is live on mainnet since 2026-09-15; re-verified:
`getBlock` with 0 fails the whole call with `-32015` on current blocks, and with 1 a sample
block decoded 213 v1, 370 v0 and 829 legacy transactions. The value is the named constant
`TRANSACTION_VERSION_MAX_SUPPORTED = 1`, bumped deliberately when a new version ships and
the Solana crates parse it; a block refused with `-32015` stops the process rather than
being guessed at. The pinned crates must be confirmed to decode v1 messages (*to confirm*
when pinning versions).

Fetch shape, in order of the author's priorities (correct, then fast, then cheap):
`encoding: json` (the message arrives pre-parsed, so no Solana wire-format crates are
needed; `base64` is 20 percent smaller but would require parsing legacy, v0 and v1 message
layouts ourselves), `Accept-Encoding: gzip` (70 to 90 percent smaller on the wire),
`rewards: false`, `transactionDetails: full` (the `accounts` level omits inner
instructions). One `getBlocks` per page, then `getBlock` calls kept in flight up to the
limiter's rate rather than one at a time.

Cost: a mainnet block is about 8 MB of JSON (6.3 MB base64) before gzip; at the 0.27 s slots
measured in 2026-10 a one-hour gap is about 13,300 `getBlock` calls, about 55 minutes on
Helius free tier, whose fill lane gets 4 of its 10 requests a second. A full-day backfill
(about 320k calls) needs a paid RPC tier or a Geyser provider with deep replay.
`getSignaturesForAddress` plus batched `getTransaction` was kept out of the assignment on
correctness grounds, not cost: it pages by signature and intra-slot order differs between
nodes. The lookup-table question is settled (2026-10-04): the Jupiter-routed fixture, which
reaches DLMM only through an address lookup table, is listed under the DLMM program, so the
address index does cover loaded program IDs. The same page corrected the volume estimate:
1,000 DLMM-touching signatures spanned 14 slots, about 70 per slot and roughly 15 million a
day including the quarter that fail. The cost comparison depends on what the provider
meters. A day of history is about 320,000 `getBlock` calls moving about 2.6 TB, against about
15,000 signature pages plus roughly 11 million `getTransaction` fetches (failed
transactions are skipped from the listing) moving about 100 GB. On a credit-metered
provider such as Helius, where a batch of 100 costs 100 credits and bandwidth is free,
blocks are about 50 times cheaper. On a call-plus-bandwidth provider such as Triton the two
land within a few percent of each other (blocks dominated by bandwidth, signatures by
calls). Exactly-once favours blocks in both cases: a slot is a checkpoint and a
`parent_slot` is a completeness proof, while a signature page trusts the node's address
index. The signature path is therefore the alternative for bandwidth-priced providers and
for very selective programs, not the default; see §12.

### 2.6 Reconnect, stall, backoff

- Stall: no block for 30 s is a dead stream, whatever pings arrive (§2.1).
- Reconnect: exponential backoff from 500 ms to a 60 s cap with full jitter, unbounded
  attempts, each logged. On reconnect `from_slot = cursor.slot + 1`; on `out_of_range`
  resubscribe without `from_slot`; the stretch missed is a coverage hole the reconciler
  opens a job for.
- Rate limiting is `governor` (GCRA, 0.10.x): smooth admission, weighted permits, jitter.
  Retries are `backon` (1.6.x): exponential backoff with jitter, a `when` predicate over our
  own error enum (so a JSON-RPC error inside an HTTP 200 is classified), and `adjust` to
  honour `Retry-After`. Both verified maintained on 2026-10-03; `tower`'s limiter (fixed
  window), `backoff` (unmaintained since 2021) and `reqwest-retry` (ignores `Retry-After`,
  cannot see JSON-RPC bodies) were rejected.
- RPC: two governor limiters from one `RPC_REQUESTS_PER_SECOND_MAX` (minimum 2): the live
  tail gets ceil(60 percent) and fills plus decimals fetches the rest, so a long fill never
  starves live blocks (found in gauntlet round 1). Burst 2 because governor's default burst
  equals the rate and would spike a provider's window counter; the permit is acquired
  inside the retried closure so every attempt pays; base 1 s, cap 30 s, six attempts;
  `Retry-After` through `adjust` and a shared pause-until across in-flight calls.
- Binance: `Quota::per_minute(6000)` with `until_n_ready(2)` per klines call; every response's
  `X-MBX-USED-WEIGHT-1m` sets a pause-until instant all callers await; `429` and `418` set it
  from `Retry-After` and retry.
- Geyser: no limiter; an unbounded `backon` loop capped at 60 s that resets after a healthy
  stream.
- Tests inject governor's `FakeRelativeClock`; its default clock ignores tokio's paused time.
- Channels: the processor's `select!` is biased to the reconcile tick first, so a busy
  stream never starves it, then live, so a heavy fill never starves the stream into missing
  pings.

### 2.7 One instance

One indexer runs. At boot it takes `pg_try_advisory_lock` on a constant; a second instance
exits with a clear message. The unique key would make a second writer safe, but two writers
would fetch the same holes twice and break the single-writer reconciler. Availability comes from the restart policy
plus a resume that costs one subscription. A second Geyser provider, if ever added, feeds
the same processor as a second origin and the same key dedupes it.

### 2.8 Gateways

Every outbound dependency is a gateway: the shell adapter an actor owns, bundling the
client, its limiter, its retry policy and its environment config. There are three:
`GeyserGateway` (owned by `GeyserSource`), `RpcGateway` (the provider instance shared by the
RPC tail, the filler's provider lane and the processor's decimals fetch; the archive instance
the filler's archive lane's alone) and `BinanceGateway` (owned by `PriceFeed`). `RpcGateway` is built per endpoint, an
`Endpoint { Provider, Archive }` kind: the provider from `RPC_URL` and
`RPC_REQUESTS_PER_SECOND_MAX`, and, when `ARCHIVE_RPC_URL` is set, the archive from it and
`ARCHIVE_REQUESTS_PER_SECOND_MAX` (default 20). The kind changes four behaviours: listing
(every slot, no request, on the archive), the `-32009` class, the fetch window (12 in flight
on the archive), and what a job does when its end block never arrives (§2.5); the
`getBlock` parameters, timeouts, `backon` retry, `governor` limiter and decoder are shared.
The archive itself is `archive/run.sh`: the `faithful-cli` release binary for the host and
hardcoded epoch configs (`archive/epochs/N.yml`, CID and five index URLs each), run beside
compose because no official image exists. The supported transaction version is a ceiling per
ingestion gateway, read from the environment: `RPC_TRANSACTION_VERSION_MAX` is passed
through as `maxSupportedTransactionVersion`, and `GEYSER_TRANSACTION_VERSION_MAX` is
enforced by the mapper because the stream has no such parameter; the proto carries no
version number, only `versioned` and an optional `config`, so the mapper reads legacy for
`!versioned`, v0 for `versioned` without `config`, v1 with `config` (*to confirm* against
agave's v1 encoding). A version above the ceiling stops the process: supporting a new version means a new mapper and a re-index, so
the operator raises the variable deliberately.

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
stable. A Jupiter route puts the DLMM swap at `stack_height 2`; deeper nesting exists
(swap at 3, events at 4); direct top-level swaps are under one percent of DLMM
transactions. One rule covers all of them.

### 3.2 Finding and decoding the event

DLMM emits events with Anchor `emit_cpi!` and has since launch: a self-CPI of the DLMM
program with exactly one account, the event authority PDA
`D1ZN9Wj1fRSUQfCjhvnu1hqDMT7hzjzBBpi12nVniYD6` (seed `__event_authority`), whose data is
`e445a52e51cb9a1d` followed by the 8-byte event discriminator and the Borsh payload. DLMM
writes no `Program data:` logs, so log truncation on long routes cannot lose a swap.

The events of a swap instruction at `stack_height h` are the DLMM self-CPIs at `h + 1` that
follow it in the flattened sequence, up to the next instruction at `stack_height <= h`.
Token transfers interleave, so adjacency is not assumed.

Every swap instruction currently emits two events for the same fill:

- `Swap`, discriminator `516ce3becdd00ac4`, 129 bytes: `lb_pair, from, start_bin_id i32,
  end_bin_id i32, amount_in u64, amount_out u64, swap_for_y bool, fee u64, protocol_fee u64,
  fee_bps u128, host_fee u64`. Unchanged since the January 2024 IDL.
- `Swap2Evt`, discriminator `2e7452d7941b544d`, 147 bytes, added in v0.11 (December 2025)
  with a different field order (not a suffix of `Swap`), separating market-maker,
  protocol, limit-order and host fees and carrying `fees_on_input`. Meteora's events page
  says new indexers should prefer it for analytics.

Decision (revised 2026-10-04): `Swap` is decoded and is the canonical source of identity,
amounts and `fee`; `Swap2Evt` is decoded too when its payload is the 147-byte 0.12.0
layout, supplying the fee split: `mm_fee`, `limit_order_fee`, `amount_left`, `fee_side`
(input or output, from `fees_on_input`) and `fee_token` (x or y, from `fees_on_token_x`).
Its `protocol_fee` and `host_fee` duplicate `Swap`'s and are not stored twice. A payload of
any other length (pre-May-2026 history) leaves those columns null and keeps the raw bytes in
`swap2_event_payload`, so a later layout can still be replayed without a re-index. A swap
with no `Swap2Evt` at all (pre-v0.11) also leaves them null.

`Swap2Evt` byte offsets (0.12.0 layout, little-endian, verified on all six fixture events on
2026-10-04): `lb_pair` 0, `from` 32, `start_bin_id` i32 at 64, `end_bin_id` i32 at 68,
`swap_for_y` u8 at 72, `fee_bps` u128 at 73, `amount_in` u64 at 89, `amount_left` u64 at
97, `amount_out` u64 at 105, `mm_fee` u64 at 113, `protocol_fee` u64 at 121,
`limit_order_fee` u64 at 129, `host_fee` u64 at 137, `fees_on_input` u8 at 145,
`fees_on_token_x` u8 at 146; total 147. Invariant on every fixture: `Swap.fee == mm_fee +
protocol_fee + limit_order_fee + host_fee`, and `amount_in`, `amount_out`, `swap_for_y`,
`protocol_fee`, `host_fee` agree between the two events. A `Swap2Evt` whose amounts disagree
with its `Swap` is a `DecodeError::EventMismatch` for that swap (chain input, never
asserted).

Chain input is validated with `Result`, never asserted: a payload length other than 129, an
unknown event discriminator under a swap, an event at the wrong stack height, or a
`Swap` self-CPI whose parent is not a recognised swap instruction (an orphan, which is how a
future `swap3` would surface) is a `DecodeError` carrying slot, signature and reason. A
transaction that cannot even be mapped (a missing `stackHeight`, an index out of range) is
`DecodeError::Unmappable` carrying the `MapError` text, and the block still maps. All of
these are persisted in `decode_failure` within the block's transaction so a later redecode
can replay them by signature; the cursor still advances because losing the cursor would lose
far more. A swap instruction with no `Swap` event in its window is also a `DecodeError`.

Amounts and fees come from `Swap`. Mints come from the parent instruction's accounts 6 and
7; `user` from account 10 (equal to the event's `from` in every fixture). `fee` is in raw
units of the fee token, which the pool's collect-fee mode decides; the API returns it raw
and never scales it.

### 3.3 Identity

`swap_ordinal` is the zero-based position of the swap instruction among all qualifying swap
instructions of the transaction in execution order. The identity of a swap is
`(signature, swap_ordinal)`. Nothing else is stored for identity.

### 3.4 Skips

Transactions with `meta.err` set are dropped when the block is mapped, before any
instruction is examined; one in four transactions touching DLMM fails, and a failed route
can carry executed `Swap` events before the failing instruction (fixture below). Vote
transactions never reference the program and so never pass the filter.

### 3.5 Decimals

The transaction does not carry decimals. On first sight of a mint the processor schedules a
`getMultipleAccounts` fetch (`dataSlice { offset: 44, length: 1 }` works for SPL Token and
Token-2022 mints; up to 100 per call) and persists it in `token`. Decoding never waits on
this: raw amounts are stored regardless, and the API scales them with `token.decimals` at
read time, returning null for the human field until known. USD pricing needs only the three
quote assets' decimals, which are constants (SOL 9, USDC 6, USDT 6).

## 4. Program design

### 4.1 Actors

| actor | owns | handles | sends |
|---|---|---|---|
| `GeyserSource` | `GeyserGateway` (stream typestate, backoff), stall timer | stream messages, `Shutdown` | `on_block(Live)` |
| `RangeFiller` | `RpcGateway` (client, governor limiter, backon retry, version ceiling), open job list, current page; in tail mode the per-tick `getBlocks` listing | job scan tick, `OnJobsOpened` nudge, `Shutdown` | `on_block(Fill)` or `on_block(Live)` |
| `BlockProcessor` | the DB connection (sole writer of swap, projection, coverage and job tables), coverage floor, pool and token caches, quote allowlist, popped block awaiting retry | `on_block`, `on_prices_filled`, reconcile tick (10 s), `Shutdown` | `OnJobsOpened` nudge, decimals fetch requests |
| `PriceFeed` | `BinanceGateway` (client, weighted limiter, pause-until, retry), sweep cursor | tick (15 s), sweep tick (60 s), `Shutdown` (its own control channel) | `on_prices_filled(MinuteRange)` on the fill channel, `try_send` |

Channel capacities are explicit newtypes: live blocks 64, fill blocks 16, nudges 16,
price notifications 16. A full channel applies backpressure to its producer, which is
correct: a source that outruns the database must slow down, not drop.

### 4.2 `BlockProcessor` transaction

For every `on_block`:

1. `decode_block(&block) -> DecodedBlock` (pure): swaps plus decode failures.
2. `enrich(decoded, &pool_cache, &token_cache, &allowlist) -> EnrichedBlock` (pure):
   resolves `(mint_in, mint_out)` from direction, picks the quote leg.
3. One database transaction: upsert new `pool` rows; upsert new `token` rows (decimals
   null); insert swaps with `ON CONFLICT DO NOTHING ... RETURNING`, computing `volume_usd`
   in SQL from the latest eligible `price` row in `(block_time - 60 s, block_time]`; `project(&inserted) -> ProjectionDeltas` (pure) over
   the rows actually inserted, then one upsert per projection table adding the deltas;
   insert `decode_failure` rows; then, for every origin, `cover(parent_slot, slot,
   block_time)` merges the block into `slot_coverage` (§2.4); for `Fill` also set
   `slot_range_job.next_slot = GREATEST(next_slot, next_slot_after)`. There is no cursor
   row: the cursor is derived from coverage's top range, so it can never disagree with
   what is stored, and a replayed block leaves coverage as it was.
4. After commit: log `block_indexed`; queue decimals fetches for unknown mints.

Between blocks, the processor's loop also runs the reconcile tick (§2.5), so job rows have
the same single writer as everything else.

**Event sourcing.** The `swap` table is the event log: append-only, immutable except for the
late-bound `volume_usd`, totally ordered by `LogPosition (slot, transaction_index,
swap_ordinal)`. Everything the API serves is a projection: a pure fold over events producing
deltas for keys, applied with `value = value + delta`. Every projection is a commutative
monoid (sums, counts, min, max), so a gap fill arriving hours late adds to a historical
bucket without caring about order. Only rows the insert actually returned feed `project`, so
a replayed block projects nothing and "ingest twice changes nothing" holds for the
projections too. Repricing is the second event type: the `UPDATE` that sets `volume_usd`
returns its rows and projects `volume_usd + v, unpriced_swap_count - 1`.

Projections: `pool_volume_1h` (per pool per hour), `pool_volume_1d` (per pool per UTC
day, its own fold over the same inserted rows, decided 2026-10-04) and `pool_stats` (per
pool, all time). Every swap row also records its `source` (`live_geyser`, `live_rpc`,
`fill`) so the reconciliation between sources is auditable; `fill_job_id` names the job.

**Back-processing.** `projection` holds one row per projection: `name`, `version`, cursor
(`LogPosition`), `state`. Adding or changing a projection is the subcommand
`indexer rebuild-projection <name>`: truncate the projection's table, zero the cursor, walk
the log in pages ordered by `LogPosition`, apply the same `project` function, advance the
cursor per page in the same transaction (a kill resumes), mark live. For this assignment it
runs offline with the indexer stopped; on restart the time it was stopped is a coverage hole
and the reconciler and filler close it. The subcommand ships in increment 2 (decided 2026-10-04). An online variant (apply a new swap immediately to every
projection whose cursor has passed its position, leave it for the replay otherwise) is
correct because projections commute, and is a next step. The requirement this puts on today:
the log stores the whole event (`start_bin_id`, `end_bin_id`, `transaction_index`, the raw
`Swap2Evt` bytes); projections store only what the API serves.

On a database error the processor keeps the popped block, retries it with backoff, and
exits after a bounded number of consecutive failures so the restart policy takes over.
Nothing partial is ever committed.

The processor is the only writer to `swap`, `pool`, `token`, `decode_failure`,
`slot_coverage`, `slot_range_job` (the filler only sets `blocked_reason`). `PriceFeed` is the only writer to `price`. The one
cross-table operation, repricing swaps after prices arrive, runs in the processor on
`on_prices_filled`.

### 4.3 Functional core

Pure, mockless, fixture-testable: `map_rpc_block`, `map_geyser_block`, `decode_block`,
`decode_transaction`, `find_swap_instructions`, `pair_events`, `decode_swap_event`,
`account_key_table`, `quote_leg`, `next_page`, `walk_start`, `verify_parent_chain`,
`classify_rpc_error`, `align_range`, `project` (and its per-projection folds
`project_pool_volume_1h`, `project_pool_stats`).

The shell is `main.rs` wiring, the four actor loops, the gRPC, RPC and HTTP clients, and
`store`. No trait objects anywhere. Tests feed `FinalizedBlock` values built from fixtures
straight into the pure functions and the store.

### 4.4 Typestates

- `GeyserStream<Disconnected> -> connect() -> GeyserStream<Connected> ->
  subscribe(filter, from_slot) -> GeyserStream<Subscribed> -> next_block()`. Only the
  subscribed state yields blocks; reconnect consumes the stream back to `Disconnected`.
- Block lifecycle: `FinalizedBlock -> DecodedBlock -> EnrichedBlock -> StoredBlock`. Each
  transition consumes its input; `StoredBlock` is the post-commit receipt the log line and
  the aggregate refresh take, so nothing downstream runs before commit.
- Swap lifecycle: `DecodedSwap -> EnrichedSwap` (adds `mint_in`, `mint_out`, `quote_leg`).

Rejected as ceremony: a typestate over job progress (`next_slot` is the state) and a
priced/unpriced swap state (pricing is SQL).

### 4.5 Newtypes and enums

`Signature([u8; 64])`, `Slot(u64)`, `UnixSeconds(i64)`, `PoolAddress([u8; 32])`,
`MintAddress([u8; 32])`, `UserAddress([u8; 32])`, `AccountAddress([u8; 32])`,
`TokenAmountRaw(u64)`, `Decimals(u8)`, `SwapOrdinal(u16)`, `StackHeight(u8)`,
`SlotRange { start, end_inclusive }`, `MinuteRange`, `JobId(i64)`, `PriceUsd(Decimal)`,
`ChannelCapacity(usize)`, `RequestsPerSecondMax(u32)`, `TransactionIndex(u16)`,
`TransactionVersionMax(u8)`, `LogPosition { slot, transaction_index, swap_ordinal }`,
`BinId(i32)`, `HourBucket(UnixSeconds)`.

Enums instead of booleans: `SwapDirection { XToY, YToX }`, `QuoteAsset { Sol, Usdc, Usdt }`,
`BlockOrigin { Live, Fill { job_id, next_slot_after } }`, `PriceSource { Binance, Peg,
CarriedForward }`, `RpcErrorClass { Retry, SkippedSlot, MissingInStorage, ConfigurationBug }`,
`LiveSource { Geyser, RpcTail }`, `ProjectionName { PoolVolume1h, PoolStats }`,
`ProjectionState { Building, Live }`, `OutputFormat { Table, Json, Raw }` (CLI).

### 4.6 Error handling per actor

- `GeyserSource`: any stream error or stall is a reconnect, never a crash.
- `RangeFiller`: per the error classes; a blocked job is reported, never completed.
- `BlockProcessor`: described in §4.2; decode errors are data, not failures.
- `PriceFeed`: failures delay prices; swaps stay unpriced until the sweep fills them.

### 4.7 Shutdown

`SIGTERM`: sources stop producing, the processor drains both block channels, commits,
exits. Compose `stop_grace_period` 30 s.

### 4.8 Assertions

`debug_assert!` only, for internal invariants the code itself establishes: `swap_ordinal`
strictly increasing within a transaction; `next_slot_after > slot`; a covered block's
`parent_slot < slot`; open jobs listed in strictly descending `end_slot`; channel capacities non-zero; the allowlist has three distinct mints; a
`StoredBlock` is produced at most once per block. Properties of chain input (payload
lengths, stack heights, `parent_slot < slot`, monotone `block_time`) are `Result`s.

## 5. Pricing

**Model.** USD volume is the quote-side leg of the swap times the quote asset's latest USD
price at the swap's block time. Dune's `add_amount_usd_dex_trades` prices the trusted side first;
DefiLlama counts a swap once on whichever side has a price. Verified on 2026-10-01 over the
top 2,491 DLMM pools by 24-hour volume ($317M, effectively all volume of 132,999 pools):
53.9 percent is quoted in USDC, 41.7 percent in SOL, 2.3 percent has SOL or USDC on the x
side, 2.1 percent has no SOL, USDC or USDT side. Three series price about 98 percent.

**Quote leg** (pure): the allowlist is three mints, never symbols (a fake "USDT" pool
exists). Exactly one allowlisted side is the quote; both (SOL-USDC) picks the stable;
neither leaves `quote_asset` null and the swap unpriced.

**Deterministic price** (squashed 2026-10-05). A `price` row is keyed `(asset, ts, source)`,
where `ts` is the instant the price was observed: for a Binance one-minute candle, its close
time (open plus 60 s). The price of a swap at time `t` is the latest row in the half-open
window `(t - PRICE_AGE_MAX_SECONDS, t]` (60 s) for its quote asset and an eligible source:
the SQL function `price_at(asset, t, sources, age)` (in the migration, inlined by the planner
onto `price_pkey`), which both the swap insert and the reprice sweep call through a
`LATERAL` join, so the two paths cannot drift apart. It orders `ts DESC, (source =
sources[1]) DESC, source`: ties on `ts` go to the configured market by name, not by enum
order, so a market appended to the enum later still wins them. On the one-minute grid this is exactly the previous rule, the close of the
last candle closed at `t`, and a future price is never used. Eligible sources are
`PRICE_SOURCE` (env, default `binance`, a `price_source` market value) plus `peg` and
`carried_forward`, which the feed derives itself (USDT's 1, and the previous close repeated
over an exchange gap) and which are part of the market's series rather than rival sources.
Rows are immutable: `PriceFeed` stores only closed candles, and if Binance has no candle for
a minute, once it is five minutes settled, the sweep writes a `carried_forward` row at that
`ts`. A swap misses only when no eligible row lies within the bound; it stays unpriced until
the sweep writes one. A minute the sweep finds unseeded (no candle in it and none in the hour
before it, once settled) is remembered in memory and left out of the span later sweeps read,
so a permanent exchange gap stops widening every sweep window; it is still repriced if a
later window happens to carry a price into it. Because the lookup is a pure function of immutable rows, a swap's
`volume_usd` is the same whichever path stores it and whenever it is computed; "ingest twice
changes nothing" holds for USD. `swap.price_ts` records the `ts` of the row used. The Binance
fetch never runs inside a block's transaction.

**Sources.** SOL from Binance `SOLUSDT` 1-minute klines (public, no key, weight 2 per call,
decimal strings, up to 1000 candles per call); USDC from `USDCUSDT`; USDT is 1 with `source
= 'peg'`. No API key: Binance limits public market data by IP (6000 weight per minute) and a
key changes nothing for klines; our load is one call every 15 s plus 44 calls for a month of
backfill. `PRICE_API_BASE_URL` defaults to `https://data-api.binance.vision`, the host
Binance's docs name for key-less market data, because `api.binance.com` returns HTTP 451 to
US egress; `api.binance.us` serves the same shape (*likely*) and the `data.binance.vision`
daily CSV zips are the bulk fallback. Jupiter's price API has no history and Birdeye needs a
key, so neither serves backfill. Considered and deferred to §13: deriving SOL/USD from our
own indexed SOL-USDC pools (self-contained, but a minute's price is final only once every
block of that minute is indexed) and one-hop pool pricing for the 2 percent of volume in
pools with no quote asset.

**Filling.** Every 60 s and at boot the sweep asks for the oldest and newest
`block_time` of `swap WHERE volume_usd IS NULL AND quote_asset IS NOT NULL` (each end an
ordered scan of the partial `swap_unpriced` index that stops at its first row, stepping over
the minutes already found unpriceable, which the feed keeps for a week per quote asset),
fetches the missing minutes
in chunks of 1000, writes them, and sends `on_prices_filled(range)`. The processor runs one
`UPDATE ... RETURNING` over unpriced swaps in that range with the same SQL function and
projects the returned rows as `volume_usd + v, unpriced_swap_count - 1`, in the same
transaction. Null-only is sufficient because a priced row can never change. This one
mechanism covers live stalls, gap fills, backfills and crashes.

**Precision.** Raw amounts are `u64` in Rust and the `u64` domain, `NUMERIC(20,0)` checked
to the u64 range, in the database (u64 exceeds `BIGINT`). Prices are decimal strings parsed into `rust_decimal::Decimal` and
stored as `NUMERIC(18,8)`. No `f64` touches a stored value. Sums use `NUMERIC` and are
returned as strings.

**Unpriced.** The API reports `unpriced_swap_count` per bucket and `volume_usd` null when a
bucket is empty or wholly unpriced; a zero is never shown for an unknown.

## 6. Data schema (TimescaleDB 2.20 or newer, pg17)

Image `timescale/timescaledb:2.30.x-pg17` (the `WITH (tsdb.*)` form needs 2.20+; exact
tag *to confirm* when pinning). TimescaleDB has one job here: chunking and later
compressing the event log. Projections are plain tables maintained by the processor, so
there are no continuous aggregates, refresh policies or real-time unions. Verified rules
that shaped the DDL: a unique index on a hypertable must contain the partition column, and
`ON CONFLICT DO NOTHING ... RETURNING` works on hypertables.

```sql
CREATE EXTENSION IF NOT EXISTS timescaledb;
-- Exclusion constraints over slot ranges.
CREATE EXTENSION IF NOT EXISTS btree_gist;

-- Domains: the invariants the Rust newtypes enforce, visible in the schema (2026-10-04).
CREATE DOMAIN solana_address   AS TEXT CHECK (VALUE ~ '^[1-9A-HJ-NP-Za-km-z]{32,44}$');
CREATE DOMAIN solana_signature AS TEXT CHECK (VALUE ~ '^[1-9A-HJ-NP-Za-km-z]{86,88}$');
-- A domain names a range, never a unit: the unit (base units, a rate x 1e9) is in the column.
CREATE DOMAIN u64              AS NUMERIC(20,0) CHECK (VALUE >= 0 AND VALUE <= 18446744073709551615);

CREATE TYPE quote_asset      AS ENUM ('sol', 'usdc', 'usdt');
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
    quote_asset          quote_asset,                -- null = unpriceable pool
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
CREATE INDEX swap_unpriced   ON swap (block_time) WHERE volume_usd IS NULL AND quote_asset IS NOT NULL;

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
    asset       quote_asset   NOT NULL,
    ts          TIMESTAMPTZ   NOT NULL,
    source      price_source  NOT NULL,
    close_usd   NUMERIC(18,8) NOT NULL,
    PRIMARY KEY (asset, ts, source)
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

CREATE FUNCTION usd_value(amount_raw NUMERIC, decimals SMALLINT, close_usd NUMERIC)
RETURNS NUMERIC LANGUAGE sql IMMUTABLE AS $$
    SELECT amount_raw * close_usd / power(10::numeric, decimals)
$$;

-- The one price rule, shared by the swap insert and the reprice sweep (§5).
CREATE FUNCTION price_at(price_asset quote_asset, at_time TIMESTAMPTZ, sources price_source[],
                         age_seconds BIGINT)
RETURNS TABLE (ts TIMESTAMPTZ, close_usd NUMERIC) LANGUAGE sql STABLE AS $$
    SELECT p.ts, p.close_usd
    FROM price p
    WHERE p.asset = price_asset
      AND p.source = ANY (sources)
      AND p.ts <= at_time
      AND p.ts > at_time - age_seconds * INTERVAL '1 second'
    ORDER BY p.ts DESC, (p.source = sources[1]) DESC, p.source
    LIMIT 1
$$;

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

-- api_reader is created by the db container's init script (roles are cluster-global and
-- sqlx migrations cannot read the environment); the migration only grants, if the role exists.
DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'api_reader') THEN
    GRANT SELECT ON pool, token, swap, price, slot_coverage, slot_range_job,
                    decode_failure, pool_volume_1h, pool_volume_1d, pool_stats, projection
      TO api_reader;
  END IF;
END $$;
```


Reasons for non-obvious choices:

- `TEXT` base58 keys rather than `BYTEA`: rows must be readable in a live interview and in
  `metclanker`; the size cost is noted under scaling.
- Unique key leads with `signature` so it also serves the swaps endpoint's lookups; the
  `swap_log_order` index serves projection rebuilds.
- `volume_x` and `volume_y` are per-side totals regardless of direction, which is what
  "volume in tokens" means for a pool; the API converts with `token.decimals`.
- `fee_rate_1e9` is the event's `fee_bps`, which is really the fee rate scaled by 1e9
  (fixtures show 108039 and 100000000); stored in the `u64` domain like the amounts (the
  event field is wider, and a rate past u64 is refused as a misdecode), and named by its
  scale because a rate is dimensionless (lamports and wei are units of SOL and ETH, not
  scales for ratios).
- Units: every amount column holds the token's base units (u64), sums stay in base units,
  and only the API divides by `token.decimals`. No human-readable twin columns: they
  double storage and create two sources of truth, and decimals are often unknown at
  insert (author, 2026-10-04, after comparing with ft-backend's `_hmr` columns).
- SQL domains: `solana_address` and `solana_signature` as base58-checked `TEXT`, and `u64`
  as `NUMERIC(20,0)` checked to the u64 range for the nine amount columns and
  `fee_rate_1e9`. A domain carries the range, the column name carries the unit (squashed
  2026-10-05 from two domains with the same range). They put the invariants the Rust
  newtypes already enforce into the schema a reviewer reads, at no runtime cost.
- `price` keeps `source` in the key so a second market can be stored beside the first
  and chosen by `PRICE_SOURCE` without rewriting rows; the lookup's range scan on
  `(asset, ts)` is served by the primary key.
- Update granularity: projection rows are upserted once per block for the keys that
  block touched, never per swap; an idle pool is never written. A pool active in every
  block gets a HOT update of its `pool_stats` row per block, which autovacuum absorbs;
  batching deltas across blocks is the lever if it ever shows.
- `fill_job_id` proves live and fill overlap safely: a swap reached by both paths keeps the
  first origin.
- Projection sums are `NUMERIC(39,0)` because a sum of u64 values exceeds u64.

**Insert, in prose.** One statement per block inserts all swaps via `unnest` arrays with
`ON CONFLICT (signature, swap_ordinal, block_time) DO NOTHING RETURNING`, computing
`volume_usd` through a `LEFT JOIN LATERAL` that takes the latest eligible `price` row with
`ts` in `(block_time - 60 s, block_time]` (§5) and `usd_value(quote_amount, quote_decimals,
p.close_usd)`. The reprice sweep uses the same lookup in a CTE, since an `UPDATE` target
cannot be referenced from a `LATERAL` in its own `FROM`. The returned rows feed `project`; the deltas are applied with
one `INSERT ... ON CONFLICT (pool, bucket) DO UPDATE SET swap_count = pool_volume_1h.swap_count
+ EXCLUDED.swap_count, ...` per projection table.

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
services; the package name avoids the reserved `core`.

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
pub struct ReconcileSummary { pub opened_job_count: u32, pub completed_job_count: u32 }

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
pub struct RequestsPerSecondMax(u32);
pub struct TokenBucket { capacity: u32, refill_count_per_second: u32 }

// core/src/store/ — write path pub(crate); read path pub
pub(crate) async fn write_block(/* conn, EnrichedBlock, BlockOrigin */) -> Result<StoredBlock, StoreError>;   // covers (parent_slot, slot] in the block's transaction
pub(super) async fn cover(/* tx, parent_slot: Slot, slot: Slot, block_time: UnixSeconds */) -> Result<(), StoreError>;
pub(crate) async fn read_cursor(/* executor */) -> Result<Option<Cursor>, StoreError>;
pub async fn reconcile(/* conn, floor: Option<Slot>, Option<ArchiveWindow> */) -> Result<ReconcileSummary, StoreError>;   // pub for core/tests/processor.rs
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
| `GET /v1/health` | always 200 while the database answers: `{ status, cursor_slot, last_block_time, lag_seconds, open_job_count, blocked_job_count }`; `cursor_slot` and `last_block_time` are the top `slot_coverage` range's `end_slot` and `end_block_time` (the newest indexed block, swaps or not), `lag_seconds` is now minus that time; open counts jobs with `completed_at IS NULL AND blocked_reason IS NULL AND next_slot <= end_slot`, blocked those with `completed_at IS NULL AND blocked_reason IS NOT NULL`; `status` precedence: `starting` (no cursor), `lagging` (`lag_seconds > 120`), `blocked` (`blocked_job_count > 0`), `backfilling` (`open_job_count > 0`), else `ok` |
| `GET /v1/pools?limit=50` | pools ordered by 24-hour volume from `pool_volume_1h`, for the CLI picker |
| `GET /v1/pools/{pool}/volume?bucket=hour\|day&from=<rfc3339>&to=<rfc3339>` | the required endpoint |
| `GET /v1/pools/{pool}/swaps?limit=20` | most recent swaps with signature, ordinal, amounts, fee raw, `fill_job_id` |

**Validation for volume**, in order: `pool` parses as base58 and exists (404); `bucket`
present; `from` and `to` parse as RFC 3339 (any offset, converted to UTC); `from` is
floored and `to` ceiled to the bucket boundary and the effective range echoed; `from < to`
after alignment; then the cap: 744 hourly buckets (31 days) or 366 daily. Buckets are
`[start, end)` labelled by `start`, aligned to UTC midnight and the top of the hour.

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
Ink and React were rejected: fifteen transitive dependencies whose retained-mode rendering
fights verbatim proof output. Bars are hand-rolled block characters, about 25 lines.

Commands: `metclanker health`, `metclanker pools [--limit]`, `metclanker volume [--pool]
[--bucket hour|day] [--from] [--to | --range 24h|7d|30d] [--compare] [--base-url]
[--timeout-ms] [--output table|json|raw] [--no-input] [--no-color]`, `metclanker swaps
[--pool] [--limit]`.

Interactive mode runs only when stdin and stdout are TTYs and neither `--no-input` nor a
non-table `--output` is set. Each missing flag triggers only its own prompt. The spinner
shows the request line while fetching and stops with `200 OK · 143 ms · 8.2 KB`; the table
has a right-aligned bar column scaled to the largest bucket, followed by a `note` with the
equivalent `curl` command.

**Proof.** `--output json` prints one JSON object and nothing else to stdout: `{ meta: {
tool, version, generated_at }, request: { method, url, params, headers }, response: {
status, latency_ms, headers, body_bytes }, data: [buckets], summary: { total_usd,
max_bucket } }`, stable key order, no ANSI. `--output raw` prints the request line, status,
headers and verbatim body. `--compare` additionally fetches Meteora's own numbers from
`https://dlmm.datapi.meteora.ag/pools/{address}/volume/history` (timeframe `1h` or `24h`,
`start_time`, `end_time`; 30 requests per second) and shows them beside ours per bucket with
the difference, which is the strongest "the system works" evidence a reviewer can get in one
command. Diagnostics go to stderr. Exit codes: 0 ok, 1 non-2xx or invalid JSON, 2 usage,
3 network or timeout, 130 cancelled. Non-TTY with missing flags exits 2, never hangs.

**Skill.** `.agents/skills/metclanker/SKILL.md` per the agentskills.io format (`name`
equal to the directory, `description` with triggers, `compatibility` naming Bun and the API
URL). Body: "always use `--output json --no-input`", the command and flag table, the output
envelope contract, exit codes, two examples, failure handling (retry once on 3, surface
status and body on 1). Symlinked under `.claude/skills/` like the other skills.

## 10. Operations

`docker-compose.yml`:

- `db`: pinned `timescale/timescaledb:2.30.x-pg17`, named volume, `healthcheck:
  pg_isready`, `TS_TUNE_MEMORY` set to the compose memory limit.
- `indexer`: `depends_on: db: condition: service_healthy`; runs `sqlx migrate run` then the
  actors; `restart: unless-stopped`; `stop_grace_period: 30s`; healthcheck tests that
  `/tmp/healthy`, touched by the processor on every commit and by the tail poll on every
  tick, is younger than two minutes.
- `api`: `depends_on: indexer: condition: service_healthy` (so migrations exist); port
  8080; `healthcheck: curl -f /v1/health`; `restart: unless-stopped`.
- `metclanker` is not a service; the README runs it with `bun run` or the compiled binary.

Environment (`.env.example` committed, `.env` ignored): `DATABASE_URL`,
`DATABASE_URL_READONLY`, `API_READER_PASSWORD`, `RPC_URL`, `RPC_REQUESTS_PER_SECOND_MAX`,
`RPC_TRANSACTION_VERSION_MAX` (default 1), `GEYSER_ENDPOINT` and `GEYSER_X_TOKEN`
(optional; absent means RPC tail), `GEYSER_TRANSACTION_VERSION_MAX` (default 1),
`PRICE_API_BASE_URL` (default `https://data-api.binance.vision`), `BACKFILL_FROM`
(optional), `LOG_FORMAT`.

Logging: `tracing` JSON lines with `slot`, `signature`, `job_id`; one INFO `block_indexed`
per block with swap and duplicate counts; one WARN per decode failure; counters through
`/v1/health` only. One multi-stage Dockerfile builds both binaries, selected by compose
`command`.

**README path** (the ten-minute test), in order, each with expected output:

1. `cp .env.example .env`, paste a free Helius or QuickNode RPC URL. (1 min)
2. `docker compose up -d`; `docker compose logs -f indexer` shows `migrations_applied`,
   `rpc_tail_started`, then one `block_indexed` line about every 270 ms. (3 min)
3. `curl localhost:8080/v1/health` returns `status: ok` with a recent `last_block_time`.
4. `bun install && bun run metclanker pools` lists pools with volume in the last minutes.
5. `bun run metclanker volume --pool <top pool> --range 24h --bucket hour --compare`
   renders the table and Meteora's figures beside it.
6. Optional: set `GEYSER_ENDPOINT` and restart to switch the live source; set
   `BACKFILL_FROM` on a fresh database to see a backfill job progress in `/v1/health`.

**Later, out of scope.** ECS Fargate: one `indexer` service (desired count 1) and one `api`
service (count 2 behind an ALB); the database on Timescale Cloud or a self-managed instance
(managed PostgreSQL offerings generally lack the Timescale extension, *to confirm* per
provider); secrets in Secrets Manager; Geyser from the provider's nearest regional
endpoint. GCP equivalents: Cloud Run for the API, one Compute Engine or GKE pod for the
indexer.

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

The failed fixture is the trap: its `meta.innerInstructions` contains a `Swap` event CPI
although the transaction failed at instruction 2, so a decoder that does not check
`meta.err` first counts a swap that never settled. Each decoder test asserts the exact swap
count, ordinals, pool, mints, user, direction, amounts and fee against values read from an
explorer, not from the decoder. A fixture whose token-balance deltas differ from the event
amounts (a Token-2022 transfer-fee mint) would prove "amounts from the event" directly and
is worth adding if one is found.

Idempotency test against the real database (compose `db`): build a `FinalizedBlock` from
two fixtures with a seeded `price` row, run `write_block` twice as `Live`, then once
as `Fill`, and after each pass assert `count(*)`, `sum(amount_in)`, `sum(volume_usd)`,
every `price_ts`, every `pool_volume_1h` and `pool_stats` row, and every
`slot_coverage` row are identical. The projection rows are the strongest assertion: increment-based read models
are where double counting hides. One test covers duplicates, live-plus-fill overlap, restart replay and USD
determinism.

Coverage tests against the database: a fake chain with skipped slots, covered in a seeded
random order, ends as one range with the last block's time, and covering every block again
changes nothing; `reconcile` opens one job per unowned hole (between ranges and down to the
floor) nearest the tip first, skips holes an open or blocked job owns, and completes a job
coverage contains; a processor-level test drives a live jump, one reconcile, the fill and the
completion; the exclusion constraint rejects an overlapping job.

Pure tests: `verify_parent_chain` with a missing slot; `quote_leg` for the four allowlist cases;
`align_range` at month and day boundaries and the cap order; `classify_rpc_error`;
`swap_ordinal` determinism (decode a fixture twice, compare); `map_rpc_block` drops failed
and vote transactions.

Not tested: Geyser connectivity (needs a credential; a manual runbook step), Binance HTTP
(a recorded response feeds the parser), Axum routing beyond one end-to-end smoke test
through the real API against the compose database.

## 12. Scaling: choke points and the queue architecture

The task's creator said the grading emphasis is designing for scale: identify the choke
points of an indexer and address them, with message queues (relayed 2026-10-03). This
section is that answer. Correctness and idempotency stay first; scale is what the same
invariants look like across more machines.

### 12.1 Choke points

| # | choke point | symptom | this design | at scale |
|---|---|---|---|---|
| 1 | ingress bandwidth | 8 MB JSON per block, about 3.7 blocks/s (0.27 s slots measured in 2026-10), free RPC tiers answer 429 with 10 s `Retry-After`; a day of history is about 320k `getBlock` calls | Geyser is primary (one protobuf message per finalized block, no polling, `from_slot` replay), the RPC tail is the fallback, both behind one two-lane GCRA limiter so fills never starve live | a Geyser producer appends every finalized block to a Kafka or Redpanda topic keyed by slot; the topic absorbs bursts, replays history and fans out to consumers; multiple Geyser providers can produce the same topic and dedupe on slot |
| 2 | decode CPU | one 8 MB block parsed and walked per 270 ms | pure functions, parsed once, no clones on the hot path | stateless decoder consumers, one per topic partition, scaled horizontally; identity `(signature, swap_ordinal)` makes redelivery harmless |
| 3 | the single writer | one transaction per block, about 7 round trips, 3 ms; the cursor is one row | 80 times headroom at today's rate; the hot path never waits on RPC | writers partitioned by pool over the same unique key: a decoded block fans out by pool to N writer partitions, each with its own `(partition, slot)` offset; the global cursor becomes the minimum offset; a slot is complete when every partition has committed it |
| 4 | hot projection rows | a pool active in every block updates its `pool_stats` row about 3.7 times a second | one upsert per touched key per block; HOT updates; autovacuum absorbs it | projections become consumers of the swap topic with their own cursors (the `projection` table already models them); deltas batched per N blocks or per second; rebuild is a replay from offset zero |
| 5 | price lookups | one lateral lookup per block, the latest row in `(t - 60 s, t]`; the sweep scans unpriced rows | immutable price rows, partial index `swap_unpriced`, bounded sweeps | the price feed as its own consumer writing a prices topic; repricing as a compaction job |
| 6 | the event log | tens of millions of rows a day at full DLMM volume; `TEXT` keys cost 88 bytes a signature | hypertable, 1-day chunks, covering indexes, `TEXT` kept for debuggability | columnstore compression after 7 days, `BYTEA` keys, then ClickHouse: `ReplacingMergeTree ORDER BY (pool, block_time, signature, swap_ordinal)` with dedupe at merge (so the block cursor remains the exactly-once guard and reads use `FINAL`), buckets as `AggregatingMergeTree` fed by incremental materialized views |
| 7 | reads | every request hits the primary | a read-only role, projection tables, bounded ranges | read replicas; the API reads projections only, so it never touches the log |
| 8 | gap-fill backlog | about 13,300 `getBlock` per hour of gap, about 55 minutes on Helius free (its fill lane is 4 rps); a day needs a paid tier; DLMM is about 70 transactions per slot (about 23M a day), a few percent of each block, so block fetching moves about 15 times the bytes it needs but 50 times fewer metered calls than fetching those transactions one by one | fill lane, parent-chain verification, blocked jobs that never complete silently; an Old Faithful archive as the deep-history endpoint (§2.5): holes a week or more old cost zero provider calls, at 0.5 to 0.7 blocks/s per server | the topic itself is the replay for anything inside retention; beyond it, deep replay (LaserStream 48 h); signature paging plus batched `getTransaction` where bandwidth, not calls, is what the provider meters (lookup-table coverage verified); for bulk history, several archive servers or local CAR files, and a sidecar that discovers newly published epochs and writes their configs (today they are hardcoded) |

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

Ordering and exactly-once under partitioning: blocks are totally ordered by slot inside
one partition of `finalized_blocks`; swaps are keyed by pool so each writer sees one pool's
swaps in log order; every insert is still `ON CONFLICT DO NOTHING RETURNING`, so a consumer
replaying its partition from its last committed offset inserts nothing twice and projects
nothing twice. Gap detection moves from `parent_slot` on one cursor to a per-partition
offset plus a slot-completeness table; the semantics are unchanged.

What each current module becomes: `GeyserSource` and the RPC tail become producers;
`decode_block` and `enrich` become the decoder consumer unchanged; `write_block` becomes
the writer consumer; `project` and `apply_deltas` become the
projection consumer; `PriceFeed` becomes a producer. Nothing in the functional core
changes, which is the point of having one.

### 12.3 Why not now

A queue adds a broker to run, a second serialisation boundary, and consumer-offset
management, and the assignment's rate (a few blocks a second, tens of swaps each) is two
orders of magnitude below where any of the choke points bind. The design keeps the queue
out and keeps the seams in: `FinalizedBlock` is the one message type, the post-commit
`block_indexed` point is where a producer attaches, and the `projection` table already
carries per-projection cursors.

## 13. Next steps (another week)

1. Decode the pre-May-2026 `Swap2Evt` layout from the stored raw payloads (the 0.12.0
   layout is decoded at ingest since 2026-10-04), as a replay with no chain access.
2. Geyser as the default live source with `from_slot` replay measured per provider.
3. Price the long tail by one hop: a token in an exotic pair takes its own SOL or USDC
   pool's bin price in the same minute (`(1 + bin_step / 10000) ^ end_bin_id`, scaled by
   decimals), with a liquidity floor and a staleness cutoff; removes most of the 2 percent
   unpriced share. Optionally derive SOL/USD from the SOL-USDC pools the same way and drop
   Binance.
4. `metclanker compare` as a scheduled check against Meteora's Data API with alerting on
   drift.
5. Online projection rebuild alongside live indexing; Prometheus metrics; a redecode
   command that replays `decode_failure` rows.
6. If projections ever leave the block transaction (an async fold worker, as ft-backend's
   aggregate worker does), they get their own monotonic cursor and a measured lag in
   health; never triggers, whose cascades capped ft-backend's worker at about 240 rows a
   minute before they went statement-level.
6. Per-pool fee volume and per-user endpoints on the same aggregate.
7. Coverage-aware `compare`: expose each job's time range from the API so a comparison
   skips (or flags) hours whose slots are still owed by an open or blocked job.
8. (withdrawn 2026-10-04: both ends of every job are real blocks, so no progress-only
   message is needed.)

## 14. Decision log

| decision | alternative rejected | why |
|---|---|---|
| Geyser `blocks` filter at finalized, one block per message | `transactions` filter | a block is an atomic unit with `parent_slot` for gap detection; transactions carry no completeness signal |
| RPC tail as the fallback live source | require Geyser | the grader's ten-minute path on a free RPC; zero new mechanism |
| Finalized only | confirmed plus reorg handling | allowed by the task; removes a class of code |
| One `RangeFiller` over `getBlocks` and `getBlock` | `getSignaturesForAddress` | slot-anchored and exactly-once friendly; the alternative pages by signature with cross-node ordering differences |
| One RPC fetch shape, `getBlocks` then `getBlock`, for tail, gap fill and backfill | `getSlot` + per-slot `getBlock` in the tail; signature paging | one mechanism means one decoder, one writer and one failure mode; blocks cost ~50x fewer metered calls than per-transaction fetching for DLMM's ~70 transactions a slot |
| Job table is the filler's queue; `OnJobsOpened` is a nudge | message as the only trigger | a crash between commit and send loses nothing |
| Completeness derived from `slot_coverage` by a reconciler | write-time gap verdict + cursor table | derived facts can be re-derived; catches gaps from any cause; completion is verified not claimed |
| `BlockProcessor` is the sole writer | per-actor writes | removes every cross-actor consistency question |
| USD as a pre-priced column, last closed price at the block time, computed in SQL | join at query time; compute in Rust; a lookback longer than one price interval | aggregates cannot join; one formula; path-independent value |
| Quote-leg pricing from three mints | per-token prices from Jupiter or Birdeye | 98 percent coverage; Jupiter has no history; Birdeye needs a key |
| Binance 1-minute klines | CoinGecko | minute granularity is enterprise-only there |
| TimescaleDB, hypertable, one hourly aggregate | plain PostgreSQL; ClickHouse; hourly plus daily aggregates | relational, exact `NUMERIC`, unique-key idempotency; a day is 24 hourly rows |
| `TEXT` base58 keys | `BYTEA` | readability in a live interview |
| Two Rust crates, write path `pub(crate)` | one crate; three crates | the compiler enforces CQRS; a third crate is ceremony |
| `decode_failure` table | log line only | "never silently lose swaps" needs a durable record |
| Decode `Swap`, ignore `Swap2Evt` | prefer `Swap2Evt` as Meteora suggests | `Swap` is always emitted and stable since 2024; `Swap2Evt` changed layout in 0.12.0 |
| `generate_series` for empty buckets | `time_bucket_gapfill` | plain SQL, explicit |
| `commander` plus `@clack/prompts` | Ink | two deps versus fifteen; verbatim proof output |
| No `EventSink` trait | trait with a logging impl | one implementation is not a seam; the log line is the hook |
| Event-sourced projections maintained by the processor | TimescaleDB continuous aggregates | one pure `project` function defines each read model, tested with fixtures, rebuildable from the log; removes refresh policies, real-time unions and the no-join rule |
| Offline projection rebuild | online rebuild with a position rule | zero new concurrency logic for the assignment; projections commute so online is a next step |
| Store raw `Swap2Evt` bytes | decode now or discard | 147 bytes a row buys "decode later" as a rebuild instead of a re-index |
| `governor` plus `backon` | `tower` limiter, `backoff`, `reqwest-retry` | GCRA with weighted permits; maintained; `Retry-After` and JSON-RPC body classification |
| Gateways with a version ceiling each from the environment | one global constant | a new transaction version means a new mapper and a re-index, so the operator raises it per source |
| `encoding: json` for RPC blocks | `base64` | no Solana wire-format crates; 20 percent more bytes |
| Store `fee_rate_1e9` | drop it | the task's fee semantics may need the rate; one `u64` column |
| Base units only in the database | raw plus human-readable twins (ft-backend `_hmr`) | one source of truth; conversion at the API edge; decimals may be unknown at insert |
| SQL domains for addresses, signatures and base amounts | plain TEXT and NUMERIC | invariants visible in the schema at no runtime cost (ft-backend's `uint256`, `eth_address`) |
| Projections synchronous in the block transaction | async fold worker with its own checkpoint | strongly consistent, no lag to measure; the `projection` cursor table reserves the async path |
| `price(asset, ts, source)`: latest eligible row in `(block_time - 60 s, block_time]`, source in the key (2026-10-05) | `price_minute` keyed by asset and minute with an exact-minute match | `ts` is when the price was observed, so the rule reads as "the last price known at `t`" and works for any source cadence; on the one-minute grid it equals the old rule; `source` in the key lets a second market sit beside the first behind `PRICE_SOURCE` |
| One `u64` SQL domain for every u64 column; the domain names the range, the column name the unit (2026-10-05) | `amount_base` and `fee_rate_1e9` domains | a domain is a value range, so two domains with one range were one domain twice; units already live in column names (`fee_rate_1e9`, base units of the fee token) |

ADR: exactly one, for the pricing model (pre-priced USD column from a three-mint quote-leg
allowlist at the last closed price at block time). It is hard to reverse (every row carries it),
surprising (a reader expects per-token prices or a query-time join), and a real trade-off
(98 percent coverage against completeness). "Block as unit of work" is the obvious design
and gets no ADR.

## 15. Review outcome and open questions

Applied from the three reviews: RPC tail fallback and README path; chain-input checks as
`Result`s; `EventSink` removed; no vendored conversion code; failed transactions dropped at
mapping; deterministic closed-minute pricing with immutable price rows and the sweep as the
single fill mechanism; daily aggregate removed; job state column removed; `AlreadySeen`
verdict and `GREATEST` cursor; parent-chain verification for missing slots and the two extra
RPC error codes; durable `decode_failure` with orphan events as errors; `block_time` null
rejected; advisory lock; biased `select!`; backfill job bounds and single creation;
`maxSupportedTransactionVersion` fixed at 1; `fee_bps` not stored; API health never 503 on
lag; compose start order and reader password; all naming and enum nits; sample caveat on
quote shares; Dune attribution; ClickHouse moved to §12; next steps added; failed fixture
found.

Rejected: renaming `core/` to `indexer/` (the author named the service CORE; the package
name already avoids the reserved word); dropping `/v1/pools` (the CLI picker and the README
step need it); dropping `protocol_fee` and `host_fee` (same event, same cost, useful in the
swaps endpoint).

Resolved on 2026-10-03: event-sourced projections replace the continuous aggregate;
rebuild runs offline; raw `Swap2Evt` bytes are stored; initial projections are
`pool_volume_1h` and `pool_stats`.

Resolved on 2026-10-04 (author): keys stay `TEXT` base58 for debuggability; the
`Swap2Evt` fee split is decoded at ingest; SQL domains are adopted; `pool_volume_1d` is a
materialised projection; per-transaction mapping failures are stored in `decode_failure`
before submission; the `rebuild-projection` subcommand ships; `swap.source` records
`live_geyser`, `live_rpc` or `fill`. Geyser is a core source with RPC fallback, and
backfill runs from a timestamp until caught up.

Open for the author:

1. **Geyser provider.** Chainstack add-on ($49, two streams, about 100-slot replay), Triton
   pay-as-you-go ($125 deposit), or Helius LaserStream ($499 per month, 48-hour replay).
   The RPC tail makes this a quality upgrade rather than a blocker.
2. **Backfill default.** None by default (designed), or a fixed one-hour window so the
   grader sees history at once at about 13,300 RPC calls (0.27 s slots)?
3. **Range caps.** 744 hourly and 366 daily buckets per request acceptable?
4. **`--compare` in scope for the assignment** or a next step? It is small and it is the
   best proof the CLI can offer.
