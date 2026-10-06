---
name: metclanker
description: "Read the Meteora DLMM swap indexer's REST API with the metclanker CLI. It reports indexer health and lag, pools ranked by 24-hour USD volume, and hourly or daily volume per pool in tokens and USD. It also shows recent swaps and a per-bucket comparison against Meteora's own Data API, and it sends backfill requests. Use it when asked about indexed DLMM swap volume, a pool's hourly or daily volume, or recent swaps of a pool. Also use it when asked whether the indexer is healthy or lagging, or whether our numbers match Meteora's. Also use it to index history from an earlier time (backfill), and to produce JSON proof of an API response."
compatibility: "Requires Bun 1.3+ (run from cli/ with `bun run src/cli.ts`, or the compiled cli/dist/metclanker) and the indexer API at http://127.0.0.1:8080 (override with --base-url or METCLANKER_BASE_URL). --compare also needs network access to https://dlmm.datapi.meteora.ag."
---

# metclanker

Agents always pass `--output json --no-input`. JSON mode prints exactly one JSON object on
stdout and nothing else. Diagnostics go to stderr. `--no-input` guarantees no prompt: if a
flag is missing, the CLI exits 2 and does not wait for a terminal.

`--interactive` (`-i`) opens a terminal browser for people. Agents must not use it. It exits 2
without a terminal, with `--no-input`, or with `--output json`.

```sh
cd cli && bun run src/cli.ts <command> [flags] --output json --no-input
```

## Commands and flags

For the definition of each response field, read `GET /openapi.json` from the API.

| command | flags | API call |
|---|---|---|
| `health` | | `GET /v1/health` |
| `pools` | `--limit N` (1 to 100, default 50) `--offset N` (default 0) | `GET /v1/pools?limit=N&offset=N` |
| `volume` | `--pool <base58>` `--bucket hour\|day` and either `--range 24h\|7d\|30d` or `--from <rfc3339> --to <rfc3339>`, and optionally `--compare` | `GET /v1/pools/{pool}/volume?bucket=&from=&to=` |
| `swaps` | `--pool <base58>` `--limit N` (1 to 100, default 20) | `GET /v1/pools/{pool}/swaps?limit=N` |
| `backfill` | `--from <rfc3339>` (not in the future) | `POST /v1/backfills` with `{"from": …}` |
| `backfill` | `--list` | `GET /v1/backfills` (the 50 newest jobs) |
| `backfill` | `--cancel <job_id>` | `DELETE /v1/backfills/{job_id}` |

Global flags, accepted before or after the command:

| flag | default | meaning |
|---|---|---|
| `--base-url <url>` | `METCLANKER_BASE_URL` or `http://127.0.0.1:8080` | API base URL |
| `--timeout-ms <ms>` | `10000` | per-request timeout |
| `--output table\|json\|raw` | `table` | `json` is for agents. `raw` prints request line, status, headers, verbatim body |
| `--no-input` | | never prompt |
| `--no-color` | | no colour (`NO_COLOR` also honoured) |

A `--range` window ends now. The CLI aligns the window outward to the bucket. For example, a
24h hourly range is 25 buckets, including the current partial hour. The CLI sends `--from` and
`--to` verbatim. The API floors `from`, ceils `to`, and echoes the effective window. Caps: 744
hourly or 366 daily buckets per request.

## Output envelope (`--output json`)

Keys always appear in this order:

```json
{
  "meta": { "tool": "metclanker", "version": "0.1.0", "generated_at": "<rfc3339>" },
  "request": { "method": "GET", "url": "…", "params": { … }, "headers": { … } },
  "response": { "status": 200, "latency_ms": 143, "headers": { … }, "body_bytes": 8396 },
  "data": …,
  "summary": …,
  "page": { "limit": 50, "offset": 0, "total": 312 },
  "compare": { "request": …, "response": …, "error": null },
  "error": null
}
```

- `page` is present only for `pools`. `total` counts every indexed pool. To read past the
  first 100 pools, use `--offset`. A `--limit` above 100 exits 2. The ranking moves as blocks
  land, so a pool can cross a page boundary between calls.
- `compare` is present only with `--compare`. In a `backfill` envelope, `request` also carries
  `body`, the JSON that the CLI sent.
- `data`, per command:
  - `health` → the health object.
  - `pools` → pool list.
  - `volume` → the API's buckets (`start`, `swap_count`, `volume_x`, `volume_x_raw`,
    `volume_y`, `volume_y_raw`, `volume_usd`, `unpriced_swap_count`).
  - `swaps` → swap list.
  - `backfill` → the job that the API inserted (`job_id`, `start_slot`, `end_slot`).
  - `backfill --list` → job list, newest first (`job_id`, `state`, `start_slot`, `end_slot`,
    `next_slot`, `end_kind`, `blocked_reason`, `completed_at`, `created_at`).
  - `backfill --cancel` → the cancelled job (`job_id`, `start_slot`, `end_slot`,
    `next_slot`, `state: "cancelled"`).
- `summary`, per command:
  - `volume` → `total_usd` (exact decimal string), `max_bucket`, `bucket_count`,
    `swap_count`, `unpriced_swap_count`, `bucket`, `from`, `to`.
  - `pools` → `pool_count`, `total_volume_usd_24h`.
  - `swaps` → `pool`, `swap_count`.
  - `health` → `status`, `lag_seconds`.
  - `backfill` → `job_id`, `slot_count`.
  - `backfill --list` → `job_count`, `state_counts`.
  - `backfill --cancel` → `job_id`, `state`, `slot_count_unfilled`.
- Health `status`. The first match wins:
  1. `starting`: no cursor yet.
  2. `lagging`: `lag_seconds > 120`.
  3. `blocked`: `blocked_job_count > 0`. The node cannot serve a slot range. An operator must
     act.
  4. `backfilling`: `open_job_count > 0`. Holes in coverage are still open, so recent buckets
     may be incomplete.
  5. `ok`: no other match.
- With `--compare`, each bucket also carries `meteora_volume_usd` (number), `difference_usd`
  and `comparison`. `difference_usd` is ours minus Meteora's, rounded to cents. It is `null`
  unless `comparison` is `compared`. `comparison` has one of these values:
  - `not_indexed`: the bucket starts before the bucket that holds `first_swap_at`. Without
    `first_swap_at`, the reference is the first non-empty bucket. Our zero means "not
    indexed".
  - `partial`: our figure is a lower bound, so the CLI computes no difference. A bucket is
    `partial` if one of these is true:
    - It holds `first_swap_at` (indexing began mid-bucket).
    - It has `unpriced_swap_count > 0`.
    - It has not closed yet (it contains now).
  - `missing_upstream`: Meteora returned no point for that bucket.
  - `compared`: all other buckets. Only these buckets carry a difference.

  `summary.compare` has `compared_bucket_count`, `not_indexed_bucket_count`,
  `partial_bucket_count`, `meteora_total_usd` and `difference_usd`. The last two are sums over
  `compared` buckets only. The table prints `n/a` or `partial` in the difference column.
- On failure, `data` and `summary` are `null`, and `error` is
  `{ kind, message, status, body }`:
  - `kind` is `http`, `invalid_json`, `network` or `timeout`.
  - `body` is the API's error envelope (`{ "error": { "code", "message" } }`) when there was
    a response.
  - `response` is `null` when no response arrived.
  - Usage errors print nothing on stdout.

Amounts are strings. `volume_usd` is `null` for empty buckets and for wholly unpriced
buckets. Meteora returns its in-progress bucket as `0`. That bucket is `partial`, and the CLI
never compares it.

## Exit codes

| code | meaning |
|---|---|
| 0 | success |
| 1 | non-2xx response or a body that is not the expected JSON |
| 2 | usage: unknown command, missing or invalid flag |
| 3 | network failure or timeout |
| 130 | cancelled at a prompt |

## Examples

Get the daily volume for the last 7 days:

```sh
bun run src/cli.ts volume --pool 5rCf1DM8LjKTw4YqhnoLcngyZYeNnQqztScTogYHAS6 \
  --bucket day --range 7d --output json --no-input
```

Check our hourly numbers against Meteora for a fixed window:

```sh
bun run src/cli.ts volume --pool 5rCf1DM8LjKTw4YqhnoLcngyZYeNnQqztScTogYHAS6 --bucket hour \
  --from 2026-10-01T00:00:00Z --to 2026-10-01T06:00:00Z --compare --output json --no-input
```

To find a pool address, run `pools --limit 10 --output json --no-input` first. Then read
`data[].address`.

To index history from an earlier time, request a backfill. Then watch the job through
`health`. Until the indexer fills the job, `status` is `backfilling` and `open_job_count` counts the
job.

```sh
bun run src/cli.ts backfill --from 2026-10-04T12:00:00Z --output json --no-input
```

Before the API answers, it resolves the instant to a slot over RPC. For this reason,
`backfill` waits at least 120 s, whatever the value of `--timeout-ms`. The API answers 202
with the job, or with one of these refusals:

- 400 `invalid_backfill_body`, `backfill_from_in_future` or `backfill_from_after_coverage`.
  `backfill_from_after_coverage` means that the instant is not older than the indexed data.
- 409 `nothing_indexed_yet` or `backfill_overlaps_job`. `backfill_overlaps_job` means that an
  earlier backfill owns part of the range.
- 502 `rpc_unavailable`.
- 503 `backfill_unavailable`. The API runs without `RPC_URL`.

Do not resubmit on 409.

The indexer splits a request older than a week at the week line, seconds after the request.
The job id that you received stays on the newest piece. `--list` shows the other pieces under
their own ids. To find the id of a job, use `--list`. To stop a backfill, use `--cancel`. Use
`--cancel` only when the user asks.

```sh
bun run src/cli.ts backfill --list --output json --no-input
bun run src/cli.ts backfill --cancel 7 --output json --no-input
```

A cancel keeps the job as blocked with reason `cancelled`, so the indexer does not reopen its
range. From then on, `health` reports `blocked`. This is expected: the history below the point
where the walk stopped is deliberately missing.

Cancel refusals:

- 404 `job_not_found`.
- 409 `job_already_completed` or `job_already_blocked`. The message names the existing
  reason.

To resume a job, an operator does a database step (README, Backfill). The CLI has no resume
command.

## Failure handling

- Exit 3: retry once with the same arguments. If it fails again, report that the API at
  `request.url` is unreachable or timed out (check `health`, `--base-url`, `--timeout-ms`).
- Exit 1: do not retry. Report `error.status` and `error.body` verbatim.
  - A 404 means that the pool is unknown to the indexer.
  - A 400 carries the validation code (for example `invalid_range`).
  - A 409 from `backfill` names why the API inserted no job.
- Exit 2: fix the arguments with the table above. Never re-run without `--no-input`.
- If `--compare` fails, the CLI keeps our `data`. It puts Meteora's failure in
  `compare.error` and `error`. The exit code follows the same table.
- `METCLANKER_METEORA_BASE_URL` points `--compare` at another Meteora deployment, such as
  `https://dlmm.dev.metdev.io`.
