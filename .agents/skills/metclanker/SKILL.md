---
name: metclanker
description: "Read the Meteora DLMM swap indexer's REST API with the metclanker CLI: indexer health and lag, pools ranked by 24-hour USD volume, hourly or daily volume per pool in tokens and USD, recent swaps, a per-bucket comparison against Meteora's own Data API, and backfill requests. Use when asked about indexed DLMM swap volume, a pool's hourly or daily volume, whether the indexer is healthy or lagging, recent swaps of a pool, or whether our numbers match Meteora's; to index history from an earlier time (backfill); also for producing JSON proof of an API response."
compatibility: "Requires Bun 1.3+ (run from cli/ with `bun run src/cli.ts`, or the compiled cli/dist/metclanker) and the indexer API at http://127.0.0.1:8080 (override with --base-url or METCLANKER_BASE_URL). --compare also needs network access to https://dlmm.datapi.meteora.ag."
---

# metclanker

Agents always pass `--output json --no-input`. JSON mode prints exactly one JSON object on
stdout and nothing else; diagnostics go to stderr. `--no-input` guarantees no prompt: a
missing flag exits 2 instead of waiting for a terminal.
`--interactive` (`-i`) opens a terminal browser for people; agents must not use it (it exits 2
without a terminal, with `--no-input` or with `--output json`).

```sh
cd cli && bun run src/cli.ts <command> [flags] --output json --no-input
```

## Commands and flags

| command | flags | API call |
|---|---|---|
| `health` | | `GET /v1/health` |
| `pools` | `--limit N` (1 to 100, default 50) `--offset N` (default 0) | `GET /v1/pools?limit=N&offset=N` |
| `volume` | `--pool <base58>` `--bucket hour\|day` and either `--range 24h\|7d\|30d` or `--from <rfc3339> --to <rfc3339>`; `--compare` | `GET /v1/pools/{pool}/volume?bucket=&from=&to=` |
| `swaps` | `--pool <base58>` `--limit N` (1 to 100, default 20) | `GET /v1/pools/{pool}/swaps?limit=N` |
| `backfill` | `--from <rfc3339>` (not in the future) | `POST /v1/backfills` with `{"from": …}` |
| `backfill` | `--list` | `GET /v1/backfills` (the 50 newest jobs) |
| `backfill` | `--cancel <job_id>` | `DELETE /v1/backfills/{job_id}` |

Global flags, accepted before or after the command:

| flag | default | meaning |
|---|---|---|
| `--base-url <url>` | `METCLANKER_BASE_URL` or `http://127.0.0.1:8080` | API base URL |
| `--timeout-ms <ms>` | `10000` | per-request timeout |
| `--output table\|json\|raw` | `table` | `json` for agents; `raw` prints request line, status, headers, verbatim body |
| `--no-input` | | never prompt |
| `--no-color` | | no colour (`NO_COLOR` also honoured) |

`--range` windows end now and are aligned outward to the bucket (a 24h hourly range is
25 buckets including the current partial hour). `--from`/`--to` are sent verbatim; the API
floors `from` and ceils `to` and echoes the effective window. Caps: 744 hourly or 366 daily
buckets per request.

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

- `page` is present only for `pools`: `total` is every indexed pool, so read past the first
  100 with `--offset` (`--limit` above 100 exits 2). The ranking moves as blocks land, so a
  pool can cross a page boundary between calls.
- `compare` is present only with `--compare`. A `backfill` envelope's `request` also carries
  `body`, the JSON sent.
- `data`: `health` → the health object; `pools` → pool list; `volume` → the API's buckets
  (`start`, `swap_count`, `volume_x`, `volume_x_raw`, `volume_y`, `volume_y_raw`,
  `volume_usd`, `unpriced_swap_count`); `swaps` → swap list; `backfill` → the job the API
  inserted (`job_id`, `start_slot`, `end_slot`); `backfill --list` → job list (`job_id`,
  `state`, `start_slot`, `end_slot`, `next_slot`, `end_kind`, `blocked_reason`,
  `completed_at`, `created_at`), newest first; `backfill --cancel` → the cancelled job
  (`job_id`, `start_slot`, `end_slot`, `next_slot`, `state: "cancelled"`).
- `summary`: `volume` → `total_usd` (exact decimal string), `max_bucket`, `bucket_count`,
  `swap_count`, `unpriced_swap_count`, `bucket`, `from`, `to`; `pools` → `pool_count`,
  `total_volume_usd_24h`; `swaps` → `pool`, `swap_count`; `health` → `status`, `lag_seconds`;
  `backfill` → `job_id`, `slot_count`; `backfill --list` → `job_count`, `state_counts`;
  `backfill --cancel` → `job_id`, `state`, `slot_count_unfilled`.
- Health `status`, first match wins: `starting` (no cursor yet), `lagging` (`lag_seconds >
  120`), `blocked` (`blocked_job_count > 0`: a slot range the node cannot serve; needs an
  operator), `backfilling` (`open_job_count > 0`: holes in coverage still being
  filled, so recent buckets may be incomplete), else `ok`.
- With `--compare` each bucket also carries `meteora_volume_usd` (number), `difference_usd`
  (ours minus Meteora's, rounded to cents, `null` unless `compared`) and `comparison`:
  - `not_indexed`: the bucket starts before the one holding `first_swap_at` (or, without
    it, the first non-empty bucket), so our zero means "not indexed".
  - `partial`: our figure is a lower bound, so no difference is computed. The bucket holds
    `first_swap_at` (indexing began mid-bucket), has `unpriced_swap_count > 0`, or has not
    closed yet (it contains now).
  - `missing_upstream`: Meteora returned no point for that bucket.
  - `compared`: everything else; only these carry a difference.

  `summary.compare` has `compared_bucket_count`, `not_indexed_bucket_count`,
  `partial_bucket_count`, and `meteora_total_usd` and `difference_usd` summed over
  `compared` buckets only. The table prints `n/a` or `partial` in the difference column.
- On failure `data` and `summary` are `null` and `error` is
  `{ kind, message, status, body }`; `kind` is `http`, `invalid_json`, `network` or
  `timeout`; `body` is the API's error envelope (`{ "error": { "code", "message" } }`) when
  there was a response. `response` is `null` when no response arrived. Usage errors print
  nothing on stdout.

Amounts are strings. `volume_usd` is `null` for empty or wholly unpriced buckets. Meteora
returns its in-progress bucket as `0`; that bucket is `partial` and never compared.

## Exit codes

| code | meaning |
|---|---|
| 0 | success |
| 1 | non-2xx response or a body that is not the expected JSON |
| 2 | usage: unknown command, missing or invalid flag |
| 3 | network failure or timeout |
| 130 | cancelled at a prompt |

## Examples

Daily volume for the last 7 days:

```sh
bun run src/cli.ts volume --pool 5rCf1DM8LjKTw4YqhnoLcngyZYeNnQqztScTogYHAS6 \
  --bucket day --range 7d --output json --no-input
```

Check our hourly numbers against Meteora for a fixed window:

```sh
bun run src/cli.ts volume --pool 5rCf1DM8LjKTw4YqhnoLcngyZYeNnQqztScTogYHAS6 --bucket hour \
  --from 2026-10-01T00:00:00Z --to 2026-10-01T06:00:00Z --compare --output json --no-input
```

Find a pool address first with `pools --limit 10 --output json --no-input` and read
`data[].address`.

Index history from an earlier time, then watch the job through `health` (`status` is
`backfilling` and `open_job_count` counts it until it is filled):

```sh
bun run src/cli.ts backfill --from 2026-10-04T12:00:00Z --output json --no-input
```

The API resolves the instant to a slot over RPC before it answers, so `backfill` waits at
least 120 s whatever `--timeout-ms` says. It answers 202 with the job, or a refusal: 400
`invalid_backfill_body`, `backfill_from_in_future` or `backfill_from_after_coverage` (the
instant is not older than what is indexed); 409 `nothing_indexed_yet` or
`backfill_overlaps_job` (an earlier backfill owns part of the range); 502 `rpc_unavailable`;
503 `backfill_unavailable` (the API runs without `RPC_URL`). Do not resubmit on 409.

A request older than a week is split at the week line by the indexer seconds later; the job
id you were given stays on the newest piece, and `--list` shows the others under their own
ids. Find a job's id with `--list`, and stop a backfill with `--cancel` (only when the user
asks):

```sh
bun run src/cli.ts backfill --list --output json --no-input
bun run src/cli.ts backfill --cancel 7 --output json --no-input
```

A cancel keeps the job as blocked with reason `cancelled`, so the indexer does not reopen its
range; from then on `health` reports `blocked`, which is expected: the history below where the
walk stopped is deliberately missing. Refusals: 404 `job_not_found`; 409
`job_already_completed` or `job_already_blocked` (the message names the existing reason).
Resuming is an operator's database step (README, Backfill), not a CLI command.

## Failure handling

- Exit 3: retry once with the same arguments. If it fails again, report that the API at
  `request.url` is unreachable or timed out (check `health`, `--base-url`, `--timeout-ms`).
- Exit 1: do not retry. Report `error.status` and `error.body` verbatim; a 404 means the pool
  is unknown to the indexer, a 400 carries the validation code (for example
  `invalid_range`), a 409 from `backfill` names why no job was inserted.
- Exit 2: fix the arguments using the table above; never re-run without `--no-input`.
- `--compare` failures keep our `data` and put Meteora's failure in `compare.error` and
  `error`; the exit code follows the same table.
- `METCLANKER_METEORA_BASE_URL` points `--compare` at another Meteora deployment, such as
  `https://dlmm.dev.metdev.io`.
