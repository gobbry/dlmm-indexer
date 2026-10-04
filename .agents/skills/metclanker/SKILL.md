---
name: metclanker
description: "Read the Meteora DLMM swap indexer's REST API with the metclanker CLI: indexer health and lag, pools ranked by 24-hour USD volume, hourly or daily volume per pool in tokens and USD, recent swaps, and a per-bucket comparison against Meteora's own Data API. Use when asked about indexed DLMM swap volume, a pool's hourly or daily volume, whether the indexer is healthy or lagging, recent swaps of a pool, or whether our numbers match Meteora's; also for producing JSON proof of an API response."
compatibility: "Requires Bun 1.3+ (run from cli/ with `bun run src/cli.ts`, or the compiled cli/dist/metclanker) and the indexer API at http://127.0.0.1:8080 (override with --base-url or METCLANKER_BASE_URL). --compare also needs network access to https://dlmm.datapi.meteora.ag."
---

# metclanker

Agents always pass `--output json --no-input`. JSON mode prints exactly one JSON object on
stdout and nothing else; diagnostics go to stderr. `--no-input` guarantees no prompt: a
missing flag exits 2 instead of waiting for a terminal.

```sh
cd cli && bun run src/cli.ts <command> [flags] --output json --no-input
```

## Commands and flags

| command | flags | API call |
|---|---|---|
| `health` | | `GET /v1/health` |
| `pools` | `--limit N` (default 50) | `GET /v1/pools?limit=N` |
| `volume` | `--pool <base58>` `--bucket hour\|day` and either `--range 24h\|7d\|30d` or `--from <rfc3339> --to <rfc3339>`; `--compare` | `GET /v1/pools/{pool}/volume?bucket=&from=&to=` |
| `swaps` | `--pool <base58>` `--limit N` (default 20) | `GET /v1/pools/{pool}/swaps?limit=N` |

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
  "compare": { "request": …, "response": …, "error": null },
  "error": null
}
```

- `compare` is present only with `--compare`.
- `data`: `health` → the health object; `pools` → pool list; `volume` → the API's buckets
  (`start`, `swap_count`, `volume_x`, `volume_x_raw`, `volume_y`, `volume_y_raw`,
  `volume_usd`, `unpriced_swap_count`); `swaps` → swap list.
- `summary`: `volume` → `total_usd` (exact decimal string), `max_bucket`, `bucket_count`,
  `swap_count`, `unpriced_swap_count`, `bucket`, `from`, `to`; `pools` → `pool_count`,
  `total_volume_usd_24h`; `swaps` → `pool`, `swap_count`; `health` → `status`, `lag_seconds`.
- Health `status`, first match wins: `starting` (no cursor yet), `lagging` (`lag_seconds >
  120`), `blocked` (`blocked_job_count > 0`: a slot range the node cannot serve; needs an
  operator), `backfilling` (`open_job_count > 0`: gap or backfill ranges still being
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

## Failure handling

- Exit 3: retry once with the same arguments. If it fails again, report that the API at
  `request.url` is unreachable or timed out (check `health`, `--base-url`, `--timeout-ms`).
- Exit 1: do not retry. Report `error.status` and `error.body` verbatim; a 404 means the pool
  is unknown to the indexer, a 400 carries the validation code (for example
  `invalid_range`).
- Exit 2: fix the arguments using the table above; never re-run without `--no-input`.
- `--compare` failures keep our `data` and put Meteora's failure in `compare.error` and
  `error`; the exit code follows the same table.
- `METCLANKER_METEORA_BASE_URL` points `--compare` at another Meteora deployment, such as
  `https://dlmm.dev.metdev.io`.
