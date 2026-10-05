import { afterAll, beforeAll, expect, test } from "bun:test";
import {
  BACKFILL_INSTANT, BACKFILL_JOB, BROKEN_POOL, GARBLED_POOL, METEORA_VOLUMES, MISSING_POOL, POOL, SLOW_POOL, VOLUME_BODY,
  runCli, startMockApi, type MockApi,
} from "./mock_api.ts";

const ENVELOPE_KEYS = ["meta", "request", "response", "data", "summary", "error"];
const ANSI_PATTERN = /\x1b\[/;

let api: MockApi;
beforeAll(() => {
  api = startMockApi();
});
afterAll(() => api.stop());

const json = (...argv: string[]) => runCli(["--base-url", api.base_url, "--output", "json", ...argv]);

function parseOnlyObject(stdout: string) {
  expect(stdout).not.toMatch(ANSI_PATTERN);
  const envelope = JSON.parse(stdout);
  expect(typeof envelope).toBe("object");
  return envelope;
}

test("health --output json prints one envelope in contract key order", async () => {
  const result = await json("health");
  const envelope = parseOnlyObject(result.stdout);
  expect(result.exit_code).toBe(0);
  expect(Object.keys(envelope)).toEqual(ENVELOPE_KEYS);
  expect(Object.keys(envelope.meta)).toEqual(["tool", "version", "generated_at"]);
  expect(Object.keys(envelope.request)).toEqual(["method", "url", "params", "headers"]);
  expect(Object.keys(envelope.response)).toEqual(["status", "latency_ms", "headers", "body_bytes"]);
  expect(envelope.meta.generated_at).toBe("2026-10-01T03:30:00.000Z");
  expect(envelope.request.url).toBe(`${api.base_url}/v1/health`);
  expect(envelope.response.status).toBe(200);
  expect(envelope.data.status).toBe("ok");
  expect(result.stderr).toBe("");
});

test("pools --output json passes the limit and lists pools", async () => {
  const result = await json("pools", "--limit", "7");
  const envelope = parseOnlyObject(result.stdout);
  expect(result.exit_code).toBe(0);
  expect(envelope.request.params).toEqual({ limit: "7" });
  expect(envelope.data.map((pool: { address: string }) => pool.address)).toEqual([POOL]);
  expect(envelope.summary).toEqual({ pool_count: 1, total_volume_usd_24h: "0.3" });
});

test("volume --range 24h sends the aligned window and sums USD exactly", async () => {
  const result = await json("volume", "--pool", POOL, "--bucket", "hour", "--range", "24h");
  const envelope = parseOnlyObject(result.stdout);
  expect(result.exit_code).toBe(0);
  expect(envelope.request.params).toEqual({ bucket: "hour", from: "2026-09-30T03:00:00Z", to: "2026-10-01T04:00:00Z" });
  expect(envelope.data).toEqual(VOLUME_BODY.buckets);
  expect(envelope.summary.total_usd).toBe("0.3");
  expect(envelope.summary.max_bucket).toEqual({ start: "2026-10-01T02:00:00Z", volume_usd: "0.2" });
});

test("swaps --output json passes pool and limit", async () => {
  const result = await json("swaps", "--pool", POOL, "--limit", "5");
  const envelope = parseOnlyObject(result.stdout);
  expect(result.exit_code).toBe(0);
  expect(envelope.request.url).toBe(`${api.base_url}/v1/pools/${POOL}/swaps?limit=5`);
  expect(envelope.summary).toEqual({ pool: POOL, swap_count: 1 });
});

test("swaps table shows which source stored each swap", async () => {
  const result = await runCli(["--base-url", api.base_url, "swaps", "--pool", POOL]);
  expect(result.exit_code).toBe(0);
  expect(result.stdout).toMatch(/\bsource\b/);
  expect(result.stdout).toContain("live_rpc");
});

test("volume --compare puts Meteora beside ours and compares only closed, fully indexed buckets", async () => {
  const result = await runCli(
    ["--base-url", api.base_url, "--output", "json", "volume", "--pool", POOL, "--bucket", "hour", "--from", "2026-10-01T00:00:00Z", "--to", "2026-10-01T04:00:00Z", "--compare"],
    { METCLANKER_METEORA_BASE_URL: api.base_url },
  );
  const envelope = parseOnlyObject(result.stdout);
  expect(result.exit_code).toBe(0);
  expect(Object.keys(envelope)).toEqual(["meta", "request", "response", "data", "summary", "compare", "error"]);
  const meteora_url = api.requested_urls.find((url) => url.pathname.endsWith("/volume/history"));
  expect(meteora_url?.searchParams.get("timeframe")).toBe("1h");
  expect(meteora_url?.searchParams.get("start_time")).toBe(String(Date.parse("2026-10-01T00:00:00Z") / 1_000));
  expect(meteora_url?.searchParams.get("end_time")).toBe(String(Date.parse("2026-10-01T04:00:00Z") / 1_000));
  const rows = envelope.data.map((row: { comparison: string; meteora_volume_usd: number; difference_usd: number | null }) => [
    row.comparison, row.meteora_volume_usd, row.difference_usd,
  ]);
  expect(rows).toEqual([
    ["not_indexed", METEORA_VOLUMES[0], null],
    ["partial", METEORA_VOLUMES[1], null],
    ["compared", METEORA_VOLUMES[2], -0.05],
    ["partial", METEORA_VOLUMES[3], null],
  ]);
  expect(envelope.summary.compare).toEqual({
    compared_bucket_count: 1,
    not_indexed_bucket_count: 1,
    partial_bucket_count: 2,
    meteora_total_usd: METEORA_VOLUMES[2],
    difference_usd: -0.05,
  });
});

test("volume table shows n/a, partial, bars, the status line and the curl note", async () => {
  const result = await runCli(
    ["--base-url", api.base_url, "volume", "--pool", POOL, "--bucket", "hour", "--range", "24h", "--compare"],
    { METCLANKER_METEORA_BASE_URL: api.base_url },
  );
  expect(result.exit_code).toBe(0);
  expect(result.stdout).toContain("n/a");
  expect(result.stdout).toContain("partial");
  expect(result.stdout).toContain("████");
  expect(result.stdout).not.toMatch(ANSI_PATTERN);
  expect(result.stderr).toMatch(/^200 OK · \d+ ms · \d+ B$/m);
  expect(result.stderr).toContain(`curl: curl -sS -H 'accept: application/json' '${api.base_url}/v1/pools/${POOL}/volume?`);
});

test("raw output prints the request line, status, headers and the verbatim body", async () => {
  const result = await runCli(["--base-url", api.base_url, "--output", "raw", "health"]);
  const [request_line, status_line] = result.stdout.split("\n");
  expect(result.exit_code).toBe(0);
  expect(request_line).toBe(`GET ${api.base_url}/v1/health`);
  expect(status_line).toBe("HTTP 200 OK");
  expect(result.stdout).toContain("content-type: application/json");
  expect(result.stdout).toContain('\n\n{"status":"ok",');
});

test.each([
  { name: "404 surfaces the API error body", pool: MISSING_POOL, exit_code: 1, kind: "http", status: 404 },
  { name: "500 surfaces the API error body", pool: BROKEN_POOL, exit_code: 1, kind: "http", status: 500 },
  { name: "a non-JSON 200 is invalid_json", pool: GARBLED_POOL, exit_code: 1, kind: "invalid_json", status: 200 },
  { name: "a slow API times out", pool: SLOW_POOL, exit_code: 3, kind: "timeout", status: null },
])("volume failure: $name", async ({ pool, exit_code, kind, status }) => {
  const result = await json("--timeout-ms", "100", "volume", "--pool", pool, "--bucket", "day", "--range", "7d");
  const envelope = parseOnlyObject(result.stdout);
  expect(result.exit_code).toBe(exit_code);
  expect(Object.keys(envelope)).toEqual(ENVELOPE_KEYS);
  expect(envelope.error.kind).toBe(kind);
  expect(envelope.error.status).toBe(status);
  expect(result.stderr).toContain("metclanker:");
  if (status === 404) {
    expect(envelope.error.body).toEqual({ error: { code: "pool_not_found", message: "no such pool" } });
  }
});

test("an unreachable API exits 3 with a network error envelope", async () => {
  const result = await runCli(["--base-url", "http://127.0.0.1:1", "--output", "json", "health"]);
  expect(result.exit_code).toBe(3);
  expect(parseOnlyObject(result.stdout).error.kind).toBe("network");
});

test.each([
  { name: "missing --bucket without a terminal", argv: ["volume", "--pool", POOL, "--range", "24h"] },
  { name: "missing --pool without a terminal", argv: ["swaps"] },
  { name: "--range together with --from", argv: ["volume", "--pool", POOL, "--bucket", "hour", "--range", "24h", "--from", "2026-10-01T00:00:00Z"] },
  { name: "--from without --to", argv: ["volume", "--pool", POOL, "--bucket", "hour", "--from", "2026-10-01T00:00:00Z"] },
  { name: "a pool that is not base58", argv: ["swaps", "--pool", "not-a-pool"] },
  { name: "an unknown --bucket", argv: ["volume", "--pool", POOL, "--bucket", "week", "--range", "24h"] },
  { name: "a non-numeric --limit", argv: ["pools", "--limit", "many"] },
  { name: "an unknown command", argv: ["bogus"] },
])("usage error exits 2 with nothing on stdout: $name", async ({ argv }) => {
  const result = await json(...argv);
  expect(result.exit_code).toBe(2);
  expect(result.stdout).toBe("");
  expect(result.stderr).not.toBe("");
});

test("the real process exits 2 on missing flags with piped stdin instead of prompting", async () => {
  const child = Bun.spawn(["bun", "run", "src/cli.ts", "volume", "--pool", POOL], {
    cwd: `${import.meta.dir}/..`,
    stdin: "pipe",
    stdout: "pipe",
    stderr: "pipe",
  });
  const exit_code = await Promise.race([child.exited, Bun.sleep(5_000).then(() => "hung")]);
  child.kill();
  expect(exit_code).toBe(2);
  expect(await new Response(child.stderr).text()).toContain("missing --bucket");
});

test("backfill --output json posts the instant and returns the job the API inserted", async () => {
  const result = await json("backfill", "--from", BACKFILL_INSTANT);
  const envelope = parseOnlyObject(result.stdout);
  expect(result.exit_code).toBe(0);
  expect(envelope.request.method).toBe("POST");
  expect(envelope.request.url).toBe(`${api.base_url}/v1/backfills`);
  expect(JSON.parse(envelope.request.body)).toEqual({ from: BACKFILL_INSTANT });
  expect(envelope.response.status).toBe(202);
  expect(envelope.data).toEqual(BACKFILL_JOB);
  expect(envelope.summary).toEqual({ job_id: 7, slot_count: 500 });
});

test("backfill exits 1 with the API's envelope when it refuses, and 2 without --from", async () => {
  const refused = await json("backfill", "--from", "2026-10-01T00:00:00Z");
  const envelope = parseOnlyObject(refused.stdout);
  expect(refused.exit_code).toBe(1);
  expect(envelope.error.status).toBe(409);
  expect(envelope.error.body.error.code).toBe("nothing_indexed_yet");
  expect(refused.stderr).toContain("POST");

  const missing = await json("backfill");
  expect(missing.exit_code).toBe(2);
  expect(missing.stderr).toContain("missing --from");
});

