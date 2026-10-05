import type { Io } from "../src/io.ts";
import { run } from "../src/run.ts";

export const POOL = "5rCf1DM8LjKTw4YqhnoLcngyZYeNnQqztScTogYHAS6";
export const MISSING_POOL = "11111111111111111111111111111111";
export const BROKEN_POOL = "BrokenPoo11111111111111111111111111";
export const GARBLED_POOL = "GarbPoo111111111111111111111111111";
export const SLOW_POOL = "SLowPoo111111111111111111111111111";
export const NOW = new Date("2026-10-01T03:30:00Z");

const HOUR_STARTS = ["2026-10-01T00:00:00Z", "2026-10-01T01:00:00Z", "2026-10-01T02:00:00Z", "2026-10-01T03:00:00Z"];

function bucket(start: string, swap_count: number, volume_usd: string | null) {
  const raw = String(swap_count * 1_000_000);
  return {
    start, swap_count,
    volume_x: String(swap_count), volume_x_raw: raw,
    volume_y: String(swap_count * 100), volume_y_raw: raw,
    volume_usd, unpriced_swap_count: 0,
  };
}

// Our indexer started mid-hour at 01:20, so 00:00 is not indexed and 01:00 is partial; NOW is
// 03:30, so 03:00 is still open and partial too.
export const VOLUME_BODY = {
  pool: POOL, mint_x: "So11111111111111111111111111111111111111112", mint_y: "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
  decimals_x: 9, decimals_y: 6, bucket: "hour", from: HOUR_STARTS[0], to: "2026-10-01T04:00:00Z",
  first_swap_at: "2026-10-01T01:20:00Z",
  buckets: [bucket(HOUR_STARTS[0]!, 0, null), bucket(HOUR_STARTS[1]!, 3, "0.1"), bucket(HOUR_STARTS[2]!, 5, "0.2"), bucket(HOUR_STARTS[3]!, 0, null)],
};

export const METEORA_VOLUMES = [9, 0.15, 0.25, 0];
const METEORA_BODY = {
  start_time: Date.parse(HOUR_STARTS[0]!) / 1_000, end_time: Date.parse(HOUR_STARTS[3]!) / 1_000, timeframe: "1h",
  data: HOUR_STARTS.map((start, index) => ({
    timestamp: Date.parse(start) / 1_000, timestamp_str: start, volume: METEORA_VOLUMES[index], fees: 0, protocol_fees: 0,
  })),
};

const HEALTH_BODY = { status: "ok", cursor_slot: 1, last_block_time: "2026-10-01T03:29:59Z", lag_seconds: 1, open_job_count: 0, blocked_job_count: 0 };
const POOLS_BODY = { pools: [{ address: POOL, mint_x: VOLUME_BODY.mint_x, mint_y: VOLUME_BODY.mint_y, swap_count_24h: 8, volume_usd_24h: "0.3" }] };
const SWAPS_BODY = {
  pool: POOL,
  swaps: [
    {
      signature: "5".repeat(88),
      swap_ordinal: 0,
      block_time: "2026-10-01T02:10:00Z",
      amount_in: "1",
      amount_out: "2",
      fee: "0",
      protocol_fee: "0",
      host_fee: "0",
      mm_fee: "0",
      limit_order_fee: "0",
      amount_left: "0",
      fee_side: "input",
      fee_token: "x",
      source: "live_rpc",
      fill_job_id: null,
    },
  ],
};

const json = (body: unknown, status = 200) => Response.json(body, { status });

export type MockApi = { base_url: string; requested_urls: URL[]; stop: () => void };

// The API's answers for a backfill: a job for BACKFILL_INSTANT, nothing indexed for anything else.
export const BACKFILL_INSTANT = "2026-10-01T03:27:00Z";
export const BACKFILL_JOB = { job_id: 7, start_slot: 1000, end_slot: 1499 };

async function backfill(request: Request): Promise<Response> {
  if (request.method !== "POST") {
    return json({ error: { code: "not_found", message: "no route" } }, 404);
  }
  const body = (await request.json()) as { from?: string };
  return body.from === BACKFILL_INSTANT
    ? json(BACKFILL_JOB, 202)
    : json({ error: { code: "nothing_indexed_yet", message: "nothing is indexed yet" } }, 409);
}

function routes(): Record<string, (request: Request) => Response | Promise<Response>> {
  return {
    "/v1/backfills": backfill,
    "/v1/health": () => json(HEALTH_BODY),
    "/v1/pools": () => json(POOLS_BODY),
    [`/v1/pools/${POOL}/volume`]: () => json(VOLUME_BODY),
    [`/v1/pools/${POOL}/swaps`]: () => json(SWAPS_BODY),
    [`/v1/pools/${MISSING_POOL}/volume`]: () => json({ error: { code: "pool_not_found", message: "no such pool" } }, 404),
    [`/v1/pools/${BROKEN_POOL}/volume`]: () => json({ error: { code: "internal", message: "database unavailable" } }, 500),
    [`/v1/pools/${GARBLED_POOL}/volume`]: () => new Response("<html>gateway</html>", { status: 200 }),
    [`/v1/pools/${SLOW_POOL}/volume`]: async () => {
      await Bun.sleep(500);
      return json(VOLUME_BODY);
    },
    [`/pools/${POOL}/volume/history`]: () => json(METEORA_BODY),
  };
}

export function startMockApi(): MockApi {
  const requested_urls: URL[] = [];
  const handlers = routes();
  const server = Bun.serve({
    port: 0,
    fetch: (request) => {
      const url = new URL(request.url);
      requested_urls.push(url);
      const handler = handlers[url.pathname];
      return handler === undefined ? json({ error: { code: "not_found", message: "no route" } }, 404) : handler(request);
    },
  });
  return { base_url: `http://127.0.0.1:${server.port}`, requested_urls, stop: () => server.stop(true) };
}

export type RunResult = { exit_code: number; stdout: string; stderr: string };

export async function runCli(argv: string[], env: Record<string, string> = {}): Promise<RunResult> {
  let stdout = "";
  let stderr = "";
  const io: Io = {
    write_stdout: (text) => { stdout += text; },
    write_stderr: (text) => { stderr += text; },
    env,
    stdin_is_tty: false,
    stdout_is_tty: false,
    stderr_is_tty: false,
    now: () => NOW,
  };
  const exit_code = await run(argv, io);
  return { exit_code, stdout, stderr };
}
