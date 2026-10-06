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
const POOL_ROW = {
  address: POOL, mint_x: VOLUME_BODY.mint_x, mint_y: VOLUME_BODY.mint_y, decimals_x: 9, decimals_y: 6,
  swap_count_24h: 8, volume_usd_24h: "0.3", first_swap_at: "2026-10-01T01:20:00Z", last_swap_at: "2026-10-01T02:10:00Z",
};

// Base58 has no 0, so the index is spelled with z for 0; x pads it to a valid 32-character address
// without colliding (a digit pad would make index 1 and index 11 the same address).
export function syntheticPool(index: number): string {
  return `Pq${String(index).replaceAll("0", "z")}`.padEnd(32, "x");
}

// Ranked by 24-hour USD volume like the API: POOL first, then the synthetic pools in index order.
function rankedPools(pool_count: number) {
  const synthetic = Array.from({ length: pool_count - 1 }, (_, index) => ({
    ...POOL_ROW, address: syntheticPool(index + 1), swap_count_24h: 1, volume_usd_24h: (0.29 - index * 0.001).toFixed(3),
  }));
  return [POOL_ROW, ...synthetic];
}

const SYNTHETIC_SWAP_COUNT = 45;

// Newest first; two swaps share each transaction so the cursor's ordinal is exercised.
function syntheticSwaps(pool: string) {
  return Array.from({ length: SYNTHETIC_SWAP_COUNT }, (_, index) => {
    const transaction = Math.floor(index / 2);
    return {
      signature: `${pool.slice(0, 4)}${String(transaction).replaceAll("0", "z")}`.padEnd(88, "1"),
      swap_ordinal: 1 - (index % 2),
      slot: 5_000 - transaction,
      transaction_index: 0,
      block_time: new Date(Date.parse("2026-10-01T03:00:00Z") - transaction * 1_000).toISOString().replace(".000Z", "Z"),
      amount_in: "1", amount_out: "2", fee: "0", source: "live_rpc", fill_job_id: null,
    };
  });
}
const SWAPS_BODY = {
  pool: POOL,
  swaps: [
    {
      signature: "5".repeat(88),
      swap_ordinal: 0,
      slot: 4_000,
      transaction_index: 0,
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

// GET lists jobs; job 7 is open and cancels, job 8 is already blocked and is refused.
export const BACKFILL_JOBS = [
  {
    job_id: 8, state: "cancelled", start_slot: 500, end_slot: 999, next_slot: 620, end_kind: "block",
    blocked_reason: "cancelled", completed_at: null, created_at: "2026-10-01T03:20:00Z",
  },
  {
    job_id: 7, state: "open", start_slot: 1000, end_slot: 1499, next_slot: 1200, end_kind: "block",
    blocked_reason: null, completed_at: null, created_at: "2026-10-01T03:10:00Z",
  },
];
export const CANCELLED_JOB = { job_id: 7, start_slot: 1000, end_slot: 1499, next_slot: 1200, state: "cancelled" };

function cancel(job_id: number) {
  return (request: Request): Response => {
    if (request.method !== "DELETE") {
      return json({ error: { code: "not_found", message: "no route" } }, 404);
    }
    return job_id === CANCELLED_JOB.job_id
      ? json(CANCELLED_JOB)
      : json({ error: { code: "job_already_blocked", message: "the job is already blocked (cancelled)" } }, 409);
  };
}

async function backfill(request: Request): Promise<Response> {
  if (request.method === "GET") {
    return json({ jobs: BACKFILL_JOBS });
  }
  if (request.method !== "POST") {
    return json({ error: { code: "not_found", message: "no route" } }, 404);
  }
  const body = (await request.json()) as { from?: string };
  return body.from === BACKFILL_INSTANT
    ? json(BACKFILL_JOB, 202)
    : json({ error: { code: "nothing_indexed_yet", message: "nothing is indexed yet" } }, 409);
}

const PAGE_LIMIT_MAX = 100;

function boundedInteger(text: string | null, fallback: number, min: number, max: number): number | null {
  if (text === null) {
    return fallback;
  }
  const value = Number(text);
  return /^(0|[1-9]\d*)$/.test(text) && value >= min && value <= max ? value : null;
}

const badRequest = (code: string) => json({ error: { code, message: code.replaceAll("_", " ") } }, 400);

function listPools(pools: readonly { address: string }[], url: URL): Response {
  const limit = boundedInteger(url.searchParams.get("limit"), 20, 1, PAGE_LIMIT_MAX);
  const offset = boundedInteger(url.searchParams.get("offset"), 0, 0, Number.MAX_SAFE_INTEGER);
  if (limit === null) {
    return badRequest("invalid_limit");
  }
  if (offset === null) {
    return badRequest("invalid_offset");
  }
  return json({ pools: pools.slice(offset, offset + limit), page: { limit, offset, total: pools.length } });
}

type KeyedSwap = { block_time: string; slot: number; transaction_index: number; swap_ordinal: number };
type SwapKey = [number, number, number, number];

function swapKey(swap: KeyedSwap): SwapKey {
  return [Date.parse(swap.block_time), swap.slot, swap.transaction_index, swap.swap_ordinal];
}

// Opaque to the CLI: base64 of block_time|slot|transaction_index|swap_ordinal of the last row returned.
function swapCursor(swap: KeyedSwap): string {
  return btoa(`${swap.block_time}|${swap.slot}|${swap.transaction_index}|${swap.swap_ordinal}`);
}

function parseCursor(text: string): SwapKey | null {
  let decoded: string;
  try {
    decoded = atob(text);
  } catch {
    return null;
  }
  const [block_time, ...numbers] = decoded.split("|");
  const instant = Date.parse(block_time ?? "");
  const integers = numbers.map(Number);
  if (Number.isNaN(instant) || integers.length !== 3 || !integers.every(Number.isSafeInteger)) {
    return null;
  }
  return [instant, ...integers] as SwapKey;
}

function isOlder(key: SwapKey, cursor: SwapKey): boolean {
  const index = key.findIndex((part, position) => part !== cursor[position]);
  return index !== -1 && key[index]! < cursor[index]!;
}

function listSwaps(pool: string, swaps: readonly KeyedSwap[], url: URL): Response {
  const limit = boundedInteger(url.searchParams.get("limit"), 20, 1, PAGE_LIMIT_MAX);
  if (limit === null) {
    return badRequest("invalid_limit");
  }
  const before = url.searchParams.get("before");
  const cursor = before === null ? null : parseCursor(before);
  if (before !== null && cursor === null) {
    return badRequest("invalid_cursor");
  }
  const older = cursor === null ? swaps : swaps.filter((swap) => isOlder(swapKey(swap), cursor));
  const page = older.slice(0, limit);
  const last = page.at(-1);
  const next_cursor = older.length > limit && last !== undefined ? swapCursor(last) : null;
  return json({ pool, swaps: page, page: { limit, next_cursor } });
}

const POOL_ROUTE = /^\/v1\/pools\/([^/]+)(?:\/(volume|swaps))?$/;

// The pool routes of the section 3 contract, for every pool the list ranks.
function poolRoute(pools: readonly (typeof POOL_ROW)[], url: URL): Response | null {
  const match = POOL_ROUTE.exec(url.pathname);
  if (match === null) {
    return null;
  }
  const [, address, sub] = match;
  const pool = pools.find((candidate) => candidate.address === address);
  if (pool === undefined) {
    return json({ error: { code: "pool_not_found", message: "no such pool" } }, 404);
  }
  if (sub === "volume") {
    return json({ ...VOLUME_BODY, pool: pool.address });
  }
  if (sub === "swaps") {
    return listSwaps(pool.address, pool.address === POOL ? SWAPS_BODY.swaps : syntheticSwaps(pool.address), url);
  }
  return json(pool);
}

type Handler = (request: Request, url: URL) => Response | Promise<Response>;

function routes(pools: readonly { address: string }[]): Record<string, Handler> {
  return {
    "/v1/backfills": backfill,
    "/v1/backfills/7": cancel(7),
    "/v1/backfills/8": cancel(8),
    "/v1/health": () => json(HEALTH_BODY),
    "/v1/pools": (_request, url) => listPools(pools, url),
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

// `pool_count` ranks that many pools (POOL first), so paging can be exercised past one page.
export function startMockApi(options: { pool_count?: number } = {}): MockApi {
  const requested_urls: URL[] = [];
  const pools = rankedPools(options.pool_count ?? 1);
  const handlers = routes(pools);
  const server = Bun.serve({
    port: 0,
    fetch: (request) => {
      const url = new URL(request.url);
      requested_urls.push(url);
      const handler = handlers[url.pathname];
      if (handler !== undefined) {
        return handler(request, url);
      }
      return poolRoute(pools, url) ?? json({ error: { code: "not_found", message: "no route" } }, 404);
    },
  });
  return { base_url: `http://127.0.0.1:${server.port}`, requested_urls, stop: () => server.stop(true) };
}

export type RunResult = { exit_code: number; stdout: string; stderr: string };

export async function runCli(argv: string[], env: Record<string, string> = {}, terminal: Partial<Pick<Io, "stdin_is_tty" | "stdout_is_tty">> = {}): Promise<RunResult> {
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
    ...terminal,
  };
  const exit_code = await run(argv, io);
  return { exit_code, stdout, stderr };
}
