import { CliFailure } from "./failure.ts";
import type { Exchange } from "./http.ts";

export type HealthBody = {
  status: string;
  cursor_slot: number | null;
  last_block_time: string | null;
  lag_seconds: number | null;
  open_job_count: number;
  blocked_job_count: number;
};

export type PoolSummary = {
  address: string;
  mint_x: string;
  mint_y: string;
  decimals_x?: number | null;
  decimals_y?: number | null;
  swap_count_24h?: number;
  volume_usd_24h?: string | null;
  first_swap_at?: string | null;
  last_swap_at?: string | null;
};

export type VolumeBucket = {
  start: string;
  swap_count: number;
  volume_x: string | null;
  volume_x_raw: string;
  volume_y: string | null;
  volume_y_raw: string;
  volume_usd: string | null;
  unpriced_swap_count: number;
};

export type VolumeBody = {
  pool: string;
  mint_x: string;
  mint_y: string;
  bucket: string;
  from: string;
  to: string;
  buckets: VolumeBucket[];
  first_swap_at?: string | null;
};

export type SwapRow = Record<string, unknown> & { signature: string };

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function shapeFailure(exchange: Exchange, expected: string): CliFailure {
  return new CliFailure("invalid_json", `${exchange.request.method} ${exchange.request.url} returned JSON without ${expected}`, { exchange });
}

// The list endpoints are read tolerantly: a bare array or an object wrapping it under its name.
function readList(body: unknown, key: string): unknown[] | null {
  if (Array.isArray(body)) {
    return body;
  }
  if (isRecord(body) && Array.isArray(body[key])) {
    return body[key];
  }
  return null;
}

export function readHealth(body: unknown, exchange: Exchange): HealthBody {
  if (!isRecord(body) || typeof body.status !== "string") {
    throw shapeFailure(exchange, "a status field");
  }
  return body as HealthBody;
}

export function readPools(body: unknown, exchange: Exchange): PoolSummary[] {
  const pools = readList(body, "pools");
  if (pools === null || !pools.every((pool) => isRecord(pool) && typeof pool.address === "string")) {
    throw shapeFailure(exchange, "a pools list");
  }
  return pools as PoolSummary[];
}

export function readVolume(body: unknown, exchange: Exchange): VolumeBody {
  if (!isRecord(body) || !Array.isArray(body.buckets)) {
    throw shapeFailure(exchange, "a buckets list");
  }
  return body as VolumeBody;
}

export type BackfillBody = {
  job_id: number;
  start_slot: number;
  end_slot: number;
};

export function readBackfill(body: unknown, exchange: Exchange): BackfillBody {
  if (!isRecord(body) || typeof body.job_id !== "number") {
    throw shapeFailure(exchange, "a job_id");
  }
  return body as BackfillBody;
}

// A job row as GET /v1/backfills lists it; a cancelled job is a blocked one with reason "cancelled".
export type JobBody = {
  job_id: number;
  state: string;
  start_slot: number;
  end_slot: number;
  next_slot: number;
  end_kind: string;
  blocked_reason: string | null;
  completed_at: string | null;
  created_at: string;
};

export type CancelBody = {
  job_id: number;
  start_slot: number;
  end_slot: number;
  next_slot: number;
  state: string;
};

export function readJobs(body: unknown, exchange: Exchange): JobBody[] {
  const jobs = readList(body, "jobs");
  if (jobs === null || !jobs.every((job) => isRecord(job) && typeof job.job_id === "number")) {
    throw shapeFailure(exchange, "a jobs list");
  }
  return jobs as JobBody[];
}

export function readCancel(body: unknown, exchange: Exchange): CancelBody {
  if (!isRecord(body) || typeof body.job_id !== "number" || typeof body.state !== "string") {
    throw shapeFailure(exchange, "a job_id and state");
  }
  return body as CancelBody;
}

export function readSwaps(body: unknown, exchange: Exchange): SwapRow[] {
  const swaps = readList(body, "swaps");
  if (swaps === null || !swaps.every((swap) => isRecord(swap) && typeof swap.signature === "string")) {
    throw shapeFailure(exchange, "a swaps list");
  }
  return swaps as SwapRow[];
}

// GET /v1/pools pages by offset; `total` is the pool count, so a reader can clamp paging.
export type OffsetPage = { limit: number; offset: number; total: number };

// GET /v1/pools/{pool}/swaps pages by an opaque keyset cursor; null means the log ends here.
export type CursorPage = { limit: number; next_cursor: string | null };

function isOffsetPage(value: unknown): value is OffsetPage {
  return isRecord(value) && typeof value.limit === "number" && typeof value.offset === "number" && typeof value.total === "number";
}

function isCursorPage(value: unknown): value is CursorPage {
  return isRecord(value) && typeof value.limit === "number" && (value.next_cursor === null || typeof value.next_cursor === "string");
}

export function readPoolsPage(body: unknown, exchange: Exchange): { pools: PoolSummary[]; page: OffsetPage } {
  const pools = readPools(body, exchange);
  if (!isRecord(body) || !isOffsetPage(body.page)) {
    throw shapeFailure(exchange, "a page with limit, offset and total");
  }
  return { pools, page: body.page };
}

export function readSwapsPage(body: unknown, exchange: Exchange): { swaps: SwapRow[]; page: CursorPage } {
  const swaps = readSwaps(body, exchange);
  if (!isRecord(body) || !isCursorPage(body.page)) {
    throw shapeFailure(exchange, "a page with limit and next_cursor");
  }
  return { swaps, page: body.page };
}

// GET /v1/pools/{pool} answers the summary bare, the same object as a list entry.
export function readPool(body: unknown, exchange: Exchange): PoolSummary {
  if (!isRecord(body) || typeof body.address !== "string") {
    throw shapeFailure(exchange, "a pool address");
  }
  return body as PoolSummary;
}
