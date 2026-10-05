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

export function readSwaps(body: unknown, exchange: Exchange): SwapRow[] {
  const swaps = readList(body, "swaps");
  if (swaps === null || !swaps.every((swap) => isRecord(swap) && typeof swap.signature === "string")) {
    throw shapeFailure(exchange, "a swaps list");
  }
  return swaps as SwapRow[];
}
