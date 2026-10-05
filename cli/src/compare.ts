import type { VolumeBucket } from "./api_types.ts";
import { BUCKET_SECONDS, floorToBucket, parseRfc3339, type BucketName, type UnixRange } from "./range.ts";

export const METEORA_BASE_URL = "https://dlmm.datapi.meteora.ag";

export type MeteoraPoint = { timestamp: number; volume: number };

export type Comparison = "compared" | "not_indexed" | "partial" | "missing_upstream";

export type ComparedBucket = VolumeBucket & {
  meteora_volume_usd: number | null;
  difference_usd: number | null;
  comparison: Comparison;
};

export function meteoraPath(pool: string): string {
  return `/pools/${pool}/volume/history`;
}

// The docs call end_time inclusive, but observed on 2026-10-03 the bucket starting at end_time
// always comes back as a zero placeholder, so the volume really covers [start_time, end_time).
export function meteoraParams(range: UnixRange, bucket: BucketName): Record<string, string> {
  return {
    timeframe: bucket === "hour" ? "1h" : "24h",
    start_time: String(range.from_unix_seconds),
    end_time: String(range.to_unix_seconds),
  };
}

export function readMeteoraHistory(body: unknown): MeteoraPoint[] | null {
  const data = typeof body === "object" && body !== null ? (body as { data?: unknown }).data : undefined;
  if (!Array.isArray(data)) {
    return null;
  }
  const points = data.filter(
    (point): point is MeteoraPoint =>
      typeof point === "object" && point !== null && typeof point.timestamp === "number" && typeof point.volume === "number",
  );
  return points.length === data.length ? points : null;
}

// Before the indexer saw the pool's first swap its zeros mean "not indexed", not "no volume".
export function firstIndexedStart(
  first_swap_at: string | null | undefined,
  buckets: readonly VolumeBucket[],
  bucket: BucketName,
): number | null {
  const declared = typeof first_swap_at === "string" ? parseRfc3339(first_swap_at) : null;
  if (declared !== null) {
    return floorToBucket(declared, bucket);
  }
  const first_non_empty = buckets.find((candidate) => candidate.swap_count > 0);
  return first_non_empty === undefined ? null : parseRfc3339(first_non_empty.start);
}

function roundCents(value: number): number {
  return Math.round(value * 100) / 100;
}

function oursUsd(bucket: VolumeBucket): number {
  return bucket.volume_usd === null ? 0 : Number(bucket.volume_usd);
}

export type CompareWindow = { first_indexed: number | null; bucket: BucketName; now_unix_seconds: number };

// Partial: indexing began inside it, some swaps lack a USD price, or it has not closed yet.
// Each makes our figure a lower bound, so a difference would be noise, not a finding.
function isPartial(bucket: VolumeBucket, start: number, window: CompareWindow): boolean {
  return (
    start === window.first_indexed ||
    bucket.unpriced_swap_count > 0 ||
    start + BUCKET_SECONDS[window.bucket] > window.now_unix_seconds
  );
}

function compareOne(bucket: VolumeBucket, meteora: number | null, window: CompareWindow): ComparedBucket {
  const start = parseRfc3339(bucket.start);
  const uncompared = { ...bucket, meteora_volume_usd: meteora, difference_usd: null };
  if (window.first_indexed === null || start === null || start < window.first_indexed) {
    return { ...uncompared, comparison: "not_indexed" };
  }
  if (isPartial(bucket, start, window)) {
    return { ...uncompared, comparison: "partial" };
  }
  if (meteora === null) {
    return { ...uncompared, comparison: "missing_upstream" };
  }
  return { ...bucket, meteora_volume_usd: meteora, difference_usd: roundCents(oursUsd(bucket) - meteora), comparison: "compared" };
}

export function compareBuckets(
  buckets: readonly VolumeBucket[],
  points: readonly MeteoraPoint[],
  window: CompareWindow,
): ComparedBucket[] {
  const meteora_by_start = new Map(points.map((point) => [point.timestamp, point.volume]));
  return buckets.map((bucket) => {
    const start = parseRfc3339(bucket.start);
    const meteora = start === null ? null : (meteora_by_start.get(start) ?? null);
    return compareOne(bucket, meteora, window);
  });
}
