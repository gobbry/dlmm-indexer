import type { VolumeBucket } from "../api_types.ts";
import { renderBars } from "../bars.ts";
import type { ComparedBucket } from "../compare.ts";
import { sumDecimalStrings } from "../decimal.ts";
import { alignRight, type Row } from "../format.ts";

export const VOLUME_COLUMNS = ["start", "swaps", "volume_x", "volume_y", "volume_usd", "volume"] as const;
export const COMPARE_COLUMNS = ["start", "swaps", "volume_x", "volume_y", "volume_usd", "meteora_usd", "difference", "volume"] as const;

const NOT_AVAILABLE = "n/a";
const PARTIAL = "partial";

export function maxBucket(buckets: readonly VolumeBucket[]): { start: string; volume_usd: string } | null {
  let best: { start: string; volume_usd: string } | null = null;
  for (const bucket of buckets) {
    if (bucket.volume_usd !== null && (best === null || Number(bucket.volume_usd) > Number(best.volume_usd))) {
      best = { start: bucket.start, volume_usd: bucket.volume_usd };
    }
  }
  return best;
}

export function volumeSummary(buckets: readonly VolumeBucket[]) {
  const priced = buckets.flatMap((bucket) => (bucket.volume_usd === null ? [] : [bucket.volume_usd]));
  return {
    total_usd: sumDecimalStrings(priced),
    max_bucket: maxBucket(buckets),
    bucket_count: buckets.length,
    swap_count: buckets.reduce((total, bucket) => total + bucket.swap_count, 0),
    unpriced_swap_count: buckets.reduce((total, bucket) => total + bucket.unpriced_swap_count, 0),
  };
}

export function compareSummary(rows: readonly ComparedBucket[]) {
  const compared = rows.filter((row) => row.comparison === "compared");
  const total = (values: number[]) => Math.round(values.reduce((sum, value) => sum + value, 0) * 100) / 100;
  return {
    compared_bucket_count: compared.length,
    not_indexed_bucket_count: rows.filter((row) => row.comparison === "not_indexed").length,
    partial_bucket_count: rows.filter((row) => row.comparison === "partial").length,
    meteora_total_usd: total(compared.map((row) => row.meteora_volume_usd ?? 0)),
    difference_usd: total(compared.map((row) => row.difference_usd ?? 0)),
  };
}

function baseRow(bucket: VolumeBucket, bar: string): Row {
  return {
    start: bucket.start,
    swaps: String(bucket.swap_count),
    volume_x: bucket.volume_x ?? `${bucket.volume_x_raw} raw`,
    volume_y: bucket.volume_y ?? `${bucket.volume_y_raw} raw`,
    volume_usd: bucket.volume_usd ?? "-",
    volume: bar,
  };
}

function formatUsd(value: number | null): string {
  return value === null ? "-" : value.toFixed(2);
}

function compareCells(row: ComparedBucket): Row {
  if (row.comparison === "not_indexed") {
    return { volume_usd: NOT_AVAILABLE, meteora_usd: formatUsd(row.meteora_volume_usd), difference: NOT_AVAILABLE };
  }
  if (row.comparison === "partial") {
    return { meteora_usd: formatUsd(row.meteora_volume_usd), difference: PARTIAL };
  }
  return { meteora_usd: formatUsd(row.meteora_volume_usd), difference: formatUsd(row.difference_usd) };
}

export function volumeRows(buckets: readonly VolumeBucket[]): Row[] {
  const bars = renderBars(buckets.map((bucket) => Number(bucket.volume_usd ?? 0)));
  const rows = buckets.map((bucket, index) => baseRow(bucket, bars[index] ?? ""));
  return alignRight(rows, ["swaps", "volume_x", "volume_y", "volume_usd"]);
}

export function compareRows(compared: readonly ComparedBucket[]): Row[] {
  const bars = renderBars(compared.map((bucket) => Number(bucket.volume_usd ?? 0)));
  const rows = compared.map((bucket, index) => ({ ...baseRow(bucket, bars[index] ?? ""), ...compareCells(bucket) }));
  return alignRight(rows, ["swaps", "volume_x", "volume_y", "volume_usd", "meteora_usd", "difference"]);
}
