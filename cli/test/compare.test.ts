import { expect, test } from "bun:test";
import type { VolumeBucket } from "../src/api_types.ts";
import { compareBuckets, firstIndexedStart, meteoraParams } from "../src/compare.ts";

const seconds = (text: string) => Date.parse(text) / 1_000;

function bucket(start: string, swap_count: number, volume_usd: string | null, unpriced_swap_count = 0): VolumeBucket {
  return { start, swap_count, volume_x: "0", volume_x_raw: "0", volume_y: "0", volume_y_raw: "0", volume_usd, unpriced_swap_count };
}

const BUCKETS = [
  bucket("2026-10-01T00:00:00Z", 0, null),
  bucket("2026-10-01T01:00:00Z", 2, "100.5"),
  bucket("2026-10-01T02:00:00Z", 3, "100.5"),
  bucket("2026-10-01T03:00:00Z", 0, null),
  bucket("2026-10-01T04:00:00Z", 2, "40", 1),
  bucket("2026-10-01T05:00:00Z", 1, "7"),
];
const POINTS = BUCKETS.map((_, index) => ({ timestamp: seconds("2026-10-01T00:00:00Z") + index * 3_600, volume: 100 }));
const NOW = seconds("2026-10-01T05:30:00Z");

test("only closed, fully indexed, fully priced buckets are compared", () => {
  const first_indexed = firstIndexedStart("2026-10-01T01:45:00Z", BUCKETS, "hour");
  const rows = compareBuckets(BUCKETS, POINTS, { first_indexed, bucket: "hour", now_unix_seconds: NOW });
  expect(rows.map((row) => [row.comparison, row.difference_usd])).toEqual([
    ["not_indexed", null],
    ["partial", null],
    ["compared", 0.5],
    ["compared", -100],
    ["partial", null],
    ["partial", null],
  ]);
});

test("without first_swap_at the first non-empty bucket is where indexing began", () => {
  const first_indexed = firstIndexedStart(undefined, BUCKETS, "hour");
  expect(first_indexed).toBe(seconds("2026-10-01T01:00:00Z"));
  const rows = compareBuckets(BUCKETS.slice(0, 3), POINTS, { first_indexed, bucket: "hour", now_unix_seconds: NOW });
  expect(rows.map((row) => row.comparison)).toEqual(["not_indexed", "partial", "compared"]);
  expect(firstIndexedStart(null, [bucket("2026-10-01T00:00:00Z", 0, null)], "hour")).toBeNull();
});

test("the daily bucket holding now is partial even though its hours are mostly closed", () => {
  const day = [bucket("2026-10-01T00:00:00Z", 1, "1")];
  const rows = compareBuckets(day, [{ timestamp: seconds("2026-10-01T00:00:00Z"), volume: 1 }], {
    first_indexed: 0, bucket: "day", now_unix_seconds: NOW,
  });
  expect(rows[0]?.comparison).toBe("partial");
});

test("a bucket Meteora did not return is missing_upstream, not zero", () => {
  const rows = compareBuckets([bucket("2026-10-01T05:00:00Z", 1, "1")], POINTS.slice(0, 5), {
    first_indexed: 0, bucket: "hour", now_unix_seconds: seconds("2026-10-02T00:00:00Z"),
  });
  expect(rows[0]?.comparison).toBe("missing_upstream");
});

test("meteoraParams asks for the same half-open window as the API", () => {
  const range = { from_unix_seconds: seconds("2026-10-01T00:00:00Z"), to_unix_seconds: seconds("2026-10-03T00:00:00Z") };
  expect(meteoraParams(range, "day")).toEqual({
    timeframe: "24h",
    start_time: String(seconds("2026-10-01T00:00:00Z")),
    end_time: String(seconds("2026-10-03T00:00:00Z")),
  });
});
