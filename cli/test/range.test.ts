import { expect, test } from "bun:test";
import { alignRange, parseRfc3339, toRfc3339, type BucketName, type RangeName } from "../src/range.ts";

const seconds = (text: string) => Date.parse(text) / 1_000;

test.each([
  { range: "24h", bucket: "hour", now: "2026-10-01T10:30:00Z", from: "2026-09-30T10:00:00Z", to: "2026-10-01T11:00:00Z" },
  { range: "24h", bucket: "hour", now: "2026-10-01T10:00:00Z", from: "2026-09-30T10:00:00Z", to: "2026-10-01T10:00:00Z" },
  { range: "7d", bucket: "day", now: "2026-10-01T10:30:00Z", from: "2026-09-24T00:00:00Z", to: "2026-10-02T00:00:00Z" },
  { range: "30d", bucket: "hour", now: "2026-10-01T10:30:00Z", from: "2026-09-01T10:00:00Z", to: "2026-10-01T11:00:00Z" },
] as const)("alignRange $range by $bucket at $now covers the range outward", ({ range, bucket, now, from, to }) => {
  const aligned = alignRange(range as RangeName, bucket as BucketName, seconds(now));
  expect(toRfc3339(aligned.from_unix_seconds)).toBe(from);
  expect(toRfc3339(aligned.to_unix_seconds)).toBe(to);
});

test("parseRfc3339 converts offsets to UTC and rejects other formats", () => {
  expect(parseRfc3339("2026-10-01T02:00:00+02:00")).toBe(seconds("2026-10-01T00:00:00Z"));
  expect(parseRfc3339("2026-10-01")).toBeNull();
  expect(parseRfc3339("yesterday")).toBeNull();
});
