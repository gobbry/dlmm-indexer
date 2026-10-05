export type BucketName = "hour" | "day";
export type RangeName = "24h" | "7d" | "30d";

export const BUCKET_NAMES: readonly BucketName[] = ["hour", "day"];
export const RANGE_NAMES: readonly RangeName[] = ["24h", "7d", "30d"];

export const BUCKET_SECONDS: Record<BucketName, number> = { hour: 3_600, day: 86_400 };
const RANGE_SECONDS: Record<RangeName, number> = { "24h": 86_400, "7d": 604_800, "30d": 2_592_000 };

export type UnixRange = { from_unix_seconds: number; to_unix_seconds: number };

export function isBucketName(text: string): text is BucketName {
  return (BUCKET_NAMES as readonly string[]).includes(text);
}

export function isRangeName(text: string): text is RangeName {
  return (RANGE_NAMES as readonly string[]).includes(text);
}

export function floorToBucket(unix_seconds: number, bucket: BucketName): number {
  const size = BUCKET_SECONDS[bucket];
  return Math.floor(unix_seconds / size) * size;
}

export function ceilToBucket(unix_seconds: number, bucket: BucketName): number {
  const size = BUCKET_SECONDS[bucket];
  return Math.ceil(unix_seconds / size) * size;
}

// Aligned outward like the API does, so the window always covers the whole named range.
export function alignRange(range: RangeName, bucket: BucketName, now_unix_seconds: number): UnixRange {
  return {
    from_unix_seconds: floorToBucket(now_unix_seconds - RANGE_SECONDS[range], bucket),
    to_unix_seconds: ceilToBucket(now_unix_seconds, bucket),
  };
}

export function toRfc3339(unix_seconds: number): string {
  return new Date(unix_seconds * 1_000).toISOString().replace(".000Z", "Z");
}

const RFC3339_PATTERN = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})$/i;

export function parseRfc3339(text: string): number | null {
  if (!RFC3339_PATTERN.test(text)) {
    return null;
  }
  const milliseconds = Date.parse(text);
  return Number.isNaN(milliseconds) ? null : Math.floor(milliseconds / 1_000);
}
