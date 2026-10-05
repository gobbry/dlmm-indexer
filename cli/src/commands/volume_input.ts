import { missingFlag, type Context } from "../context.ts";
import { usageFailure } from "../failure.ts";
import { promptBucket, promptRange } from "../prompts.ts";
import {
  alignRange,
  ceilToBucket,
  floorToBucket,
  isBucketName,
  isRangeName,
  parseRfc3339,
  toRfc3339,
  type BucketName,
  type RangeName,
  type UnixRange,
} from "../range.ts";

export type VolumeFlags = {
  pool?: string;
  bucket?: string;
  range?: string;
  from?: string;
  to?: string;
  compare?: boolean;
};

export type VolumeWindow = { from: string; to: string; range: UnixRange };

export async function resolveBucket(context: Context, bucket: string | undefined): Promise<BucketName> {
  if (bucket === undefined) {
    if (context.interaction === "non_interactive") {
      throw missingFlag("--bucket hour|day", "volume");
    }
    return promptBucket();
  }
  if (!isBucketName(bucket)) {
    throw usageFailure(`--bucket must be hour or day, got '${bucket}'`);
  }
  return bucket;
}

export function windowFromRange(range: RangeName, bucket: BucketName, now: Date): VolumeWindow {
  const aligned = alignRange(range, bucket, Math.floor(now.getTime() / 1_000));
  return { from: toRfc3339(aligned.from_unix_seconds), to: toRfc3339(aligned.to_unix_seconds), range: aligned };
}

// Explicit bounds are sent verbatim; the API aligns them, the local range only seeds --compare.
export function windowFromBounds(from: string, to: string, bucket: BucketName): VolumeWindow {
  const from_unix_seconds = parseRfc3339(from);
  const to_unix_seconds = parseRfc3339(to);
  if (from_unix_seconds === null || to_unix_seconds === null) {
    throw usageFailure("--from and --to must be RFC 3339 timestamps such as 2026-10-01T00:00:00Z");
  }
  return {
    from,
    to,
    range: { from_unix_seconds: floorToBucket(from_unix_seconds, bucket), to_unix_seconds: ceilToBucket(to_unix_seconds, bucket) },
  };
}

export async function resolveWindow(context: Context, flags: VolumeFlags, bucket: BucketName): Promise<VolumeWindow> {
  const has_bounds = flags.from !== undefined || flags.to !== undefined;
  if (flags.range !== undefined && has_bounds) {
    throw usageFailure("use either --range or --from with --to, not both");
  }
  if (has_bounds) {
    if (flags.from === undefined || flags.to === undefined) {
      throw usageFailure("--from and --to must be given together");
    }
    return windowFromBounds(flags.from, flags.to, bucket);
  }
  if (flags.range !== undefined) {
    if (!isRangeName(flags.range)) {
      throw usageFailure(`--range must be 24h, 7d or 30d, got '${flags.range}'`);
    }
    return windowFromRange(flags.range, bucket, context.io.now());
  }
  if (context.interaction === "non_interactive") {
    throw missingFlag("--range 24h|7d|30d (or --from and --to)", "volume");
  }
  return windowFromRange(await promptRange(), bucket, context.io.now());
}
