import { isCancel, select } from "@clack/prompts";
import type { PoolSummary } from "./api_types.ts";
import { CliFailure } from "./failure.ts";
import { shortenMiddle } from "./format.ts";
import { RANGE_NAMES, type BucketName, type RangeName } from "./range.ts";

function settle<Answer>(answer: Answer): Exclude<Answer, symbol> {
  if (isCancel(answer)) {
    throw new CliFailure("cancelled", "cancelled");
  }
  return answer as Exclude<Answer, symbol>;
}

export async function promptPool(pools: readonly PoolSummary[]): Promise<string> {
  if (pools.length === 0) {
    throw new CliFailure("usage", "the API lists no pools yet; pass --pool");
  }
  const options = pools.map((pool) => ({
    value: pool.address,
    label: pool.address,
    hint: `${shortenMiddle(pool.mint_x, 4)} / ${shortenMiddle(pool.mint_y, 4)} · 24h $${pool.volume_usd_24h ?? "-"}`,
  }));
  return settle(await select({ message: "Pool", options }));
}

export async function promptBucket(): Promise<BucketName> {
  const options: { value: BucketName; label: string }[] = [
    { value: "hour", label: "hour" },
    { value: "day", label: "day" },
  ];
  return settle(await select({ message: "Bucket", options }));
}

export async function promptRange(): Promise<RangeName> {
  const options = RANGE_NAMES.map((range) => ({ value: range, label: range }));
  return settle(await select<RangeName>({ message: "Range", options }));
}
