import { readPools } from "../api_types.ts";
import { fetchApi, missingFlag, type Context } from "../context.ts";
import { usageFailure } from "../failure.ts";
import { promptPool } from "../prompts.ts";

export const BASE58_ADDRESS_PATTERN = /^[1-9A-HJ-NP-Za-km-z]{32,44}$/;
const PICKER_POOL_COUNT = 50;

export async function resolvePool(context: Context, pool: string | undefined, command: string): Promise<string> {
  if (pool !== undefined) {
    if (!BASE58_ADDRESS_PATTERN.test(pool)) {
      throw usageFailure(`--pool must be a base58 address, got '${pool}'`);
    }
    return pool;
  }
  if (context.interaction === "non_interactive") {
    throw missingFlag("--pool", command);
  }
  const { exchange, body } = await fetchApi(context, "/v1/pools", { limit: String(PICKER_POOL_COUNT) });
  return promptPool(readPools(body, exchange));
}
