import { readPoolsPage, type PoolSummary } from "../api_types.ts";
import { renderBars } from "../bars.ts";
import { paintBars } from "../color.ts";
import { fetchApi, type CommandResult, type Context } from "../context.ts";
import { sumDecimalStrings } from "../decimal.ts";
import { EXIT_CODE_OK } from "../failure.ts";
import { alignRight, curlCommand, renderTable, shortenMiddle } from "../format.ts";

export const POOL_COLUMNS = ["address", "mint_x", "mint_y", "swaps_24h", "volume_usd_24h", "volume"] as const;

export function poolRows(pools: readonly PoolSummary[]) {
  const bars = renderBars(pools.map((pool) => Number(pool.volume_usd_24h ?? 0)));
  const rows = pools.map((pool, index) => ({
    address: pool.address,
    mint_x: shortenMiddle(pool.mint_x, 4),
    mint_y: shortenMiddle(pool.mint_y, 4),
    swaps_24h: String(pool.swap_count_24h ?? ""),
    volume_usd_24h: pool.volume_usd_24h ?? "-",
    volume: bars[index] ?? "",
  }));
  return alignRight(rows, ["swaps_24h", "volume_usd_24h"]);
}

export type PoolsFlags = { limit: number; offset: number };

export async function runPools(context: Context, flags: PoolsFlags): Promise<CommandResult> {
  const params = { limit: String(flags.limit), offset: String(flags.offset) };
  const { exchange, body } = await fetchApi(context, "/v1/pools", params);
  const { pools, page } = readPoolsPage(body, exchange);
  const volumes = pools.flatMap((pool) => (typeof pool.volume_usd_24h === "string" ? [pool.volume_usd_24h] : []));
  return {
    exchanges: [exchange],
    data: pools,
    summary: { pool_count: pools.length, total_volume_usd_24h: sumDecimalStrings(volumes) },
    page,
    table: () => paintBars(renderTable(poolRows(pools), POOL_COLUMNS), context.paint),
    note: { title: "curl", message: curlCommand(exchange.request) },
    exit_code: EXIT_CODE_OK,
  };
}
