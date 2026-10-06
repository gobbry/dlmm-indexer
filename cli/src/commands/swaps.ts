import { readSwaps, type SwapRow } from "../api_types.ts";
import { fetchApi, type CommandResult, type Context } from "../context.ts";
import { EXIT_CODE_OK } from "../failure.ts";
import { alignRight, curlCommand, displayValue, renderTable, shortenMiddle } from "../format.ts";
import { resolvePool } from "./pool_input.ts";

export const SWAP_COLUMNS = ["block_time", "signature", "ordinal", "source", "direction", "amount_in", "amount_out", "fee", "volume_usd"] as const;

export function swapRows(swaps: readonly SwapRow[]) {
  const rows = swaps.map((swap) => ({
    block_time: displayValue(swap.block_time),
    signature: shortenMiddle(swap.signature, 8),
    ordinal: displayValue(swap.swap_ordinal),
    source: displayValue(swap.source),
    direction: displayValue(swap.direction),
    amount_in: displayValue(swap.amount_in),
    amount_out: displayValue(swap.amount_out),
    fee: displayValue(swap.fee),
    volume_usd: displayValue(swap.volume_usd) || "-",
  }));
  return alignRight(rows, ["ordinal", "amount_in", "amount_out", "fee", "volume_usd"]);
}

export type SwapsFlags = { pool?: string; limit: number };

export async function runSwaps(context: Context, flags: SwapsFlags): Promise<CommandResult> {
  const pool = await resolvePool(context, flags.pool, "swaps");
  const { exchange, body } = await fetchApi(context, `/v1/pools/${pool}/swaps`, { limit: String(flags.limit) });
  const swaps = readSwaps(body, exchange);
  return {
    exchanges: [exchange],
    data: swaps,
    summary: { pool, swap_count: swaps.length },
    table: () => renderTable(swapRows(swaps), SWAP_COLUMNS),
    note: { title: "curl", message: curlCommand(exchange.request) },
    exit_code: EXIT_CODE_OK,
  };
}
