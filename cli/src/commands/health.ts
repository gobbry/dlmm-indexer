import { readHealth } from "../api_types.ts";
import { fetchApi, type CommandResult, type Context } from "../context.ts";
import { EXIT_CODE_OK } from "../failure.ts";
import { curlCommand, displayValue, renderTable } from "../format.ts";

export async function runHealth(context: Context): Promise<CommandResult> {
  const { exchange, body } = await fetchApi(context, "/v1/health");
  const health = readHealth(body, exchange);
  const rows = Object.entries(health).map(([field, value]) => ({ field, value: displayValue(value) }));
  return {
    exchanges: [exchange],
    data: health,
    summary: { status: health.status, lag_seconds: health.lag_seconds ?? null },
    table: () => renderTable(rows, ["field", "value"]),
    note: { title: "curl", message: curlCommand(exchange.request) },
    exit_code: EXIT_CODE_OK,
  };
}
