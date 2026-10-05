import { readBackfill } from "../api_types.ts";
import { missingFlag, postApi, type CommandResult, type Context } from "../context.ts";
import { EXIT_CODE_OK } from "../failure.ts";
import { curlCommand, displayValue, renderTable } from "../format.ts";

// The API resolves the instant to a slot by bisection over RPC before it answers: tens of
// seconds on a free endpoint, so the global default would time out a request that succeeds.
const BACKFILL_TIMEOUT_MS_MIN = 120_000;

export async function runBackfill(context: Context, from: string | undefined): Promise<CommandResult> {
  if (from === undefined) {
    throw missingFlag("--from", "backfill");
  }
  const timeout_ms = Math.max(context.options.timeout_ms, BACKFILL_TIMEOUT_MS_MIN);
  const { exchange, body } = await postApi(context, "/v1/backfills", { from }, timeout_ms);
  const job = readBackfill(body, exchange);
  const rows = Object.entries(job).map(([field, value]) => ({ field, value: displayValue(value) }));
  return {
    exchanges: [exchange],
    data: job,
    summary: { job_id: job.job_id, slot_count: job.end_slot - job.start_slot + 1 },
    table: () => renderTable(rows, ["field", "value"]),
    note: { title: "curl", message: curlCommand(exchange.request) },
    exit_code: EXIT_CODE_OK,
  };
}
