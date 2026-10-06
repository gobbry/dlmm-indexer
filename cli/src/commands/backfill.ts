import { readBackfill, readCancel, readJobs, type BackfillBody, type JobBody } from "../api_types.ts";
import { deleteApi, fetchApi, missingFlag, postApi, type CommandResult, type Context } from "../context.ts";
import { EXIT_CODE_OK, usageFailure } from "../failure.ts";
import { alignRight, curlCommand, fieldTable, renderTable } from "../format.ts";
import type { Exchange } from "../http.ts";

// The API resolves the instant to a slot by bisection over RPC before it answers: tens of
// seconds on a free endpoint, so the global default would time out a request that succeeds.
const BACKFILL_TIMEOUT_MS_MIN = 120_000;

const JOB_COLUMNS = ["job_id", "state", "start_slot", "end_slot", "next_slot", "blocked_reason", "created_at"] as const;

export type BackfillOptions = {
  from: string | undefined;
  cancel: number | undefined;
  list: boolean;
};

// The request alone, for the interactive shell, which shows the job rather than an envelope.
export async function postBackfill(context: Context, from: string): Promise<{ exchange: Exchange; job: BackfillBody }> {
  const timeout_ms = Math.max(context.options.timeout_ms, BACKFILL_TIMEOUT_MS_MIN);
  const { exchange, body } = await postApi(context, "/v1/backfills", { from }, timeout_ms);
  return { exchange, job: readBackfill(body, exchange) };
}

async function requestBackfill(context: Context, from: string): Promise<CommandResult> {
  const { exchange, job } = await postBackfill(context, from);
  return {
    exchanges: [exchange],
    data: job,
    summary: { job_id: job.job_id, slot_count: job.end_slot - job.start_slot + 1 },
    table: () => fieldTable(job),
    note: { title: "curl", message: curlCommand(exchange.request) },
    exit_code: EXIT_CODE_OK,
  };
}

// A cancel blocks the job rather than deleting it, so the indexer does not reopen its range.
async function cancelBackfill(context: Context, job_id: number): Promise<CommandResult> {
  const { exchange, body } = await deleteApi(context, `/v1/backfills/${job_id}`);
  const job = readCancel(body, exchange);
  return {
    exchanges: [exchange],
    data: job,
    summary: { job_id: job.job_id, state: job.state, slot_count_unfilled: Math.max(job.end_slot - job.next_slot + 1, 0) },
    table: () => fieldTable(job),
    note: { title: "curl", message: curlCommand(exchange.request) },
    exit_code: EXIT_CODE_OK,
  };
}

function jobRows(jobs: readonly JobBody[]) {
  const rows = jobs.map((job) => ({
    job_id: String(job.job_id),
    state: job.state,
    start_slot: String(job.start_slot),
    end_slot: String(job.end_slot),
    next_slot: String(job.next_slot),
    blocked_reason: job.blocked_reason ?? "-",
    created_at: job.created_at,
  }));
  return alignRight(rows, ["job_id", "start_slot", "end_slot", "next_slot"]);
}

async function listBackfills(context: Context): Promise<CommandResult> {
  const { exchange, body } = await fetchApi(context, "/v1/backfills");
  const jobs = readJobs(body, exchange);
  const state_counts: Record<string, number> = {};
  for (const job of jobs) {
    state_counts[job.state] = (state_counts[job.state] ?? 0) + 1;
  }
  return {
    exchanges: [exchange],
    data: jobs,
    summary: { job_count: jobs.length, state_counts },
    table: () => renderTable(jobRows(jobs), JOB_COLUMNS),
    note: { title: "curl", message: curlCommand(exchange.request) },
    exit_code: EXIT_CODE_OK,
  };
}

export async function runBackfill(context: Context, options: BackfillOptions): Promise<CommandResult> {
  const chosen = [options.from !== undefined, options.cancel !== undefined, options.list].filter(Boolean).length;
  if (chosen > 1) {
    throw usageFailure("pass only one of --from, --cancel and --list to 'backfill'");
  }
  if (options.cancel !== undefined) {
    return cancelBackfill(context, options.cancel);
  }
  if (options.list) {
    return listBackfills(context);
  }
  if (options.from === undefined) {
    throw missingFlag("--from", "backfill");
  }
  return requestBackfill(context, options.from);
}
