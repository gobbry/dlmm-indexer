import { readVolume, type VolumeBody } from "../api_types.ts";
import { paintBars } from "../color.ts";
import {
  compareBuckets,
  firstIndexedStart,
  meteoraParams,
  meteoraPath,
  METEORA_BASE_URL,
  readMeteoraHistory,
} from "../compare.ts";
import { fetchApi, fetchExchange, type CommandResult, type Context } from "../context.ts";
import { envelopeError } from "../envelope.ts";
import { CliFailure, EXIT_CODE_OK, exitCodeFor } from "../failure.ts";
import { curlCommand, renderTable } from "../format.ts";
import { buildRequest, type Exchange } from "../http.ts";
import { parseRfc3339, type BucketName, type UnixRange } from "../range.ts";
import { resolvePool } from "./pool_input.ts";
import { resolveBucket, resolveWindow, type VolumeFlags } from "./volume_input.ts";
import { compareRows, compareSummary, COMPARE_COLUMNS, volumeRows, volumeSummary, VOLUME_COLUMNS } from "./volume_view.ts";

function plainResult(context: Context, exchange: Exchange, volume: VolumeBody): CommandResult {
  return {
    exchanges: [exchange],
    data: volume.buckets,
    summary: { ...volumeSummary(volume.buckets), bucket: volume.bucket, from: volume.from, to: volume.to },
    table: () => paintBars(renderTable(volumeRows(volume.buckets), VOLUME_COLUMNS), context.paint),
    note: { title: "curl", message: curlCommand(exchange.request) },
    exit_code: EXIT_CODE_OK,
  };
}

// The API echoes the aligned window it used; Meteora is asked for exactly that window.
function effectiveRange(volume: VolumeBody, fallback: UnixRange): UnixRange {
  const from_unix_seconds = parseRfc3339(volume.from);
  const to_unix_seconds = parseRfc3339(volume.to);
  return from_unix_seconds === null || to_unix_seconds === null ? fallback : { from_unix_seconds, to_unix_seconds };
}

// The override exists for Meteora's dev deployment (https://dlmm.dev.metdev.io).
function meteoraBaseUrl(context: Context): string {
  return context.io.env.METCLANKER_METEORA_BASE_URL ?? METEORA_BASE_URL;
}

async function fetchMeteora(context: Context, pool: string, range: UnixRange, bucket: BucketName) {
  const exchange_result = await fetchExchange(context, meteoraBaseUrl(context), meteoraPath(pool), meteoraParams(range, bucket));
  const points = readMeteoraHistory(exchange_result.body);
  if (points === null) {
    throw new CliFailure("invalid_json", "Meteora volume history has no data list", { exchange: exchange_result.exchange });
  }
  return { exchange: exchange_result.exchange, points };
}

type CompareTarget = { base_url: string; pool: string; range: UnixRange; bucket: BucketName };

function compareFailureResult(base: CommandResult, failure: CliFailure, target: CompareTarget): CommandResult {
  const request = failure.request ?? buildRequest(target.base_url, meteoraPath(target.pool), meteoraParams(target.range, target.bucket));
  const error = envelopeError(failure);
  return {
    ...base,
    exchanges: failure.exchange === undefined ? base.exchanges : [...base.exchanges, failure.exchange],
    compare: { request, response: failure.exchange?.response ?? null, error },
    error,
    exit_code: exitCodeFor(failure.kind),
  };
}

async function withComparison(context: Context, base: CommandResult, volume: VolumeBody, range: UnixRange, bucket: BucketName): Promise<CommandResult> {
  const effective = effectiveRange(volume, range);
  try {
    const meteora = await fetchMeteora(context, volume.pool, effective, bucket);
    const first_indexed = firstIndexedStart(volume.first_swap_at, volume.buckets, bucket);
    const now_unix_seconds = Math.floor(context.io.now().getTime() / 1_000);
    const compared = compareBuckets(volume.buckets, meteora.points, { first_indexed, bucket, now_unix_seconds });
    return {
      ...base,
      exchanges: [...base.exchanges, meteora.exchange],
      data: compared,
      summary: { ...(base.summary as object), compare: compareSummary(compared) },
      compare: { request: meteora.exchange.request, response: meteora.exchange.response, error: null },
      table: () => paintBars(renderTable(compareRows(compared), COMPARE_COLUMNS), context.paint),
    };
  } catch (error) {
    if (!(error instanceof CliFailure)) {
      throw error;
    }
    context.io.write_stderr(`metclanker: --compare failed: ${error.message}\n`);
    return compareFailureResult(base, error, { base_url: meteoraBaseUrl(context), pool: volume.pool, range: effective, bucket });
  }
}

export async function runVolume(context: Context, flags: VolumeFlags): Promise<CommandResult> {
  const pool = await resolvePool(context, flags.pool, "volume");
  const bucket = await resolveBucket(context, flags.bucket);
  const window = await resolveWindow(context, flags, bucket);
  const params = { bucket, from: window.from, to: window.to };
  const { exchange, body } = await fetchApi(context, `/v1/pools/${pool}/volume`, params);
  const volume = readVolume(body, exchange);
  const base = plainResult(context, exchange, volume);
  if (flags.compare !== true) {
    return base;
  }
  return withComparison(context, base, volume, window.range, bucket);
}
