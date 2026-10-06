import { readHealth, readPool, readPoolsPage, readSwapsPage } from "../api_types.ts";
import { postBackfill } from "../commands/backfill.ts";
import { loadVolume } from "../commands/volume.ts";
import { fetchApi, type Context } from "../context.ts";
import { CliFailure } from "../failure.ts";
import { errorBody } from "../http.ts";
import type { Data, Effect } from "./screen.ts";

export type ApiEffect = Exclude<Effect, { kind: "none" | "blank" | "settings" | "toggle_color" }>;

async function health(context: Context): Promise<Data> {
  const { exchange, body } = await fetchApi(context, "/v1/health");
  return { kind: "health", health: readHealth(body, exchange) };
}

async function pools(context: Context, limit: number, offset: number): Promise<Data> {
  const { exchange, body } = await fetchApi(context, "/v1/pools", { limit: String(limit), offset: String(offset) });
  const page = readPoolsPage(body, exchange);
  return { kind: "pools", pools: page.pools, total: page.page.total };
}

async function pool(context: Context, address: string): Promise<Data> {
  const { exchange, body } = await fetchApi(context, `/v1/pools/${address}`);
  return { kind: "pool", summary: readPool(body, exchange) };
}

async function swaps(context: Context, address: string, limit: number, before: string | null): Promise<Data> {
  const params: Record<string, string> = before === null ? { limit: String(limit) } : { limit: String(limit), before };
  const { exchange, body } = await fetchApi(context, `/v1/pools/${address}/swaps`, params);
  const page = readSwapsPage(body, exchange);
  return { kind: "swaps", swaps: page.swaps, next_cursor: page.page.next_cursor };
}

// The subcommand's loader does the window alignment and the Meteora comparison; the shell
// only keeps its rows.
async function volume(context: Context, effect: Extract<Effect, { kind: "volume" }>): Promise<Data> {
  const window = effect.range.kind === "named" ? { range: effect.range.name } : { from: effect.range.from, to: effect.range.to };
  const { volume: body, comparison } = await loadVolume(context, { pool: effect.address, bucket: effect.bucket, compare: effect.compare, ...window });
  const compared = comparison?.kind === "compared" ? comparison.compared : null;
  return {
    kind: "volume",
    buckets: compared ?? body.buckets,
    compared,
    compare_error: comparison?.kind === "failed" ? comparison.failure.message : null,
  };
}

async function backfill(context: Context, from: string): Promise<Data> {
  const { job } = await postBackfill(context, from);
  return { kind: "backfill", job };
}

function perform(context: Context, effect: ApiEffect): Promise<Data> {
  switch (effect.kind) {
    case "health":
      return health(context);
    case "pools":
      return pools(context, effect.limit, effect.offset);
    case "pool":
      return pool(context, effect.address);
    case "volume":
      return volume(context, effect);
    case "swaps":
      return swaps(context, effect.address, effect.limit, effect.before);
    case "backfill":
      return backfill(context, effect.from);
  }
}

// A failed call becomes data the screen shows, so one bad request never ends the session.
export async function runEffect(context: Context, effect: ApiEffect): Promise<Data> {
  try {
    return await perform(context, effect);
  } catch (error) {
    if (!(error instanceof CliFailure)) {
      throw error;
    }
    const detail = error.exchange === undefined ? "" : ` ${JSON.stringify(errorBody(error.exchange))}`;
    return { kind: "failed", message: `${error.message}${detail}` };
  }
}
