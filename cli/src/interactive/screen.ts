import type { BackfillBody, HealthBody, PoolSummary, SwapRow, VolumeBucket } from "../api_types.ts";
import type { ComparedBucket } from "../compare.ts";
import { BUCKET_NAMES, RANGE_NAMES, type BucketName, type RangeName } from "../range.ts";
import { addressError, baseUrlError, instantError, parseBaseUrl, parseWidth, widthError, type Key } from "./keys.ts";

export const POOLS_PAGE_SIZE = 10;
export const SWAPS_PAGE_SIZE = 20;

export type VolumeRange = { kind: "named"; name: RangeName } | { kind: "custom"; from: string; to: string };
export type VolumeQuery = { bucket: BucketName; range: VolumeRange; compare: boolean };

// Every screen but Home and Quit keeps the screen Left returns to, exactly as it was left, so
// the highlighted row survives a round trip.
export type HomeScreen = { kind: "home"; selected: number };
export type PoolScreen = { kind: "pool"; address: string; selected: number; back: Screen };

export type TextField =
  | { kind: "address" }
  | { kind: "backfill" }
  | { kind: "base_url" }
  | { kind: "width" }
  | { kind: "from"; pool: PoolScreen; bucket: BucketName }
  | { kind: "to"; pool: PoolScreen; bucket: BucketName; from: string };

export type TextScreen = { kind: "text"; field: TextField; value: string; caret: number; error: string | null; back: Screen };

export type Screen =
  | HomeScreen
  | { kind: "pools"; page: number; selected: number; back: Screen }
  | PoolScreen
  | { kind: "bucket"; pool: PoolScreen; selected: number; back: Screen }
  | { kind: "range"; pool: PoolScreen; bucket: BucketName; selected: number; back: Screen }
  | { kind: "compare"; pool: PoolScreen; bucket: BucketName; range: VolumeRange; selected: number; back: Screen }
  | { kind: "volume"; pool: PoolScreen; query: VolumeQuery; selected: number }
  // The `before` cursor of every page seen so far, newest page first at index 0 (null).
  | { kind: "swaps"; pool: PoolScreen; cursor_stack: readonly (string | null)[]; selected: number }
  | { kind: "backfill"; from: string; selected: number; back: Screen }
  | { kind: "health"; selected: number; back: Screen }
  | { kind: "settings"; selected: number; back: Screen }
  | TextScreen
  | { kind: "quit"; exit_code: 0 | 1 | 130 };

export type ListScreen = Exclude<Screen, TextScreen | { kind: "quit" }>;

export type Settings = { base_url: string; width: number | null; color: boolean };

export type Effect =
  | { kind: "none" }
  // A screen with nothing to load; the previous screen's data must not show through.
  | { kind: "blank" }
  | { kind: "health" }
  | { kind: "pools"; limit: number; offset: number }
  | { kind: "pool"; address: string }
  | ({ kind: "volume"; address: string } & VolumeQuery)
  | { kind: "swaps"; address: string; limit: number; before: string | null }
  | { kind: "backfill"; from: string }
  | { kind: "settings"; patch: Partial<Settings> }
  | { kind: "toggle_color" };

export type Data =
  | { kind: "none" }
  | { kind: "failed"; message: string }
  | { kind: "health"; health: HealthBody }
  | { kind: "pools"; pools: PoolSummary[]; total: number }
  | { kind: "pool"; summary: PoolSummary }
  | { kind: "volume"; buckets: VolumeBucket[]; compared: ComparedBucket[] | null; compare_error: string | null }
  | { kind: "swaps"; swaps: SwapRow[]; next_cursor: string | null }
  | { kind: "backfill"; job: BackfillBody };

// The shell's timer tick re-reads health while Home or Health is shown.
export type Input = Key | { kind: "tick" };

export type Step = { screen: Screen; effect: Effect };

export type Action = { kind: "open"; screen: Screen } | { kind: "reload" } | { kind: "toggle_color" } | { kind: "none" };
export type Row = { label: string; action: Action };

export const HOME: HomeScreen = { kind: "home", selected: 0 };
const SEARCH_LABEL = "Search by address…";
const RETRY_LABEL = "Retry";
export const REFRESH_MS = 5_000;

const NONE: Effect = { kind: "none" };
const BLANK: Effect = { kind: "blank" };

export function loadEffect(screen: Screen): Effect {
  switch (screen.kind) {
    case "home":
    case "health":
      return { kind: "health" };
    case "pools":
      return { kind: "pools", limit: POOLS_PAGE_SIZE, offset: screen.page * POOLS_PAGE_SIZE };
    case "pool":
      return { kind: "pool", address: screen.address };
    case "volume":
      return { kind: "volume", address: screen.pool.address, ...screen.query };
    case "swaps":
      return { kind: "swaps", address: screen.pool.address, limit: SWAPS_PAGE_SIZE, before: screen.cursor_stack.at(-1) ?? null };
    case "backfill":
      return { kind: "backfill", from: screen.from };
    case "bucket":
    case "range":
    case "compare":
    case "settings":
    case "text":
    case "quit":
      return BLANK;
  }
}

function go(screen: Screen): Step {
  return { screen, effect: loadEffect(screen) };
}

function stay(screen: Screen): Step {
  return { screen, effect: NONE };
}

function backOf(screen: Screen): Screen | null {
  switch (screen.kind) {
    case "home":
    case "quit":
      return null;
    case "volume":
    case "swaps":
      return screen.pool;
    default:
      return screen.back;
  }
}

function open(screen: Screen): Action {
  return { kind: "open", screen };
}

function textInput(field: TextField, back: Screen): Screen {
  return { kind: "text", field, value: "", caret: 0, error: null, back };
}

const RETRY_ROW: Row = { label: RETRY_LABEL, action: { kind: "reload" } };

function withRetry(rows: Row[], data: Data): Row[] {
  return data.kind === "failed" ? [...rows, RETRY_ROW] : rows;
}

// The rows a list screen offers, in display order; the highlight indexes into this list.
export function rows(screen: ListScreen, data: Data): Row[] {
  switch (screen.kind) {
    case "home":
      return [
        { label: "Pools", action: open({ kind: "pools", page: 0, selected: 0, back: screen }) },
        { label: "Health", action: open({ kind: "health", selected: 0, back: screen }) },
        { label: "Backfill", action: open(textInput({ kind: "backfill" }, screen)) },
        { label: "Settings", action: open({ kind: "settings", selected: 0, back: screen }) },
      ];
    case "pools": {
      const pools = data.kind === "pools" ? data.pools : [];
      const page_rows = pools.map((pool) => ({ label: pool.address, action: open({ kind: "pool", address: pool.address, selected: 0, back: screen }) }));
      return withRetry([...page_rows, { label: SEARCH_LABEL, action: open(textInput({ kind: "address" }, screen)) }], data);
    }
    case "pool":
      return withRetry(
        [
          { label: "Volume", action: open({ kind: "bucket", pool: screen, selected: 0, back: screen }) },
          { label: "Swaps", action: open({ kind: "swaps", pool: screen, cursor_stack: [null], selected: 0 }) },
          { label: "Backfill", action: open(textInput({ kind: "backfill" }, screen)) },
        ],
        data,
      );
    case "bucket":
      return BUCKET_NAMES.map((bucket) => ({ label: bucket, action: open({ kind: "range", pool: screen.pool, bucket, selected: 0, back: screen }) }));
    case "range":
      return [
        ...RANGE_NAMES.map((name) => ({
          label: name,
          action: open({ kind: "compare", pool: screen.pool, bucket: screen.bucket, range: { kind: "named", name }, selected: 0, back: screen }),
        })),
        { label: "custom…", action: open(textInput({ kind: "from", pool: screen.pool, bucket: screen.bucket }, screen)) },
      ];
    case "compare":
      return [false, true].map((compare) => ({
        label: compare ? "yes, compare with Meteora's Data API" : "no",
        action: open({ kind: "volume", pool: screen.pool, query: { bucket: screen.bucket, range: screen.range, compare }, selected: 0 }),
      }));
    case "volume":
      return withRetry([{ label: "New query", action: open({ kind: "bucket", pool: screen.pool, selected: 0, back: screen }) }], data);
    case "swaps": {
      // Swap rows open nothing; they are rows so the highlight can walk them while reading.
      const swaps = data.kind === "swaps" ? data.swaps : [];
      return withRetry(swaps.map((swap) => ({ label: swap.signature, action: { kind: "none" } })), data);
    }
    case "backfill":
      // No Retry: a POST that timed out may have inserted the job, and a second one would duplicate it.
      return [];
    case "health":
      return [{ label: "Refresh", action: { kind: "reload" } }];
    case "settings":
      return [
        { label: "Base URL", action: open(textInput({ kind: "base_url" }, screen)) },
        { label: "Width", action: open(textInput({ kind: "width" }, screen)) },
        { label: "Colour", action: { kind: "toggle_color" } },
      ];
  }
}

export function clampSelected(selected: number, row_count: number): number {
  return Math.min(Math.max(selected, 0), Math.max(row_count - 1, 0));
}

function lastPoolsPage(total: number): number {
  return Math.max(Math.ceil(total / POOLS_PAGE_SIZE) - 1, 0);
}

// `[` needs only the screen; `]` needs the loaded page to know a next one exists.
function pageStep(screen: ListScreen, char: string, data: Data): Step {
  if (screen.kind === "pools") {
    if (char === "[" && screen.page > 0) {
      return go({ ...screen, page: screen.page - 1, selected: 0 });
    }
    if (char === "]" && data.kind === "pools" && screen.page < lastPoolsPage(data.total)) {
      return go({ ...screen, page: screen.page + 1, selected: 0 });
    }
  }
  if (screen.kind === "swaps") {
    if (char === "[" && screen.cursor_stack.length > 1) {
      return go({ ...screen, cursor_stack: screen.cursor_stack.slice(0, -1), selected: 0 });
    }
    if (char === "]" && data.kind === "swaps" && data.next_cursor !== null) {
      return go({ ...screen, cursor_stack: [...screen.cursor_stack, data.next_cursor], selected: 0 });
    }
  }
  return stay(screen);
}

function act(screen: ListScreen, action: Action | undefined): Step {
  switch (action?.kind) {
    case "open":
      return go(action.screen);
    case "reload":
      return go(screen);
    case "toggle_color":
      return { screen, effect: { kind: "toggle_color" } };
    case "none":
    case undefined:
      return stay(screen);
  }
}

function listStep(screen: ListScreen, input: Input, data: Data): Step {
  const offered = rows(screen, data);
  const selected = clampSelected(screen.selected, offered.length);
  switch (input.kind) {
    case "up":
      return stay({ ...screen, selected: clampSelected(selected - 1, offered.length) });
    case "down":
      return stay({ ...screen, selected: clampSelected(selected + 1, offered.length) });
    case "enter":
    case "right":
      return act(screen, offered[selected]?.action);
    case "left": {
      const back = backOf(screen);
      return back === null ? stay(screen) : go(back);
    }
    case "char":
      return pageStep(screen, input.char, data);
    case "tick":
      return screen.kind === "home" || screen.kind === "health" ? { screen, effect: { kind: "health" } } : stay(screen);
    default:
      return stay(screen);
  }
}

function fieldError(field: TextField, value: string): string | undefined {
  switch (field.kind) {
    case "address":
      return addressError(value);
    case "backfill":
    case "from":
    case "to":
      return instantError(value);
    case "base_url":
      return baseUrlError(value);
    case "width":
      return widthError(value);
  }
}

// Called with a value its field's validator accepted.
function submit(screen: TextScreen, value: string): Step {
  const { field, back } = screen;
  switch (field.kind) {
    case "address":
      return go({ kind: "pool", address: value, selected: 0, back });
    case "backfill":
      return go({ kind: "backfill", from: value, selected: 0, back });
    case "base_url":
      return { screen: back, effect: { kind: "settings", patch: { base_url: parseBaseUrl(value) ?? value } } };
    case "width": {
      const width = parseWidth(value);
      return { screen: back, effect: { kind: "settings", patch: { width: width === "invalid" ? null : width } } };
    }
    case "from":
      return go(textInput({ kind: "to", pool: field.pool, bucket: field.bucket, from: value }, screen));
    case "to":
      return go({ kind: "compare", pool: field.pool, bucket: field.bucket, range: { kind: "custom", from: field.from, to: value }, selected: 0, back: screen });
  }
}

// Left leaves only an empty input: a held Left overshooting caret 0 would otherwise throw away a
// pasted address. Esc quits the session from anywhere, so Left is the way back.
function textStep(screen: TextScreen, input: Input): Step {
  const { value, caret } = screen;
  switch (input.kind) {
    case "char":
      return stay({ ...screen, value: value.slice(0, caret) + input.char + value.slice(caret), caret: caret + input.char.length, error: null });
    case "backspace":
      return caret === 0 ? stay(screen) : stay({ ...screen, value: value.slice(0, caret - 1) + value.slice(caret), caret: caret - 1, error: null });
    case "left":
      if (caret > 0) {
        return stay({ ...screen, caret: caret - 1 });
      }
      return value === "" ? go(screen.back) : stay(screen);
    case "right":
      return stay({ ...screen, caret: Math.min(caret + 1, value.length) });
    case "home":
      return stay({ ...screen, caret: 0 });
    case "end":
      return stay({ ...screen, caret: value.length });
    case "enter": {
      const error = fieldError(screen.field, value);
      return error === undefined ? submit(screen, value.trim()) : stay({ ...screen, error });
    }
    default:
      return stay(screen);
  }
}

export function transition(screen: Screen, input: Input, data: Data): Step {
  if (screen.kind === "quit") {
    return stay(screen);
  }
  if (input.kind === "escape") {
    return stay({ kind: "quit", exit_code: 0 });
  }
  if (input.kind === "interrupt") {
    return stay({ kind: "quit", exit_code: 130 });
  }
  if (input.kind === "closed") {
    return stay({ kind: "quit", exit_code: input.exit_code });
  }
  return screen.kind === "text" ? textStep(screen, input) : listStep(screen, input, data);
}
