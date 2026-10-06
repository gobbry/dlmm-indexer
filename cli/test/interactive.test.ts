import { afterAll, beforeAll, expect, test } from "bun:test";
import { EventEmitter } from "node:events";
import { PassThrough } from "node:stream";
import type { PoolSummary, SwapRow, VolumeBucket } from "../src/api_types.ts";
import type { GlobalOptions } from "../src/context.ts";
import { parseWidth, streamKeys, type Key, type KeySource } from "../src/interactive/keys.ts";
import { paintFrame, renderFrame, type Frame, type Viewport } from "../src/interactive/render.ts";
import {
  HOME,
  rows,
  transition,
  type Data,
  type Input,
  type ListScreen,
  type PoolScreen,
  type Screen,
  type Settings,
  type TextField,
  type TextScreen,
} from "../src/interactive/screen.ts";
import { runShell } from "../src/interactive/shell.ts";
import { enterSession, leaveSession, processTerminal, type ProcessLike, type Terminal } from "../src/interactive/terminal.ts";
import type { Io } from "../src/io.ts";
import { BACKFILL_INSTANT, MISSING_POOL, NOW, POOL, SLOW_POOL, startMockApi, syntheticPool, type MockApi } from "./mock_api.ts";

// ---- fixtures -------------------------------------------------------------------------------

// The bytes a person sees and a terminal obeys are the contract, so they are spelled out here.
const MARKER = "▸ ";
const SEARCH_LABEL = "Search by address…";
const ALTERNATE_SCREEN_ON = "\x1b[?1049h";
const LEAVE = "\x1b[0 q\x1b[?25h\x1b[?1049l";

const KEY = {
  up: { kind: "up" },
  down: { kind: "down" },
  left: { kind: "left" },
  right: { kind: "right" },
  enter: { kind: "enter" },
  escape: { kind: "escape" },
  interrupt: { kind: "interrupt" },
  backspace: { kind: "backspace" },
  home: { kind: "home" },
  end: { kind: "end" },
  previous_page: { kind: "char", char: "[" },
  next_page: { kind: "char", char: "]" },
  letter: { kind: "char", char: "q" },
  unknown: { kind: "unknown" },
} as const satisfies Record<string, Key>;
const ALL_INPUTS: readonly Input[] = [...Object.values(KEY), { kind: "tick" }];
const NONE = { kind: "none" } as const;
const BLANK = { kind: "blank" } as const;

const POOLS_SCREEN = { kind: "pools", page: 1, selected: 0, back: HOME } as const satisfies Screen;
const POOL_SCREEN: PoolScreen = { kind: "pool", address: POOL, selected: 0, back: POOLS_SCREEN };
const BUCKET_SCREEN = { kind: "bucket", pool: POOL_SCREEN, selected: 0, back: POOL_SCREEN } as const satisfies Screen;
const RANGE_SCREEN = { kind: "range", pool: POOL_SCREEN, bucket: "hour", selected: 0, back: BUCKET_SCREEN } as const satisfies Screen;
const COMPARE_SCREEN = { kind: "compare", pool: POOL_SCREEN, bucket: "hour", range: { kind: "named", name: "24h" }, selected: 0, back: RANGE_SCREEN } as const satisfies Screen;
const QUERY = { bucket: "hour", range: { kind: "named", name: "24h" }, compare: false } as const;
const VOLUME_SCREEN = { kind: "volume", pool: POOL_SCREEN, query: QUERY, selected: 0 } as const satisfies Screen;
const SWAPS_SCREEN = { kind: "swaps", pool: POOL_SCREEN, cursor_stack: [null, "c1"], selected: 0 } as const satisfies Screen;
const BACKFILL_SCREEN = { kind: "backfill", from: "2026-10-01T00:00:00Z", selected: 0, back: POOL_SCREEN } as const satisfies Screen;
const HEALTH_SCREEN = { kind: "health", selected: 0, back: HOME } as const satisfies Screen;
const SETTINGS_SCREEN = { kind: "settings", selected: 0, back: HOME } as const satisfies Screen;

function textScreen(field: TextField, value = "", back: Screen = HOME): TextScreen {
  return { kind: "text", field, value, caret: value.length, error: null, back };
}

function pool(address: string, volume_usd_24h: string): PoolSummary {
  return { address, mint_x: "So11111111111111111111111111111111111111112", mint_y: "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v", swap_count_24h: 4, volume_usd_24h };
}

function swap(index: number): SwapRow {
  return { signature: `${index}`.repeat(88).slice(0, 88), swap_ordinal: 0, block_time: "2026-10-01T02:10:00Z", source: "live_rpc", amount_in: "7" };
}

const HEALTH = { status: "backfilling", cursor_slot: 9, last_block_time: "2026-10-01T03:29:59Z", lag_seconds: 3, open_job_count: 1, blocked_job_count: 0 };
const HEALTH_DATA: Data = { kind: "health", health: HEALTH };
const POOLS_PAGE: Data = { kind: "pools", pools: [pool(syntheticPool(11), "12.5"), pool(syntheticPool(12), "3"), pool(syntheticPool(13), "1")], total: 23 };
const LAST_POOLS_PAGE: Data = { kind: "pools", pools: [pool(syntheticPool(21), "1")], total: 23 };
const SWAPS_MORE: Data = { kind: "swaps", swaps: [swap(1), swap(2)], next_cursor: "c2" };
const SWAPS_END: Data = { kind: "swaps", swaps: [swap(1)], next_cursor: null };
const FAILED: Data = { kind: "failed", message: "GET /v1/pools returned 503" };
const NO_DATA: Data = { kind: "none" };

// Each list screen with the data it shows once loaded, so its rows are the real ones.
const LISTS: { name: string; screen: ListScreen; data: Data }[] = [
  { name: "home", screen: HOME, data: HEALTH_DATA },
  { name: "pools", screen: POOLS_SCREEN, data: POOLS_PAGE },
  { name: "pool", screen: POOL_SCREEN, data: { kind: "pool", summary: pool(POOL, "0.3") } },
  { name: "bucket", screen: BUCKET_SCREEN, data: NO_DATA },
  { name: "range", screen: RANGE_SCREEN, data: NO_DATA },
  { name: "compare", screen: COMPARE_SCREEN, data: NO_DATA },
  { name: "volume", screen: VOLUME_SCREEN, data: { kind: "volume", buckets: [], compared: null, compare_error: null } },
  { name: "swaps", screen: SWAPS_SCREEN, data: SWAPS_MORE },
  { name: "backfill", screen: BACKFILL_SCREEN, data: { kind: "backfill", job: { job_id: 7, start_slot: 1, end_slot: 2 } } },
  { name: "health", screen: HEALTH_SCREEN, data: HEALTH_DATA },
  { name: "settings", screen: SETTINGS_SCREEN, data: NO_DATA },
];
const TEXTS: TextScreen[] = [
  textScreen({ kind: "address" }, "", POOLS_SCREEN),
  textScreen({ kind: "backfill" }),
  textScreen({ kind: "base_url" }, "", SETTINGS_SCREEN),
  textScreen({ kind: "width" }, "", SETTINGS_SCREEN),
  textScreen({ kind: "from", pool: POOL_SCREEN, bucket: "day" }, "", RANGE_SCREEN),
];

function selectedOn(screen: ListScreen, selected: number): ListScreen {
  return { ...screen, selected } as ListScreen;
}

// ---- transition: rules that hold on every screen ----------------------------------------------

test.each([...LISTS.map(({ screen }) => screen), ...TEXTS])("Esc quits with 0 and Ctrl-C with 130 from $kind", (screen) => {
  expect(transition(screen, KEY.escape, NO_DATA)).toEqual({ screen: { kind: "quit", exit_code: 0 }, effect: NONE });
  expect(transition(screen, KEY.interrupt, NO_DATA)).toEqual({ screen: { kind: "quit", exit_code: 130 }, effect: NONE });
});

test("the quit screen ignores every key", () => {
  const quit: Screen = { kind: "quit", exit_code: 0 };
  for (const input of ALL_INPUTS) {
    expect(transition(quit, input, NO_DATA)).toEqual({ screen: quit, effect: NONE });
  }
});

test.each(LISTS)("Up and Down move the highlight on $name and clamp at both ends", ({ screen, data }) => {
  const row_count = rows(screen, data).length;
  const last = Math.max(row_count - 1, 0);
  expect(transition(screen, KEY.up, data)).toEqual({ screen: selectedOn(screen, 0), effect: NONE });
  let moved: Screen = screen;
  for (let step = 0; step < row_count + 2; step += 1) {
    moved = transition(moved, KEY.down, data).screen;
  }
  expect(moved).toEqual(selectedOn(screen, last));
  expect(transition(moved, KEY.down, data)).toEqual({ screen: moved, effect: NONE });
  if (row_count > 1) {
    expect(transition(screen, KEY.down, data).screen).toEqual(selectedOn(screen, 1));
    expect(transition(moved, KEY.up, data).screen).toEqual(selectedOn(screen, last - 1));
  }
});

test.each(LISTS)("Right opens the highlighted row of $name exactly as Enter does", ({ screen, data }) => {
  for (let selected = 0; selected < rows(screen, data).length; selected += 1) {
    const on = selectedOn(screen, selected);
    expect(transition(on, KEY.right, data)).toEqual(transition(on, KEY.enter, data));
  }
});

test.each([
  { name: "home stays", screen: HOME, back: HOME, effect: NONE },
  { name: "pools → home", screen: POOLS_SCREEN, back: HOME, effect: { kind: "health" } },
  { name: "pool → its pools page", screen: POOL_SCREEN, back: POOLS_SCREEN, effect: { kind: "pools", limit: 10, offset: 10 } },
  { name: "bucket → pool", screen: BUCKET_SCREEN, back: POOL_SCREEN, effect: { kind: "pool", address: POOL } },
  { name: "range → bucket", screen: RANGE_SCREEN, back: BUCKET_SCREEN, effect: BLANK },
  { name: "compare → range", screen: COMPARE_SCREEN, back: RANGE_SCREEN, effect: BLANK },
  { name: "volume → pool", screen: VOLUME_SCREEN, back: POOL_SCREEN, effect: { kind: "pool", address: POOL } },
  { name: "swaps → pool", screen: SWAPS_SCREEN, back: POOL_SCREEN, effect: { kind: "pool", address: POOL } },
  { name: "backfill → where it was asked", screen: BACKFILL_SCREEN, back: POOL_SCREEN, effect: { kind: "pool", address: POOL } },
  { name: "health → home", screen: HEALTH_SCREEN, back: HOME, effect: { kind: "health" } },
  { name: "settings → home", screen: SETTINGS_SCREEN, back: HOME, effect: { kind: "health" } },
] as const)("Left goes back one screen: $name", ({ screen, back, effect }) => {
  expect(transition(screen, KEY.left, NO_DATA)).toEqual({ screen: back, effect } as never);
});

test("Left returns to the screen as it was left, highlight included", () => {
  const pools = selectedOn(POOLS_SCREEN, 2);
  const opened = transition(pools, KEY.enter, POOLS_PAGE).screen;
  expect(opened).toMatchObject({ kind: "pool", address: syntheticPool(13) });
  expect(transition(opened, KEY.left, NO_DATA).screen).toEqual(pools);
});

test.each(LISTS)("keys with no meaning on $name are no-ops", ({ screen, data }) => {
  const paging = screen.kind === "pools" || screen.kind === "swaps";
  const inert = [KEY.backspace, KEY.home, KEY.end, KEY.letter, KEY.unknown, ...(paging ? [] : [KEY.previous_page, KEY.next_page])];
  for (const key of inert) {
    expect(transition(screen, key, data)).toEqual({ screen, effect: NONE });
  }
});

test.each(LISTS)("a timer tick re-reads health on Home and Health only: $name", ({ screen, data }) => {
  const refreshes = screen.kind === "home" || screen.kind === "health";
  expect(transition(screen, { kind: "tick" }, data)).toEqual({ screen, effect: refreshes ? { kind: "health" } : NONE });
});

// ---- transition: Enter per screen --------------------------------------------------------------

function enterOn(screen: ListScreen, selected: number, data: Data) {
  return transition(selectedOn(screen, selected), KEY.enter, data);
}

test("Home opens Pools, Health, the backfill input and Settings, each returning to Home's highlight", () => {
  expect(enterOn(HOME, 0, HEALTH_DATA)).toEqual({ screen: { kind: "pools", page: 0, selected: 0, back: HOME }, effect: { kind: "pools", limit: 10, offset: 0 } });
  const on_health = selectedOn(HOME, 1);
  expect(enterOn(HOME, 1, HEALTH_DATA)).toEqual({ screen: { kind: "health", selected: 0, back: on_health }, effect: { kind: "health" } });
  expect(enterOn(HOME, 2, HEALTH_DATA)).toEqual({ screen: textScreen({ kind: "backfill" }, "", selectedOn(HOME, 2)), effect: BLANK });
  expect(enterOn(HOME, 3, HEALTH_DATA)).toEqual({ screen: { kind: "settings", selected: 0, back: selectedOn(HOME, 3) }, effect: BLANK });
});

test("Home's menu stays usable when health fails: Settings is how a wrong base URL gets fixed", () => {
  expect(rows(HOME, FAILED).map((row) => row.label)).toEqual(["Pools", "Health", "Backfill", "Settings"]);
});

test("Pools: a page row opens its pool, the last row opens the address input, Retry appears only on failure", () => {
  expect(rows(POOLS_SCREEN, POOLS_PAGE).map((row) => row.label)).toEqual([syntheticPool(11), syntheticPool(12), syntheticPool(13), SEARCH_LABEL]);
  expect(enterOn(POOLS_SCREEN, 1, POOLS_PAGE)).toEqual({
    screen: { kind: "pool", address: syntheticPool(12), selected: 0, back: selectedOn(POOLS_SCREEN, 1) },
    effect: { kind: "pool", address: syntheticPool(12) },
  });
  expect(enterOn(POOLS_SCREEN, 3, POOLS_PAGE)).toEqual({ screen: textScreen({ kind: "address" }, "", selectedOn(POOLS_SCREEN, 3)), effect: BLANK });
  expect(rows(POOLS_SCREEN, FAILED).map((row) => row.label)).toEqual([SEARCH_LABEL, "Retry"]);
  expect(enterOn(POOLS_SCREEN, 1, FAILED)).toEqual({ screen: selectedOn(POOLS_SCREEN, 1), effect: { kind: "pools", limit: 10, offset: 10 } });
});

test("Pool opens the volume form, its swaps from the newest page, and the backfill input", () => {
  expect(enterOn(POOL_SCREEN, 0, NO_DATA)).toEqual({ screen: { kind: "bucket", pool: POOL_SCREEN, selected: 0, back: POOL_SCREEN }, effect: BLANK });
  const on_swaps = selectedOn(POOL_SCREEN, 1) as PoolScreen;
  expect(enterOn(POOL_SCREEN, 1, NO_DATA)).toEqual({
    screen: { kind: "swaps", pool: on_swaps, cursor_stack: [null], selected: 0 },
    effect: { kind: "swaps", address: POOL, limit: 20, before: null },
  });
  expect(enterOn(POOL_SCREEN, 2, NO_DATA).screen).toMatchObject({ kind: "text", field: { kind: "backfill" } });
  expect(rows(POOL_SCREEN, FAILED).at(-1)?.label).toBe("Retry");
  expect(enterOn(POOL_SCREEN, 3, FAILED).effect).toEqual({ kind: "pool", address: POOL });
});

test("the volume form is three list screens, then the result loads", () => {
  expect(enterOn(BUCKET_SCREEN, 1, NO_DATA)).toEqual({ screen: { kind: "range", pool: POOL_SCREEN, bucket: "day", selected: 0, back: selectedOn(BUCKET_SCREEN, 1) }, effect: BLANK });
  expect(rows(RANGE_SCREEN, NO_DATA).map((row) => row.label)).toEqual(["24h", "7d", "30d", "custom…"]);
  expect(enterOn(RANGE_SCREEN, 1, NO_DATA).screen).toMatchObject({ kind: "compare", bucket: "hour", range: { kind: "named", name: "7d" } });
  expect(enterOn(RANGE_SCREEN, 3, NO_DATA).screen).toMatchObject({ kind: "text", field: { kind: "from", bucket: "hour" } });
  expect(enterOn(COMPARE_SCREEN, 0, NO_DATA)).toEqual({
    screen: { kind: "volume", pool: POOL_SCREEN, query: QUERY, selected: 0 },
    effect: { kind: "volume", address: POOL, ...QUERY },
  });
  expect(enterOn(COMPARE_SCREEN, 1, NO_DATA).effect).toEqual({ kind: "volume", address: POOL, ...QUERY, compare: true });
});

test("the volume result offers a new query that comes back to it on Left, and Retry on failure", () => {
  const step = enterOn(VOLUME_SCREEN, 0, NO_DATA);
  expect(step).toEqual({ screen: { kind: "bucket", pool: POOL_SCREEN, selected: 0, back: VOLUME_SCREEN }, effect: BLANK });
  expect(transition(step.screen, KEY.left, NO_DATA).effect).toEqual({ kind: "volume", address: POOL, ...QUERY });
  expect(rows(VOLUME_SCREEN, FAILED).map((row) => row.label)).toEqual(["New query", "Retry"]);
});

test("swap rows are for reading: Enter on one does nothing, Retry on a failed page reloads it", () => {
  expect(enterOn(SWAPS_SCREEN, 1, SWAPS_MORE)).toEqual({ screen: selectedOn(SWAPS_SCREEN, 1), effect: NONE });
  expect(enterOn(SWAPS_SCREEN, 0, FAILED)).toEqual({ screen: SWAPS_SCREEN, effect: { kind: "swaps", address: POOL, limit: 20, before: "c1" } });
});

test("the backfill result has no rows, so Enter can never post a second job", () => {
  expect(rows(BACKFILL_SCREEN, FAILED)).toEqual([]);
  expect(enterOn(BACKFILL_SCREEN, 0, FAILED)).toEqual({ screen: BACKFILL_SCREEN, effect: NONE });
});

test("Health's Refresh row reloads it", () => {
  expect(enterOn(HEALTH_SCREEN, 0, HEALTH_DATA)).toEqual({ screen: HEALTH_SCREEN, effect: { kind: "health" } });
});

test("Settings opens the base URL and width inputs and toggles colour in place", () => {
  expect(enterOn(SETTINGS_SCREEN, 0, NO_DATA).screen).toMatchObject({ kind: "text", field: { kind: "base_url" } });
  expect(enterOn(SETTINGS_SCREEN, 1, NO_DATA).screen).toMatchObject({ kind: "text", field: { kind: "width" } });
  expect(enterOn(SETTINGS_SCREEN, 2, NO_DATA)).toEqual({ screen: selectedOn(SETTINGS_SCREEN, 2), effect: { kind: "toggle_color" } });
});

// ---- transition: paging ------------------------------------------------------------------------

test.each([
  { name: "] from a middle page", screen: POOLS_SCREEN, key: "next_page", data: POOLS_PAGE, page: 2 },
  { name: "] on the last page clamps", screen: { ...POOLS_SCREEN, page: 2 }, key: "next_page", data: LAST_POOLS_PAGE, page: null },
  { name: "] on a failed page stays (its Retry row reloads)", screen: POOLS_SCREEN, key: "next_page", data: FAILED, page: null },
  { name: "[ from a middle page", screen: POOLS_SCREEN, key: "previous_page", data: POOLS_PAGE, page: 0 },
  { name: "[ from a failed page still knows the previous one", screen: POOLS_SCREEN, key: "previous_page", data: FAILED, page: 0 },
  { name: "[ on the first page clamps", screen: { ...POOLS_SCREEN, page: 0 }, key: "previous_page", data: POOLS_PAGE, page: null },
] as const)("pools paging: $name", ({ screen, key, data, page }) => {
  const from = { ...screen, selected: 2 } as ListScreen;
  const step = transition(from, KEY[key], data);
  if (page === null) {
    expect(step).toEqual({ screen: from, effect: NONE });
  } else {
    expect(step).toEqual({ screen: { ...POOLS_SCREEN, page, selected: 0 }, effect: { kind: "pools", limit: 10, offset: page * 10 } });
  }
});

test.each([
  { name: "] pushes the page's next_cursor", screen: SWAPS_SCREEN, key: "next_page", data: SWAPS_MORE, stack: [null, "c1", "c2"] },
  { name: "] at the end of the log clamps", screen: SWAPS_SCREEN, key: "next_page", data: SWAPS_END, stack: null },
  { name: "] on a failed page stays", screen: SWAPS_SCREEN, key: "next_page", data: FAILED, stack: null },
  { name: "[ pops back to the newer page", screen: SWAPS_SCREEN, key: "previous_page", data: SWAPS_MORE, stack: [null] },
  { name: "[ on the newest page clamps", screen: { ...SWAPS_SCREEN, cursor_stack: [null] }, key: "previous_page", data: SWAPS_MORE, stack: null },
] as const)("swaps paging: $name", ({ screen, key, data, stack }) => {
  const step = transition(screen as Screen, KEY[key], data);
  if (stack === null) {
    expect(step).toEqual({ screen: screen as Screen, effect: NONE });
  } else {
    expect(step.screen).toEqual({ kind: "swaps", pool: POOL_SCREEN, cursor_stack: stack, selected: 0 });
    expect(step.effect).toEqual({ kind: "swaps", address: POOL, limit: 20, before: stack.at(-1) ?? null });
  }
});

// ---- text input ------------------------------------------------------------------------------

function typeInto(screen: Screen, keys: readonly Key[]): Screen {
  return keys.reduce((current, key) => transition(current, key, NO_DATA).screen, screen);
}

function chars(text: string): Key[] {
  return [...text].map((char) => ({ kind: "char", char }));
}

test("typing inserts at the caret, Backspace deletes before it, and the caret moves and clamps", () => {
  const empty = textScreen({ kind: "width" }, "", SETTINGS_SCREEN);
  expect(typeInto(empty, chars("120"))).toMatchObject({ value: "120", caret: 3 });
  expect(typeInto(empty, [...chars("10"), KEY.left, ...chars("2")])).toMatchObject({ value: "120", caret: 2 });
  expect(typeInto(empty, [...chars("1x0"), KEY.left, KEY.backspace])).toMatchObject({ value: "10", caret: 1 });
  expect(typeInto(empty, [...chars("80"), KEY.home, KEY.backspace])).toMatchObject({ value: "80", caret: 0 });
  expect(typeInto(empty, [...chars("80"), KEY.home, KEY.end, KEY.right, KEY.right])).toMatchObject({ value: "80", caret: 2 });
  expect(typeInto(empty, [...chars("[::1]")])).toMatchObject({ value: "[::1]" });
});

test("Up, Down and a tick leave a text input alone", () => {
  const screen = textScreen({ kind: "width" }, "12", SETTINGS_SCREEN);
  for (const input of [KEY.up, KEY.down, KEY.unknown, { kind: "tick" } as const]) {
    expect(transition(screen, input, NO_DATA)).toEqual({ screen, effect: NONE });
  }
});

test("Left at the start of an empty input goes back to the screen that opened it", () => {
  const screen = textScreen({ kind: "address" }, "", POOLS_SCREEN);
  expect(transition(screen, KEY.left, NO_DATA)).toEqual({ screen: POOLS_SCREEN, effect: { kind: "pools", limit: 10, offset: 10 } });
});

test("a held Left stops at the start of a draft instead of discarding it", () => {
  const draft = textScreen({ kind: "address" }, POOL, POOLS_SCREEN);
  const held = typeInto(draft, Array(POOL.length + 5).fill(KEY.left));
  expect(held).toEqual({ ...draft, caret: 0 });
  expect(renderFrame(held, NO_DATA, SETTINGS, null).lines.at(-1)).toBe("type · ←→ caret · ⌫ to clear · ← back when empty · ⏎ submit · esc quit");
  expect(renderFrame(textScreen({ kind: "address" }, "", POOLS_SCREEN), NO_DATA, SETTINGS, null).lines.at(-1)).toBe("type · ← back · ⏎ submit · esc quit");
});

test.each(TEXTS)("an invalid $field.kind keeps editing with the error, and typing clears it", (screen) => {
  const invalid = typeInto(screen, chars("nope"));
  const step = transition(invalid, KEY.enter, NO_DATA);
  expect(step.effect).toEqual(NONE);
  expect(step.screen).toMatchObject({ kind: "text", value: "nope", caret: 4 });
  expect((step.screen as TextScreen).error).toEqual(expect.any(String));
  expect(transition(step.screen, chars("x")[0]!, NO_DATA).screen).toMatchObject({ error: null });
});

test.each([
  { field: { kind: "address" }, back: POOLS_SCREEN, value: POOL, screen: { kind: "pool", address: POOL, selected: 0, back: POOLS_SCREEN }, effect: { kind: "pool", address: POOL } },
  { field: { kind: "backfill" }, back: HOME, value: BACKFILL_INSTANT, screen: { kind: "backfill", from: BACKFILL_INSTANT, selected: 0, back: HOME }, effect: { kind: "backfill", from: BACKFILL_INSTANT } },
  { field: { kind: "base_url" }, back: SETTINGS_SCREEN, value: "http://[::1]:9000", screen: SETTINGS_SCREEN, effect: { kind: "settings", patch: { base_url: "http://[::1]:9000" } } },
  { field: { kind: "width" }, back: SETTINGS_SCREEN, value: "auto", screen: SETTINGS_SCREEN, effect: { kind: "settings", patch: { width: null } } },
  { field: { kind: "width" }, back: SETTINGS_SCREEN, value: "120", screen: SETTINGS_SCREEN, effect: { kind: "settings", patch: { width: 120 } } },
] as const)("a valid $field.kind submits: $value", ({ field, back, value, screen, effect }) => {
  expect(transition(textScreen(field, value, back), KEY.enter, NO_DATA)).toEqual({ screen, effect } as never);
});

test("a custom range asks from, then to, then the compare question", () => {
  const from = textScreen({ kind: "from", pool: POOL_SCREEN, bucket: "day" }, "2026-09-01T00:00:00Z", RANGE_SCREEN);
  const to = transition(from, KEY.enter, NO_DATA).screen;
  expect(to).toEqual(textScreen({ kind: "to", pool: POOL_SCREEN, bucket: "day", from: "2026-09-01T00:00:00Z" }, "", from));
  const compare = transition(typeInto(to, chars("2026-10-01T00:00:00Z")), KEY.enter, NO_DATA).screen;
  expect(compare).toMatchObject({ kind: "compare", bucket: "day", range: { kind: "custom", from: "2026-09-01T00:00:00Z", to: "2026-10-01T00:00:00Z" } });
});

test("parseWidth accepts auto and a bounded column count only", () => {
  expect(parseWidth("auto")).toBeNull();
  expect(parseWidth("")).toBeNull();
  expect(parseWidth("120")).toBe(120);
  expect(parseWidth("39")).toBe("invalid");
  expect(parseWidth("wide")).toBe("invalid");
});

// ---- key decoding from raw bytes ---------------------------------------------------------------

async function keysFromBytes(bytes: readonly string[], settle_ms: number): Promise<{ source: KeySource; stream: PassThrough }> {
  const stream = new PassThrough();
  const source = streamKeys(stream);
  for (const chunk of bytes) {
    stream.write(chunk);
  }
  await Bun.sleep(settle_ms);
  return { source, stream };
}

async function take(source: KeySource, count: number): Promise<Key[]> {
  const keys: Key[] = [];
  for (let index = 0; index < count; index += 1) {
    keys.push(await source.next());
  }
  return keys;
}

test("raw bytes decode to keys, queued in order until read", async () => {
  const { source } = await keysFromBytes(["\x1b[A", "\x1b[B", "\x1b[C\x1b[D", "\r", "\x03", "[", "]", "\x7f", "a", "\x1b[H", "\x1b[F", "\t"], 20);
  expect(await take(source, 13)).toEqual([
    KEY.up, KEY.down, KEY.right, KEY.left, KEY.enter, KEY.interrupt, KEY.previous_page, KEY.next_page, KEY.backspace,
    { kind: "char", char: "a" }, KEY.home, KEY.end, KEY.unknown,
  ]);
  source.close();
});

test("a lone \\x1b is Esc only after the escape-sequence timeout; \\x1b[A is Up at once", async () => {
  const { source, stream } = await keysFromBytes(["\x1b"], 50);
  const seen: Key[] = [];
  void source.next().then((key) => seen.push(key));
  await Bun.sleep(20);
  expect(seen).toEqual([]);
  await Bun.sleep(600);
  expect(seen).toEqual([KEY.escape]);
  stream.write("\x1b[A");
  expect(await source.next()).toEqual(KEY.up);
  source.close();
});

test("a character within readline's Esc window is Esc then that character, never lost", async () => {
  const { source } = await keysFromBytes(["\x1b]", "\x1ba", "\x01", "b"], 20);
  expect(await take(source, 6)).toEqual([KEY.escape, KEY.next_page, KEY.escape, { kind: "char", char: "a" }, KEY.unknown, { kind: "char", char: "b" }]);
  source.close();
});

test("Ctrl-C swallowed by an unfinished escape sequence still interrupts", async () => {
  const { source, stream } = await keysFromBytes(["\x1b["], 20);
  stream.write("\x03");
  expect(await source.next()).toEqual(KEY.interrupt);
  await source.interrupted;
  source.close();
});

test("a key held through a stall queues only the newest 32 keys", async () => {
  const { source } = await keysFromBytes([`${"\x1b[B".repeat(40)}\r`], 20);
  expect(await take(source, 32)).toEqual([...Array(31).fill(KEY.down), KEY.enter]);
  expect(source.keepLatest()).toBe(false);
  source.close();
});

test("keys typed during a load collapse to the last one", async () => {
  const { source } = await keysFromBytes(["\x1b[B\x1b[B\x1b[A\r"], 20);
  expect(source.keepLatest()).toBe(true);
  expect(await source.next()).toEqual(KEY.enter);
  expect(source.keepLatest()).toBe(false);
  source.close();
});

test("a key that ends the session survives a load ahead of later keys", async () => {
  const { source } = await keysFromBytes(["\x1b[B", "\x03", "\x1b[A"], 20);
  source.keepLatest();
  expect(await take(source, 1)).toEqual([KEY.interrupt]);
  source.close();
});

test("stdin ending or failing reads as a closed session, now and on every later read", async () => {
  const ended = await keysFromBytes([], 0);
  const waiting = ended.source.next();
  ended.stream.end();
  expect(await waiting).toEqual({ kind: "closed", exit_code: 0 });
  expect(await ended.source.next()).toEqual({ kind: "closed", exit_code: 0 });
  const failed = await keysFromBytes([], 0);
  failed.stream.destroy(new Error("EIO"));
  expect(await failed.source.next()).toEqual({ kind: "closed", exit_code: 1 });
});

// ---- rendering -------------------------------------------------------------------------------

const SETTINGS: Settings = { base_url: "http://127.0.0.1:8080", width: null, color: false };
const UNBOUNDED: Viewport = { columns: null, rows: null };
const INVERSE = "\x1b[7m";

function lineWith(lines: readonly string[], text: string): string {
  const line = lines.find((candidate) => candidate.includes(text));
  if (line === undefined) {
    throw new Error(`no line contains ${text}`);
  }
  return line;
}

function bucket(start: string, swap_count: number, volume_usd: string | null): VolumeBucket {
  return { start, swap_count, volume_x: String(swap_count), volume_x_raw: "0", volume_y: String(swap_count), volume_y_raw: "0", volume_usd, unpriced_swap_count: 0 };
}

test("Home marks the highlighted row only, after the health line", () => {
  const frame = renderFrame(selectedOn(HOME, 2), HEALTH_DATA, SETTINGS, null);
  expect(frame.lines.slice(2)).toEqual([
    "  indexer backfilling · lag 3 s · 1 open · 0 blocked jobs",
    "",
    "  Pools",
    "  Health",
    `${MARKER}Backfill`,
    "  Settings",
    "",
    "↑↓ move · ⏎/→ open · esc quit",
  ]);
  expect(frame.lines[frame.highlighted!]).toBe(`${MARKER}Backfill`);
  expect(frame.caret).toBeNull();
});

test("colour on paints the highlighted row in inverse video; colour off leaves the marker alone", () => {
  const frame = renderFrame(selectedOn(HOME, 1), HEALTH_DATA, SETTINGS, null);
  const colored = paintFrame(frame, true, UNBOUNDED).lines;
  expect(lineWith(colored, "Health")).toBe(`${INVERSE}${MARKER}Health\x1b[27m`);
  expect(colored.filter((line) => line.includes(INVERSE))).toHaveLength(1);
  const plain = paintFrame(frame, false, UNBOUNDED).lines;
  expect(plain.join("\n")).not.toContain("\x1b[");
  expect(lineWith(plain, "Health")).toBe(`${MARKER}Health`);
});

test("on Pools the highlight lands on the table line of the pool, numbered by rank", () => {
  const frame = renderFrame(selectedOn(POOLS_SCREEN, 1), POOLS_PAGE, SETTINGS, null);
  const marked = frame.lines[frame.highlighted!]!;
  expect(marked.startsWith(MARKER)).toBe(true);
  expect(marked).toMatch(/│ 12 │ Pq12x+ │/);
  expect(frame.lines.filter((line) => line.startsWith(MARKER))).toHaveLength(1);
  expect(frame.lines).toContain("  page 2 of 3 · 23 pools");
  expect(frame.lines).toContain(`  ${SEARCH_LABEL}`);
  const on_search = renderFrame(selectedOn(POOLS_SCREEN, 3), POOLS_PAGE, SETTINGS, null);
  expect(on_search.lines[on_search.highlighted!]).toBe(`${MARKER}${SEARCH_LABEL}`);
});

test("inverse video survives the bar colouring's resets on a highlighted table row", () => {
  const frame = renderFrame(POOLS_SCREEN, POOLS_PAGE, SETTINGS, null);
  const line = paintFrame(frame, true, UNBOUNDED).lines[frame.highlighted!]!;
  expect(line.startsWith(INVERSE)).toBe(true);
  expect(line).toContain(`\x1b[0m${INVERSE}`);
});

test("a failed load shows the error above the list and offers Retry", () => {
  const frame = renderFrame(POOLS_SCREEN, FAILED, SETTINGS, null);
  const error_line = frame.lines.indexOf("  error: GET /v1/pools returned 503");
  expect(error_line).toBeGreaterThan(0);
  expect(frame.lines.indexOf(`${MARKER}${SEARCH_LABEL}`)).toBeGreaterThan(error_line);
  expect(frame.lines).toContain("  Retry");
  expect(frame.lines.join("\n")).not.toMatch(/page \d+ of/);
});

test("a load in flight shows its spinner line above the legend", () => {
  const frame = renderFrame(HOME, NO_DATA, SETTINGS, "⠋ loading health…");
  expect(frame.lines.slice(-2)).toEqual(["  ⠋ loading health…", "↑↓ move · ⏎/→ open · esc quit"]);
});

test("a text input shows the value, the caret position and the error under it", () => {
  const screen: TextScreen = { ...textScreen({ kind: "width" }, "12x", SETTINGS_SCREEN), caret: 1, error: "auto, or a column count from 40 to 1000" };
  const frame = renderFrame(screen, NO_DATA, SETTINGS, null);
  expect(frame.lines).toContain("  > 12x");
  expect(frame.lines).toContain("  expected auto, or a column count from 40 to 1000");
  expect(frame.caret).toEqual({ line: frame.lines.indexOf("  > 12x"), column: "  > 1".length });
  expect(frame.highlighted).toBeNull();
});

test("the legend adapts: no back on Home, paging on Pools and Swaps, nothing to open on a read-only page", () => {
  const legend = (screen: Screen, data: Data) => renderFrame(screen, data, SETTINGS, null).lines.at(-1);
  expect(legend(HOME, NO_DATA)).toBe("↑↓ move · ⏎/→ open · esc quit");
  expect(legend(POOLS_SCREEN, POOLS_PAGE)).toBe("↑↓ move · ⏎/→ open · ← back · [ ] page · esc quit");
  expect(legend(SWAPS_SCREEN, SWAPS_MORE)).toBe("↑↓ move · ← back · [ ] page · esc quit");
  expect(legend(BACKFILL_SCREEN, NO_DATA)).toBe("← back · esc quit");
  expect(legend(POOL_SCREEN, NO_DATA)).toBe("↑↓ move · ⏎/→ open · ← back · esc quit");
});

test("pool, volume, swaps, backfill, health and settings render their data above their rows", () => {
  const text = (screen: Screen, data: Data) => renderFrame(screen, data, SETTINGS, null).lines.join("\n");
  expect(text(POOL_SCREEN, { kind: "pool", summary: { ...pool(POOL, "0.3"), decimals_x: 9 } })).toMatch(/decimals_x\s*│\s*9[\s\S]*▸ Volume/);
  const buckets = [bucket("2026-10-01T00:00:00Z", 2, "0.1"), bucket("2026-10-01T01:00:00Z", 3, "0.2")];
  expect(text(VOLUME_SCREEN, { kind: "volume", buckets, compared: null, compare_error: "GET x returned 502" })).toMatch(
    /total \$0\.3 · 5 swaps · 2 buckets · 0 unpriced\n {2}compare failed: GET x returned 502[\s\S]*▸ New query/,
  );
  expect(text(SWAPS_SCREEN, SWAPS_MORE)).toContain("page 2 · newest first");
  expect(text(SWAPS_SCREEN, SWAPS_MORE)).toContain("older swaps: ]");
  expect(text(SWAPS_SCREEN, SWAPS_END)).toContain("end of the log");
  expect(text(BACKFILL_SCREEN, { kind: "backfill", job: { job_id: 7, start_slot: 1000, end_slot: 1499 } })).toMatch(/end_slot\s*│\s*1499[\s\S]*job 7 queued/);
  expect(text(HEALTH_SCREEN, HEALTH_DATA)).toMatch(/lag_seconds\s*│\s*3[\s\S]*▸ Refresh/);
  expect(text(SETTINGS_SCREEN, NO_DATA)).toMatch(/▸ Base URL {2}http:\/\/127\.0\.0\.1:8080\n {2}Width {5}auto\n {2}Colour {4}off/);
});

test("painting clips each line to the width with an ellipsis, and auto clips nothing", () => {
  const frame = renderFrame(SETTINGS_SCREEN, NO_DATA, SETTINGS, null);
  expect(paintFrame(frame, false, { columns: 12, rows: null }).lines).toContain("▸ Base URL …");
  expect(paintFrame(frame, false, UNBOUNDED).lines).toContain("▸ Base URL  http://127.0.0.1:8080");
});

// ---- windowing to the terminal's height ---------------------------------------------------------

const HOURS_7D = Array.from({ length: 168 }, (_, hour) => bucket(new Date(Date.UTC(2026, 8, 24) + hour * 3_600_000).toISOString().replace(".000", ""), 1, "0.1"));
const VOLUME_7D: Data = { kind: "volume", buckets: HOURS_7D, compared: null, compare_error: null };
const SWAPS_PAGE: Data = { kind: "swaps", swaps: Array.from({ length: 20 }, (_, index) => swap(index % 9 + 1)), next_cursor: "c2" };
const VOLUME_7D_SCREEN = { ...VOLUME_SCREEN, query: { ...QUERY, range: { kind: "named", name: "7d" } } } as const satisfies Screen;

function expectWindowed(frame: Frame, rows: number, focus: string): void {
  const painted = paintFrame(frame, false, { columns: 100, rows });
  expect(frame.lines.length).toBeGreaterThan(rows);
  expect(painted.lines.length).toBeLessThanOrEqual(rows - 1);
  expect(painted.lines[0]).toBe(frame.lines[0]!);
  expect(painted.lines.at(-1)).toBe(frame.lines.at(-1)!);
  expect(painted.lines.filter((line) => line.startsWith(MARKER))).toEqual([expect.stringContaining(focus)]);
}

test.each([24, 12, 6])("a 168-hour volume table in %i rows keeps the title, the legend and New query on screen", (rows) => {
  const on_new_query = renderFrame(VOLUME_7D_SCREEN, VOLUME_7D, SETTINGS, null);
  expect(on_new_query.lines.at(-1)).toBe("↑↓ move · ⏎/→ open · ← back · esc quit");
  expectWindowed(on_new_query, rows, "New query");
});

test.each([24, 12])("a swaps page in %i rows scrolls to the highlighted swap", (rows) => {
  expectWindowed(renderFrame(SWAPS_SCREEN, SWAPS_PAGE, SETTINGS, null), rows, `${MARKER}│`);
  const last = renderFrame(selectedOn(SWAPS_SCREEN, 19), SWAPS_PAGE, SETTINGS, null);
  expectWindowed(last, rows, swap(19 % 9 + 1).signature.slice(0, 8));
  expect(paintFrame(last, false, { columns: 100, rows }).lines[0]).toStartWith("Swaps");
});

test("a load's spinner stays on screen with the legend however the body is cut", () => {
  const painted = paintFrame(renderFrame(VOLUME_7D_SCREEN, VOLUME_7D, SETTINGS, "⠋ loading volume…"), false, { columns: 100, rows: 12 });
  expect(painted.lines.slice(-2)).toEqual(["  ⠋ loading volume…", "↑↓ move · ⏎/→ open · ← back · esc quit"]);
});

test("a frame that fits is painted whole", () => {
  const frame = renderFrame(HOME, HEALTH_DATA, SETTINGS, null);
  expect(paintFrame(frame, false, { columns: 100, rows: 24 }).lines).toEqual(frame.lines);
});

test("the caret follows its line when the window moves and stops at the last column of a clipped input", () => {
  const url = `http://example.com/${"x".repeat(120)}`;
  const screen: TextScreen = { ...textScreen({ kind: "base_url" }, url, SETTINGS_SCREEN), error: "an http:// or https:// URL" };
  const painted = paintFrame(renderFrame(screen, NO_DATA, SETTINGS, null), false, { columns: 60, rows: 4 });
  expect(painted.lines.length).toBeLessThanOrEqual(3);
  expect(painted.caret).toEqual({ line: painted.lines.findIndex((line) => line.startsWith("  > http")), column: 59 });
});

// ---- the shell against the mock API ---------------------------------------------------------------

let api: MockApi;
beforeAll(() => {
  api = startMockApi({ pool_count: 23 });
});
afterAll(() => api.stop());

type MemoryTerminal = Terminal & { output: () => string; raw: () => boolean; resize: (rows: number) => void };

function memoryTerminal(initial_rows: number | null = null): MemoryTerminal {
  let output = "";
  let raw = false;
  let rows = initial_rows;
  const listeners = new Set<() => void>();
  return {
    write: (text) => {
      output += text;
    },
    setRawMode: (on) => {
      raw = on;
    },
    columns: () => null,
    rows: () => rows,
    onResize: (listener) => {
      listeners.add(listener);
      return () => listeners.delete(listener);
    },
    output: () => output,
    raw: () => raw,
    resize: (next) => {
      rows = next;
      for (const listener of listeners) {
        listener();
      }
    },
  };
}

const NEVER = new Promise<void>(() => {});

// Asking for one more key than the script holds is a test failure. Scripted keys are pressed
// only when read, so none is ever typed during a load.
function scriptedKeys(keys: readonly Key[]): KeySource & { remaining: () => number } {
  const queue = [...keys];
  return {
    next: async () => {
      const key = queue.shift();
      if (key === undefined) {
        throw new Error("the shell read more keys than the script holds");
      }
      return key;
    },
    keepLatest: () => false,
    interrupted: NEVER,
    close: () => {},
    remaining: () => queue.length,
  };
}

// Keys typed during a load collapse to the last one, so a script that navigates leaves each
// (local, fast) load time to land before the next key.
function typedKeys(): { keys: KeySource; type: (bytes: string) => void; press: (keys: readonly string[]) => Promise<void>; stream: PassThrough } {
  const stream = new PassThrough();
  const press = async (keys: readonly string[]) => {
    for (const key of keys) {
      stream.write(key);
      await Bun.sleep(25);
    }
  };
  return { keys: streamKeys(stream), type: (bytes) => stream.write(bytes), press, stream };
}

const DOWN = "\x1b[B";

function shellIo(): Io {
  return { write_stdout: () => {}, write_stderr: () => {}, env: {}, stdin_is_tty: true, stdout_is_tty: true, stderr_is_tty: true, now: () => NOW };
}

function options(base_url = api.base_url): GlobalOptions {
  return { base_url, timeout_ms: 2_000, output: "table", input: true, color: false };
}

function requestsSince(start: number): string[] {
  return api.requested_urls.slice(start).map((url) => `${url.pathname}${url.search}`);
}

function lastFrame(terminal: MemoryTerminal): string {
  const frames = terminal.output().split("\x1b[H");
  return (frames.at(-1) ?? "").replace(/\x1b\[[0-9;?]*[A-Za-z ]/g, "").replaceAll("\r\n", "\n");
}

function expectRestored(terminal: MemoryTerminal): void {
  const output = terminal.output();
  expect(output.startsWith(ALTERNATE_SCREEN_ON)).toBe(true);
  expect(output.endsWith(LEAVE)).toBe(true);
  expect(terminal.raw()).toBe(false);
}

const { up, down, left, enter, escape, interrupt, next_page } = KEY;

test("shell: pools → page 2 → a pool → volume → swaps → page 2 → back twice → Esc, each screen one call", async () => {
  const start = api.requested_urls.length;
  const terminal = memoryTerminal();
  const keys = scriptedKeys([
    enter, next_page, down, down, enter, // Pools, page 2, third row
    enter, enter, enter, enter, // Volume: hour, 24h, no compare
    left, down, enter, next_page, // back to the pool, Swaps, older page
    left, left, escape,
  ]);
  const exit_code = await runShell(shellIo(), options(), { terminal, keys });
  const picked = syntheticPool(12);
  expect(exit_code).toBe(0);
  expect(keys.remaining()).toBe(0);
  const calls = requestsSince(start);
  expect(calls.slice(0, 7)).toEqual([
    "/v1/health",
    "/v1/pools?limit=10&offset=0",
    "/v1/pools?limit=10&offset=10",
    `/v1/pools/${picked}`,
    `/v1/pools/${picked}/volume?bucket=hour&from=2026-09-30T03%3A00%3A00Z&to=2026-10-01T04%3A00%3A00Z`,
    `/v1/pools/${picked}`,
    `/v1/pools/${picked}/swaps?limit=20`,
  ]);
  expect(calls[7]).toStartWith(`/v1/pools/${picked}/swaps?limit=20&before=`);
  expect(calls.slice(8)).toEqual([`/v1/pools/${picked}`, "/v1/pools?limit=10&offset=10"]);
  // Back on page 2 with the pool it opened still highlighted.
  expect(lastFrame(terminal)).toMatch(new RegExp(`${MARKER}│ 13 │ ${picked}`));
  expectRestored(terminal);
});

test("shell: Ctrl-C in the middle of the volume form ends with 130 and hands the terminal back", async () => {
  const terminal = memoryTerminal();
  const keys = scriptedKeys([enter, enter, enter, down, interrupt]);
  expect(await runShell(shellIo(), options(), { terminal, keys })).toBe(130);
  expect(keys.remaining()).toBe(0);
  expectRestored(terminal);
});

test("shell: Esc inside a text input quits with 0", async () => {
  const terminal = memoryTerminal();
  const keys = scriptedKeys([down, down, enter, ...chars("2026"), escape]);
  expect(await runShell(shellIo(), options(), { terminal, keys })).toBe(0);
  expectRestored(terminal);
});

test("shell: a thrown error still hands the terminal back", async () => {
  const terminal = memoryTerminal();
  const keys = scriptedKeys([enter]);
  await expect(runShell(shellIo(), options(), { terminal, keys })).rejects.toThrow("more keys than the script holds");
  expectRestored(terminal);
});

test("shell: a failed load keeps the session alive and its Retry row reloads", async () => {
  const start = api.requested_urls.length;
  const terminal = memoryTerminal();
  // Pools, up to "Search by address…" from the top, the missing pool, then Retry (the row after Backfill).
  const keys = scriptedKeys([enter, up, ...Array(11).fill(down), enter, ...chars(MISSING_POOL), enter, down, down, down, enter, escape]);
  expect(await runShell(shellIo(), options(), { terminal, keys })).toBe(0);
  expect(requestsSince(start).slice(-2)).toEqual([`/v1/pools/${MISSING_POOL}`, `/v1/pools/${MISSING_POOL}`]);
  expect(lastFrame(terminal)).toContain("error: GET");
  expect(lastFrame(terminal)).toContain("pool_not_found");
  expect(lastFrame(terminal)).toContain(`${MARKER}Retry`);
});

// Pools, the search row, the slow pool, Volume, hour, 24h, no compare: the last is a 500 ms load.
const TO_SLOW_VOLUME = ["\r", ...Array(10).fill(DOWN), "\r", ...SLOW_POOL, "\r", "\r", "\r", "\r", "\r"];

test("shell: a slow load shows a spinner, and of the keys typed meanwhile only the last acts", async () => {
  const start = api.requested_urls.length;
  const terminal = memoryTerminal();
  const { keys, type, press } = typedKeys();
  const run = runShell(shellIo(), options(), { terminal, keys });
  await press(TO_SLOW_VOLUME);
  await Bun.sleep(200);
  // Three Lefts would walk back to Home; after the 500 ms load only one does.
  type("\x1b[D\x1b[D\x1b[D");
  await Bun.sleep(400);
  expect(lastFrame(terminal)).toContain(`Pool ${SLOW_POOL}`);
  type("\x03");
  expect(await run).toBe(130);
  expect(terminal.output()).toContain("loading volume…");
  expect(terminal.output()).toContain(`Volume ${SLOW_POOL} · hour · 24h`);
  expect(requestsSince(start).at(-1)).toBe(`/v1/pools/${SLOW_POOL}`);
});

test("shell: Ctrl-C during a load that never answers quits at once with 130", async () => {
  const silent = Bun.serve({ port: 0, fetch: () => new Promise<Response>(() => {}) });
  try {
    const terminal = memoryTerminal();
    const { keys, type } = typedKeys();
    const run = runShell(shellIo(), options(`http://127.0.0.1:${silent.port}`), { terminal, keys });
    await Bun.sleep(250);
    expect(terminal.output()).toContain("loading health…");
    const pressed = performance.now();
    type("\x03");
    expect(await run).toBe(130);
    expect(performance.now() - pressed).toBeLessThan(200);
    expectRestored(terminal);
  } finally {
    silent.stop(true);
  }
});

test("shell: stdin ending quits with 0 and failing quits with 1, the terminal handed back both times", async () => {
  for (const [close, exit_code] of [[(stream: PassThrough) => stream.end(), 0], [(stream: PassThrough) => stream.destroy(new Error("EIO")), 1]] as const) {
    const terminal = memoryTerminal();
    const { keys, stream } = typedKeys();
    const run = runShell(shellIo(), options(), { terminal, keys, refresh_ms: 20 });
    await Bun.sleep(50);
    close(stream);
    expect(await run).toBe(exit_code);
    expectRestored(terminal);
  }
});

test("shell: Home re-reads health on every tick without losing the key that arrives later", async () => {
  const start = api.requested_urls.length;
  const terminal = memoryTerminal();
  const keys: KeySource = { ...scriptedKeys([]), next: () => Bun.sleep(130).then(() => escape) };
  expect(await runShell(shellIo(), options(), { terminal, keys, refresh_ms: 40 })).toBe(0);
  expect(requestsSince(start).length).toBeGreaterThanOrEqual(3);
  expect(new Set(requestsSince(start))).toEqual(new Set(["/v1/health"]));
});

test("shell: keys that load nothing do not postpone Home's refresh", async () => {
  const start = api.requested_urls.length;
  const terminal = memoryTerminal();
  const { keys, type } = typedKeys();
  const run = runShell(shellIo(), options(), { terminal, keys, refresh_ms: 200 });
  for (let press = 0; press < 10; press += 1) {
    await Bun.sleep(50);
    type(press % 2 === 0 ? DOWN : "\x1b[A");
  }
  type("\x03");
  expect(await run).toBe(130);
  // 500 ms of a key every 50 ms: the entry load plus at least two refreshes.
  expect(requestsSince(start).filter((url) => url === "/v1/health").length).toBeGreaterThanOrEqual(3);
});

test("shell: Volume hour · 7d fits a 12-row terminal, and a resize repaints at the new height", async () => {
  const terminal = memoryTerminal(40);
  const { keys, type, press } = typedKeys();
  const run = runShell(shellIo(), options(), { terminal, keys });
  // Pools, the top pool, Volume, hour, 7d, no compare.
  await press(["\r", "\r", "\r", "\r", DOWN, "\r", "\r"]);
  await Bun.sleep(100);
  const tall = lastFrame(terminal).split("\n");
  expect(tall[0]).toBe(`Volume ${POOL} · hour · 7d`);
  terminal.resize(12);
  const short = lastFrame(terminal).split("\n");
  expect(short.length).toBeLessThan(tall.length);
  expect(short.length).toBeLessThanOrEqual(11);
  expect(short[0]).toBe(tall[0]);
  expect(short).toContain(`${MARKER}New query`);
  expect(short.at(-1)).toBe("↑↓ move · ⏎/→ open · ← back · esc quit");
  type("\x03");
  expect(await run).toBe(130);
});

test("shell: Settings change the base URL for the next call and toggle colour", async () => {
  const other = startMockApi();
  try {
    const terminal = memoryTerminal();
    const keys = scriptedKeys([down, down, down, enter, enter, ...chars(other.base_url), enter, down, down, enter, left, escape]);
    expect(await runShell(shellIo(), options(), { terminal, keys })).toBe(0);
    expect(other.requested_urls.map((url) => url.pathname)).toEqual(["/v1/health"]);
    expect(terminal.output()).toContain(`Base URL  ${other.base_url}`);
    expect(terminal.output()).toContain("Colour    on");
  } finally {
    other.stop();
  }
});

// ---- the process terminal ------------------------------------------------------------------------

type FakeProcess = ProcessLike & { written: string[]; raw: boolean[]; exits: number[]; events: EventEmitter; stdout_events: EventEmitter };

function fakeProcess(options: { write_throws?: boolean } = {}): FakeProcess {
  const events = new EventEmitter();
  const stdout_events = new EventEmitter();
  const written: string[] = [];
  const raw: boolean[] = [];
  const exits: number[] = [];
  return {
    on: (event, listener) => events.on(event, listener),
    off: (event, listener) => events.off(event, listener),
    exit: (code) => {
      exits.push(code);
    },
    stdin: { setRawMode: (on) => raw.push(on) },
    stdout: {
      on: (event, listener) => stdout_events.on(event, listener),
      off: (event, listener) => stdout_events.off(event, listener),
      write: (text) => {
        if (options.write_throws === true) {
          throw new Error("EIO");
        }
        written.push(text);
      },
      columns: 100,
      rows: 24,
    },
    written,
    raw,
    exits,
    events,
    stdout_events,
  };
}

test.each([
  { signal: "SIGINT", exit_code: 130 },
  { signal: "SIGQUIT", exit_code: 131 },
  { signal: "SIGTERM", exit_code: 143 },
  { signal: "SIGHUP", exit_code: 129 },
])("$signal during a session hands the terminal back once and exits $exit_code", ({ signal, exit_code }) => {
  const proc = fakeProcess();
  const terminal = processTerminal(proc);
  enterSession(terminal);
  proc.events.emit(signal);
  proc.events.emit("exit");
  expect(proc.written.filter((text) => text === LEAVE)).toHaveLength(1);
  expect(proc.raw).toEqual([true, false]);
  expect(proc.exits).toEqual([exit_code]);
});

test("a session left normally unhooks every signal, so a later one does nothing", () => {
  const proc = fakeProcess();
  const terminal = processTerminal(proc);
  enterSession(terminal);
  leaveSession(terminal);
  for (const event of ["SIGINT", "SIGQUIT", "SIGTERM", "SIGHUP", "exit"]) {
    proc.events.emit(event);
  }
  expect(proc.written.filter((text) => text === LEAVE)).toHaveLength(1);
  expect(proc.exits).toEqual([]);
});

test("restoring a terminal that refuses writes still leaves raw mode and exits without throwing", () => {
  const proc = fakeProcess({ write_throws: true });
  const terminal = processTerminal(proc);
  terminal.setRawMode(true);
  expect(() => proc.events.emit("SIGTERM")).not.toThrow();
  expect(proc.raw).toEqual([true, false]);
  expect(proc.exits).toEqual([143]);
});

test("the process terminal reports stdout's size and its resizes until unsubscribed", () => {
  const proc = fakeProcess();
  const terminal = processTerminal(proc);
  let resizes = 0;
  const unsubscribe = terminal.onResize(() => {
    resizes += 1;
  });
  proc.stdout_events.emit("resize");
  unsubscribe();
  proc.stdout_events.emit("resize");
  expect(resizes).toBe(1);
  expect([terminal.columns(), terminal.rows()]).toEqual([100, 24]);
});
