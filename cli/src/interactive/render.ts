import type { HealthBody } from "../api_types.ts";
import { paintBars, createPaint } from "../color.ts";
import { POOL_COLUMNS, poolRows } from "../commands/pools.ts";
import { SWAP_COLUMNS, swapRows } from "../commands/swaps.ts";
import { compareRows, COMPARE_COLUMNS, volumeRows, volumeSummary, VOLUME_COLUMNS } from "../commands/volume_view.ts";
import { fieldTable, renderTable } from "../format.ts";
import {
  clampSelected,
  POOLS_PAGE_SIZE,
  REFRESH_MS,
  rows,
  type Data,
  type ListScreen,
  type Row,
  type Screen,
  type Settings,
  type TextField,
  type TextScreen,
  type VolumeRange,
} from "./screen.ts";

const MARKER = "▸ ";
const PAD = "  ";
const INVERSE_ON = "\x1b[7m";
const INVERSE_OFF = "\x1b[27m";
const RESET = "\x1b[0m";
// Bun.inspect.table draws a top border, a header and a separator before the first row.
const TABLE_HEAD_LINES = 3;

// Plain text, because colour must wait until after clipping: a clipped ANSI sequence would
// garble the line. The last `footer` lines (spinner and legend) stay visible however short the
// terminal is, like the title.
export type Frame = { lines: string[]; highlighted: number | null; caret: Caret | null; footer: number };
type Caret = { line: number; column: number };

export type Viewport = { columns: number | null; rows: number | null };
export type Painted = { lines: string[]; caret: Caret | null };

type Builder = { lines: string[]; highlighted: number | null };

function pushText(builder: Builder, text: string): void {
  for (const line of text.replace(/\n$/, "").split("\n")) {
    builder.lines.push(`${PAD}${line}`);
  }
}

function pushRow(builder: Builder, label: string, highlighted: boolean): void {
  if (highlighted) {
    builder.highlighted = builder.lines.length;
  }
  builder.lines.push(`${highlighted ? MARKER : PAD}${label}`);
}

// The table's own rows are the screen's first rows, so the highlight lands on a table line.
function pushTable(builder: Builder, table: string, row_count: number, selected: number): void {
  const lines = table.replace(/\n$/, "").split("\n");
  lines.forEach((line, index) => {
    const row = index - TABLE_HEAD_LINES;
    if (row >= 0 && row < row_count) {
      pushRow(builder, line, row === selected);
    } else {
      builder.lines.push(`${PAD}${line}`);
    }
  });
}

function pushRows(builder: Builder, offered: readonly Row[], first: number, selected: number, label: (row: Row) => string = (row) => row.label): void {
  offered.slice(first).forEach((row, index) => pushRow(builder, label(row), first + index === selected));
}

function healthLine(health: HealthBody): string {
  const lag = health.lag_seconds === null ? "lag -" : `lag ${health.lag_seconds} s`;
  return `indexer ${health.status} · ${lag} · ${health.open_job_count} open · ${health.blocked_job_count} blocked jobs`;
}

function rangeLabel(range: VolumeRange): string {
  return range.kind === "named" ? range.name : `${range.from} to ${range.to}`;
}

function title(screen: Screen): string {
  switch (screen.kind) {
    case "home":
      return "metclanker";
    case "pools":
      return `Pools · page ${screen.page + 1}`;
    case "pool":
      return `Pool ${screen.address}`;
    case "bucket":
      return `Volume ${screen.pool.address} · bucket`;
    case "range":
      return `Volume ${screen.pool.address} · ${screen.bucket} · range`;
    case "compare":
      return `Volume ${screen.pool.address} · ${screen.bucket} · ${rangeLabel(screen.range)} · compare with Meteora?`;
    case "volume": {
      const { bucket, range, compare } = screen.query;
      return `Volume ${screen.pool.address} · ${bucket} · ${rangeLabel(range)}${compare ? " · vs Meteora" : ""}`;
    }
    case "swaps":
      return `Swaps ${screen.pool.address} · page ${screen.cursor_stack.length} · newest first`;
    case "backfill":
      return `Backfill from ${screen.from}`;
    case "health":
      return `Health · refreshes every ${REFRESH_MS / 1_000} s`;
    case "settings":
      return "Settings (this session only)";
    case "text":
      return fieldTitle(screen.field);
    case "quit":
      return "";
  }
}

function fieldTitle(field: TextField): string {
  switch (field.kind) {
    case "address":
      return "Open a pool by address (base58)";
    case "backfill":
      return "Backfill from (RFC 3339, not in the future)";
    case "base_url":
      return "API base URL";
    case "width":
      return "Output width (auto, or columns)";
    case "from":
      return `Volume ${field.pool.address} · ${field.bucket} · from (RFC 3339)`;
    case "to":
      return `Volume ${field.pool.address} · ${field.bucket} · from ${field.from} · to (RFC 3339)`;
  }
}

function settingValue(label: string, settings: Settings): string {
  const values: Record<string, string> = {
    "Base URL": settings.base_url,
    Width: settings.width === null ? "auto" : String(settings.width),
    Colour: settings.color ? "on" : "off",
  };
  return `${label.padEnd(10)}${values[label] ?? ""}`;
}

function listBody(builder: Builder, screen: ListScreen, data: Data, settings: Settings): void {
  const offered = rows(screen, data);
  const selected = clampSelected(screen.selected, offered.length);
  if (screen.kind === "pools" && data.kind === "pools") {
    const page_count = Math.max(Math.ceil(data.total / POOLS_PAGE_SIZE), 1);
    // Numbered by rank across pages, not by row of this page.
    pushTable(builder, renderTable(poolRows(data.pools), POOL_COLUMNS, screen.page * POOLS_PAGE_SIZE + 1), data.pools.length, selected);
    pushText(builder, `page ${screen.page + 1} of ${page_count} · ${data.total} pools\n`);
    pushRows(builder, offered, data.pools.length, selected);
    return;
  }
  if (screen.kind === "swaps" && data.kind === "swaps") {
    pushTable(builder, renderTable(swapRows(data.swaps), SWAP_COLUMNS), data.swaps.length, selected);
    pushText(builder, data.next_cursor === null ? "end of the log" : "older swaps: ]");
    return;
  }
  if (screen.kind === "home" && data.kind === "health") {
    pushText(builder, `${healthLine(data.health)}\n`);
    builder.lines.push("");
  }
  if (screen.kind === "pool" && data.kind === "pool") {
    pushText(builder, fieldTable(data.summary));
    builder.lines.push("");
  }
  if (screen.kind === "volume" && data.kind === "volume") {
    pushText(builder, data.compared === null ? renderTable(volumeRows(data.buckets), VOLUME_COLUMNS) : renderTable(compareRows(data.compared), COMPARE_COLUMNS));
    const summary = volumeSummary(data.buckets);
    pushText(builder, `total $${summary.total_usd} · ${summary.swap_count} swaps · ${summary.bucket_count} buckets · ${summary.unpriced_swap_count} unpriced`);
    if (data.compare_error !== null) {
      pushText(builder, `compare failed: ${data.compare_error}`);
    }
    builder.lines.push("");
  }
  if (screen.kind === "backfill" && data.kind === "backfill") {
    pushText(builder, `${fieldTable(data.job)}job ${data.job.job_id} queued; watch it on Health`);
  }
  if (screen.kind === "health" && data.kind === "health") {
    pushText(builder, fieldTable(data.health));
    builder.lines.push("");
  }
  pushRows(builder, offered, 0, selected, screen.kind === "settings" ? (row) => settingValue(row.label, settings) : undefined);
}

function textBody(builder: Builder, screen: TextScreen): Caret {
  const prompt = "> ";
  const line = builder.lines.length;
  pushText(builder, `${prompt}${screen.value}`);
  if (screen.error !== null) {
    pushText(builder, `expected ${screen.error}`);
  }
  return { line, column: Bun.stringWidth(`${PAD}${prompt}${screen.value.slice(0, screen.caret)}`) };
}

function legend(screen: Screen, data: Data): string {
  switch (screen.kind) {
    case "text":
      return screen.value === "" ? "type · ← back · ⏎ submit · esc quit" : "type · ←→ caret · ⌫ to clear · ← back when empty · ⏎ submit · esc quit";
    case "home":
      return "↑↓ move · ⏎/→ open · esc quit";
    case "pools":
      return "↑↓ move · ⏎/→ open · ← back · [ ] page · esc quit";
    case "swaps":
      return data.kind === "failed" ? "↑↓ move · ⏎/→ open · ← back · [ ] page · esc quit" : "↑↓ move · ← back · [ ] page · esc quit";
    case "backfill":
      return "← back · esc quit";
    default:
      return "↑↓ move · ⏎/→ open · ← back · esc quit";
  }
}

// `status` is the spinner line of a load in flight.
export function renderFrame(screen: Screen, data: Data, settings: Settings, status: string | null): Frame {
  if (screen.kind === "quit") {
    return { lines: [], highlighted: null, caret: null, footer: 0 };
  }
  const builder: Builder = { lines: [title(screen), ""], highlighted: null };
  if (data.kind === "failed") {
    pushText(builder, `error: ${data.message}`);
    builder.lines.push("");
  }
  let caret: Caret | null = null;
  if (screen.kind === "text") {
    caret = textBody(builder, screen);
  } else {
    listBody(builder, screen, data, settings);
  }
  builder.lines.push("");
  if (status !== null) {
    pushText(builder, status);
  }
  builder.lines.push(legend(screen, data));
  return { lines: builder.lines, highlighted: builder.highlighted, caret, footer: status === null ? 1 : 2 };
}

// At most rows − 1 lines: a frame reaching the last row can scroll the alternate screen (a line
// break or a pending wrap there), and every later CURSOR_HOME would land on the scrolled view.
// The body slice is centred on the highlight or caret so Enter never acts on an unseen row.
function visibleLines(frame: Frame, rows: number | null): number[] {
  const all = frame.lines.map((_, index) => index);
  const limit = rows === null ? all.length : Math.max(rows - 1, 0);
  if (all.length <= limit) {
    return all;
  }
  const head = limit >= 1 ? [0] : [];
  const footer = all.slice(all.length - Math.min(frame.footer, Math.max(limit - 1, 0)));
  const body = all.slice(1, all.length - frame.footer);
  const room = Math.max(limit - head.length - footer.length, 0);
  const focus = frame.highlighted ?? frame.caret?.line ?? 0;
  const start = Math.min(Math.max(focus - 1 - Math.floor(room / 2), 0), Math.max(body.length - room, 0));
  return [...head, ...body.slice(start, start + room), ...footer];
}

// Colour off keeps the marker alone; colour on adds inverse video, re-armed after every reset
// the bar colouring inserts.
export function paintFrame(frame: Frame, color: boolean, viewport: Viewport): Painted {
  const paint = createPaint(color);
  const visible = visibleLines(frame, viewport.rows);
  const lines = visible.map((index) => {
    const painted = paintBars(clipLine(frame.lines[index] ?? "", viewport.columns), paint);
    if (!color || index !== frame.highlighted) {
      return painted;
    }
    return `${INVERSE_ON}${painted.replaceAll(RESET, `${RESET}${INVERSE_ON}`)}${INVERSE_OFF}`;
  });
  const caret_line = frame.caret === null ? -1 : visible.indexOf(frame.caret.line);
  if (frame.caret === null || caret_line < 0) {
    return { lines, caret: null };
  }
  // A clipped input ends in "…" at the last column; past it the cursor would wrap.
  const column = viewport.columns === null ? frame.caret.column : Math.min(frame.caret.column, viewport.columns - 1);
  return { lines, caret: { line: caret_line, column } };
}

function clipLine(line: string, width: number | null): string {
  return width === null || Bun.stringWidth(line) <= width ? line : `${sliceToWidth(line, width - 1)}…`;
}

function sliceToWidth(line: string, width: number): string {
  let taken = "";
  let taken_width = 0;
  for (const character of line) {
    taken_width += Bun.stringWidth(character);
    if (taken_width > width) {
      break;
    }
    taken += character;
  }
  return taken;
}
