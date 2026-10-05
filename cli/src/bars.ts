const FULL_BLOCK = "█";
// Index n is a block n eighths wide; index 0 is the empty remainder.
const PARTIAL_BLOCKS = ["", "▏", "▎", "▍", "▌", "▋", "▊", "▉"];
const EIGHTHS_PER_CELL = 8;

export const BAR_WIDTH_DEFAULT = 24;

export function renderBar(value: number, max: number, width: number = BAR_WIDTH_DEFAULT): string {
  if (!(max > 0) || !(value > 0) || width <= 0) {
    return " ".repeat(Math.max(width, 0));
  }
  const ratio = Math.min(value, max) / max;
  // A non-zero bucket always shows at least one eighth so it never reads as empty.
  const eighths = Math.max(1, Math.round(ratio * width * EIGHTHS_PER_CELL));
  const full_count = Math.floor(eighths / EIGHTHS_PER_CELL);
  const partial = PARTIAL_BLOCKS[eighths % EIGHTHS_PER_CELL] ?? "";
  return (FULL_BLOCK.repeat(full_count) + partial).padEnd(width, " ");
}

export function renderBars(values: number[], width: number = BAR_WIDTH_DEFAULT): string[] {
  const max = values.reduce((largest, value) => Math.max(largest, value), 0);
  return values.map((value) => renderBar(value, max, width));
}
