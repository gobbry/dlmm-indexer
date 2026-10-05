import type { Io } from "./io.ts";

const RESET = "\x1b[0m";

export type Paint = (text: string, color_name: string) => string;

export function colorEnabled(io: Io, color_flag: boolean): boolean {
  const no_color = io.env.NO_COLOR;
  return color_flag && (no_color === undefined || no_color === "") && io.stdout_is_tty;
}

export function createPaint(enabled: boolean): Paint {
  if (!enabled) {
    return (text) => text;
  }
  return (text, color_name) => {
    const code = Bun.color(color_name, "ansi-256");
    return code === null ? text : `${code}${text}${RESET}`;
  };
}

const BAR_RUN_PATTERN = /[▏▎▍▌▋▊▉█]+/g;

// Bars are coloured after the table is laid out because Bun.inspect.table counts ANSI codes as width.
export function paintBars(table: string, paint: Paint): string {
  return table.replace(BAR_RUN_PATTERN, (run) => paint(run, "#7c3aed"));
}
