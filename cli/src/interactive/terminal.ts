// Tests pass an in-memory terminal, so no TTY is needed to drive the shell.
export type Terminal = {
  write: (text: string) => void;
  setRawMode: (on: boolean) => void;
  columns: () => number | null;
  rows: () => number | null;
  onResize: (listener: () => void) => () => void;
};

const ALTERNATE_SCREEN_ON = "\x1b[?1049h";
const ALTERNATE_SCREEN_OFF = "\x1b[?1049l";
export const CURSOR_HIDE = "\x1b[?25l";
export const CURSOR_SHOW = "\x1b[?25h";
export const CURSOR_BLINKING_BAR = "\x1b[5 q";
const CURSOR_STYLE_DEFAULT = "\x1b[0 q";
export const CURSOR_HOME = "\x1b[H";
export const CLEAR_LINE_END = "\x1b[K";
export const CLEAR_SCREEN_END = "\x1b[J";

const ENTER = `${ALTERNATE_SCREEN_ON}${CURSOR_HIDE}`;
const LEAVE = `${CURSOR_STYLE_DEFAULT}${CURSOR_SHOW}${ALTERNATE_SCREEN_OFF}`;

export function enterSession(terminal: Terminal): void {
  terminal.setRawMode(true);
  terminal.write(ENTER);
}

export function leaveSession(terminal: Terminal): void {
  terminal.write(LEAVE);
  terminal.setRawMode(false);
}

const SIGNAL_EXIT_CODES = { SIGINT: 130, SIGQUIT: 131, SIGTERM: 143, SIGHUP: 129 } as const;
type Signal = keyof typeof SIGNAL_EXIT_CODES;
const SIGNALS = Object.keys(SIGNAL_EXIT_CODES) as Signal[];

type Emitter = { on: (event: string, listener: () => void) => unknown; off: (event: string, listener: () => void) => unknown };

export type ProcessLike = Emitter & {
  exit: (code: number) => void;
  stdin: { setRawMode: (on: boolean) => unknown };
  stdout: Emitter & { write: (text: string) => unknown; columns?: number; rows?: number };
};

// Raw mode turns Ctrl-C into a key, so a signal here is an external kill or a closed terminal;
// either must still hand the person back a normal screen. `exit` covers a process.exit from
// anywhere. Restoring never throws: it runs inside exit and signal hooks, where a throw would
// replace the real exit code, and a terminal that refuses the write is already gone.
export function processTerminal(proc: ProcessLike = process): Terminal {
  let raw = false;
  const restore = () => {
    if (!raw) {
      return;
    }
    raw = false;
    try {
      proc.stdout.write(LEAVE);
    } catch {}
    try {
      proc.stdin.setRawMode(false);
    } catch {}
  };
  const hooks: [string, () => void][] = [
    ["exit", restore],
    ...SIGNALS.map((signal): [string, () => void] => [
      signal,
      () => {
        restore();
        proc.exit(SIGNAL_EXIT_CODES[signal]);
      },
    ]),
  ];
  return {
    write: (text) => {
      proc.stdout.write(text);
    },
    setRawMode: (on) => {
      proc.stdin.setRawMode(on);
      raw = on;
      for (const [event, hook] of hooks) {
        if (on) {
          proc.on(event, hook);
        } else {
          proc.off(event, hook);
        }
      }
    },
    columns: () => proc.stdout.columns ?? null,
    rows: () => proc.stdout.rows ?? null,
    onResize: (listener) => {
      proc.stdout.on("resize", listener);
      return () => {
        proc.stdout.off("resize", listener);
      };
    },
  };
}
