import { colorEnabled } from "../color.ts";
import { createContext, type Context, type GlobalOptions } from "../context.ts";
import type { Io } from "../io.ts";
import { runEffect, type ApiEffect } from "./effects.ts";
import { endsSession, streamKeys, type Key, type KeySource } from "./keys.ts";
import { paintFrame, renderFrame } from "./render.ts";
import { HOME, loadEffect, REFRESH_MS, transition, type Data, type Effect, type Input, type Screen, type Settings } from "./screen.ts";
import {
  CLEAR_LINE_END,
  CLEAR_SCREEN_END,
  CURSOR_BLINKING_BAR,
  CURSOR_HIDE,
  CURSOR_HOME,
  CURSOR_SHOW,
  enterSession,
  leaveSession,
  processTerminal,
  type Terminal,
} from "./terminal.ts";

export type ShellDeps = { terminal: Terminal; keys: KeySource; refresh_ms?: number };

// An agent must never sit in a session, so anything short of a person at a terminal is refused.
export function interactiveRefusal(io: Io, options: GlobalOptions): string | null {
  if (!options.input) {
    return "--interactive cannot be combined with --no-input; use the subcommands instead";
  }
  if (options.output === "json") {
    return "--interactive cannot be combined with --output json; use the subcommands instead";
  }
  if (!io.stdin_is_tty || !io.stdout_is_tty) {
    return "--interactive needs a terminal on stdin and stdout; use the subcommands instead";
  }
  return null;
}

export function processShellDeps(): ShellDeps {
  return { terminal: processTerminal(), keys: streamKeys(process.stdin) };
}

const NO_DATA: Data = { kind: "none" };
const SPINNER_FRAMES = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"];
// A fast load never flashes a spinner; a slow one animates until it lands.
const SPINNER_DELAY_MS = 150;
const SPINNER_FRAME_MS = 80;

// The subcommands narrate requests on stderr and warn there; in the alternate screen either
// would tear the frame, and every failure is already shown as data.
function sessionContext(io: Io, options: GlobalOptions, settings: Settings): Context {
  const quiet: Io = { ...io, write_stdout: () => {}, write_stderr: () => {}, stderr_is_tty: false };
  return createContext(quiet, { ...options, base_url: settings.base_url, output: "table" });
}

type Draw = (screen: Screen, data: Data, status: string | null) => void;

// Each frame is one write that rewrites every line in place, so nothing flickers; the latest
// arguments are kept so a resize can repaint at the new size without a key.
function painter(terminal: Terminal, settings: () => Settings): { draw: Draw; repaint: () => void } {
  let repaint = () => {};
  const draw: Draw = (screen, data, status) => {
    const current = settings();
    const frame = renderFrame(screen, data, current, status);
    const painted = paintFrame(frame, current.color, { columns: current.width ?? terminal.columns(), rows: terminal.rows() });
    const body = `${CURSOR_HOME}${painted.lines.map((line) => `${line}${CLEAR_LINE_END}`).join("\r\n")}${CLEAR_SCREEN_END}`;
    const caret = painted.caret;
    const cursor = caret === null ? CURSOR_HIDE : `\x1b[${caret.line + 1};${caret.column + 1}H${CURSOR_BLINKING_BAR}${CURSOR_SHOW}`;
    terminal.write(`${body}${cursor}`);
    repaint = () => draw(screen, data, status);
  };
  return { draw, repaint: () => repaint() };
}

type Reader = {
  read: (tick_ms: number | null) => Promise<Input>;
  settle: () => void;
};

// A tick that wins the race leaves the key read pending, so the next read still gets that key.
function inputReader(keys: KeySource): Reader {
  let pending: Promise<Key> | null = null;
  let caught: Key | null = null;
  return {
    read: async (tick_ms) => {
      if (pending === null) {
        caught = null;
        pending = keys.next();
        // The read itself reports a failure; this copy only watches for a key.
        pending.then(
          (key) => {
            caught = key;
          },
          () => {},
        );
      }
      let timer: ReturnType<typeof setTimeout> | undefined;
      const tick = new Promise<null>((resolve) => {
        if (tick_ms !== null) {
          timer = setTimeout(() => resolve(null), tick_ms);
        }
      });
      const key = await Promise.race([pending, tick]);
      clearTimeout(timer);
      if (key === null) {
        return { kind: "tick" };
      }
      pending = null;
      return key;
    },
    settle: () => {
      const queued = keys.keepLatest();
      // A read left pending by a tick caught the first key typed during the tick's load; a later
      // key supersedes it unless it ends the session.
      if (pending !== null && caught !== null && queued && !endsSession(caught)) {
        pending = null;
      }
    },
  };
}

type Session = { settings: Settings; context: Context; data: Data };

const INTERRUPTED = Symbol("interrupted");
const EXIT_CODE_INTERRUPTED = 130;

// Raw mode means Ctrl-C raises no SIGINT, so a load races the key stream for it instead of
// holding the person until the request times out. The abandoned request dies with the process.
async function load(draw: Draw, keys: KeySource, session: Session, screen: Screen, effect: ApiEffect, shown: Data): Promise<Data | typeof INTERRUPTED> {
  let frame = 0;
  let interval: ReturnType<typeof setInterval> | undefined;
  const spin = () => {
    draw(screen, shown, `${SPINNER_FRAMES[frame % SPINNER_FRAMES.length]} loading ${effect.kind}…`);
    frame += 1;
  };
  const delay = setTimeout(() => {
    spin();
    interval = setInterval(spin, SPINNER_FRAME_MS);
  }, SPINNER_DELAY_MS);
  const loading = runEffect(session.context, effect);
  try {
    return await Promise.race([loading, keys.interrupted.then((): typeof INTERRUPTED => INTERRUPTED)]);
  } finally {
    loading.catch(() => {});
    clearTimeout(delay);
    clearInterval(interval);
  }
}

type Shell = { io: Io; options: GlobalOptions; draw: Draw; keys: KeySource; reader: Reader };

async function apply(shell: Shell, session: Session, screen: Screen, effect: Effect, shown: Data): Promise<Session | typeof INTERRUPTED> {
  switch (effect.kind) {
    case "none":
      return session;
    case "blank":
      return { ...session, data: NO_DATA };
    case "settings":
    case "toggle_color": {
      const patch = effect.kind === "settings" ? effect.patch : { color: !session.settings.color };
      const settings = { ...session.settings, ...patch };
      return { settings, context: sessionContext(shell.io, shell.options, settings), data: NO_DATA };
    }
    default: {
      const data = await load(shell.draw, shell.keys, session, screen, effect, shown);
      if (data === INTERRUPTED) {
        return INTERRUPTED;
      }
      shell.reader.settle();
      return { ...session, data };
    }
  }
}

function refreshes(screen: Screen): boolean {
  return screen.kind === "home" || screen.kind === "health";
}

// render → next key or tick → transition → effect → render; the terminal is handed back as it
// was on every way out, a thrown error included.
export async function runShell(io: Io, options: GlobalOptions, deps: ShellDeps): Promise<number> {
  const { terminal, keys } = deps;
  const refresh_ms = deps.refresh_ms ?? REFRESH_MS;
  const settings: Settings = { base_url: options.base_url, width: null, color: colorEnabled(io, options.color) };
  let session: Session = { settings, context: sessionContext(io, options, settings), data: NO_DATA };
  const { draw, repaint } = painter(terminal, () => session.settings);
  const reader = inputReader(keys);
  const shell: Shell = { io, options, draw, keys, reader };
  enterSession(terminal);
  const unsubscribe = terminal.onResize(repaint);
  try {
    let previous: Screen = HOME;
    let screen: Screen = HOME;
    let effect: Effect = loadEffect(HOME);
    // Measured from the last health load, not the last key, so a busy person still sees fresh health.
    let refresh_at = 0;
    for (;;) {
      // A slow load keeps the old data on screen only while it is the same kind of screen.
      const shown = previous.kind === screen.kind ? session.data : NO_DATA;
      const applied = await apply(shell, session, screen, effect, shown);
      if (applied === INTERRUPTED) {
        return EXIT_CODE_INTERRUPTED;
      }
      if (effect.kind === "health") {
        refresh_at = performance.now() + refresh_ms;
      }
      session = applied;
      draw(screen, session.data, null);
      const tick_ms = refreshes(screen) ? Math.max(refresh_at - performance.now(), 0) : null;
      const step = transition(screen, await reader.read(tick_ms), session.data);
      if (step.screen.kind === "quit") {
        return step.screen.exit_code;
      }
      previous = screen;
      ({ screen, effect } = step);
    }
  } finally {
    unsubscribe();
    leaveSession(terminal);
    keys.close();
  }
}
