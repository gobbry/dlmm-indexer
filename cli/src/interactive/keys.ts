import { emitKeypressEvents } from "node:readline";
import { BASE58_ADDRESS_PATTERN } from "../commands/pool_input.ts";
import { parseRfc3339 } from "../range.ts";

// `[` and `]` stay characters because a text input types them (an IPv6 base URL has brackets);
// list screens read them as paging.
export type Key =
  | { kind: "up" }
  | { kind: "down" }
  | { kind: "left" }
  | { kind: "right" }
  | { kind: "enter" }
  | { kind: "escape" }
  | { kind: "interrupt" }
  | { kind: "backspace" }
  | { kind: "home" }
  | { kind: "end" }
  | { kind: "char"; char: string }
  | { kind: "unknown" }
  // stdin ended (0) or failed (1): no person is left to press anything.
  | { kind: "closed"; exit_code: 0 | 1 };

// What node:readline reports for one keypress; Bun implements the same event.
type Keypress = { name?: string; sequence?: string; ctrl?: boolean; meta?: boolean };

const NAMED: Record<string, Key> = {
  up: { kind: "up" },
  down: { kind: "down" },
  left: { kind: "left" },
  right: { kind: "right" },
  return: { kind: "enter" },
  enter: { kind: "enter" },
  escape: { kind: "escape" },
  backspace: { kind: "backspace" },
  home: { kind: "home" },
  end: { kind: "end" },
};

const PRINTABLE_PATTERN = /^[^\x00-\x1f\x7f]$/u;
// readline folds any byte that follows Esc within its 500 ms window into one meta keypress.
const META_CHAR_PATTERN = /^\x1b([^\x00-\x1f\x7f])$/u;
const QUEUE_CAPACITY = 32;

function decodeKeypress(text: string | undefined, keypress: Keypress | undefined): readonly Key[] {
  // readline has no timeout for an unfinished CSI, so `\x1b[` swallows the next byte as its
  // final one; a Ctrl-C caught that way must still interrupt.
  if ((keypress?.ctrl === true && keypress.name === "c") || keypress?.sequence?.includes("\x03") === true) {
    return [{ kind: "interrupt" }];
  }
  // A lone Esc arrives as meta, being the meta prefix with nothing after it.
  if (keypress?.name === "escape") {
    return [{ kind: "escape" }];
  }
  const meta_char = keypress?.meta === true ? META_CHAR_PATTERN.exec(keypress.sequence ?? "") : null;
  if (meta_char?.[1] !== undefined) {
    return [{ kind: "escape" }, { kind: "char", char: meta_char[1] }];
  }
  if (keypress?.ctrl === true || keypress?.meta === true) {
    return [{ kind: "unknown" }];
  }
  const named = keypress?.name === undefined ? undefined : NAMED[keypress.name];
  if (named !== undefined) {
    return [named];
  }
  return [text !== undefined && PRINTABLE_PATTERN.test(text) ? { kind: "char", char: text } : { kind: "unknown" }];
}

export function endsSession(key: Key): boolean {
  return key.kind === "escape" || key.kind === "interrupt" || key.kind === "closed";
}

export type KeySource = {
  next: () => Promise<Key>;
  // Keys typed during a load were aimed at a screen that has since changed, so only the last
  // one (or the first that ends the session) survives it. Answers whether a key is still queued.
  keepLatest: () => boolean;
  // Settles on the first Ctrl-C, so a load in flight can be abandoned without waiting for it.
  interrupted: Promise<void>;
  close: () => void;
};

export function streamKeys(stream: NodeJS.ReadableStream): KeySource {
  const queued: Key[] = [];
  const waiting: ((key: Key) => void)[] = [];
  let closed: Key | null = null;
  let interrupt = () => {};
  const interrupted = new Promise<void>((resolve) => {
    interrupt = resolve;
  });
  const deliver = (key: Key) => {
    if (key.kind === "interrupt") {
      interrupt();
    }
    const resolve = waiting.shift();
    if (resolve !== undefined) {
      resolve(key);
      return;
    }
    queued.push(key);
    // A held key during a stalled load must not pile up without bound.
    if (queued.length > QUEUE_CAPACITY) {
      queued.shift();
    }
  };
  const listener = (text: string | undefined, keypress: Keypress | undefined) => {
    for (const key of decodeKeypress(text, keypress)) {
      deliver(key);
    }
  };
  const close = (exit_code: 0 | 1) => {
    closed ??= { kind: "closed", exit_code };
    for (const resolve of waiting.splice(0)) {
      resolve(closed);
    }
  };
  const onEnd = () => close(0);
  const onError = () => close(1);
  emitKeypressEvents(stream);
  stream.on("keypress", listener);
  stream.once("end", onEnd);
  stream.once("close", onEnd);
  stream.once("error", onError);
  stream.resume();
  return {
    next: () => {
      const key = queued.shift() ?? closed;
      return key === null ? new Promise((resolve) => waiting.push(resolve)) : Promise.resolve(key);
    },
    keepLatest: () => {
      const kept = queued.find(endsSession) ?? queued.at(-1);
      queued.splice(0, queued.length, ...(kept === undefined ? [] : [kept]));
      return kept !== undefined;
    },
    interrupted,
    close: () => {
      stream.off("keypress", listener);
      stream.off("end", onEnd);
      stream.off("close", onEnd);
      stream.off("error", onError);
      stream.pause();
    },
  };
}

// Validators answer with an error message, or undefined when the text is accepted.
export function instantError(text: string): string | undefined {
  return parseRfc3339(text.trim()) === null ? "an RFC 3339 instant such as 2026-10-01T00:00:00Z" : undefined;
}

export function addressError(text: string): string | undefined {
  return BASE58_ADDRESS_PATTERN.test(text.trim()) ? undefined : "a base58 pool address (32 to 44 characters)";
}

export function baseUrlError(text: string): string | undefined {
  return parseBaseUrl(text) === null ? "an http:// or https:// URL" : undefined;
}

export function parseBaseUrl(text: string): string | null {
  const trimmed = text.trim();
  if (!URL.canParse(trimmed)) {
    return null;
  }
  const protocol = new URL(trimmed).protocol;
  return protocol === "http:" || protocol === "https:" ? trimmed : null;
}

const WIDTH_MIN = 40;
const WIDTH_MAX = 1_000;

// "auto" (or nothing) clips to the terminal's own width.
export function parseWidth(text: string): number | null | "invalid" {
  const trimmed = text.trim().toLowerCase();
  if (trimmed === "" || trimmed === "auto") {
    return null;
  }
  const width = Number(trimmed);
  return Number.isSafeInteger(width) && width >= WIDTH_MIN && width <= WIDTH_MAX ? width : "invalid";
}

export function widthError(text: string): string | undefined {
  return parseWidth(text) === "invalid" ? `auto, or a column count from ${WIDTH_MIN} to ${WIDTH_MAX}` : undefined;
}
