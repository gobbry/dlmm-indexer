import { note, spinner } from "@clack/prompts";
import type { Context } from "./context.ts";
import { CliFailure } from "./failure.ts";
import { requestLine, statusLine } from "./format.ts";
import type { Exchange, RequestRecord } from "./http.ts";

export type Progress = {
  start: (request: RequestRecord) => void;
  stop: (exchange: Exchange) => void;
  fail: (error: unknown) => void;
};

const SILENT: Progress = { start: () => {}, stop: () => {}, fail: () => {} };

function failureLine(error: unknown): string {
  if (error instanceof CliFailure && error.exchange !== undefined) {
    return statusLine(error.exchange);
  }
  return error instanceof Error ? error.message : String(error);
}

function terminalProgress(): Progress {
  const indicator = spinner({ output: process.stderr });
  return {
    start: (request) => indicator.start(requestLine(request)),
    stop: (exchange) => indicator.stop(statusLine(exchange)),
    fail: (error) => indicator.error(failureLine(error)),
  };
}

function plainProgress(context: Context): Progress {
  const write = context.io.write_stderr;
  return {
    start: (request) => write(`${requestLine(request)}\n`),
    stop: (exchange) => write(`${statusLine(exchange)}\n`),
    fail: () => {},
  };
}

// Only table output narrates progress; json and raw keep stderr for failures alone.
export function createProgress(context: Context): Progress {
  if (context.options.output !== "table") {
    return SILENT;
  }
  return context.io.stderr_is_tty ? terminalProgress() : plainProgress(context);
}

// A boxed note wraps to the terminal width; a narrow or unknown width would break it per character.
const NOTE_COLUMNS_MIN = 60;

export function showNote(context: Context, title: string, message: string): void {
  if (!context.io.stdout_is_tty) {
    context.io.write_stderr(`${title}: ${message}\n`);
    return;
  }
  if ((process.stdout.columns ?? 0) < NOTE_COLUMNS_MIN) {
    context.io.write_stdout(`${title}: ${message}\n`);
    return;
  }
  note(message, title);
}
