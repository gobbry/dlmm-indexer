import { colorEnabled, createPaint, type Paint } from "./color.ts";
import { buildEnvelope, serializeEnvelope, type CompareSection, type EnvelopeError } from "./envelope.ts";
import { usageFailure } from "./failure.ts";
import { rawText } from "./format.ts";
import { buildPostRequest, buildRequest, decodeBody, send, type Exchange, type RequestRecord } from "./http.ts";
import type { Io } from "./io.ts";
import { createProgress, showNote } from "./ui.ts";

export type OutputFormat = "table" | "json" | "raw";
export const OUTPUT_FORMATS: readonly OutputFormat[] = ["table", "json", "raw"];

export type GlobalOptions = {
  base_url: string;
  timeout_ms: number;
  output: OutputFormat;
  input: boolean;
  color: boolean;
};

export type Interaction = "interactive" | "non_interactive";

export type Context = {
  io: Io;
  options: GlobalOptions;
  interaction: Interaction;
  paint: Paint;
};

export type CommandResult = {
  exchanges: Exchange[];
  data: unknown;
  summary: unknown;
  compare?: CompareSection;
  error?: EnvelopeError | null;
  table: () => string;
  note?: { title: string; message: string };
  exit_code: number;
};

export function createContext(io: Io, options: GlobalOptions): Context {
  const interactive = io.stdin_is_tty && io.stdout_is_tty && options.input && options.output === "table";
  return {
    io,
    options,
    interaction: interactive ? "interactive" : "non_interactive",
    paint: createPaint(options.output === "table" && colorEnabled(io, options.color)),
  };
}

export async function fetchExchange(
  context: Context,
  base_url: string,
  path: string,
  params: Record<string, string>,
): Promise<{ exchange: Exchange; body: unknown }> {
  return sendRequest(context, buildRequest(base_url, path, params), context.options.timeout_ms);
}

async function sendRequest(
  context: Context,
  request: RequestRecord,
  timeout_ms: number,
): Promise<{ exchange: Exchange; body: unknown }> {
  const progress = createProgress(context);
  progress.start(request);
  try {
    const exchange = await send(request, timeout_ms);
    progress.stop(exchange);
    return { exchange, body: decodeBody(exchange) };
  } catch (error) {
    progress.fail(error);
    throw error;
  }
}

export function fetchApi(context: Context, path: string, params: Record<string, string> = {}) {
  return fetchExchange(context, context.options.base_url, path, params);
}

export function postApi(context: Context, path: string, body: unknown, timeout_ms: number) {
  return sendRequest(context, buildPostRequest(context.options.base_url, path, body), timeout_ms);
}

export function missingFlag(flag: string, command: string): Error {
  return usageFailure(`missing ${flag} (required by '${command}' without a terminal; see metclanker ${command} --help)`);
}

export function present(context: Context, result: CommandResult): number {
  const { io, options } = context;
  if (options.output === "json") {
    const envelope = buildEnvelope({
      generated_at: io.now(),
      exchange: result.exchanges[0] ?? null,
      data: result.data,
      summary: result.summary,
      ...(result.compare === undefined ? {} : { compare: result.compare }),
      error: result.error ?? null,
    });
    io.write_stdout(serializeEnvelope(envelope));
  } else if (options.output === "raw") {
    io.write_stdout(result.exchanges.map(rawText).join("\n"));
  } else {
    io.write_stdout(result.table());
    if (result.note !== undefined) {
      showNote(context, result.note.title, result.note.message);
    }
  }
  return result.exit_code;
}
