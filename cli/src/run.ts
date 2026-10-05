import { Command, CommanderError, InvalidArgumentError, Option } from "commander";
import { runBackfill } from "./commands/backfill.ts";
import { runHealth } from "./commands/health.ts";
import { runPools } from "./commands/pools.ts";
import { runSwaps } from "./commands/swaps.ts";
import { runVolume } from "./commands/volume.ts";
import { createContext, OUTPUT_FORMATS, present, type CommandResult, type Context, type GlobalOptions } from "./context.ts";
import { failureEnvelope, serializeEnvelope } from "./envelope.ts";
import { CliFailure, EXIT_CODE_OK, exitCodeFor } from "./failure.ts";
import { rawText } from "./format.ts";
import { errorBody } from "./http.ts";
import type { Io } from "./io.ts";
import { TOOL_NAME, TOOL_VERSION } from "./version.ts";

const BASE_URL_DEFAULT = "http://127.0.0.1:8080";
const TIMEOUT_MS_DEFAULT = 10_000;
const POOLS_LIMIT_DEFAULT = 50;
const SWAPS_LIMIT_DEFAULT = 20;
const EXIT_CODE_USAGE = exitCodeFor("usage");

function positiveInteger(text: string): number {
  const value = Number(text);
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new InvalidArgumentError("expected a positive integer");
  }
  return value;
}

function globalOptions(command: Command): GlobalOptions {
  const options = command.optsWithGlobals();
  return {
    base_url: options.baseUrl,
    timeout_ms: options.timeoutMs,
    output: options.output,
    input: options.input,
    color: options.color,
  };
}

type Runner = (context: Context, options: Record<string, unknown>) => Promise<CommandResult>;
type Outcome = { exit_code: number };

function action(io: Io, outcome: Outcome, runner: Runner) {
  return async (options: Record<string, unknown>, command: Command) => {
    const context = createContext(io, globalOptions(command));
    try {
      outcome.exit_code = present(context, await runner(context, options));
    } catch (error) {
      if (!(error instanceof CliFailure)) {
        throw error;
      }
      outcome.exit_code = reportFailure(context, error);
    }
  };
}

function reportFailure(context: Context, failure: CliFailure): number {
  const { io, options } = context;
  const exit_code = exitCodeFor(failure.kind);
  if (failure.kind === "usage" || failure.kind === "cancelled") {
    io.write_stderr(`${TOOL_NAME}: ${failure.message}\n`);
    return exit_code;
  }
  if (options.output === "json") {
    io.write_stdout(serializeEnvelope(failureEnvelope(failure, io.now())));
  } else if (options.output === "raw" && failure.exchange !== undefined) {
    io.write_stdout(rawText(failure.exchange));
  }
  io.write_stderr(`${TOOL_NAME}: ${failure.message}\n`);
  if (options.output === "table" && failure.exchange !== undefined) {
    io.write_stderr(`${JSON.stringify(errorBody(failure.exchange))}\n`);
  }
  return exit_code;
}

function addGlobalOptions(program: Command, io: Io): void {
  program
    .option("--base-url <url>", "API base URL (env METCLANKER_BASE_URL)", io.env.METCLANKER_BASE_URL ?? BASE_URL_DEFAULT)
    .option("--timeout-ms <ms>", "request timeout in milliseconds", positiveInteger, TIMEOUT_MS_DEFAULT)
    .addOption(new Option("--output <format>", "table for people, json for agents, raw for the verbatim HTTP exchange").choices(OUTPUT_FORMATS).default("table"))
    .option("--no-input", "never prompt; missing flags exit 2")
    .option("--no-color", "disable colour (NO_COLOR is honoured too)");
}

function addCommands(program: Command, io: Io, outcome: Outcome): void {
  program.command("health").description("indexer health: cursor, lag, open and blocked jobs").action(action(io, outcome, (context) => runHealth(context)));
  program
    .command("pools")
    .description("pools ordered by 24-hour USD volume")
    .option("--limit <count>", "pool count", positiveInteger, POOLS_LIMIT_DEFAULT)
    .action(action(io, outcome, (context, options) => runPools(context, options.limit as number)));
  program
    .command("volume")
    .description("hourly or daily volume of one pool, in tokens and USD")
    .option("--pool <address>", "pool address (base58)")
    .addOption(new Option("--bucket <bucket>", "bucket size").choices(["hour", "day"]))
    .addOption(new Option("--range <range>", "window ending now").choices(["24h", "7d", "30d"]))
    .option("--from <rfc3339>", "window start, RFC 3339 (with --to)")
    .option("--to <rfc3339>", "window end, RFC 3339 (with --from)")
    .option("--compare", "also fetch Meteora's Data API volume per bucket and show the difference")
    .action(action(io, outcome, (context, options) => runVolume(context, options)));
  program
    .command("swaps")
    .description("most recent swaps of one pool")
    .option("--pool <address>", "pool address (base58)")
    .option("--limit <count>", "swap count", positiveInteger, SWAPS_LIMIT_DEFAULT)
    .action(action(io, outcome, (context, options) => runSwaps(context, { pool: options.pool as string | undefined, limit: options.limit as number })));
  program
    .command("backfill")
    .description("ask the indexer to fill history from an instant up to the oldest indexed block")
    .option("--from <rfc3339>", "first instant to fill, RFC 3339, not in the future")
    .action(action(io, outcome, (context, options) => runBackfill(context, options.from as string | undefined)));
}

function buildProgram(io: Io, outcome: Outcome): Command {
  const program = new Command(TOOL_NAME)
    .description("Read the Meteora DLMM swap indexer API. Agents: always pass --output json --no-input.")
    .version(TOOL_VERSION)
    .exitOverride()
    .configureOutput({ writeOut: io.write_stdout, writeErr: io.write_stderr });
  addGlobalOptions(program, io);
  addCommands(program, io, outcome);
  return program;
}

const HELP_CODES = new Set(["commander.helpDisplayed", "commander.version"]);

export async function run(argv: readonly string[], io: Io): Promise<number> {
  const outcome: Outcome = { exit_code: EXIT_CODE_OK };
  try {
    await buildProgram(io, outcome).parseAsync([...argv], { from: "user" });
    return outcome.exit_code;
  } catch (error) {
    if (error instanceof CommanderError) {
      return HELP_CODES.has(error.code) ? EXIT_CODE_OK : EXIT_CODE_USAGE;
    }
    throw error;
  }
}
