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
import { interactiveRefusal, processShellDeps, runShell } from "./interactive/shell.ts";
import type { Io } from "./io.ts";
import { TOOL_NAME, TOOL_VERSION } from "./version.ts";

const BASE_URL_DEFAULT = "http://127.0.0.1:8080";
const TIMEOUT_MS_DEFAULT = 10_000;
const POOLS_LIMIT_DEFAULT = 50;
const SWAPS_LIMIT_DEFAULT = 20;
const PAGE_LIMIT_MAX = 100;
const EXIT_CODE_USAGE = exitCodeFor("usage");

function positiveInteger(text: string): number {
  const value = Number(text);
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new InvalidArgumentError("expected a positive integer");
  }
  return value;
}

// The API's page cap: a larger value is the caller's mistake, so it is a usage error here
// rather than an HTTP 400 there.
function pageLimit(text: string): number {
  const value = positiveInteger(text);
  if (value > PAGE_LIMIT_MAX) {
    throw new InvalidArgumentError(`expected 1 to ${PAGE_LIMIT_MAX}`);
  }
  return value;
}

function nonNegativeInteger(text: string): number {
  const value = Number(text);
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new InvalidArgumentError("expected 0 or a positive integer");
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

// --interactive wins over any subcommand, so `metclanker -i pools` opens the browser too.
async function runInteractive(io: Io, options: GlobalOptions): Promise<number> {
  const refusal = interactiveRefusal(io, options);
  if (refusal !== null) {
    io.write_stderr(`${TOOL_NAME}: ${refusal}\n`);
    return EXIT_CODE_USAGE;
  }
  const exit_code = await runShell(io, options, processShellDeps());
  // A load abandoned by Ctrl-C still holds its socket until the timeout; the session printed its
  // last byte when it left the alternate screen, so nothing is lost by ending here.
  process.exit(exit_code);
}

type Runner = (context: Context, options: Record<string, unknown>) => Promise<CommandResult>;
type Outcome = { exit_code: number };

function action(io: Io, outcome: Outcome, runner: Runner) {
  return async (options: Record<string, unknown>, command: Command) => {
    if (command.optsWithGlobals().interactive === true) {
      outcome.exit_code = await runInteractive(io, globalOptions(command));
      return;
    }
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
    .option("--no-color", "disable colour (NO_COLOR is honoured too)")
    .option("-i, --interactive", "browse pools, volume and swaps in a terminal session (people only; agents use the subcommands)");
}

function addCommands(program: Command, io: Io, outcome: Outcome): void {
  program.command("health").description("indexer health: cursor, lag, open and blocked jobs").action(action(io, outcome, (context) => runHealth(context)));
  program
    .command("pools")
    .description("pools ordered by 24-hour USD volume")
    .option("--limit <count>", `pool count, 1 to ${PAGE_LIMIT_MAX}`, pageLimit, POOLS_LIMIT_DEFAULT)
    .option("--offset <count>", "pools to skip in the ranking, to page past the limit", nonNegativeInteger, 0)
    .action(action(io, outcome, (context, options) => runPools(context, { limit: options.limit as number, offset: options.offset as number })));
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
    .option("--limit <count>", `swap count, 1 to ${PAGE_LIMIT_MAX}`, pageLimit, SWAPS_LIMIT_DEFAULT)
    .action(action(io, outcome, (context, options) => runSwaps(context, { pool: options.pool as string | undefined, limit: options.limit as number })));
  program
    .command("backfill")
    .description("ask the indexer to fill history from an instant up to the oldest indexed block; list or cancel its jobs")
    .option("--from <rfc3339>", "first instant to fill, RFC 3339, not in the future")
    .option("--cancel <job_id>", "cancel a job: it stays as blocked (reason cancelled) so its range is not reopened", positiveInteger)
    .option("--list", "the 50 newest jobs, with their ids and states")
    .action(
      action(io, outcome, (context, options) =>
        runBackfill(context, {
          from: options.from as string | undefined,
          cancel: options.cancel as number | undefined,
          list: options.list === true,
        }),
      ),
    );
}

function buildProgram(io: Io, outcome: Outcome): Command {
  const program = new Command(TOOL_NAME)
    .description("Read the Meteora DLMM swap indexer API. Agents: always pass --output json --no-input.")
    .version(TOOL_VERSION)
    .exitOverride()
    .configureOutput({ writeOut: io.write_stdout, writeErr: io.write_stderr });
  addGlobalOptions(program, io);
  addCommands(program, io, outcome);
  // Without --interactive a bare invocation keeps printing help and exiting 2. Excess arguments
  // reach this action (set after addCommands, so subcommands stay strict) to keep the
  // "unknown command" wording a root action would otherwise turn into "too many arguments".
  program.allowExcessArguments(true).action(async (_options: Record<string, unknown>, command: Command) => {
    const [unknown] = command.args;
    if (unknown !== undefined) {
      command.error(`error: unknown command '${unknown}'`);
    }
    if (command.opts().interactive !== true) {
      command.help({ error: true });
    }
    outcome.exit_code = await runInteractive(io, globalOptions(command));
  });
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
