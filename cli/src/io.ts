export type Io = {
  write_stdout: (text: string) => void;
  write_stderr: (text: string) => void;
  env: Record<string, string | undefined>;
  stdin_is_tty: boolean;
  stdout_is_tty: boolean;
  stderr_is_tty: boolean;
  now: () => Date;
};

export function processIo(): Io {
  return {
    write_stdout: (text) => process.stdout.write(text),
    write_stderr: (text) => process.stderr.write(text),
    env: process.env,
    stdin_is_tty: process.stdin.isTTY === true,
    stdout_is_tty: process.stdout.isTTY === true,
    stderr_is_tty: process.stderr.isTTY === true,
    now: () => new Date(),
  };
}
