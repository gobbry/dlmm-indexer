import type { Exchange, RequestRecord } from "./http.ts";

const BYTES_PER_KILOBYTE = 1_024;

export function formatBytes(byte_count: number): string {
  if (byte_count < BYTES_PER_KILOBYTE) {
    return `${byte_count} B`;
  }
  const kilobytes = byte_count / BYTES_PER_KILOBYTE;
  if (kilobytes < BYTES_PER_KILOBYTE) {
    return `${kilobytes.toFixed(1)} KB`;
  }
  return `${(kilobytes / BYTES_PER_KILOBYTE).toFixed(1)} MB`;
}

export function requestLine(request: RequestRecord): string {
  return `${request.method} ${request.url}`;
}

export function statusLine(exchange: Exchange): string {
  const status = `${exchange.response.status} ${exchange.status_text}`.trim();
  return `${status} · ${exchange.response.latency_ms} ms · ${formatBytes(exchange.response.body_bytes)}`;
}

function shellQuote(text: string): string {
  return `'${text.replaceAll("'", `'\\''`)}'`;
}

export function curlCommand(request: RequestRecord): string {
  const accept = `-H ${shellQuote(`accept: ${request.headers.accept ?? "application/json"}`)}`;
  if (request.body !== undefined) {
    return `curl -sS -X POST ${accept} -H ${shellQuote("content-type: application/json")} -d ${shellQuote(request.body)} ${shellQuote(request.url)}`;
  }
  return `curl -sS ${accept} ${shellQuote(request.url)}`;
}

export function rawText(exchange: Exchange): string {
  const status = `HTTP ${exchange.response.status} ${exchange.status_text}`.trim();
  const headers = Object.entries(exchange.response.headers).map(([name, value]) => `${name}: ${value}`);
  const body = exchange.body_text.endsWith("\n") ? exchange.body_text : `${exchange.body_text}\n`;
  return [requestLine(exchange.request), status, ...headers, "", body].join("\n");
}

export type Row = Record<string, string>;

// Bun.inspect.table pads cells on the right, so numeric columns are pre-padded on the left.
export function alignRight(rows: Row[], columns: readonly string[]): Row[] {
  const widths = new Map(
    columns.map((column) => [column, rows.reduce((widest, row) => Math.max(widest, Bun.stringWidth(row[column] ?? "")), 0)]),
  );
  return rows.map((row) => {
    const aligned: Row = { ...row };
    for (const column of columns) {
      const cell = row[column] ?? "";
      aligned[column] = " ".repeat((widths.get(column) ?? 0) - Bun.stringWidth(cell)) + cell;
    }
    return aligned;
  });
}

export function renderTable(rows: Row[], columns: readonly string[]): string {
  if (rows.length === 0) {
    return "(no rows)\n";
  }
  return `${Bun.inspect.table(rows, [...columns])}`;
}

export function shortenMiddle(text: string, keep_each_side: number): string {
  if (text.length <= keep_each_side * 2 + 1) {
    return text;
  }
  return `${text.slice(0, keep_each_side)}…${text.slice(-keep_each_side)}`;
}

export function displayValue(value: unknown): string {
  if (value === null || value === undefined) {
    return "";
  }
  return typeof value === "string" ? value : JSON.stringify(value);
}
