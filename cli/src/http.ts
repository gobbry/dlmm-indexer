import { CliFailure } from "./failure.ts";
import { TOOL_NAME, TOOL_VERSION } from "./version.ts";

export type RequestRecord = {
  method: "GET" | "POST";
  url: string;
  params: Record<string, string>;
  headers: Record<string, string>;
  // Only on POST, so a GET envelope keeps its four request keys.
  body?: string;
};

export type ResponseRecord = {
  status: number;
  latency_ms: number;
  headers: Record<string, string>;
  body_bytes: number;
};

export type Exchange = {
  request: RequestRecord;
  response: ResponseRecord;
  status_text: string;
  body_text: string;
};

export function buildRequest(base_url: string, path: string, params: Record<string, string>): RequestRecord {
  const query = new URLSearchParams(params).toString();
  const url = `${base_url.replace(/\/+$/, "")}${path}${query === "" ? "" : `?${query}`}`;
  return {
    method: "GET",
    url,
    params,
    headers: { accept: "application/json", "user-agent": `${TOOL_NAME}/${TOOL_VERSION}` },
  };
}

export function buildPostRequest(base_url: string, path: string, body: unknown): RequestRecord {
  return {
    method: "POST",
    url: `${base_url.replace(/\/+$/, "")}${path}`,
    params: {},
    headers: {
      accept: "application/json",
      "content-type": "application/json",
      "user-agent": `${TOOL_NAME}/${TOOL_VERSION}`,
    },
    body: JSON.stringify(body),
  };
}

function sortedHeaders(headers: Headers): Record<string, string> {
  const entries = [...headers.entries()].sort(([left], [right]) => left.localeCompare(right));
  return Object.fromEntries(entries);
}

function transportFailure(request: RequestRecord, error: unknown, timeout_ms: number): CliFailure {
  const name = error instanceof Error ? error.name : "";
  if (name === "TimeoutError" || name === "AbortError") {
    return new CliFailure("timeout", `${request.method} ${request.url} timed out after ${timeout_ms} ms`, { request });
  }
  const reason = error instanceof Error ? error.message : String(error);
  return new CliFailure("network", `${request.method} ${request.url} failed: ${reason}`, { request });
}

export async function send(request: RequestRecord, timeout_ms: number): Promise<Exchange> {
  const started_ms = performance.now();
  try {
    const response = await fetch(request.url, {
      method: request.method,
      headers: request.headers,
      ...(request.body === undefined ? {} : { body: request.body }),
      signal: AbortSignal.timeout(timeout_ms),
    });
    const body = new Uint8Array(await response.arrayBuffer());
    return {
      request,
      response: {
        status: response.status,
        latency_ms: Math.round(performance.now() - started_ms),
        headers: sortedHeaders(response.headers),
        body_bytes: body.byteLength,
      },
      status_text: response.statusText,
      body_text: new TextDecoder().decode(body),
    };
  } catch (error) {
    throw transportFailure(request, error, timeout_ms);
  }
}

function parseJson(text: string): { ok: true; value: unknown } | { ok: false } {
  try {
    return { ok: true, value: JSON.parse(text) };
  } catch {
    return { ok: false };
  }
}

export function errorBody(exchange: Exchange): unknown {
  const parsed = parseJson(exchange.body_text);
  return parsed.ok ? parsed.value : exchange.body_text;
}

export function decodeBody(exchange: Exchange): unknown {
  const { status } = exchange.response;
  if (status < 200 || status > 299) {
    const message = `${exchange.request.method} ${exchange.request.url} returned ${status} ${exchange.status_text}`.trim();
    throw new CliFailure("http", message, { exchange });
  }
  const parsed = parseJson(exchange.body_text);
  if (!parsed.ok) {
    throw new CliFailure("invalid_json", `${exchange.request.method} ${exchange.request.url} returned a body that is not JSON`, { exchange });
  }
  return parsed.value;
}
