import type { CliFailure } from "./failure.ts";
import { errorBody, type Exchange, type RequestRecord, type ResponseRecord } from "./http.ts";
import { TOOL_NAME, TOOL_VERSION } from "./version.ts";

export type EnvelopeError = {
  kind: string;
  message: string;
  status: number | null;
  body: unknown;
};

export type CompareSection = {
  request: RequestRecord;
  response: ResponseRecord | null;
  error: EnvelopeError | null;
};

export type Envelope = {
  meta: { tool: string; version: string; generated_at: string };
  request: RequestRecord | null;
  response: ResponseRecord | null;
  data: unknown;
  summary: unknown;
  compare?: CompareSection;
  error: EnvelopeError | null;
};

export type EnvelopeParts = {
  generated_at: Date;
  exchange: Exchange | null;
  request?: RequestRecord | null;
  data: unknown;
  summary: unknown;
  compare?: CompareSection;
  error?: EnvelopeError | null;
};

// Keys are assigned in contract order because JSON.stringify keeps insertion order.
export function buildEnvelope(parts: EnvelopeParts): Envelope {
  return {
    meta: { tool: TOOL_NAME, version: TOOL_VERSION, generated_at: parts.generated_at.toISOString() },
    request: parts.exchange?.request ?? parts.request ?? null,
    response: parts.exchange?.response ?? null,
    data: parts.data,
    summary: parts.summary,
    ...(parts.compare === undefined ? {} : { compare: parts.compare }),
    error: parts.error ?? null,
  };
}

export function envelopeError(failure: CliFailure): EnvelopeError {
  const exchange = failure.exchange;
  return {
    kind: failure.kind,
    message: failure.message,
    status: exchange?.response.status ?? null,
    body: exchange === undefined ? null : errorBody(exchange),
  };
}

export function failureEnvelope(failure: CliFailure, generated_at: Date): Envelope {
  return buildEnvelope({
    generated_at,
    exchange: failure.exchange ?? null,
    request: failure.request ?? null,
    data: null,
    summary: null,
    error: envelopeError(failure),
  });
}

export function serializeEnvelope(envelope: Envelope): string {
  return `${JSON.stringify(envelope, null, 2)}\n`;
}
