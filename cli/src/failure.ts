import type { Exchange, RequestRecord } from "./http.ts";

export type FailureKind = "usage" | "http" | "invalid_json" | "network" | "timeout" | "cancelled";

export const EXIT_CODE_OK = 0;

const EXIT_CODE_BY_KIND: Record<FailureKind, number> = {
  http: 1,
  invalid_json: 1,
  usage: 2,
  network: 3,
  timeout: 3,
  cancelled: 130,
};

export type FailureDetail = {
  request?: RequestRecord;
  exchange?: Exchange;
};

export class CliFailure extends Error {
  readonly kind: FailureKind;
  readonly request: RequestRecord | undefined;
  readonly exchange: Exchange | undefined;

  constructor(kind: FailureKind, message: string, detail: FailureDetail = {}) {
    super(message);
    this.kind = kind;
    this.request = detail.request ?? detail.exchange?.request;
    this.exchange = detail.exchange;
  }
}

export function exitCodeFor(kind: FailureKind): number {
  return EXIT_CODE_BY_KIND[kind];
}

export function usageFailure(message: string): CliFailure {
  return new CliFailure("usage", message);
}
