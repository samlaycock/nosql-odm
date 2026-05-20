import type { EngineQueryDiagnostics, EngineQueryResult, QueryParams } from "./types";

export function queryDiagnosticsForIndexedQuery(params: QueryParams): EngineQueryDiagnostics {
  return {
    mode: "native_pushdown",
    reason: "native_pushdown",
    ...(params.index ? { index: params.index } : {}),
  };
}

export function queryDiagnosticsForCollectionScan(): EngineQueryDiagnostics {
  return {
    mode: "fallback_scan",
    reason: "full_scan",
  };
}

export function queryDiagnosticsForUnsupportedFilter(params: QueryParams): EngineQueryDiagnostics {
  return {
    mode: "fallback_scan",
    reason: "unsupported_filter",
    ...(params.index ? { index: params.index } : {}),
  };
}

export function withQueryDiagnostics(
  result: EngineQueryResult,
  diagnostics: EngineQueryDiagnostics,
): EngineQueryResult {
  return {
    ...result,
    diagnostics,
  };
}
