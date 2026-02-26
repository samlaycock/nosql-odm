import type { QueryParams } from "./types";

type QueryCursorPosition =
  | {
      kind: "key";
      key: string;
    }
  | {
      kind: "scan";
      key: string;
      createdAt: number;
    }
  | {
      kind: "sorted-index";
      key: string;
      indexValue: string;
      createdAt?: number;
    };

interface QueryCursorPayload {
  kind: "nosql-odm-query-cursor";
  version: 1;
  signature: string;
  position: QueryCursorPosition;
}

export interface QueryCursorRecordPosition {
  key: string;
  createdAt?: number;
  indexValue?: string;
}

export function encodeQueryPageCursor(
  collection: string,
  params: QueryParams,
  position: QueryCursorRecordPosition,
): string {
  const payload: QueryCursorPayload = {
    kind: "nosql-odm-query-cursor",
    version: 1,
    signature: buildQueryCursorSignature(collection, params),
    position: buildCursorPosition(params, position),
  };

  return Buffer.from(JSON.stringify(payload), "utf8").toString("base64url");
}

export function resolveQueryPageStartIndex<TRecord>(
  records: TRecord[],
  collection: string,
  params: QueryParams,
  getPosition: (record: TRecord, params: QueryParams) => QueryCursorRecordPosition,
): number {
  if (!params.cursor) {
    return 0;
  }

  const payload = decodeQueryPageCursor(params.cursor);
  const expectedSignature = buildQueryCursorSignature(collection, params);
  const cursorPosition = payload.position;

  if (payload.signature !== expectedSignature) {
    throw new Error("Query cursor does not match the requested query");
  }

  if (cursorPosition.kind === "key") {
    const index = records.findIndex((record) => getPosition(record, params).key === cursorPosition.key);

    if (index === -1) {
      throw new Error("Query cursor no longer points to a valid result");
    }

    return index + 1;
  }

  if (cursorPosition.kind === "scan") {
    const index = records.findIndex((record) => {
      const position = getPosition(record, params);
      return compareScanPosition(position, cursorPosition) > 0;
    });

    return index === -1 ? records.length : index;
  }

  const index = records.findIndex((record) => {
    const position = getPosition(record, params);
    return compareSortedIndexPosition(position, cursorPosition, params) > 0;
  });

  return index === -1 ? records.length : index;
}

function buildCursorPosition(
  params: QueryParams,
  position: QueryCursorRecordPosition,
): QueryCursorPosition {
  if (params.sort && params.index) {
    if (position.createdAt !== undefined) {
      return {
        kind: "sorted-index",
        key: position.key,
        indexValue: position.indexValue ?? "",
        createdAt: position.createdAt,
      };
    }

    return {
      kind: "sorted-index",
      key: position.key,
      indexValue: position.indexValue ?? "",
    };
  }

  if (position.createdAt !== undefined) {
    return {
      kind: "scan",
      key: position.key,
      createdAt: position.createdAt,
    };
  }

  return {
    kind: "key",
    key: position.key,
  };
}

function compareScanPosition(
  candidate: QueryCursorRecordPosition,
  cursor: Extract<QueryCursorPosition, { kind: "scan" }>,
): number {
  const createdAt = candidate.createdAt;

  if (typeof createdAt !== "number" || !Number.isFinite(createdAt)) {
    throw new Error("Query pagination record is missing createdAt metadata");
  }

  if (createdAt !== cursor.createdAt) {
    return createdAt - cursor.createdAt;
  }

  return candidate.key.localeCompare(cursor.key);
}

function compareSortedIndexPosition(
  candidate: QueryCursorRecordPosition,
  cursor: Extract<QueryCursorPosition, { kind: "sorted-index" }>,
  params: QueryParams,
): number {
  const indexValue = candidate.indexValue ?? "";
  let cmp = indexValue.localeCompare(cursor.indexValue);

  if (params.sort === "desc") {
    cmp = -cmp;
  }

  if (cmp !== 0) {
    return cmp;
  }

  if (cursor.createdAt !== undefined) {
    const createdAt = candidate.createdAt;

    if (typeof createdAt !== "number" || !Number.isFinite(createdAt)) {
      throw new Error("Query pagination record is missing createdAt metadata");
    }

    if (createdAt !== cursor.createdAt) {
      return createdAt - cursor.createdAt;
    }
  }

  return candidate.key.localeCompare(cursor.key);
}

function decodeQueryPageCursor(encoded: string): QueryCursorPayload {
  let parsed: unknown;

  try {
    parsed = JSON.parse(Buffer.from(encoded, "base64url").toString("utf8"));
  } catch {
    throw new Error("Invalid query cursor");
  }

  if (!isRecord(parsed)) {
    throw new Error("Invalid query cursor");
  }

  if (parsed.kind !== "nosql-odm-query-cursor" || parsed.version !== 1) {
    throw new Error("Invalid query cursor");
  }

  if (typeof parsed.signature !== "string" || parsed.signature.length === 0) {
    throw new Error("Invalid query cursor");
  }

  const position = parseCursorPosition(parsed.position);

  return {
    kind: "nosql-odm-query-cursor",
    version: 1,
    signature: parsed.signature,
    position,
  };
}

function parseCursorPosition(raw: unknown): QueryCursorPosition {
  if (!isRecord(raw) || typeof raw.kind !== "string" || typeof raw.key !== "string") {
    throw new Error("Invalid query cursor");
  }

  if (raw.kind === "key") {
    return {
      kind: "key",
      key: raw.key,
    };
  }

  if (raw.kind === "scan") {
    if (typeof raw.createdAt !== "number" || !Number.isFinite(raw.createdAt)) {
      throw new Error("Invalid query cursor");
    }

    return {
      kind: "scan",
      key: raw.key,
      createdAt: raw.createdAt,
    };
  }

  if (raw.kind === "sorted-index") {
    if (typeof raw.indexValue !== "string") {
      throw new Error("Invalid query cursor");
    }

    if (
      raw.createdAt !== undefined &&
      (typeof raw.createdAt !== "number" || !Number.isFinite(raw.createdAt))
    ) {
      throw new Error("Invalid query cursor");
    }

    return {
      kind: "sorted-index",
      key: raw.key,
      indexValue: raw.indexValue,
      ...(raw.createdAt !== undefined ? { createdAt: raw.createdAt } : {}),
    };
  }

  throw new Error("Invalid query cursor");
}

function buildQueryCursorSignature(collection: string, params: QueryParams): string {
  return stableSerialize({
    collection,
    index: params.index ?? null,
    filter: params.filter?.value ?? null,
    sort: params.sort ?? null,
  });
}

function stableSerialize(value: unknown): string {
  if (value === null) {
    return "null";
  }

  if (typeof value === "string" || typeof value === "number" || typeof value === "boolean") {
    return JSON.stringify(value);
  }

  if (Array.isArray(value)) {
    return `[${value.map((item) => stableSerialize(item)).join(",")}]`;
  }

  if (typeof value !== "object") {
    return JSON.stringify(null);
  }

  const entries = Object.entries(value as Record<string, unknown>).sort(([a], [b]) =>
    a.localeCompare(b),
  );

  return `{${entries
    .map(([key, entryValue]) => `${JSON.stringify(key)}:${stableSerialize(entryValue)}`)
    .join(",")}}`;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}
