const preparedDocumentTag = Symbol("nosql-odm.prepared-document");

type JsonPrimitive = string | number | boolean | null;
type JsonValue = JsonPrimitive | JsonValue[] | { [key: string]: JsonValue };

export type DocumentPreparationMode = "clone" | "serialize";

export interface PreparedDocument {
  readonly [preparedDocumentTag]: true;
  readonly mode: DocumentPreparationMode;
  readonly value: Record<string, unknown> | string;
}

export function validateJsonCompatibleDocument(
  value: object,
  collection: string,
  key: string,
): Record<string, unknown> {
  validateValue(value, "$", new WeakSet<object>(), collection, key);
  return value as Record<string, unknown>;
}

export function prepareDocumentForStorage(
  value: object,
  collection: string,
  key: string,
  mode: DocumentPreparationMode,
): PreparedDocument {
  return {
    [preparedDocumentTag]: true,
    mode,
    value:
      mode === "clone"
        ? (visitClonedValue(value, "$", new WeakSet<object>(), collection, key) as Record<
            string,
            unknown
          >)
        : visitSerializedValue(value, "$", new WeakSet<object>(), collection, key),
  };
}

export function getPreparedClone(doc: unknown): Record<string, unknown> | null {
  if (!isPreparedDocument(doc) || doc.mode !== "clone") {
    return null;
  }

  return doc.value as Record<string, unknown>;
}

export function getPreparedSerializedDocument(doc: unknown): string | null {
  if (!isPreparedDocument(doc) || doc.mode !== "serialize") {
    return null;
  }

  return doc.value as string;
}

function isPreparedDocument(doc: unknown): doc is PreparedDocument {
  return typeof doc === "object" && doc !== null && preparedDocumentTag in doc;
}

function visitClonedValue(
  candidate: unknown,
  path: string,
  visited: WeakSet<object>,
  collection: string,
  key: string,
): JsonValue {
  validatePrimitiveValue(candidate, path, collection, key);

  if (candidate === null) {
    return null;
  }

  const candidateType = typeof candidate;

  if (candidateType === "string" || candidateType === "boolean" || candidateType === "number") {
    return candidate as JsonPrimitive;
  }

  if (candidateType !== "object") {
    throwNotJsonCompatible(collection, key, path, "unsupported value type");
  }

  const objectValue = candidate as object;

  if (visited.has(objectValue)) {
    throwNotJsonCompatible(collection, key, path, "circular references are not allowed");
  }

  visited.add(objectValue);

  try {
    if (Array.isArray(candidate)) {
      const cloned: JsonValue[] = [];

      for (let i = 0; i < candidate.length; i++) {
        cloned.push(
          visitClonedValue(candidate[i], `${path}[${String(i)}]`, visited, collection, key),
        );
      }

      return cloned;
    }

    const proto = Object.getPrototypeOf(candidate);

    if (proto !== Object.prototype && proto !== null) {
      const constructorName =
        (candidate as { constructor?: { name?: string } }).constructor?.name ?? "Object";
      throwNotJsonCompatible(collection, key, path, `unsupported object type "${constructorName}"`);
    }

    const cloned: Record<string, JsonValue> =
      proto === null ? (Object.create(null) as Record<string, JsonValue>) : {};

    for (const [entryKey, entryValue] of Object.entries(candidate as Record<string, unknown>)) {
      cloned[entryKey] = visitClonedValue(
        entryValue,
        `${path}.${entryKey}`,
        visited,
        collection,
        key,
      );
    }

    return cloned;
  } finally {
    visited.delete(objectValue);
  }
}

function visitSerializedValue(
  candidate: unknown,
  path: string,
  visited: WeakSet<object>,
  collection: string,
  key: string,
): string {
  validatePrimitiveValue(candidate, path, collection, key);

  if (candidate === null) {
    return "null";
  }

  const candidateType = typeof candidate;

  if (candidateType === "string") {
    return JSON.stringify(candidate as string);
  }

  if (candidateType === "boolean") {
    return (candidate as boolean) ? "true" : "false";
  }

  if (candidateType === "number") {
    return JSON.stringify(candidate as number);
  }

  if (candidateType !== "object") {
    throwNotJsonCompatible(collection, key, path, "unsupported value type");
  }

  const objectValue = candidate as object;

  if (visited.has(objectValue)) {
    throwNotJsonCompatible(collection, key, path, "circular references are not allowed");
  }

  visited.add(objectValue);

  try {
    if (Array.isArray(candidate)) {
      const serializedParts: string[] = [];

      for (let i = 0; i < candidate.length; i++) {
        serializedParts.push(
          visitSerializedValue(candidate[i], `${path}[${String(i)}]`, visited, collection, key),
        );
      }

      return `[${serializedParts.join(",")}]`;
    }

    const proto = Object.getPrototypeOf(candidate);

    if (proto !== Object.prototype && proto !== null) {
      const constructorName =
        (candidate as { constructor?: { name?: string } }).constructor?.name ?? "Object";
      throwNotJsonCompatible(collection, key, path, `unsupported object type "${constructorName}"`);
    }

    const serializedParts: string[] = [];

    for (const [entryKey, entryValue] of Object.entries(candidate as Record<string, unknown>)) {
      serializedParts.push(
        `${JSON.stringify(entryKey)}:${visitSerializedValue(
          entryValue,
          `${path}.${entryKey}`,
          visited,
          collection,
          key,
        )}`,
      );
    }

    return `{${serializedParts.join(",")}}`;
  } finally {
    visited.delete(objectValue);
  }
}

function validateValue(
  candidate: unknown,
  path: string,
  visited: WeakSet<object>,
  collection: string,
  key: string,
): void {
  validatePrimitiveValue(candidate, path, collection, key);

  if (candidate === null) {
    return;
  }

  const candidateType = typeof candidate;

  if (candidateType === "string" || candidateType === "boolean" || candidateType === "number") {
    return;
  }

  if (candidateType !== "object") {
    throwNotJsonCompatible(collection, key, path, "unsupported value type");
  }

  const objectValue = candidate as object;

  if (visited.has(objectValue)) {
    throwNotJsonCompatible(collection, key, path, "circular references are not allowed");
  }

  visited.add(objectValue);

  try {
    if (Array.isArray(candidate)) {
      for (let i = 0; i < candidate.length; i++) {
        validateValue(candidate[i], `${path}[${String(i)}]`, visited, collection, key);
      }
      return;
    }

    const proto = Object.getPrototypeOf(candidate);

    if (proto !== Object.prototype && proto !== null) {
      const constructorName =
        (candidate as { constructor?: { name?: string } }).constructor?.name ?? "Object";
      throwNotJsonCompatible(collection, key, path, `unsupported object type "${constructorName}"`);
    }

    for (const [entryKey, entryValue] of Object.entries(candidate as Record<string, unknown>)) {
      validateValue(entryValue, `${path}.${entryKey}`, visited, collection, key);
    }
  } finally {
    visited.delete(objectValue);
  }
}

function validatePrimitiveValue(
  candidate: unknown,
  path: string,
  collection: string,
  key: string,
): void {
  if (candidate === null) {
    return;
  }

  const candidateType = typeof candidate;

  if (candidateType === "string" || candidateType === "boolean") {
    return;
  }

  if (candidateType === "number") {
    if (!Number.isFinite(candidate as number)) {
      throwNotJsonCompatible(collection, key, path, "non-finite numbers are not allowed");
    }
    return;
  }

  if (candidateType === "undefined") {
    throwNotJsonCompatible(collection, key, path, "undefined is not allowed");
  }

  if (candidateType === "bigint") {
    throwNotJsonCompatible(collection, key, path, "bigint is not allowed");
  }

  if (candidateType === "symbol") {
    throwNotJsonCompatible(collection, key, path, "symbol is not allowed");
  }

  if (candidateType === "function") {
    throwNotJsonCompatible(collection, key, path, "function is not allowed");
  }
}

function throwNotJsonCompatible(
  collection: string,
  key: string,
  path: string,
  reason: string,
): never {
  throw new Error(
    `Document "${key}" in model "${collection}" is not JSON-compatible at ${path}: ${reason}`,
  );
}
