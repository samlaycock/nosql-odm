const preparedDocumentTag = Symbol("nosql-odm.prepared-document");

type JsonPrimitive = string | number | boolean | null;
type JsonValue = JsonPrimitive | JsonValue[] | { [key: string]: JsonValue };

export type DocumentPreparationMode = "clone" | "serialize";

export interface PreparedDocument {
  readonly [preparedDocumentTag]: true;
  readonly mode: DocumentPreparationMode;
  readonly value: Record<string, unknown> | string;
}

interface PreparedNode {
  readonly cloned: JsonValue;
  readonly serialized: string;
}

export function prepareDocumentForStorage(
  value: object,
  collection: string,
  key: string,
  mode: DocumentPreparationMode,
): PreparedDocument {
  const visited = new WeakSet<object>();
  const prepared = visitValue(value, "$", visited, collection, key);

  return {
    [preparedDocumentTag]: true,
    mode,
    value: mode === "clone" ? (prepared.cloned as Record<string, unknown>) : prepared.serialized,
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

function visitValue(
  candidate: unknown,
  path: string,
  visited: WeakSet<object>,
  collection: string,
  key: string,
): PreparedNode {
  if (candidate === null) {
    return { cloned: null, serialized: "null" };
  }

  const candidateType = typeof candidate;

  if (candidateType === "string") {
    const stringValue = candidate as string;

    return {
      cloned: stringValue,
      serialized: JSON.stringify(stringValue),
    };
  }

  if (candidateType === "boolean") {
    const booleanValue = candidate as boolean;

    return {
      cloned: booleanValue,
      serialized: booleanValue ? "true" : "false",
    };
  }

  if (candidateType === "number") {
    const numberValue = candidate as number;

    if (!Number.isFinite(numberValue)) {
      throw new Error(
        `Document "${key}" in model "${collection}" is not JSON-compatible at ${path}: non-finite numbers are not allowed`,
      );
    }

    return {
      cloned: numberValue,
      serialized: JSON.stringify(numberValue),
    };
  }

  if (candidateType === "undefined") {
    throw new Error(
      `Document "${key}" in model "${collection}" is not JSON-compatible at ${path}: undefined is not allowed`,
    );
  }

  if (candidateType === "bigint") {
    throw new Error(
      `Document "${key}" in model "${collection}" is not JSON-compatible at ${path}: bigint is not allowed`,
    );
  }

  if (candidateType === "symbol") {
    throw new Error(
      `Document "${key}" in model "${collection}" is not JSON-compatible at ${path}: symbol is not allowed`,
    );
  }

  if (candidateType === "function") {
    throw new Error(
      `Document "${key}" in model "${collection}" is not JSON-compatible at ${path}: function is not allowed`,
    );
  }

  if (candidateType !== "object") {
    throw new Error(
      `Document "${key}" in model "${collection}" is not JSON-compatible at ${path}: unsupported value type`,
    );
  }

  const objectValue = candidate as object;

  if (visited.has(objectValue)) {
    throw new Error(
      `Document "${key}" in model "${collection}" is not JSON-compatible at ${path}: circular references are not allowed`,
    );
  }

  visited.add(objectValue);

  if (Array.isArray(candidate)) {
    const cloned: JsonValue[] = [];
    const serializedParts: string[] = [];

    for (let i = 0; i < candidate.length; i++) {
      const prepared = visitValue(candidate[i], `${path}[${String(i)}]`, visited, collection, key);
      cloned.push(prepared.cloned);
      serializedParts.push(prepared.serialized);
    }

    return {
      cloned,
      serialized: `[${serializedParts.join(",")}]`,
    };
  }

  const proto = Object.getPrototypeOf(candidate);

  if (proto !== Object.prototype && proto !== null) {
    const constructorName =
      (candidate as { constructor?: { name?: string } }).constructor?.name ?? "Object";
    throw new Error(
      `Document "${key}" in model "${collection}" is not JSON-compatible at ${path}: unsupported object type "${constructorName}"`,
    );
  }

  const entries = Object.entries(candidate as Record<string, unknown>);
  const cloned: Record<string, JsonValue> =
    proto === null ? (Object.create(null) as Record<string, JsonValue>) : {};
  const serializedParts: string[] = [];

  for (const [entryKey, entryValue] of entries) {
    const prepared = visitValue(entryValue, `${path}.${entryKey}`, visited, collection, key);
    cloned[entryKey] = prepared.cloned;
    serializedParts.push(`${JSON.stringify(entryKey)}:${prepared.serialized}`);
  }

  return {
    cloned,
    serialized: `{${serializedParts.join(",")}}`,
  };
}
