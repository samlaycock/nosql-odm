import {
  EngineDocumentAlreadyExistsError,
  EngineDocumentNotFoundError,
  type ComparableVersion,
  type EngineQueryResult,
  type FieldCondition,
  type KeyedDocument,
  type MigrationCriteria,
  type MigrationLock,
  type QueryEngine,
  type QueryParams,
  type ResolvedIndexKeys,
} from "./types";

const DEFAULT_KEY_PREFIX = "nosql_odm";
const OUTDATED_PAGE_LIMIT = 100;

const CREATE_DOCUMENT_SCRIPT = `
if redis.call("EXISTS", KEYS[1]) == 1 then
  return 0
end
redis.call("HSET", KEYS[1], "createdAt", ARGV[1], "doc", ARGV[2], "indexes", ARGV[3])
redis.call("ZADD", KEYS[2], ARGV[1], ARGV[4])
return 1
`;

const PUT_DOCUMENT_SCRIPT = `
local createdAt = redis.call("HGET", KEYS[1], "createdAt")
if not createdAt then
  createdAt = ARGV[1]
end
redis.call("HSET", KEYS[1], "createdAt", createdAt, "doc", ARGV[2], "indexes", ARGV[3])
redis.call("ZADD", KEYS[2], createdAt, ARGV[4])
return createdAt
`;

const UPDATE_DOCUMENT_SCRIPT = `
local createdAt = redis.call("HGET", KEYS[1], "createdAt")
if not createdAt then
  return 0
end
redis.call("HSET", KEYS[1], "createdAt", createdAt, "doc", ARGV[1], "indexes", ARGV[2])
redis.call("ZADD", KEYS[2], createdAt, ARGV[3])
return 1
`;

const DELETE_DOCUMENT_SCRIPT = `
redis.call("DEL", KEYS[1])
redis.call("ZREM", KEYS[2], ARGV[1])
return 1
`;

const ACQUIRE_LOCK_SCRIPT = `
if redis.call("EXISTS", KEYS[1]) == 0 then
  redis.call("HSET", KEYS[1], "lockId", ARGV[1], "acquiredAt", ARGV[2])
  return 1
end

if ARGV[3] ~= "1" then
  return 0
end

local existing = tonumber(redis.call("HGET", KEYS[1], "acquiredAt"))
if not existing then
  return -1
end

local staleBefore = tonumber(ARGV[4])
if existing <= staleBefore then
  redis.call("HSET", KEYS[1], "lockId", ARGV[1], "acquiredAt", ARGV[2])
  return 1
end

return 0
`;

const RELEASE_LOCK_SCRIPT = `
local lockId = redis.call("HGET", KEYS[1], "lockId")
if lockId and lockId == ARGV[1] then
  redis.call("DEL", KEYS[1])
  return 1
end

return 0
`;

const SAVE_CHECKPOINT_SCRIPT = `
local lockId = redis.call("HGET", KEYS[1], "lockId")
if lockId and lockId == ARGV[1] then
  redis.call("SET", KEYS[2], ARGV[2])
  return 1
end

return 0
`;

interface RedisEvalOptionsLike {
  keys: string[];
  arguments: string[];
}

interface RedisClientLike {
  get(key: string): Promise<unknown>;
  del(keys: string | string[]): Promise<unknown>;
  hGetAll(key: string): Promise<unknown>;
  zRange(key: string, start: number, stop: number): Promise<unknown>;
  incr(key: string): Promise<unknown>;
  eval(script: string, options: RedisEvalOptionsLike): Promise<unknown>;
}

export interface RedisEngineOptions {
  client: RedisClientLike;
  keyPrefix?: string;
}

export interface RedisQueryEngine extends QueryEngine<never> {}

interface StoredDocumentRecord {
  key: string;
  createdAt: number;
  doc: Record<string, unknown>;
  indexes: ResolvedIndexKeys;
}

interface LockRecord {
  lockId: string;
  acquiredAt: number;
}

export function redisEngine(options: RedisEngineOptions): RedisQueryEngine {
  const client = options.client;
  const keyPrefix = normalizePrefix(options.keyPrefix ?? DEFAULT_KEY_PREFIX);

  const engine: RedisQueryEngine = {
    async get(collection, key) {
      const record = await loadDocumentRecord(client, keyPrefix, collection, key);

      if (!record) {
        return null;
      }

      return structuredClone(record.doc);
    },

    async create(collection, key, doc, indexes) {
      const createdAt = await nextCreatedAt(client, keyPrefix, collection);
      const result = await evalScript(
        client,
        CREATE_DOCUMENT_SCRIPT,
        [documentHashKey(keyPrefix, collection, key), collectionOrderKey(keyPrefix, collection)],
        [String(createdAt), serializeDocument(doc), serializeIndexes(indexes), key],
      );

      if (parseInteger(result, "Redis returned an invalid create result") !== 1) {
        throw new EngineDocumentAlreadyExistsError(collection, key);
      }
    },

    async put(collection, key, doc, indexes) {
      const createdAt = await nextCreatedAt(client, keyPrefix, collection);
      const result = await evalScript(
        client,
        PUT_DOCUMENT_SCRIPT,
        [documentHashKey(keyPrefix, collection, key), collectionOrderKey(keyPrefix, collection)],
        [String(createdAt), serializeDocument(doc), serializeIndexes(indexes), key],
      );

      parseInteger(result, "Redis returned an invalid put result");
    },

    async update(collection, key, doc, indexes) {
      const result = await evalScript(
        client,
        UPDATE_DOCUMENT_SCRIPT,
        [documentHashKey(keyPrefix, collection, key), collectionOrderKey(keyPrefix, collection)],
        [serializeDocument(doc), serializeIndexes(indexes), key],
      );

      if (parseInteger(result, "Redis returned an invalid update result") !== 1) {
        throw new EngineDocumentNotFoundError(collection, key);
      }
    },

    async delete(collection, key) {
      const result = await evalScript(
        client,
        DELETE_DOCUMENT_SCRIPT,
        [documentHashKey(keyPrefix, collection, key), collectionOrderKey(keyPrefix, collection)],
        [key],
      );

      parseInteger(result, "Redis returned an invalid delete result");
    },

    async query(collection, params) {
      const records = await listCollectionDocuments(client, keyPrefix, collection);
      const matched = matchDocuments(records, params);

      return paginate(matched, params);
    },

    async batchGet(collection, keys) {
      const fetched = new Map<string, StoredDocumentRecord>();
      const results: KeyedDocument[] = [];

      for (const key of uniqueStrings(keys)) {
        const record = await loadDocumentRecord(client, keyPrefix, collection, key);

        if (!record) {
          continue;
        }

        fetched.set(key, record);
      }

      for (const key of keys) {
        const record = fetched.get(key);

        if (!record) {
          continue;
        }

        results.push({
          key,
          doc: structuredClone(record.doc),
        });
      }

      return results;
    },

    async batchSet(collection, items) {
      for (const item of items) {
        const createdAt = await nextCreatedAt(client, keyPrefix, collection);
        const result = await evalScript(
          client,
          PUT_DOCUMENT_SCRIPT,
          [
            documentHashKey(keyPrefix, collection, item.key),
            collectionOrderKey(keyPrefix, collection),
          ],
          [
            String(createdAt),
            serializeDocument(item.doc),
            serializeIndexes(item.indexes),
            item.key,
          ],
        );

        parseInteger(result, "Redis returned an invalid batchSet result");
      }
    },

    async batchDelete(collection, keys) {
      for (const key of keys) {
        const result = await evalScript(
          client,
          DELETE_DOCUMENT_SCRIPT,
          [documentHashKey(keyPrefix, collection, key), collectionOrderKey(keyPrefix, collection)],
          [key],
        );

        parseInteger(result, "Redis returned an invalid batchDelete result");
      }
    },

    migration: {
      async acquireLock(collection, options) {
        const lock: MigrationLock = {
          id: randomId(),
          collection,
          acquiredAt: Date.now(),
        };

        const ttl = options?.ttl;
        const allowSteal = ttl !== undefined && Number.isFinite(ttl) && ttl >= 0;
        const staleBefore = allowSteal ? lock.acquiredAt - ttl : 0;
        const result = await evalScript(
          client,
          ACQUIRE_LOCK_SCRIPT,
          [migrationLockKey(keyPrefix, collection)],
          [lock.id, String(lock.acquiredAt), allowSteal ? "1" : "0", String(staleBefore)],
        );
        const code = parseInteger(result, "Redis returned an invalid lock acquire result");

        if (code === 1) {
          return lock;
        }

        if (code === 0) {
          return null;
        }

        if (code === -1) {
          throw new Error("Redis returned an invalid migration lock record");
        }

        throw new Error("Redis returned an invalid lock acquire result");
      },

      async releaseLock(lock) {
        const result = await evalScript(
          client,
          RELEASE_LOCK_SCRIPT,
          [migrationLockKey(keyPrefix, lock.collection)],
          [lock.id],
        );

        parseInteger(result, "Redis returned an invalid lock release result");
      },

      async getOutdated(collection, criteria, cursor) {
        const records = await listCollectionDocuments(client, keyPrefix, collection);
        const parseVersion = criteria.parseVersion ?? defaultParseVersion;
        const compareVersions = criteria.compareVersions ?? defaultCompareVersions;
        const outdated: StoredDocumentRecord[] = [];

        for (const record of records) {
          if (isOutdated(record.doc, criteria, parseVersion, compareVersions)) {
            outdated.push(record);
          }
        }

        return paginate(outdated, {
          cursor,
          limit: OUTDATED_PAGE_LIMIT,
        });
      },

      async saveCheckpoint(lock, cursor) {
        const result = await evalScript(
          client,
          SAVE_CHECKPOINT_SCRIPT,
          [
            migrationLockKey(keyPrefix, lock.collection),
            migrationCheckpointKey(keyPrefix, lock.collection),
          ],
          [lock.id, cursor],
        );

        parseInteger(result, "Redis returned an invalid migration checkpoint result");
      },

      async loadCheckpoint(collection) {
        const value = await client.get(migrationCheckpointKey(keyPrefix, collection));

        if (value === null) {
          return null;
        }

        if (typeof value !== "string") {
          throw new Error("Redis returned an invalid migration checkpoint record");
        }

        return value;
      },

      async clearCheckpoint(collection) {
        await client.del(migrationCheckpointKey(keyPrefix, collection));
      },

      async getStatus(collection) {
        const rawLock = await client.hGetAll(migrationLockKey(keyPrefix, collection));
        const fields = parseStringRecord(rawLock, "migration lock record");

        if (Object.keys(fields).length === 0) {
          return null;
        }

        const lock = parseLockRecord(fields);
        const cursorRaw = await client.get(migrationCheckpointKey(keyPrefix, collection));

        if (cursorRaw !== null && typeof cursorRaw !== "string") {
          throw new Error("Redis returned an invalid migration checkpoint record");
        }

        return {
          lock: {
            id: lock.lockId,
            collection,
            acquiredAt: lock.acquiredAt,
          },
          cursor: cursorRaw ?? null,
        };
      },
    },
  };

  return engine;
}

async function evalScript(
  client: RedisClientLike,
  script: string,
  keys: string[],
  args: string[],
): Promise<unknown> {
  return client.eval(script, {
    keys,
    arguments: args,
  });
}

async function nextCreatedAt(
  client: RedisClientLike,
  keyPrefix: string,
  collection: string,
): Promise<number> {
  const raw = await client.incr(collectionSequenceKey(keyPrefix, collection));

  return parseInteger(raw, "Redis returned an invalid sequence value");
}

async function listCollectionDocuments(
  client: RedisClientLike,
  keyPrefix: string,
  collection: string,
): Promise<StoredDocumentRecord[]> {
  const rawKeys = await client.zRange(collectionOrderKey(keyPrefix, collection), 0, -1);
  const keys = parseStringArray(rawKeys, "query key list");
  const records: StoredDocumentRecord[] = [];

  for (const key of keys) {
    const record = await loadDocumentRecord(client, keyPrefix, collection, key);

    if (!record) {
      continue;
    }

    records.push(record);
  }

  return records;
}

async function loadDocumentRecord(
  client: RedisClientLike,
  keyPrefix: string,
  collection: string,
  key: string,
): Promise<StoredDocumentRecord | null> {
  const raw = await client.hGetAll(documentHashKey(keyPrefix, collection, key));
  const fields = parseStringRecord(raw, "document record");

  if (Object.keys(fields).length === 0) {
    return null;
  }

  return parseStoredDocumentRecord(key, fields);
}

function parseStoredDocumentRecord(
  key: string,
  fields: Record<string, string>,
): StoredDocumentRecord {
  const createdAt = parseInteger(fields.createdAt, "Redis returned an invalid document record");
  const doc = parseDocument(fields.doc);
  const indexes = parseIndexes(fields.indexes);

  return {
    key,
    createdAt,
    doc,
    indexes,
  };
}

function parseLockRecord(fields: Record<string, string>): LockRecord {
  const lockId = fields.lockId;
  const acquiredAt = parseInteger(
    fields.acquiredAt,
    "Redis returned an invalid migration lock record",
  );

  if (typeof lockId !== "string" || lockId.length === 0) {
    throw new Error("Redis returned an invalid migration lock record");
  }

  return {
    lockId,
    acquiredAt,
  };
}

function parseDocument(raw: unknown): Record<string, unknown> {
  if (typeof raw !== "string") {
    throw new Error("Redis returned an invalid document record");
  }

  let parsed: unknown;

  try {
    parsed = JSON.parse(raw);
  } catch {
    throw new Error("Redis returned an invalid document record");
  }

  if (!isRecord(parsed)) {
    throw new Error("Redis returned an invalid document record");
  }

  return parsed;
}

function parseIndexes(raw: unknown): ResolvedIndexKeys {
  if (typeof raw !== "string") {
    throw new Error("Redis returned an invalid document record");
  }

  let parsed: unknown;

  try {
    parsed = JSON.parse(raw);
  } catch {
    throw new Error("Redis returned an invalid document record");
  }

  if (!isRecord(parsed)) {
    throw new Error("Redis returned an invalid document record");
  }

  const indexes: ResolvedIndexKeys = {};

  for (const [name, value] of Object.entries(parsed)) {
    if (typeof value !== "string") {
      throw new Error("Redis returned an invalid document record");
    }

    indexes[name] = value;
  }

  return indexes;
}

function serializeDocument(doc: unknown): string {
  const cloned = structuredClone(doc);

  if (!isRecord(cloned)) {
    throw new Error("Redis received a non-object document");
  }

  const serialized = JSON.stringify(cloned);

  if (typeof serialized !== "string") {
    throw new Error("Redis failed to serialize document");
  }

  return serialized;
}

function serializeIndexes(indexes: ResolvedIndexKeys): string {
  const normalized: ResolvedIndexKeys = {};

  for (const [name, value] of Object.entries(indexes)) {
    if (typeof value !== "string") {
      throw new Error("Redis received invalid index values");
    }

    normalized[name] = value;
  }

  const serialized = JSON.stringify(normalized);

  if (typeof serialized !== "string") {
    throw new Error("Redis failed to serialize index values");
  }

  return serialized;
}

function parseStringRecord(value: unknown, context: string): Record<string, string> {
  if (!isRecord(value)) {
    throw new Error(`Redis returned an invalid ${context}`);
  }

  const record: Record<string, string> = {};

  for (const [key, raw] of Object.entries(value)) {
    if (typeof raw !== "string") {
      throw new Error(`Redis returned an invalid ${context}`);
    }

    record[key] = raw;
  }

  return record;
}

function parseStringArray(value: unknown, context: string): string[] {
  if (!Array.isArray(value)) {
    throw new Error(`Redis returned an invalid ${context}`);
  }

  const values: string[] = [];

  for (const item of value) {
    if (typeof item !== "string") {
      throw new Error(`Redis returned an invalid ${context}`);
    }

    values.push(item);
  }

  return values;
}

function parseInteger(value: unknown, message: string): number {
  if (typeof value === "number") {
    if (!Number.isFinite(value)) {
      throw new Error(message);
    }

    return value;
  }

  if (typeof value === "bigint") {
    const asNumber = Number(value);

    if (!Number.isFinite(asNumber)) {
      throw new Error(message);
    }

    return asNumber;
  }

  if (typeof value === "string") {
    const parsed = Number(value);

    if (!Number.isFinite(parsed)) {
      throw new Error(message);
    }

    return parsed;
  }

  throw new Error(message);
}

function matchDocuments(
  records: StoredDocumentRecord[],
  params: QueryParams,
): StoredDocumentRecord[] {
  const indexName = params.index;
  const results: StoredDocumentRecord[] = [];

  for (const record of records) {
    if (!indexName || !params.filter) {
      results.push(record);
      continue;
    }

    const indexValue = record.indexes[indexName];

    if (indexValue !== undefined && matchesFilter(indexValue, params.filter.value)) {
      results.push(record);
    }
  }

  if (params.sort && indexName) {
    results.sort((a, b) => {
      const aValue = a.indexes[indexName] ?? "";
      const bValue = b.indexes[indexName] ?? "";
      const base =
        params.sort === "desc" ? bValue.localeCompare(aValue) : aValue.localeCompare(bValue);

      if (base !== 0) {
        return base;
      }

      if (a.createdAt !== b.createdAt) {
        return a.createdAt - b.createdAt;
      }

      return a.key.localeCompare(b.key);
    });
  }

  return results;
}

function paginate(records: StoredDocumentRecord[], params: QueryParams): EngineQueryResult {
  let startIndex = 0;

  if (params.cursor) {
    const cursorIndex = records.findIndex((record) => record.key === params.cursor);

    if (cursorIndex !== -1) {
      startIndex = cursorIndex + 1;
    }
  }

  const normalizedLimit = normalizeLimit(params.limit);
  const limit = normalizedLimit ?? records.length;
  const hasLimit = normalizedLimit !== null;

  if (limit <= 0) {
    return {
      documents: [],
      cursor: null,
    };
  }

  const page = records.slice(startIndex, startIndex + limit);
  const cursor =
    page.length > 0 && hasLimit && startIndex + limit < records.length
      ? page[page.length - 1]!.key
      : null;

  return {
    documents: page.map((record) => ({
      key: record.key,
      doc: structuredClone(record.doc),
    })),
    cursor,
  };
}

function matchesFilter(indexValue: string, filter: string | number | FieldCondition): boolean {
  if (typeof filter === "string" || typeof filter === "number") {
    return indexValue === String(filter);
  }

  return matchesCondition(indexValue, filter);
}

function matchesCondition(value: string, condition: FieldCondition): boolean {
  if (condition.$eq !== undefined && value !== String(condition.$eq as string | number)) {
    return false;
  }

  if (condition.$gt !== undefined && !(value > String(condition.$gt as string | number))) {
    return false;
  }

  if (condition.$gte !== undefined && !(value >= String(condition.$gte as string | number))) {
    return false;
  }

  if (condition.$lt !== undefined && !(value < String(condition.$lt as string | number))) {
    return false;
  }

  if (condition.$lte !== undefined && !(value <= String(condition.$lte as string | number))) {
    return false;
  }

  if (condition.$begins !== undefined && !value.startsWith(condition.$begins)) {
    return false;
  }

  if (condition.$between !== undefined) {
    const [low, high] = condition.$between as [string | number, string | number];

    if (!(value >= String(low) && value <= String(high))) {
      return false;
    }
  }

  return true;
}

function isOutdated(
  doc: Record<string, unknown>,
  criteria: MigrationCriteria,
  parseVersion: (raw: unknown) => ComparableVersion | null,
  compareVersions: (a: ComparableVersion, b: ComparableVersion) => number,
): boolean {
  const parsedVersion = parseVersion(doc[criteria.versionField]);
  const storedIndexes = doc[criteria.indexesField];
  const versionState = classifyVersionState(parsedVersion, criteria.version, compareVersions);

  if (versionState === "stale") {
    return true;
  }

  if (versionState !== "current") {
    return false;
  }

  if (!Array.isArray(storedIndexes)) {
    return true;
  }

  if (storedIndexes.length !== criteria.indexes.length) {
    return true;
  }

  return !storedIndexes.every((name, i) => name === criteria.indexes[i]);
}

type VersionState = "current" | "stale" | "ahead" | "unknown";

function classifyVersionState(
  parsedVersion: ComparableVersion | null,
  latest: number,
  compareVersions: (a: ComparableVersion, b: ComparableVersion) => number,
): VersionState {
  if (parsedVersion === null) {
    return "unknown";
  }

  const cmp = safeCompare(parsedVersion, latest, compareVersions);

  if (cmp === null) {
    return "unknown";
  }

  if (cmp < 0) {
    return "stale";
  }

  if (cmp > 0) {
    return "ahead";
  }

  return "current";
}

function safeCompare(
  a: ComparableVersion,
  b: ComparableVersion,
  compareVersions: (a: ComparableVersion, b: ComparableVersion) => number,
): -1 | 0 | 1 | null {
  try {
    const raw = compareVersions(a, b);

    if (!Number.isFinite(raw)) {
      return null;
    }

    if (raw < 0) {
      return -1;
    }

    if (raw > 0) {
      return 1;
    }

    return 0;
  } catch {
    return null;
  }
}

function defaultParseVersion(raw: unknown): ComparableVersion | null {
  if (raw === undefined || raw === null) {
    return 1;
  }

  if (typeof raw === "number") {
    return Number.isFinite(raw) ? raw : null;
  }

  if (typeof raw === "string") {
    const trimmed = raw.trim();

    if (trimmed.length === 0) {
      return null;
    }

    const numeric = normalizeNumericVersion(trimmed);

    if (numeric !== null) {
      return numeric;
    }

    return trimmed;
  }

  return null;
}

function defaultCompareVersions(a: ComparableVersion, b: ComparableVersion): number {
  if (typeof a === "number" && typeof b === "number") {
    return a - b;
  }

  const numericA = normalizeNumericVersion(String(a));
  const numericB = normalizeNumericVersion(String(b));

  if (numericA !== null && numericB !== null) {
    return numericA - numericB;
  }

  return String(a).localeCompare(String(b), undefined, {
    numeric: true,
    sensitivity: "base",
  });
}

function normalizeNumericVersion(value: string): number | null {
  const trimmed = value.trim();
  const match = /^v?(-?\d+)$/i.exec(trimmed);

  if (!match) {
    return null;
  }

  const parsed = Number(match[1]);

  return Number.isFinite(parsed) ? parsed : null;
}

function normalizeLimit(limit: number | undefined): number | null {
  if (limit === undefined || !Number.isFinite(limit)) {
    return null;
  }

  if (limit <= 0) {
    return 0;
  }

  return Math.floor(limit);
}

function normalizePrefix(prefix: string): string {
  const normalized = prefix.trim();

  if (normalized.length === 0) {
    throw new Error("Redis keyPrefix must not be empty");
  }

  return normalized;
}

function documentHashKey(prefix: string, collection: string, key: string): string {
  return `${prefix}:doc:${collection}:${key}`;
}

function collectionOrderKey(prefix: string, collection: string): string {
  return `${prefix}:order:${collection}`;
}

function collectionSequenceKey(prefix: string, collection: string): string {
  return `${prefix}:sequence:${collection}`;
}

function migrationLockKey(prefix: string, collection: string): string {
  return `${prefix}:migration:lock:${collection}`;
}

function migrationCheckpointKey(prefix: string, collection: string): string {
  return `${prefix}:migration:checkpoint:${collection}`;
}

function randomId(): string {
  if (typeof crypto !== "undefined" && typeof crypto.randomUUID === "function") {
    return crypto.randomUUID();
  }

  const random = Math.random().toString(16).slice(2);
  const now = Date.now().toString(16);

  return `${now}-${random}`;
}

function uniqueStrings(values: string[]): string[] {
  const unique: string[] = [];
  const seen = new Set<string>();

  for (const value of values) {
    if (seen.has(value)) {
      continue;
    }

    seen.add(value);
    unique.push(value);
  }

  return unique;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}
