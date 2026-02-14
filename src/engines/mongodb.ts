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
import { DefaultMigrator } from "../migrator";

const DEFAULT_DOCUMENTS_COLLECTION = "nosql_odm_documents";
const DEFAULT_METADATA_COLLECTION = "nosql_odm_metadata";
const OUTDATED_PAGE_LIMIT = 100;

interface MongoFindOneAndUpdateOptionsLike {
  upsert?: boolean;
  returnDocument?: "before" | "after";
}

interface MongoUpdateResultLike {
  matchedCount?: unknown;
}

interface MongoCursorLike {
  sort(sort: Record<string, 1 | -1>): MongoCursorLike;
  toArray(): Promise<unknown[]>;
}

interface MongoCollectionLike {
  createIndex(keys: Record<string, 1 | -1>, options?: Record<string, unknown>): Promise<unknown>;
  findOne(filter: Record<string, unknown>): Promise<unknown>;
  insertOne(document: Record<string, unknown>): Promise<unknown>;
  updateOne(
    filter: Record<string, unknown>,
    update: Record<string, unknown>,
    options?: Record<string, unknown>,
  ): Promise<MongoUpdateResultLike>;
  deleteOne(filter: Record<string, unknown>): Promise<unknown>;
  find(filter: Record<string, unknown>): MongoCursorLike;
  findOneAndUpdate(
    filter: Record<string, unknown>,
    update: Record<string, unknown>,
    options?: MongoFindOneAndUpdateOptionsLike,
  ): Promise<unknown>;
}

interface MongoDatabaseLike {
  collection(name: string): MongoCollectionLike;
}

export interface MongoDbEngineOptions {
  database: MongoDatabaseLike;
  documentsCollection?: string;
  metadataCollection?: string;
}

export interface MongoDbQueryEngine extends QueryEngine<never> {}

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

export function mongoDbEngine(options: MongoDbEngineOptions): MongoDbQueryEngine {
  const database = options.database;
  const documentsCollection = database.collection(
    options.documentsCollection ?? DEFAULT_DOCUMENTS_COLLECTION,
  );
  const metadataCollection = database.collection(
    options.metadataCollection ?? DEFAULT_METADATA_COLLECTION,
  );

  const ready = ensureSchema(documentsCollection, metadataCollection);

  const engine: MongoDbQueryEngine = {
    async get(collection, key) {
      await ready;

      const raw = await documentsCollection.findOne({
        collection,
        key,
      });

      if (!raw) {
        return null;
      }

      const record = parseStoredDocumentRecord(raw);

      return structuredClone(record.doc);
    },

    async create(collection, key, doc, indexes) {
      await ready;

      const createdAt = await nextCreatedAt(metadataCollection, collection);
      const record = createStoredDocumentRecord(collection, key, createdAt, doc, indexes);

      try {
        await documentsCollection.insertOne(record);
      } catch (error) {
        if (isMongoDuplicateKeyError(error)) {
          throw new EngineDocumentAlreadyExistsError(collection, key);
        }

        throw error;
      }
    },

    async put(collection, key, doc, indexes) {
      await ready;

      const createdAt = await nextCreatedAt(metadataCollection, collection);
      const normalizedDoc = normalizeDocument(doc);
      const normalizedIndexes = normalizeIndexes(indexes);

      await documentsCollection.updateOne(
        {
          collection,
          key,
        },
        {
          $set: {
            doc: normalizedDoc,
            indexes: normalizedIndexes,
          },
          $setOnInsert: {
            collection,
            key,
            createdAt,
          },
        },
        {
          upsert: true,
        },
      );
    },

    async update(collection, key, doc, indexes) {
      await ready;

      const normalizedDoc = normalizeDocument(doc);
      const normalizedIndexes = normalizeIndexes(indexes);
      const result = await documentsCollection.updateOne(
        {
          collection,
          key,
        },
        {
          $set: {
            doc: normalizedDoc,
            indexes: normalizedIndexes,
          },
        },
      );

      if (parseMatchedCount(result, "MongoDB returned an invalid update result") === 0) {
        throw new EngineDocumentNotFoundError(collection, key);
      }
    },

    async delete(collection, key) {
      await ready;

      await documentsCollection.deleteOne({
        collection,
        key,
      });
    },

    async query(collection, params) {
      await ready;

      const records = await listCollectionDocuments(documentsCollection, collection);
      const matched = matchDocuments(records, params);

      return paginate(matched, params);
    },

    async batchGet(collection, keys) {
      await ready;

      const uniqueKeys = uniqueStrings(keys);

      if (uniqueKeys.length === 0) {
        return [];
      }

      const raws = await documentsCollection
        .find({
          collection,
          key: {
            $in: uniqueKeys,
          },
        })
        .toArray();

      const fetched = new Map<string, StoredDocumentRecord>();

      for (const raw of raws) {
        const record = parseStoredDocumentRecord(raw);
        fetched.set(record.key, record);
      }

      const results: KeyedDocument[] = [];

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
      await ready;

      for (const item of items) {
        const createdAt = await nextCreatedAt(metadataCollection, collection);
        const normalizedDoc = normalizeDocument(item.doc);
        const normalizedIndexes = normalizeIndexes(item.indexes);

        await documentsCollection.updateOne(
          {
            collection,
            key: item.key,
          },
          {
            $set: {
              doc: normalizedDoc,
              indexes: normalizedIndexes,
            },
            $setOnInsert: {
              collection,
              key: item.key,
              createdAt,
            },
          },
          {
            upsert: true,
          },
        );
      }
    },

    async batchDelete(collection, keys) {
      await ready;

      for (const key of keys) {
        await documentsCollection.deleteOne({
          collection,
          key,
        });
      }
    },

    migration: {
      async acquireLock(collection, options) {
        await ready;

        const lock: MigrationLock = {
          id: randomId(),
          collection,
          acquiredAt: Date.now(),
        };

        try {
          await metadataCollection.insertOne({
            collection,
            kind: "lock",
            lockId: lock.id,
            acquiredAt: lock.acquiredAt,
          });

          return lock;
        } catch (error) {
          if (!isMongoDuplicateKeyError(error)) {
            throw error;
          }
        }

        const ttl = options?.ttl;
        const allowSteal = ttl !== undefined && Number.isFinite(ttl) && ttl >= 0;

        if (!allowSteal) {
          return null;
        }

        const existingRaw = await metadataCollection.findOne({
          collection,
          kind: "lock",
        });

        if (!existingRaw) {
          return null;
        }

        const existing = parseLockRecord(existingRaw);

        if (existing.acquiredAt > lock.acquiredAt - ttl) {
          return null;
        }

        const result = await metadataCollection.updateOne(
          {
            collection,
            kind: "lock",
            lockId: existing.lockId,
            acquiredAt: existing.acquiredAt,
          },
          {
            $set: {
              lockId: lock.id,
              acquiredAt: lock.acquiredAt,
            },
          },
        );

        return parseMatchedCount(result, "MongoDB returned an invalid lock steal result") > 0
          ? lock
          : null;
      },

      async releaseLock(lock) {
        await ready;

        await metadataCollection.deleteOne({
          collection: lock.collection,
          kind: "lock",
          lockId: lock.id,
        });
      },

      async getOutdated(collection, criteria, cursor) {
        await ready;

        const records = await listCollectionDocuments(documentsCollection, collection);
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
        await ready;

        const lockRaw = await metadataCollection.findOne({
          collection: lock.collection,
          kind: "lock",
        });

        if (!lockRaw) {
          return;
        }

        const existing = parseLockRecord(lockRaw);

        if (existing.lockId !== lock.id) {
          return;
        }

        await metadataCollection.updateOne(
          {
            collection: lock.collection,
            kind: "checkpoint",
          },
          {
            $set: {
              collection: lock.collection,
              kind: "checkpoint",
              cursor,
            },
          },
          {
            upsert: true,
          },
        );
      },

      async loadCheckpoint(collection) {
        await ready;

        const raw = await metadataCollection.findOne({
          collection,
          kind: "checkpoint",
        });

        if (!raw) {
          return null;
        }

        return parseCheckpointRecord(raw);
      },

      async clearCheckpoint(collection) {
        await ready;

        await metadataCollection.deleteOne({
          collection,
          kind: "checkpoint",
        });
      },

      async getStatus(collection) {
        await ready;

        const lockRaw = await metadataCollection.findOne({
          collection,
          kind: "lock",
        });

        if (!lockRaw) {
          return null;
        }

        const lock = parseLockRecord(lockRaw);
        const checkpointRaw = await metadataCollection.findOne({
          collection,
          kind: "checkpoint",
        });

        return {
          lock: {
            id: lock.lockId,
            collection,
            acquiredAt: lock.acquiredAt,
          },
          cursor: checkpointRaw ? parseCheckpointRecord(checkpointRaw) : null,
        };
      },
    },
  };

  engine.migrator = new DefaultMigrator(engine);

  return engine;
}

async function ensureSchema(
  documentsCollection: MongoCollectionLike,
  metadataCollection: MongoCollectionLike,
): Promise<void> {
  await documentsCollection.createIndex(
    {
      collection: 1,
      key: 1,
    },
    {
      unique: true,
    },
  );

  await metadataCollection.createIndex(
    {
      collection: 1,
      kind: 1,
    },
    {
      unique: true,
    },
  );
}

async function nextCreatedAt(
  metadataCollection: MongoCollectionLike,
  collection: string,
): Promise<number> {
  const raw = await metadataCollection.findOneAndUpdate(
    {
      collection,
      kind: "sequence",
    },
    {
      $setOnInsert: {
        collection,
        kind: "sequence",
      },
      $inc: {
        value: 1,
      },
    },
    {
      upsert: true,
      returnDocument: "after",
    },
  );

  const valueDoc = parseFindOneAndUpdateResult(raw, "sequence record");
  const value = readFiniteNumber(valueDoc, "value", "sequence record");

  return value;
}

async function listCollectionDocuments(
  documentsCollection: MongoCollectionLike,
  collection: string,
): Promise<StoredDocumentRecord[]> {
  const raws = await documentsCollection
    .find({
      collection,
    })
    .sort({
      createdAt: 1,
      key: 1,
    })
    .toArray();

  return raws.map(parseStoredDocumentRecord);
}

function parseStoredDocumentRecord(raw: unknown): StoredDocumentRecord {
  const record = parseRecord(raw, "document record");
  const key = readString(record, "key", "document record");
  const createdAt = readFiniteNumber(record, "createdAt", "document record");
  const docRaw = record.doc;

  if (!isRecord(docRaw)) {
    throw new Error("MongoDB returned an invalid document record");
  }

  const indexes = parseIndexes(record.indexes);

  return {
    key,
    createdAt,
    doc: docRaw,
    indexes,
  };
}

function parseLockRecord(raw: unknown): LockRecord {
  const record = parseRecord(raw, "migration lock record");
  const lockId = readString(record, "lockId", "migration lock record");
  const acquiredAt = readFiniteNumber(record, "acquiredAt", "migration lock record");

  return {
    lockId,
    acquiredAt,
  };
}

function parseCheckpointRecord(raw: unknown): string {
  const record = parseRecord(raw, "migration checkpoint record");
  return readString(record, "cursor", "migration checkpoint record");
}

function parseFindOneAndUpdateResult(raw: unknown, context: string): Record<string, unknown> {
  const wrapped = parseFindOneAndUpdateWrappedValue(raw);

  if (wrapped.found) {
    return parseRecord(wrapped.value, context);
  }

  return parseRecord(raw, context);
}

type WrappedFindOneAndUpdateValue = { found: true; value: unknown } | { found: false };

function parseFindOneAndUpdateWrappedValue(raw: unknown): WrappedFindOneAndUpdateValue {
  if (!isRecord(raw)) {
    return { found: false };
  }

  // MongoDB Node drivers prior to v6 return a ModifyResult wrapper that
  // includes metadata fields like `ok` / `lastErrorObject` and a `value`.
  if (!("ok" in raw) && !("lastErrorObject" in raw)) {
    return { found: false };
  }

  if (!("value" in raw)) {
    return { found: false };
  }

  return {
    found: true,
    value: raw.value,
  };
}

function parseIndexes(raw: unknown): ResolvedIndexKeys {
  if (!isRecord(raw)) {
    throw new Error("MongoDB returned an invalid document record");
  }

  const indexes: ResolvedIndexKeys = {};

  for (const [name, value] of Object.entries(raw)) {
    if (typeof value !== "string") {
      throw new Error("MongoDB returned an invalid document record");
    }

    indexes[name] = value;
  }

  return indexes;
}

function normalizeDocument(doc: unknown): Record<string, unknown> {
  const cloned = structuredClone(doc);

  if (!isRecord(cloned)) {
    throw new Error("MongoDB received a non-object document");
  }

  return cloned;
}

function normalizeIndexes(indexes: ResolvedIndexKeys): ResolvedIndexKeys {
  const normalized: ResolvedIndexKeys = {};

  for (const [name, value] of Object.entries(indexes)) {
    if (typeof value !== "string") {
      throw new Error("MongoDB received invalid index values");
    }

    normalized[name] = value;
  }

  return normalized;
}

function createStoredDocumentRecord(
  collection: string,
  key: string,
  createdAt: number,
  doc: unknown,
  indexes: ResolvedIndexKeys,
): Record<string, unknown> {
  return {
    collection,
    key,
    createdAt,
    doc: normalizeDocument(doc),
    indexes: normalizeIndexes(indexes),
  };
}

function parseRecord(raw: unknown, context: string): Record<string, unknown> {
  if (!isRecord(raw)) {
    throw new Error(`MongoDB returned an invalid ${context}`);
  }

  return raw;
}

function readString(record: Record<string, unknown>, field: string, context: string): string {
  const value = record[field];

  if (typeof value !== "string") {
    throw new Error(`MongoDB returned an invalid ${context}`);
  }

  return value;
}

function readFiniteNumber(record: Record<string, unknown>, field: string, context: string): number {
  const value = record[field];

  if (typeof value === "number") {
    if (!Number.isFinite(value)) {
      throw new Error(`MongoDB returned an invalid ${context}`);
    }

    return value;
  }

  if (typeof value === "bigint") {
    const asNumber = Number(value);

    if (!Number.isFinite(asNumber)) {
      throw new Error(`MongoDB returned an invalid ${context}`);
    }

    return asNumber;
  }

  if (isRecord(value) && typeof value.toNumber === "function") {
    const asNumber = value.toNumber();

    if (typeof asNumber !== "number" || !Number.isFinite(asNumber)) {
      throw new Error(`MongoDB returned an invalid ${context}`);
    }

    return asNumber;
  }

  throw new Error(`MongoDB returned an invalid ${context}`);
}

function parseMatchedCount(result: unknown, message: string): number {
  if (!isRecord(result)) {
    throw new Error(message);
  }

  const value = result.matchedCount;

  if (typeof value !== "number" || !Number.isFinite(value)) {
    throw new Error(message);
  }

  return value;
}

function isMongoDuplicateKeyError(error: unknown): boolean {
  return isRecord(error) && error.code === 11000;
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
