import { DefaultMigrator } from "../migrator";
import { encodeQueryPageCursor, resolveQueryPageStartIndex } from "./query-cursor";
import {
  EngineDocumentAlreadyExistsError,
  EngineDocumentNotFoundError,
  type ComparableVersion,
  type EngineQueryResult,
  type FieldCondition,
  type KeyedDocument,
  type MigrationCriteria,
  type QueryEngine,
  type QueryParams,
  type ResolvedIndexKeys,
} from "./types";

// ---------------------------------------------------------------------------
// Public options / types
// ---------------------------------------------------------------------------

export interface IndexedDbEngineOptions {
  /**
   * Database name used by IndexedDB. Defaults to "nosql-odm".
   */
  databaseName?: string;
  /**
   * Optional IndexedDB factory. Pass this in non-browser environments
   * (for example, fake-indexeddb in tests).
   */
  factory?: IndexedDbFactoryLike;
}

export interface IndexedDbQueryEngine extends QueryEngine<never> {
  close(): void;
  deleteDatabase(): Promise<void>;
}

// ---------------------------------------------------------------------------
// Minimal IndexedDB-like interfaces (avoids DOM lib dependency)
// ---------------------------------------------------------------------------

type TransactionMode = "readonly" | "readwrite";

type RequestHandler = ((event: unknown) => void) | null;

interface IndexedDbRequestLike<T> {
  readonly result: T;
  readonly error: unknown;
  onsuccess: RequestHandler;
  onerror: RequestHandler;
}

interface IndexedDbOpenRequestLike<TDatabase> extends IndexedDbRequestLike<TDatabase> {
  onupgradeneeded: RequestHandler;
}

interface IndexedDbObjectStoreLike {
  get(key: string): IndexedDbRequestLike<unknown>;
  getAll(): IndexedDbRequestLike<unknown[]>;
  put(value: unknown): IndexedDbRequestLike<unknown>;
  delete(key: string): IndexedDbRequestLike<unknown>;
}

interface IndexedDbTransactionLike {
  objectStore(name: string): IndexedDbObjectStoreLike;
  readonly error: unknown;
  oncomplete: RequestHandler;
  onabort: RequestHandler;
  onerror: RequestHandler;
  abort(): void;
}

interface IndexedDbDatabaseLike {
  readonly objectStoreNames: {
    contains(name: string): boolean;
  };
  createObjectStore(name: string, options?: { keyPath?: string }): IndexedDbObjectStoreLike;
  transaction(storeNames: string | string[], mode?: TransactionMode): IndexedDbTransactionLike;
  close(): void;
}

interface IndexedDbFactoryLike {
  open(name: string, version?: number): IndexedDbOpenRequestLike<IndexedDbDatabaseLike>;
  deleteDatabase(name: string): IndexedDbRequestLike<unknown>;
}

// ---------------------------------------------------------------------------
// Storage model
// ---------------------------------------------------------------------------

const DB_VERSION = 1;

const STORE_DOCUMENTS = "documents";
const STORE_META = "meta";
const STORE_MIGRATION_LOCKS = "migration_locks";
const STORE_MIGRATION_CHECKPOINTS = "migration_checkpoints";

const META_SEQUENCE_KEY = "sequence";
const OUTDATED_PAGE_LIMIT = 100;

interface StoredDocumentRecord {
  id: string;
  collection: string;
  key: string;
  createdAt: number;
  doc: Record<string, unknown>;
  indexes: ResolvedIndexKeys;
}

interface MetaSequenceRecord {
  key: typeof META_SEQUENCE_KEY;
  value: number;
}

interface MigrationLockRecord {
  collection: string;
  lockId: string;
  acquiredAt: number;
}

interface MigrationCheckpointRecord {
  collection: string;
  cursor: string;
}

// ---------------------------------------------------------------------------
// Engine implementation
// ---------------------------------------------------------------------------

export function indexedDbEngine(options?: IndexedDbEngineOptions): IndexedDbQueryEngine {
  const databaseName = options?.databaseName ?? "nosql-odm";
  const factory = resolveFactory(options?.factory);
  const dbPromise = openDatabase(factory, databaseName);

  const engine: IndexedDbQueryEngine = {
    capabilities: {
      uniqueConstraints: "atomic",
    },

    close() {
      void dbPromise.then((db) => db.close()).catch(() => undefined);
    },

    async deleteDatabase() {
      const db = await dbPromise;
      db.close();
      await requestToPromise(factory.deleteDatabase(databaseName));
    },

    async get(collection, key) {
      const db = await dbPromise;
      const docId = makeDocumentId(collection, key);

      return withTransaction(db, [STORE_DOCUMENTS], "readonly", async (tx) => {
        const raw = await requestToPromise(tx.objectStore(STORE_DOCUMENTS).get(docId));

        if (raw === undefined) {
          return null;
        }

        const record = parseStoredDocumentRecord(raw);

        return structuredClone(record.doc);
      });
    },

    async create(collection, key, doc, indexes) {
      const db = await dbPromise;
      const docId = makeDocumentId(collection, key);

      await withTransaction(db, [STORE_DOCUMENTS, STORE_META], "readwrite", async (tx) => {
        const docsStore = tx.objectStore(STORE_DOCUMENTS);
        const metaStore = tx.objectStore(STORE_META);

        const existing = await requestToPromise(docsStore.get(docId));

        if (existing !== undefined) {
          throw new EngineDocumentAlreadyExistsError(collection, key);
        }

        const sequence = (await loadSequence(metaStore)) + 1;

        await requestToPromise(
          docsStore.put(
            createStoredDocumentRecord({
              id: docId,
              collection,
              key,
              createdAt: sequence,
              doc,
              indexes,
            }),
          ),
        );

        await saveSequence(metaStore, sequence);
      });
    },

    async put(collection, key, doc, indexes) {
      const db = await dbPromise;
      const docId = makeDocumentId(collection, key);

      await withTransaction(db, [STORE_DOCUMENTS, STORE_META], "readwrite", async (tx) => {
        const docsStore = tx.objectStore(STORE_DOCUMENTS);
        const metaStore = tx.objectStore(STORE_META);

        const existingRaw = await requestToPromise(docsStore.get(docId));

        let createdAt: number;

        if (existingRaw === undefined) {
          createdAt = (await loadSequence(metaStore)) + 1;
          await saveSequence(metaStore, createdAt);
        } else {
          createdAt = parseStoredDocumentRecord(existingRaw).createdAt;
        }

        await requestToPromise(
          docsStore.put(
            createStoredDocumentRecord({
              id: docId,
              collection,
              key,
              createdAt,
              doc,
              indexes,
            }),
          ),
        );
      });
    },

    async update(collection, key, doc, indexes) {
      const db = await dbPromise;
      const docId = makeDocumentId(collection, key);

      await withTransaction(db, [STORE_DOCUMENTS], "readwrite", async (tx) => {
        const docsStore = tx.objectStore(STORE_DOCUMENTS);
        const existingRaw = await requestToPromise(docsStore.get(docId));

        if (existingRaw === undefined) {
          throw new EngineDocumentNotFoundError(collection, key);
        }

        const existing = parseStoredDocumentRecord(existingRaw);

        await requestToPromise(
          docsStore.put(
            createStoredDocumentRecord({
              id: docId,
              collection,
              key,
              createdAt: existing.createdAt,
              doc,
              indexes,
            }),
          ),
        );
      });
    },

    async delete(collection, key) {
      const db = await dbPromise;
      const docId = makeDocumentId(collection, key);

      await withTransaction(db, [STORE_DOCUMENTS], "readwrite", async (tx) => {
        await requestToPromise(tx.objectStore(STORE_DOCUMENTS).delete(docId));
      });
    },

    async query(collection, params) {
      const db = await dbPromise;
      const records = await listCollectionDocuments(db, collection);
      const matched = matchDocuments(records, params);

      return paginateQuery(collection, matched, params);
    },

    async batchGet(collection, keys) {
      const db = await dbPromise;

      return withTransaction(db, [STORE_DOCUMENTS], "readonly", async (tx) => {
        const docsStore = tx.objectStore(STORE_DOCUMENTS);
        const results: KeyedDocument[] = [];

        for (const key of keys) {
          const raw = await requestToPromise(docsStore.get(makeDocumentId(collection, key)));

          if (raw === undefined) {
            continue;
          }

          const record = parseStoredDocumentRecord(raw);
          results.push({ key, doc: structuredClone(record.doc) });
        }

        return results;
      });
    },

    async batchSet(collection, items) {
      const db = await dbPromise;

      await withTransaction(db, [STORE_DOCUMENTS, STORE_META], "readwrite", async (tx) => {
        const docsStore = tx.objectStore(STORE_DOCUMENTS);
        const metaStore = tx.objectStore(STORE_META);

        let sequence = await loadSequence(metaStore);
        let sequenceChanged = false;

        for (const item of items) {
          const docId = makeDocumentId(collection, item.key);
          const existingRaw = await requestToPromise(docsStore.get(docId));

          let createdAt: number;

          if (existingRaw === undefined) {
            sequence += 1;
            sequenceChanged = true;
            createdAt = sequence;
          } else {
            createdAt = parseStoredDocumentRecord(existingRaw).createdAt;
          }

          await requestToPromise(
            docsStore.put(
              createStoredDocumentRecord({
                id: docId,
                collection,
                key: item.key,
                createdAt,
                doc: item.doc,
                indexes: item.indexes,
              }),
            ),
          );
        }

        if (sequenceChanged) {
          await saveSequence(metaStore, sequence);
        }
      });
    },

    async batchDelete(collection, keys) {
      const db = await dbPromise;

      await withTransaction(db, [STORE_DOCUMENTS], "readwrite", async (tx) => {
        const docsStore = tx.objectStore(STORE_DOCUMENTS);

        for (const key of keys) {
          await requestToPromise(docsStore.delete(makeDocumentId(collection, key)));
        }
      });
    },

    migration: {
      async acquireLock(collection, options) {
        const db = await dbPromise;

        return withTransaction(db, [STORE_MIGRATION_LOCKS], "readwrite", async (tx) => {
          const locksStore = tx.objectStore(STORE_MIGRATION_LOCKS);
          const existingRaw = await requestToPromise(locksStore.get(collection));
          const now = Date.now();

          if (existingRaw !== undefined) {
            const existing = parseMigrationLockRecord(existingRaw);
            const ttl = options?.ttl;
            const canSteal =
              ttl !== undefined &&
              Number.isFinite(ttl) &&
              ttl >= 0 &&
              now - existing.acquiredAt >= ttl;

            if (!canSteal) {
              return null;
            }
          }

          const lockRecord: MigrationLockRecord = {
            collection,
            lockId: randomId(),
            acquiredAt: now,
          };

          await requestToPromise(locksStore.put(lockRecord));

          return {
            id: lockRecord.lockId,
            collection,
            acquiredAt: lockRecord.acquiredAt,
          };
        });
      },

      async releaseLock(lock) {
        const db = await dbPromise;

        await withTransaction(db, [STORE_MIGRATION_LOCKS], "readwrite", async (tx) => {
          const locksStore = tx.objectStore(STORE_MIGRATION_LOCKS);
          const existingRaw = await requestToPromise(locksStore.get(lock.collection));

          if (existingRaw === undefined) {
            return;
          }

          const existing = parseMigrationLockRecord(existingRaw);

          if (existing.lockId === lock.id) {
            await requestToPromise(locksStore.delete(lock.collection));
          }
        });
      },

      async getOutdated(collection, criteria, cursor) {
        const db = await dbPromise;
        const records = await listCollectionDocuments(db, collection);
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
          limit: normalizeOutdatedPageLimit(criteria.pageSizeHint),
        });
      },

      async saveCheckpoint(lock, cursor) {
        const db = await dbPromise;

        await withTransaction(
          db,
          [STORE_MIGRATION_LOCKS, STORE_MIGRATION_CHECKPOINTS],
          "readwrite",
          async (tx) => {
            const locksStore = tx.objectStore(STORE_MIGRATION_LOCKS);
            const checkpointsStore = tx.objectStore(STORE_MIGRATION_CHECKPOINTS);
            const existingRaw = await requestToPromise(locksStore.get(lock.collection));

            if (existingRaw === undefined) {
              return;
            }

            const existing = parseMigrationLockRecord(existingRaw);

            if (existing.lockId !== lock.id) {
              return;
            }

            const checkpoint: MigrationCheckpointRecord = {
              collection: lock.collection,
              cursor,
            };

            await requestToPromise(checkpointsStore.put(checkpoint));
          },
        );
      },

      async loadCheckpoint(collection) {
        const db = await dbPromise;

        return withTransaction(db, [STORE_MIGRATION_CHECKPOINTS], "readonly", async (tx) => {
          const raw = await requestToPromise(
            tx.objectStore(STORE_MIGRATION_CHECKPOINTS).get(collection),
          );

          if (raw === undefined) {
            return null;
          }

          return parseMigrationCheckpointRecord(raw).cursor;
        });
      },

      async clearCheckpoint(collection) {
        const db = await dbPromise;

        await withTransaction(db, [STORE_MIGRATION_CHECKPOINTS], "readwrite", async (tx) => {
          await requestToPromise(tx.objectStore(STORE_MIGRATION_CHECKPOINTS).delete(collection));
        });
      },

      async getStatus(collection) {
        const db = await dbPromise;

        return withTransaction(
          db,
          [STORE_MIGRATION_LOCKS, STORE_MIGRATION_CHECKPOINTS],
          "readonly",
          async (tx) => {
            const locksStore = tx.objectStore(STORE_MIGRATION_LOCKS);
            const checkpointsStore = tx.objectStore(STORE_MIGRATION_CHECKPOINTS);

            const lockRaw = await requestToPromise(locksStore.get(collection));

            if (lockRaw === undefined) {
              return null;
            }

            const lockRecord = parseMigrationLockRecord(lockRaw);
            const checkpointRaw = await requestToPromise(checkpointsStore.get(collection));

            return {
              lock: {
                id: lockRecord.lockId,
                collection,
                acquiredAt: lockRecord.acquiredAt,
              },
              cursor:
                checkpointRaw === undefined
                  ? null
                  : parseMigrationCheckpointRecord(checkpointRaw).cursor,
            };
          },
        );
      },
    },
  };

  engine.migrator = new DefaultMigrator(engine);

  return engine;
}

// ---------------------------------------------------------------------------
// Data access helpers
// ---------------------------------------------------------------------------

async function listCollectionDocuments(
  db: IndexedDbDatabaseLike,
  collection: string,
): Promise<StoredDocumentRecord[]> {
  return withTransaction(db, [STORE_DOCUMENTS], "readonly", async (tx) => {
    const rawRecords = await requestToPromise(tx.objectStore(STORE_DOCUMENTS).getAll());

    const records = rawRecords
      .map((record) => parseStoredDocumentRecord(record))
      .filter((record) => record.collection === collection);

    records.sort((a, b) => {
      if (a.createdAt !== b.createdAt) {
        return a.createdAt - b.createdAt;
      }

      return a.key.localeCompare(b.key);
    });

    return records;
  });
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

function paginateQuery(
  collection: string,
  records: StoredDocumentRecord[],
  params: QueryParams,
): EngineQueryResult {
  const startIndex = resolveQueryPageStartIndex(
    records,
    collection,
    params,
    (record, queryParams) => ({
      key: record.key,
      createdAt: record.createdAt,
      indexValue: queryParams.index ? (record.indexes[queryParams.index] ?? "") : undefined,
    }),
  );
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
      ? encodeQueryPageCursor(collection, params, {
          key: page[page.length - 1]!.key,
          createdAt: page[page.length - 1]!.createdAt,
          indexValue: params.index
            ? (page[page.length - 1]!.indexes[params.index] ?? "")
            : undefined,
        })
      : null;

  return {
    documents: page.map((record) => ({
      key: record.key,
      doc: structuredClone(record.doc),
    })),
    cursor,
  };
}

// ---------------------------------------------------------------------------
// Migration helpers
// ---------------------------------------------------------------------------

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

function normalizeOutdatedPageLimit(value: number | undefined): number {
  if (value === undefined || !Number.isFinite(value)) {
    return OUTDATED_PAGE_LIMIT;
  }

  return Math.max(1, Math.floor(value));
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

// ---------------------------------------------------------------------------
// Query filter helpers
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// IndexedDB primitives
// ---------------------------------------------------------------------------

async function openDatabase(
  factory: IndexedDbFactoryLike,
  databaseName: string,
): Promise<IndexedDbDatabaseLike> {
  return new Promise((resolve, reject) => {
    const request = factory.open(databaseName, DB_VERSION);

    request.onupgradeneeded = () => {
      const db = request.result;

      if (!db.objectStoreNames.contains(STORE_DOCUMENTS)) {
        db.createObjectStore(STORE_DOCUMENTS, { keyPath: "id" });
      }

      if (!db.objectStoreNames.contains(STORE_META)) {
        db.createObjectStore(STORE_META, { keyPath: "key" });
      }

      if (!db.objectStoreNames.contains(STORE_MIGRATION_LOCKS)) {
        db.createObjectStore(STORE_MIGRATION_LOCKS, { keyPath: "collection" });
      }

      if (!db.objectStoreNames.contains(STORE_MIGRATION_CHECKPOINTS)) {
        db.createObjectStore(STORE_MIGRATION_CHECKPOINTS, { keyPath: "collection" });
      }
    };

    request.onsuccess = () => {
      resolve(request.result);
    };

    request.onerror = () => {
      reject(toError(request.error, `Failed to open IndexedDB database "${databaseName}"`));
    };
  });
}

async function withTransaction<T>(
  db: IndexedDbDatabaseLike,
  storeNames: string[],
  mode: TransactionMode,
  run: (tx: IndexedDbTransactionLike) => Promise<T>,
): Promise<T> {
  const tx = db.transaction(storeNames, mode);
  const done = waitForTransaction(tx);

  try {
    const result = await run(tx);
    await done;
    return result;
  } catch (error) {
    try {
      tx.abort();
    } catch {
      // ignore abort failures
    }

    await done.catch(() => undefined);
    throw error;
  }
}

function waitForTransaction(tx: IndexedDbTransactionLike): Promise<void> {
  return new Promise((resolve, reject) => {
    tx.oncomplete = () => resolve();
    tx.onabort = () => reject(toError(tx.error, "IndexedDB transaction aborted"));
    tx.onerror = () => reject(toError(tx.error, "IndexedDB transaction failed"));
  });
}

function requestToPromise<T>(request: IndexedDbRequestLike<T>): Promise<T> {
  return new Promise((resolve, reject) => {
    request.onsuccess = () => resolve(request.result);
    request.onerror = () => reject(toError(request.error, "IndexedDB request failed"));
  });
}

function resolveFactory(factory?: IndexedDbFactoryLike): IndexedDbFactoryLike {
  if (factory) {
    return factory;
  }

  const globalFactory = (globalThis as { indexedDB?: IndexedDbFactoryLike }).indexedDB;

  if (!globalFactory) {
    throw new Error(
      "No IndexedDB factory found. Pass { factory } (for example, fake-indexeddb in tests) or run in an environment with global indexedDB.",
    );
  }

  return globalFactory;
}

// ---------------------------------------------------------------------------
// Record serialization / validation
// ---------------------------------------------------------------------------

function createStoredDocumentRecord(input: {
  id: string;
  collection: string;
  key: string;
  createdAt: number;
  doc: unknown;
  indexes: ResolvedIndexKeys;
}): StoredDocumentRecord {
  return {
    id: input.id,
    collection: input.collection,
    key: input.key,
    createdAt: input.createdAt,
    doc: structuredClone(input.doc) as Record<string, unknown>,
    indexes: { ...input.indexes },
  };
}

function parseStoredDocumentRecord(value: unknown): StoredDocumentRecord {
  if (!isRecord(value)) {
    throw new Error("IndexedDB documents store contains an invalid record (expected object)");
  }

  const id = value.id;
  const collection = value.collection;
  const key = value.key;
  const createdAt = value.createdAt;
  const doc = value.doc;
  const indexes = value.indexes;

  if (typeof id !== "string" || typeof collection !== "string" || typeof key !== "string") {
    throw new Error("IndexedDB documents store contains an invalid record (bad id/collection/key)");
  }

  if (typeof createdAt !== "number" || !Number.isFinite(createdAt)) {
    throw new Error("IndexedDB documents store contains an invalid record (bad createdAt)");
  }

  if (!isRecord(doc)) {
    throw new Error("IndexedDB documents store contains an invalid record (bad document)");
  }

  if (!isRecord(indexes)) {
    throw new Error("IndexedDB documents store contains an invalid record (bad indexes)");
  }

  const resolvedIndexes: ResolvedIndexKeys = {};

  for (const [name, indexValue] of Object.entries(indexes)) {
    if (typeof indexValue !== "string") {
      throw new Error("IndexedDB documents store contains an invalid record (non-string index)");
    }

    resolvedIndexes[name] = indexValue;
  }

  return {
    id,
    collection,
    key,
    createdAt,
    doc: doc as Record<string, unknown>,
    indexes: resolvedIndexes,
  };
}

function parseMigrationLockRecord(value: unknown): MigrationLockRecord {
  if (!isRecord(value)) {
    throw new Error("IndexedDB migration_locks store contains an invalid record");
  }

  const collection = value.collection;
  const lockId = value.lockId;
  const acquiredAt = value.acquiredAt;

  if (
    typeof collection !== "string" ||
    typeof lockId !== "string" ||
    typeof acquiredAt !== "number" ||
    !Number.isFinite(acquiredAt)
  ) {
    throw new Error("IndexedDB migration_locks store contains an invalid record");
  }

  return { collection, lockId, acquiredAt };
}

function parseMigrationCheckpointRecord(value: unknown): MigrationCheckpointRecord {
  if (!isRecord(value)) {
    throw new Error("IndexedDB migration_checkpoints store contains an invalid record");
  }

  const collection = value.collection;
  const cursor = value.cursor;

  if (typeof collection !== "string" || typeof cursor !== "string") {
    throw new Error("IndexedDB migration_checkpoints store contains an invalid record");
  }

  return { collection, cursor };
}

async function loadSequence(metaStore: IndexedDbObjectStoreLike): Promise<number> {
  const raw = await requestToPromise(metaStore.get(META_SEQUENCE_KEY));

  if (raw === undefined) {
    return 0;
  }

  if (!isRecord(raw)) {
    throw new Error("IndexedDB meta store contains an invalid sequence record");
  }

  const key = raw.key;
  const value = raw.value;

  if (key !== META_SEQUENCE_KEY || typeof value !== "number" || !Number.isFinite(value)) {
    throw new Error("IndexedDB meta store contains an invalid sequence record");
  }

  return value;
}

async function saveSequence(metaStore: IndexedDbObjectStoreLike, value: number): Promise<void> {
  const record: MetaSequenceRecord = { key: META_SEQUENCE_KEY, value };
  await requestToPromise(metaStore.put(record));
}

// ---------------------------------------------------------------------------
// General helpers
// ---------------------------------------------------------------------------

function normalizeLimit(limit: number | undefined): number | null {
  if (limit === undefined || !Number.isFinite(limit)) {
    return null;
  }

  if (limit <= 0) {
    return 0;
  }

  return Math.floor(limit);
}

function makeDocumentId(collection: string, key: string): string {
  return `${collection}\u0000${key}`;
}

function randomId(): string {
  if (typeof crypto !== "undefined" && typeof crypto.randomUUID === "function") {
    return crypto.randomUUID();
  }

  const random = Math.random().toString(16).slice(2);
  const now = Date.now().toString(16);

  return `${now}-${random}`;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

function toError(cause: unknown, fallbackMessage: string): Error {
  if (cause instanceof Error) {
    return cause;
  }

  if (typeof cause === "string" && cause.length > 0) {
    return new Error(cause);
  }

  return new Error(fallbackMessage);
}
