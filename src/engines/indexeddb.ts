import { DefaultMigrator } from "../migrator";
import { getPreparedClone, prepareDocumentForStorage } from "./document-preparation";
import { encodeQueryPageCursor, resolveQueryPageStartIndex } from "./query-cursor";
import {
  EngineDocumentAlreadyExistsError,
  EngineDocumentNotFoundError,
  EngineUniqueConstraintError,
  type ComparableVersion,
  type EngineQueryResult,
  type EngineQueryDiagnostics,
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
  readonly indexNames?: {
    contains(name: string): boolean;
  };
  get(key: string): IndexedDbRequestLike<unknown>;
  getAll(query?: unknown): IndexedDbRequestLike<unknown[]>;
  put(value: unknown): IndexedDbRequestLike<unknown>;
  delete(key: string): IndexedDbRequestLike<unknown>;
  index(name: string): IndexedDbIndexLike;
  createIndex(name: string, keyPath: string | string[]): unknown;
  deleteIndex?(name: string): void;
}

interface IndexedDbIndexLike {
  getAll(query?: unknown): IndexedDbRequestLike<unknown[]>;
  openCursor(query?: unknown): IndexedDbRequestLike<IndexedDbCursorLike | null>;
}

interface IndexedDbCursorLike {
  readonly value: unknown;
  continue(): void;
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

const DB_VERSION = 3;

const STORE_DOCUMENTS = "documents";
const STORE_META = "meta";
const STORE_MIGRATION_LOCKS = "migration_locks";
const STORE_MIGRATION_CHECKPOINTS = "migration_checkpoints";
const STORE_QUERY_INDEX_ENTRIES = "query_index_entries";

const QUERY_INDEX_LOOKUP = "lookup";

const META_SEQUENCE_KEY = "sequence";
const META_QUERY_INDEX_BACKFILL_KEY = "queryIndexEntriesBackfilledV2";
const OUTDATED_PAGE_LIMIT = 100;

interface StoredDocumentRecord {
  id: string;
  collection: string;
  key: string;
  createdAt: number;
  writeVersion: number;
  doc: Record<string, unknown>;
  indexes: ResolvedIndexKeys;
  uniqueIndexes: ResolvedIndexKeys;
}

interface QueryIndexEntryRecord {
  id: string;
  collection: string;
  indexName: string;
  indexValue: string;
  createdAt: number;
  key: string;
}

interface IndexedDbKeyRangeFactoryLike {
  bound(lower: unknown, upper: unknown, lowerOpen?: boolean, upperOpen?: boolean): object;
  only(value: unknown): object;
}

interface IndexedDbIndexQueryResult {
  records: StoredDocumentRecord[];
  diagnostics: EngineQueryDiagnostics;
}

interface IndexEntryRange {
  query?: object;
}

interface MetaSequenceRecord {
  key: typeof META_SEQUENCE_KEY | typeof META_QUERY_INDEX_BACKFILL_KEY;
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

interface LoadedDocumentRecord {
  readonly key: string;
  readonly record: StoredDocumentRecord | null;
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

    prepareDocumentForWrite(doc, collection, key) {
      return prepareDocumentForStorage(doc, collection, key, "clone");
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

    async getWithMetadata(collection, key) {
      const db = await dbPromise;
      const docId = makeDocumentId(collection, key);

      return withTransaction(db, [STORE_DOCUMENTS], "readonly", async (tx) => {
        const raw = await requestToPromise(tx.objectStore(STORE_DOCUMENTS).get(docId));

        if (raw === undefined) {
          return null;
        }

        const record = parseStoredDocumentRecord(raw);

        return {
          doc: structuredClone(record.doc),
          writeToken: String(record.writeVersion),
        };
      });
    },

    async create(collection, key, doc, indexes, _options, _migrationMetadata, uniqueIndexes) {
      const db = await dbPromise;
      const docId = makeDocumentId(collection, key);

      await withTransaction(
        db,
        [STORE_DOCUMENTS, STORE_META, STORE_QUERY_INDEX_ENTRIES],
        "readwrite",
        async (tx) => {
          const docsStore = tx.objectStore(STORE_DOCUMENTS);
          const metaStore = tx.objectStore(STORE_META);
          const indexStore = tx.objectStore(STORE_QUERY_INDEX_ENTRIES);
          const collectionRecords = await loadCollectionRecordsFromStore(docsStore, collection);

          const existing = await requestToPromise(docsStore.get(docId));

          if (existing !== undefined) {
            throw new EngineDocumentAlreadyExistsError(collection, key);
          }

          assertUniqueIndexes(collection, key, uniqueIndexes ?? {}, collectionRecords);

          const sequence = (await loadSequence(metaStore)) + 1;

          const record = createStoredDocumentRecord({
            id: docId,
            collection,
            key,
            createdAt: sequence,
            writeVersion: 1,
            doc,
            indexes,
            uniqueIndexes,
          });

          await requestToPromise(docsStore.put(record));
          await replaceQueryIndexEntries(indexStore, collection, key, sequence, {}, indexes);

          await saveSequence(metaStore, sequence);
        },
      );
    },

    async put(collection, key, doc, indexes, _options, _migrationMetadata, uniqueIndexes) {
      const db = await dbPromise;
      const docId = makeDocumentId(collection, key);

      await withTransaction(
        db,
        [STORE_DOCUMENTS, STORE_META, STORE_QUERY_INDEX_ENTRIES],
        "readwrite",
        async (tx) => {
          const docsStore = tx.objectStore(STORE_DOCUMENTS);
          const metaStore = tx.objectStore(STORE_META);
          const indexStore = tx.objectStore(STORE_QUERY_INDEX_ENTRIES);
          const collectionRecords = await loadCollectionRecordsFromStore(docsStore, collection);

          const existingRaw = await requestToPromise(docsStore.get(docId));

          let createdAt: number;
          let writeVersion: number;
          let existingIndexes: ResolvedIndexKeys = {};

          if (existingRaw === undefined) {
            createdAt = (await loadSequence(metaStore)) + 1;
            writeVersion = 1;
            await saveSequence(metaStore, createdAt);
          } else {
            const existing = parseStoredDocumentRecord(existingRaw);
            createdAt = existing.createdAt;
            writeVersion = existing.writeVersion + 1;
            existingIndexes = existing.indexes;
          }

          assertUniqueIndexes(collection, key, uniqueIndexes ?? {}, collectionRecords);

          const record = createStoredDocumentRecord({
            id: docId,
            collection,
            key,
            createdAt,
            writeVersion,
            doc,
            indexes,
            uniqueIndexes,
          });

          await requestToPromise(docsStore.put(record));
          await replaceQueryIndexEntries(
            indexStore,
            collection,
            key,
            createdAt,
            existingIndexes,
            indexes,
          );
        },
      );
    },

    async update(collection, key, doc, indexes, _options, _migrationMetadata, uniqueIndexes) {
      const db = await dbPromise;
      const docId = makeDocumentId(collection, key);

      await withTransaction(
        db,
        [STORE_DOCUMENTS, STORE_QUERY_INDEX_ENTRIES],
        "readwrite",
        async (tx) => {
          const docsStore = tx.objectStore(STORE_DOCUMENTS);
          const indexStore = tx.objectStore(STORE_QUERY_INDEX_ENTRIES);
          const collectionRecords = await loadCollectionRecordsFromStore(docsStore, collection);
          const existingRaw = await requestToPromise(docsStore.get(docId));

          if (existingRaw === undefined) {
            throw new EngineDocumentNotFoundError(collection, key);
          }

          const existing = parseStoredDocumentRecord(existingRaw);

          assertUniqueIndexes(collection, key, uniqueIndexes ?? {}, collectionRecords);

          const record = createStoredDocumentRecord({
            id: docId,
            collection,
            key,
            createdAt: existing.createdAt,
            writeVersion: existing.writeVersion + 1,
            doc,
            indexes,
            uniqueIndexes,
          });

          await requestToPromise(docsStore.put(record));
          await replaceQueryIndexEntries(
            indexStore,
            collection,
            key,
            existing.createdAt,
            existing.indexes,
            indexes,
          );
        },
      );
    },

    async delete(collection, key) {
      const db = await dbPromise;
      const docId = makeDocumentId(collection, key);

      await withTransaction(
        db,
        [STORE_DOCUMENTS, STORE_QUERY_INDEX_ENTRIES],
        "readwrite",
        async (tx) => {
          const docsStore = tx.objectStore(STORE_DOCUMENTS);
          const existingRaw = await requestToPromise(docsStore.get(docId));

          if (existingRaw !== undefined) {
            const existing = parseStoredDocumentRecord(existingRaw);
            await replaceQueryIndexEntries(
              tx.objectStore(STORE_QUERY_INDEX_ENTRIES),
              collection,
              key,
              existing.createdAt,
              existing.indexes,
              {},
            );
          }

          await requestToPromise(docsStore.delete(docId));
        },
      );
    },

    async query(collection, params) {
      const db = await dbPromise;
      const indexed = await listCollectionDocumentsByIndex(db, collection, params);
      const records = indexed?.records ?? (await listCollectionDocuments(db, collection));
      const matched = matchDocuments(records, params);

      return withDiagnostics(
        paginateQuery(collection, matched, params),
        indexed?.diagnostics ?? {
          mode: "fallback_scan",
          reason: "fallback_scan",
          ...(params.index ? { index: params.index } : {}),
        },
      );
    },

    async queryWithMetadata(collection, params) {
      const db = await dbPromise;
      const indexed = await listCollectionDocumentsByIndex(db, collection, params);
      const records = indexed?.records ?? (await listCollectionDocuments(db, collection));
      const matched = matchDocuments(records, params);

      return withDiagnostics(
        paginateQuery(collection, matched, params, true),
        indexed?.diagnostics ?? {
          mode: "fallback_scan",
          reason: "fallback_scan",
          ...(params.index ? { index: params.index } : {}),
        },
      );
    },

    async batchGet(collection, keys) {
      const db = await dbPromise;

      return withTransaction(db, [STORE_DOCUMENTS], "readonly", async (tx) => {
        const docsStore = tx.objectStore(STORE_DOCUMENTS);
        const results: KeyedDocument[] = [];
        const records = await loadDocumentsByKeysFromStore(docsStore, collection, keys);

        for (const entry of records) {
          if (entry.record === null) {
            continue;
          }

          results.push({ key: entry.key, doc: structuredClone(entry.record.doc) });
        }

        return results;
      });
    },

    async batchGetWithMetadata(collection, keys) {
      const db = await dbPromise;

      return withTransaction(db, [STORE_DOCUMENTS], "readonly", async (tx) => {
        const docsStore = tx.objectStore(STORE_DOCUMENTS);
        const results: KeyedDocument[] = [];
        const records = await loadDocumentsByKeysFromStore(docsStore, collection, keys);

        for (const entry of records) {
          if (entry.record === null) {
            continue;
          }

          results.push({
            key: entry.key,
            doc: structuredClone(entry.record.doc),
            writeToken: String(entry.record.writeVersion),
          });
        }

        return results;
      });
    },

    async batchSet(collection, items) {
      const db = await dbPromise;

      await withTransaction(
        db,
        [STORE_DOCUMENTS, STORE_META, STORE_QUERY_INDEX_ENTRIES],
        "readwrite",
        async (tx) => {
          const docsStore = tx.objectStore(STORE_DOCUMENTS);
          const metaStore = tx.objectStore(STORE_META);
          const indexStore = tx.objectStore(STORE_QUERY_INDEX_ENTRIES);
          const collectionRecords = await loadCollectionRecordsFromStore(docsStore, collection);
          const recordsByKey = new Map(
            collectionRecords.map((record) => [record.key, record] as const),
          );

          let sequence = await loadSequence(metaStore);
          let sequenceChanged = false;

          for (const item of items) {
            const docId = makeDocumentId(collection, item.key);
            const existingRaw = await requestToPromise(docsStore.get(docId));

            let createdAt: number;
            let writeVersion: number;
            let existingIndexes: ResolvedIndexKeys = {};

            if (existingRaw === undefined) {
              sequence += 1;
              sequenceChanged = true;
              createdAt = sequence;
              writeVersion = 1;
            } else {
              const existing = parseStoredDocumentRecord(existingRaw);
              createdAt = existing.createdAt;
              writeVersion = existing.writeVersion + 1;
              existingIndexes = existing.indexes;
            }

            const uniqueIndexes = item.uniqueIndexes ?? {};

            assertUniqueIndexes(collection, item.key, uniqueIndexes, [...recordsByKey.values()]);

            const storedRecord = createStoredDocumentRecord({
              id: docId,
              collection,
              key: item.key,
              createdAt,
              writeVersion,
              doc: item.doc,
              indexes: item.indexes,
              uniqueIndexes,
            });

            await requestToPromise(docsStore.put(storedRecord));
            await replaceQueryIndexEntries(
              indexStore,
              collection,
              item.key,
              createdAt,
              existingIndexes,
              item.indexes,
            );
            recordsByKey.set(item.key, storedRecord);
          }

          if (sequenceChanged) {
            await saveSequence(metaStore, sequence);
          }
        },
      );
    },

    async batchSetWithResult(collection, items) {
      const db = await dbPromise;

      return withTransaction(
        db,
        [STORE_DOCUMENTS, STORE_META, STORE_QUERY_INDEX_ENTRIES],
        "readwrite",
        async (tx) => {
          const docsStore = tx.objectStore(STORE_DOCUMENTS);
          const metaStore = tx.objectStore(STORE_META);
          const indexStore = tx.objectStore(STORE_QUERY_INDEX_ENTRIES);
          const collectionRecords = await loadCollectionRecordsFromStore(docsStore, collection);
          const recordsByKey = new Map(
            collectionRecords.map((record) => [record.key, record] as const),
          );
          const persistedKeys: string[] = [];
          const conflictedKeys: string[] = [];

          let sequence = await loadSequence(metaStore);
          let sequenceChanged = false;

          for (const item of items) {
            const existing = recordsByKey.get(item.key);

            if (
              item.expectedWriteToken !== undefined &&
              (existing === undefined || String(existing.writeVersion) !== item.expectedWriteToken)
            ) {
              conflictedKeys.push(item.key);
              continue;
            }

            const docId = makeDocumentId(collection, item.key);
            const createdAt = existing?.createdAt ?? sequence + 1;

            if (!existing) {
              sequence = createdAt;
              sequenceChanged = true;
            }

            const uniqueIndexes = item.uniqueIndexes ?? {};

            assertUniqueIndexes(collection, item.key, uniqueIndexes, [...recordsByKey.values()]);

            const storedRecord = createStoredDocumentRecord({
              id: docId,
              collection,
              key: item.key,
              createdAt,
              writeVersion: (existing?.writeVersion ?? 0) + 1,
              doc: item.doc,
              indexes: item.indexes,
              uniqueIndexes,
            });

            await requestToPromise(docsStore.put(storedRecord));
            await replaceQueryIndexEntries(
              indexStore,
              collection,
              item.key,
              createdAt,
              existing?.indexes ?? {},
              item.indexes,
            );
            recordsByKey.set(item.key, storedRecord);
            persistedKeys.push(item.key);
          }

          if (sequenceChanged) {
            await saveSequence(metaStore, sequence);
          }

          return { persistedKeys, conflictedKeys };
        },
      );
    },

    async batchDelete(collection, keys) {
      const db = await dbPromise;

      await withTransaction(
        db,
        [STORE_DOCUMENTS, STORE_QUERY_INDEX_ENTRIES],
        "readwrite",
        async (tx) => {
          const docsStore = tx.objectStore(STORE_DOCUMENTS);
          const indexStore = tx.objectStore(STORE_QUERY_INDEX_ENTRIES);

          for (const key of keys) {
            const existingRaw = await requestToPromise(
              docsStore.get(makeDocumentId(collection, key)),
            );

            if (existingRaw !== undefined) {
              const existing = parseStoredDocumentRecord(existingRaw);
              await replaceQueryIndexEntries(
                indexStore,
                collection,
                key,
                existing.createdAt,
                existing.indexes,
                {},
              );
            }

            await requestToPromise(docsStore.delete(makeDocumentId(collection, key)));
          }
        },
      );
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
    return loadCollectionRecordsFromStore(tx.objectStore(STORE_DOCUMENTS), collection);
  });
}

async function loadCollectionRecordsFromStore(
  docsStore: IndexedDbObjectStoreLike,
  collection: string,
): Promise<StoredDocumentRecord[]> {
  const rawRecords = await requestToPromise(docsStore.getAll());
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
}

async function loadDocumentsByKeysFromStore(
  docsStore: IndexedDbObjectStoreLike,
  collection: string,
  keys: readonly string[],
): Promise<LoadedDocumentRecord[]> {
  const requests = keys.map((key) =>
    requestToPromise(docsStore.get(makeDocumentId(collection, key))).then((raw) => ({
      key,
      raw,
    })),
  );
  const rawEntries = await Promise.all(requests);

  return rawEntries.map((entry) => ({
    key: entry.key,
    record: entry.raw === undefined ? null : parseStoredDocumentRecord(entry.raw),
  }));
}

async function listCollectionDocumentsByIndex(
  db: IndexedDbDatabaseLike,
  collection: string,
  params: QueryParams,
): Promise<IndexedDbIndexQueryResult | null> {
  if (!params.index || !params.filter) {
    return null;
  }

  const range = resolveIndexEntryRange(collection, params.index, params.filter.value);

  if (range === null) {
    return null;
  }

  return withTransaction(
    db,
    [STORE_DOCUMENTS, STORE_QUERY_INDEX_ENTRIES],
    "readonly",
    async (tx) => {
      const indexEntriesStore = tx.objectStore(STORE_QUERY_INDEX_ENTRIES);
      const docsStore = tx.objectStore(STORE_DOCUMENTS);
      const entries = await loadMatchingIndexEntries(
        indexEntriesStore,
        collection,
        params.index!,
        params.filter!.value,
        range.query ?? null,
      );
      const records = (
        await loadDocumentsByKeysFromStore(
          docsStore,
          collection,
          entries.map((entry) => entry.key),
        )
      )
        .map((entry) => entry.record)
        .filter((record) => record !== null);

      records.sort((a, b) => {
        if (a.createdAt !== b.createdAt) {
          return a.createdAt - b.createdAt;
        }

        return a.key.localeCompare(b.key);
      });

      return {
        records,
        diagnostics: {
          mode: "native_pushdown",
          reason: "native_pushdown",
          index: params.index!,
        },
      };
    },
  );
}

async function loadMatchingIndexEntries(
  indexEntriesStore: IndexedDbObjectStoreLike,
  collection: string,
  indexName: string,
  filter: string | number | FieldCondition,
  range: unknown,
): Promise<QueryIndexEntryRecord[]> {
  const index = indexEntriesStore.index(QUERY_INDEX_LOOKUP);
  const rawEntries =
    range === null
      ? await cursorValuesToArray(index.openCursor())
      : await requestToPromise(index.getAll(range));

  return rawEntries
    .map((entry) => parseQueryIndexEntryRecord(entry))
    .filter(
      (entry) =>
        entry.collection === collection &&
        entry.indexName === indexName &&
        matchesFilter(entry.indexValue, filter),
    );
}

function resolveIndexEntryRange(
  collection: string,
  indexName: string,
  filter: string | number | FieldCondition,
): IndexEntryRange | null {
  const keyRange = resolveKeyRangeFactory();

  if (!keyRange) {
    return {};
  }

  if (typeof filter === "string" || typeof filter === "number") {
    return {
      query: keyRange.bound(
        [collection, indexName, String(filter)],
        [collection, indexName, String(filter), []],
      ),
    };
  }

  const entries = Object.entries(filter).filter(([, value]) => value !== undefined);

  if (entries.length === 0) {
    return null;
  }

  if (filter.$eq !== undefined) {
    const value = String(filter.$eq as string | number);

    return {
      query: keyRange.bound([collection, indexName, value], [collection, indexName, value, []]),
    };
  }

  const rangeBounds = resolveIndexRangeBounds(filter);

  if (rangeBounds === null) {
    return null;
  }

  return {
    query: keyRange.bound(
      [collection, indexName, rangeBounds.lower],
      buildIndexRangeUpperKey(collection, indexName, rangeBounds),
      rangeBounds.lowerOpen,
      rangeBounds.upperOpen,
    ),
  };
}

function buildIndexRangeUpperKey(
  collection: string,
  indexName: string,
  rangeBounds: { upper: unknown; upperOpen: boolean },
): unknown[] {
  const key = [collection, indexName, rangeBounds.upper];

  if (!rangeBounds.upperOpen) {
    key.push([]);
  }

  return key;
}

function resolveIndexRangeBounds(condition: FieldCondition): {
  lower: unknown;
  upper: unknown;
  lowerOpen: boolean;
  upperOpen: boolean;
} | null {
  if (condition.$begins !== undefined) {
    return {
      lower: condition.$begins,
      upper: `${condition.$begins}\uffff`,
      lowerOpen: false,
      upperOpen: false,
    };
  }

  if (condition.$between !== undefined) {
    const [low, high] = condition.$between as [string | number, string | number];

    return {
      lower: String(low),
      upper: String(high),
      lowerOpen: false,
      upperOpen: false,
    };
  }

  let lower = "";
  let upper: unknown = [];
  let lowerOpen = false;
  let upperOpen = false;

  if (condition.$gt !== undefined) {
    lower = String(condition.$gt as string | number);
    lowerOpen = true;
  }

  if (condition.$gte !== undefined) {
    lower = String(condition.$gte as string | number);
    lowerOpen = false;
  }

  if (condition.$lt !== undefined) {
    upper = String(condition.$lt as string | number);
    upperOpen = true;
  }

  if (condition.$lte !== undefined) {
    upper = String(condition.$lte as string | number);
    upperOpen = false;
  }

  if (
    condition.$gt === undefined &&
    condition.$gte === undefined &&
    condition.$lt === undefined &&
    condition.$lte === undefined
  ) {
    return null;
  }

  return { lower, upper, lowerOpen, upperOpen };
}

function resolveKeyRangeFactory(): IndexedDbKeyRangeFactoryLike | null {
  const maybeFactory = (globalThis as { IDBKeyRange?: IndexedDbKeyRangeFactoryLike }).IDBKeyRange;

  return maybeFactory ?? null;
}

async function cursorValuesToArray(
  request: IndexedDbRequestLike<IndexedDbCursorLike | null>,
): Promise<unknown[]> {
  return new Promise((resolve, reject) => {
    const values: unknown[] = [];

    request.onsuccess = () => {
      const cursor = request.result;

      if (cursor === null) {
        resolve(values);
        return;
      }

      values.push(cursor.value);
      cursor.continue();
    };

    request.onerror = () => reject(toError(request.error, "IndexedDB cursor request failed"));
  });
}

async function replaceQueryIndexEntries(
  indexStore: IndexedDbObjectStoreLike,
  collection: string,
  key: string,
  createdAt: number,
  previousIndexes: ResolvedIndexKeys,
  nextIndexes: ResolvedIndexKeys,
): Promise<void> {
  for (const indexName of Object.keys(previousIndexes)) {
    await requestToPromise(indexStore.delete(makeQueryIndexEntryId(collection, indexName, key)));
  }

  for (const [indexName, rawValue] of Object.entries(nextIndexes)) {
    const indexValue = String(rawValue);

    await requestToPromise(
      indexStore.put({
        id: makeQueryIndexEntryId(collection, indexName, key),
        collection,
        indexName,
        indexValue,
        createdAt,
        key,
      }),
    );
  }
}

function assertUniqueIndexes(
  collection: string,
  key: string,
  uniqueIndexes: ResolvedIndexKeys,
  records: readonly StoredDocumentRecord[],
): void {
  for (const [indexName, indexValue] of Object.entries(uniqueIndexes)) {
    const normalizedValue = String(indexValue);

    for (const record of records) {
      if (record.key === key) {
        continue;
      }

      if (record.uniqueIndexes[indexName] === normalizedValue) {
        throw new EngineUniqueConstraintError(
          collection,
          key,
          indexName,
          normalizedValue,
          record.key,
        );
      }
    }
  }
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
      writeToken: String(record.writeVersion),
    })),
    cursor,
  };
}

function paginateQuery(
  collection: string,
  records: StoredDocumentRecord[],
  params: QueryParams,
  includeMetadata = false,
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
    documents: page.map((record) => {
      const document = {
        key: record.key,
        doc: structuredClone(record.doc),
      };

      return includeMetadata
        ? {
            ...document,
            writeToken: String(record.writeVersion),
          }
        : document;
    }),
    cursor,
  };
}

function withDiagnostics(
  result: EngineQueryResult,
  diagnostics: EngineQueryDiagnostics,
): EngineQueryResult {
  return {
    ...result,
    diagnostics,
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

      if (!db.objectStoreNames.contains(STORE_QUERY_INDEX_ENTRIES)) {
        const queryIndexEntries = db.createObjectStore(STORE_QUERY_INDEX_ENTRIES, {
          keyPath: "id",
        });

        createQueryIndexLookup(queryIndexEntries);
      } else {
        const tx = (request as unknown as { transaction?: IndexedDbTransactionLike }).transaction;
        const queryIndexEntries = tx?.objectStore(STORE_QUERY_INDEX_ENTRIES);

        if (queryIndexEntries) {
          if (queryIndexEntries.indexNames?.contains(QUERY_INDEX_LOOKUP)) {
            queryIndexEntries.deleteIndex?.(QUERY_INDEX_LOOKUP);
          }

          createQueryIndexLookup(queryIndexEntries);
        }
      }
    };

    request.onsuccess = () => {
      const db = request.result;

      ensureQueryIndexEntriesBackfilled(db).then(
        () => resolve(db),
        (error: unknown) => reject(error),
      );
    };

    request.onerror = () => {
      reject(toError(request.error, `Failed to open IndexedDB database "${databaseName}"`));
    };
  });
}

function createQueryIndexLookup(queryIndexEntries: IndexedDbObjectStoreLike): void {
  queryIndexEntries.createIndex(QUERY_INDEX_LOOKUP, [
    "collection",
    "indexName",
    "indexValue",
    "createdAt",
    "key",
  ]);
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

async function ensureQueryIndexEntriesBackfilled(db: IndexedDbDatabaseLike): Promise<void> {
  await withTransaction(
    db,
    [STORE_DOCUMENTS, STORE_QUERY_INDEX_ENTRIES, STORE_META],
    "readwrite",
    async (tx) => {
      const indexStore = tx.objectStore(STORE_QUERY_INDEX_ENTRIES);
      const metaStore = tx.objectStore(STORE_META);
      const backfilledRaw = await requestToPromise(metaStore.get(META_QUERY_INDEX_BACKFILL_KEY));

      if (backfilledRaw !== undefined) {
        parseMetaFlagRecord(backfilledRaw, META_QUERY_INDEX_BACKFILL_KEY);
        return;
      }

      const rawRecords = await requestToPromise(tx.objectStore(STORE_DOCUMENTS).getAll());

      for (const rawRecord of rawRecords) {
        const record = parseStoredDocumentRecord(rawRecord);
        await replaceQueryIndexEntries(
          indexStore,
          record.collection,
          record.key,
          record.createdAt,
          {},
          record.indexes,
        );
      }

      await saveMetaFlag(metaStore, META_QUERY_INDEX_BACKFILL_KEY);
    },
  );
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
  writeVersion: number;
  doc: unknown;
  indexes: ResolvedIndexKeys;
  uniqueIndexes?: ResolvedIndexKeys;
}): StoredDocumentRecord {
  return {
    id: input.id,
    collection: input.collection,
    key: input.key,
    createdAt: input.createdAt,
    writeVersion: input.writeVersion,
    doc: getPreparedClone(input.doc) ?? (structuredClone(input.doc) as Record<string, unknown>),
    indexes: { ...input.indexes },
    uniqueIndexes: { ...input.uniqueIndexes },
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
  const writeVersion = value.writeVersion;
  const doc = value.doc;
  const indexes = value.indexes;
  const uniqueIndexes = value.uniqueIndexes;

  if (typeof id !== "string" || typeof collection !== "string" || typeof key !== "string") {
    throw new Error("IndexedDB documents store contains an invalid record (bad id/collection/key)");
  }

  if (typeof createdAt !== "number" || !Number.isFinite(createdAt)) {
    throw new Error("IndexedDB documents store contains an invalid record (bad createdAt)");
  }

  if (
    writeVersion !== undefined &&
    (typeof writeVersion !== "number" || !Number.isFinite(writeVersion) || writeVersion < 1)
  ) {
    throw new Error("IndexedDB documents store contains an invalid record (bad writeVersion)");
  }

  if (!isRecord(doc)) {
    throw new Error("IndexedDB documents store contains an invalid record (bad document)");
  }

  if (!isRecord(indexes)) {
    throw new Error("IndexedDB documents store contains an invalid record (bad indexes)");
  }

  const resolvedIndexes: ResolvedIndexKeys = {};
  const resolvedUniqueIndexes: ResolvedIndexKeys = {};

  for (const [name, indexValue] of Object.entries(indexes)) {
    if (typeof indexValue !== "string") {
      throw new Error("IndexedDB documents store contains an invalid record (non-string index)");
    }

    resolvedIndexes[name] = indexValue;
  }

  if (uniqueIndexes !== undefined) {
    if (!isRecord(uniqueIndexes)) {
      throw new Error("IndexedDB documents store contains an invalid record (bad uniqueIndexes)");
    }

    for (const [name, indexValue] of Object.entries(uniqueIndexes)) {
      if (typeof indexValue !== "string") {
        throw new Error(
          "IndexedDB documents store contains an invalid record (non-string unique index)",
        );
      }

      resolvedUniqueIndexes[name] = indexValue;
    }
  }

  return {
    id,
    collection,
    key,
    createdAt,
    writeVersion: writeVersion ?? 1,
    doc: doc as Record<string, unknown>,
    indexes: resolvedIndexes,
    uniqueIndexes: resolvedUniqueIndexes,
  };
}

function parseQueryIndexEntryRecord(value: unknown): QueryIndexEntryRecord {
  if (!isRecord(value)) {
    throw new Error("IndexedDB query_index_entries store contains an invalid record");
  }

  const id = value.id;
  const collection = value.collection;
  const indexName = value.indexName;
  const indexValue = value.indexValue;
  const createdAt = value.createdAt;
  const key = value.key;

  if (
    typeof id !== "string" ||
    typeof collection !== "string" ||
    typeof indexName !== "string" ||
    typeof indexValue !== "string" ||
    typeof createdAt !== "number" ||
    !Number.isFinite(createdAt) ||
    typeof key !== "string"
  ) {
    throw new Error("IndexedDB query_index_entries store contains an invalid record");
  }

  return { id, collection, indexName, indexValue, createdAt, key };
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

function parseMetaFlagRecord(
  value: unknown,
  expectedKey: typeof META_QUERY_INDEX_BACKFILL_KEY,
): void {
  if (!isRecord(value)) {
    throw new Error("IndexedDB meta store contains an invalid flag record");
  }

  const key = value.key;
  const flagValue = value.value;

  if (key !== expectedKey || flagValue !== 1) {
    throw new Error("IndexedDB meta store contains an invalid flag record");
  }
}

async function saveMetaFlag(
  metaStore: IndexedDbObjectStoreLike,
  key: typeof META_QUERY_INDEX_BACKFILL_KEY,
): Promise<void> {
  const record: MetaSequenceRecord = { key, value: 1 };
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

function makeQueryIndexEntryId(collection: string, indexName: string, key: string): string {
  return `${collection}\u0000${indexName}\u0000${key}`;
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
