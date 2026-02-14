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

interface FirestoreSetOptionsLike {
  merge?: boolean;
}

interface FirestoreDocumentSnapshotLike {
  exists: boolean;
  id: string;
  data(): unknown;
}

interface FirestoreQuerySnapshotLike {
  docs: FirestoreDocumentSnapshotLike[];
}

interface FirestoreDocumentReferenceLike {
  id: string;
  get(): Promise<unknown>;
  set(data: Record<string, unknown>, options?: FirestoreSetOptionsLike): Promise<unknown>;
  delete(): Promise<unknown>;
}

interface FirestoreQueryLike {
  where(fieldPath: string, opStr: string, value: unknown): FirestoreQueryLike;
  get(): Promise<unknown>;
}

interface FirestoreCollectionLike extends FirestoreQueryLike {
  doc(id?: string): FirestoreDocumentReferenceLike;
}

interface FirestoreTransactionLike {
  get(ref: FirestoreDocumentReferenceLike): Promise<unknown>;
  create(
    ref: FirestoreDocumentReferenceLike,
    data: Record<string, unknown>,
  ): FirestoreTransactionLike;
  set(
    ref: FirestoreDocumentReferenceLike,
    data: Record<string, unknown>,
    options?: FirestoreSetOptionsLike,
  ): FirestoreTransactionLike;
  delete(ref: FirestoreDocumentReferenceLike): FirestoreTransactionLike;
}

interface FirestoreLike {
  collection(path: string): FirestoreCollectionLike;
  runTransaction<T>(
    updateFunction: (transaction: FirestoreTransactionLike) => Promise<T>,
  ): Promise<T>;
}

export interface FirestoreEngineOptions {
  database: FirestoreLike;
  documentsCollection?: string;
  metadataCollection?: string;
}

export interface FirestoreQueryEngine extends QueryEngine<never> {}

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

export function firestoreEngine(options: FirestoreEngineOptions): FirestoreQueryEngine {
  const database = options.database;
  const documentsCollection = database.collection(
    options.documentsCollection ?? DEFAULT_DOCUMENTS_COLLECTION,
  );
  const metadataCollection = database.collection(
    options.metadataCollection ?? DEFAULT_METADATA_COLLECTION,
  );

  const engine: FirestoreQueryEngine = {
    async get(collection, key) {
      const raw = await documentRef(documentsCollection, collection, key).get();
      const snapshot = parseDocumentSnapshot(raw, "document record");

      if (!snapshot.exists) {
        return null;
      }

      const record = parseStoredDocumentRecord(snapshotData(snapshot, "document record"));

      return structuredClone(record.doc);
    },

    async create(collection, key, doc, indexes) {
      const record = await database.runTransaction(async (transaction) => {
        const ref = documentRef(documentsCollection, collection, key);
        const existingRaw = await transaction.get(ref);
        const existing = parseDocumentSnapshot(existingRaw, "document record");

        if (existing.exists) {
          throw new EngineDocumentAlreadyExistsError(collection, key);
        }

        const createdAt = await nextCreatedAt(transaction, metadataCollection, collection);
        const created = createStoredDocumentRecord(collection, key, createdAt, doc, indexes);

        transaction.create(ref, created);

        return created;
      });

      void record;
    },

    async put(collection, key, doc, indexes) {
      await database.runTransaction(async (transaction) => {
        const ref = documentRef(documentsCollection, collection, key);
        const existingRaw = await transaction.get(ref);
        const existing = parseDocumentSnapshot(existingRaw, "document record");
        const createdAt = existing.exists
          ? parseStoredDocumentRecord(snapshotData(existing, "document record")).createdAt
          : await nextCreatedAt(transaction, metadataCollection, collection);
        const updated = createStoredDocumentRecord(collection, key, createdAt, doc, indexes);

        transaction.set(ref, updated);
      });
    },

    async update(collection, key, doc, indexes) {
      await database.runTransaction(async (transaction) => {
        const ref = documentRef(documentsCollection, collection, key);
        const existingRaw = await transaction.get(ref);
        const existing = parseDocumentSnapshot(existingRaw, "document record");

        if (!existing.exists) {
          throw new EngineDocumentNotFoundError(collection, key);
        }

        const existingRecord = parseStoredDocumentRecord(snapshotData(existing, "document record"));
        const updated = createStoredDocumentRecord(
          collection,
          key,
          existingRecord.createdAt,
          doc,
          indexes,
        );

        transaction.set(ref, updated);
      });
    },

    async delete(collection, key) {
      await documentRef(documentsCollection, collection, key).delete();
    },

    async query(collection, params) {
      const records = await listCollectionDocuments(documentsCollection, collection);
      const matched = matchDocuments(records, params);

      return paginate(matched, params);
    },

    async batchGet(collection, keys) {
      const uniqueKeys = uniqueStrings(keys);
      const fetchedEntries = await Promise.all(
        uniqueKeys.map(async (key) => {
          const raw = await documentRef(documentsCollection, collection, key).get();
          const snapshot = parseDocumentSnapshot(raw, "document record");

          if (!snapshot.exists) {
            return null;
          }

          const record = parseStoredDocumentRecord(snapshotData(snapshot, "document record"));

          return [key, record] as const;
        }),
      );

      const fetched = new Map<string, StoredDocumentRecord>();

      for (const entry of fetchedEntries) {
        if (!entry) {
          continue;
        }

        fetched.set(entry[0], entry[1]);
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
      for (const item of items) {
        await database.runTransaction(async (transaction) => {
          const ref = documentRef(documentsCollection, collection, item.key);
          const existingRaw = await transaction.get(ref);
          const existing = parseDocumentSnapshot(existingRaw, "document record");
          const createdAt = existing.exists
            ? parseStoredDocumentRecord(snapshotData(existing, "document record")).createdAt
            : await nextCreatedAt(transaction, metadataCollection, collection);
          const updated = createStoredDocumentRecord(
            collection,
            item.key,
            createdAt,
            item.doc,
            item.indexes,
          );

          transaction.set(ref, updated);
        });
      }
    },

    async batchDelete(collection, keys) {
      for (const key of keys) {
        await documentRef(documentsCollection, collection, key).delete();
      }
    },

    migration: {
      async acquireLock(collection, options) {
        const lock: MigrationLock = {
          id: randomId(),
          collection,
          acquiredAt: Date.now(),
        };

        const result = await database.runTransaction(async (transaction) => {
          const ref = metadataRef(metadataCollection, lockDocId(collection));
          const existingRaw = await transaction.get(ref);
          const existing = parseDocumentSnapshot(existingRaw, "migration lock record");

          if (!existing.exists) {
            transaction.set(ref, {
              kind: "lock",
              collection,
              lockId: lock.id,
              acquiredAt: lock.acquiredAt,
            });

            return lock;
          }

          const record = parseLockRecord(snapshotData(existing, "migration lock record"));
          const ttl = options?.ttl;
          const allowSteal = ttl !== undefined && Number.isFinite(ttl) && ttl >= 0;

          if (!allowSteal) {
            return null;
          }

          if (record.acquiredAt > lock.acquiredAt - ttl) {
            return null;
          }

          transaction.set(ref, {
            kind: "lock",
            collection,
            lockId: lock.id,
            acquiredAt: lock.acquiredAt,
          });

          return lock;
        });

        return result;
      },

      async releaseLock(lock) {
        await database.runTransaction(async (transaction) => {
          const ref = metadataRef(metadataCollection, lockDocId(lock.collection));
          const existingRaw = await transaction.get(ref);
          const existing = parseDocumentSnapshot(existingRaw, "migration lock record");

          if (!existing.exists) {
            return;
          }

          const record = parseLockRecord(snapshotData(existing, "migration lock record"));

          if (record.lockId !== lock.id) {
            return;
          }

          transaction.delete(ref);
        });
      },

      async getOutdated(collection, criteria, cursor) {
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
        await database.runTransaction(async (transaction) => {
          const lockRef = metadataRef(metadataCollection, lockDocId(lock.collection));
          const lockRaw = await transaction.get(lockRef);
          const lockSnapshot = parseDocumentSnapshot(lockRaw, "migration lock record");

          if (!lockSnapshot.exists) {
            return;
          }

          const existingLock = parseLockRecord(snapshotData(lockSnapshot, "migration lock record"));

          if (existingLock.lockId !== lock.id) {
            return;
          }

          transaction.set(metadataRef(metadataCollection, checkpointDocId(lock.collection)), {
            kind: "checkpoint",
            collection: lock.collection,
            cursor,
          });
        });
      },

      async loadCheckpoint(collection) {
        const raw = await metadataRef(metadataCollection, checkpointDocId(collection)).get();
        const snapshot = parseDocumentSnapshot(raw, "migration checkpoint record");

        if (!snapshot.exists) {
          return null;
        }

        return parseCheckpointRecord(snapshotData(snapshot, "migration checkpoint record"));
      },

      async clearCheckpoint(collection) {
        await metadataRef(metadataCollection, checkpointDocId(collection)).delete();
      },

      async getStatus(collection) {
        const lockRaw = await metadataRef(metadataCollection, lockDocId(collection)).get();
        const lockSnapshot = parseDocumentSnapshot(lockRaw, "migration lock record");

        if (!lockSnapshot.exists) {
          return null;
        }

        const lock = parseLockRecord(snapshotData(lockSnapshot, "migration lock record"));

        const checkpointRaw = await metadataRef(
          metadataCollection,
          checkpointDocId(collection),
        ).get();
        const checkpointSnapshot = parseDocumentSnapshot(
          checkpointRaw,
          "migration checkpoint record",
        );

        return {
          lock: {
            id: lock.lockId,
            collection,
            acquiredAt: lock.acquiredAt,
          },
          cursor: checkpointSnapshot.exists
            ? parseCheckpointRecord(snapshotData(checkpointSnapshot, "migration checkpoint record"))
            : null,
        };
      },
    },
  };

  engine.migrator = new DefaultMigrator(engine);

  return engine;
}

function documentRef(
  documentsCollection: FirestoreCollectionLike,
  collection: string,
  key: string,
): FirestoreDocumentReferenceLike {
  return documentsCollection.doc(documentDocId(collection, key));
}

function metadataRef(
  metadataCollection: FirestoreCollectionLike,
  id: string,
): FirestoreDocumentReferenceLike {
  return metadataCollection.doc(id);
}

function documentDocId(collection: string, key: string): string {
  return `doc:${encodeIdPart(collection)}:${encodeIdPart(key)}`;
}

function lockDocId(collection: string): string {
  return `lock:${encodeIdPart(collection)}`;
}

function checkpointDocId(collection: string): string {
  return `checkpoint:${encodeIdPart(collection)}`;
}

function sequenceDocId(collection: string): string {
  return `sequence:${encodeIdPart(collection)}`;
}

function encodeIdPart(value: string): string {
  return encodeURIComponent(value);
}

async function nextCreatedAt(
  transaction: FirestoreTransactionLike,
  metadataCollection: FirestoreCollectionLike,
  collection: string,
): Promise<number> {
  const ref = metadataRef(metadataCollection, sequenceDocId(collection));
  const raw = await transaction.get(ref);
  const snapshot = parseDocumentSnapshot(raw, "sequence record");

  const value = snapshot.exists
    ? readFiniteNumber(
        parseRecord(snapshotData(snapshot, "sequence record"), "sequence record"),
        "value",
        "sequence record",
      )
    : 0;
  const next = value + 1;

  transaction.set(ref, {
    kind: "sequence",
    collection,
    value: next,
  });

  return next;
}

async function listCollectionDocuments(
  documentsCollection: FirestoreCollectionLike,
  collection: string,
): Promise<StoredDocumentRecord[]> {
  const raw = await documentsCollection.where("collection", "==", collection).get();
  const snapshot = parseQuerySnapshot(raw, "document record");
  const records: StoredDocumentRecord[] = [];

  for (const doc of snapshot.docs) {
    records.push(parseStoredDocumentRecord(snapshotData(doc, "document record")));
  }

  records.sort((a, b) => {
    if (a.createdAt !== b.createdAt) {
      return a.createdAt - b.createdAt;
    }

    return a.key.localeCompare(b.key);
  });

  return records;
}

function parseStoredDocumentRecord(raw: unknown): StoredDocumentRecord {
  const record = parseRecord(raw, "document record");
  const key = readString(record, "key", "document record");
  const createdAt = readFiniteNumber(record, "createdAt", "document record");
  const docRaw = record.doc;

  if (!isRecord(docRaw)) {
    throw new Error("Firestore returned an invalid document record");
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

function parseIndexes(raw: unknown): ResolvedIndexKeys {
  if (!isRecord(raw)) {
    throw new Error("Firestore returned an invalid document record");
  }

  const indexes: ResolvedIndexKeys = {};

  for (const [name, value] of Object.entries(raw)) {
    if (typeof value !== "string") {
      throw new Error("Firestore returned an invalid document record");
    }

    indexes[name] = value;
  }

  return indexes;
}

function normalizeDocument(doc: unknown): Record<string, unknown> {
  const cloned = structuredClone(doc);

  if (!isRecord(cloned)) {
    throw new Error("Firestore received a non-object document");
  }

  return cloned;
}

function normalizeIndexes(indexes: ResolvedIndexKeys): ResolvedIndexKeys {
  const normalized: ResolvedIndexKeys = {};

  for (const [name, value] of Object.entries(indexes)) {
    if (typeof value !== "string") {
      throw new Error("Firestore received invalid index values");
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

function parseDocumentSnapshot(raw: unknown, context: string): FirestoreDocumentSnapshotLike {
  if (!isRecord(raw)) {
    throw new Error(`Firestore returned an invalid ${context}`);
  }

  if (typeof raw.exists !== "boolean" || typeof raw.data !== "function") {
    throw new Error(`Firestore returned an invalid ${context}`);
  }

  const id = raw.id;

  if (typeof id !== "string") {
    throw new Error(`Firestore returned an invalid ${context}`);
  }

  return raw as unknown as FirestoreDocumentSnapshotLike;
}

function parseQuerySnapshot(raw: unknown, context: string): FirestoreQuerySnapshotLike {
  if (!isRecord(raw)) {
    throw new Error(`Firestore returned an invalid ${context}`);
  }

  const docs = raw.docs;

  if (!Array.isArray(docs)) {
    throw new Error(`Firestore returned an invalid ${context}`);
  }

  for (const doc of docs) {
    parseDocumentSnapshot(doc, context);
  }

  return raw as unknown as FirestoreQuerySnapshotLike;
}

function snapshotData(snapshot: FirestoreDocumentSnapshotLike, context: string): unknown {
  const data = snapshot.data();

  if (data === undefined || data === null) {
    throw new Error(`Firestore returned an invalid ${context}`);
  }

  return data;
}

function parseRecord(raw: unknown, context: string): Record<string, unknown> {
  if (!isRecord(raw)) {
    throw new Error(`Firestore returned an invalid ${context}`);
  }

  return raw;
}

function readString(record: Record<string, unknown>, field: string, context: string): string {
  const value = record[field];

  if (typeof value !== "string") {
    throw new Error(`Firestore returned an invalid ${context}`);
  }

  return value;
}

function readFiniteNumber(record: Record<string, unknown>, field: string, context: string): number {
  const value = record[field];

  if (typeof value === "number") {
    if (!Number.isFinite(value)) {
      throw new Error(`Firestore returned an invalid ${context}`);
    }

    return value;
  }

  if (typeof value === "bigint") {
    const asNumber = Number(value);

    if (!Number.isFinite(asNumber)) {
      throw new Error(`Firestore returned an invalid ${context}`);
    }

    return asNumber;
  }

  if (isRecord(value) && typeof value.toNumber === "function") {
    const asNumber = value.toNumber();

    if (typeof asNumber !== "number" || !Number.isFinite(asNumber)) {
      throw new Error(`Firestore returned an invalid ${context}`);
    }

    return asNumber;
  }

  throw new Error(`Firestore returned an invalid ${context}`);
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
