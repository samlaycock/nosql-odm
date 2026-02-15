import {
  type BatchSetResult,
  EngineDocumentAlreadyExistsError,
  EngineDocumentNotFoundError,
  type ComparableVersion,
  type EngineQueryResult,
  type FieldCondition,
  type KeyedDocument,
  type MigrationDocumentMetadata,
  type MigrationCriteria,
  type MigrationLock,
  type MigrationVersionState,
  type QueryEngine,
  type QueryParams,
  type ResolvedIndexKeys,
} from "./types";
import { DefaultMigrator } from "../migrator";

const DEFAULT_DOCUMENTS_COLLECTION = "nosql_odm_documents";
const DEFAULT_METADATA_COLLECTION = "nosql_odm_metadata";
const OUTDATED_PAGE_LIMIT = 100;
const OUTDATED_SYNC_CHUNK_SIZE = 100;

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
  limit(limit: number): FirestoreQueryLike;
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
  writeVersion: number;
  doc: Record<string, unknown>;
  indexes: ResolvedIndexKeys;
  migrationTargetVersion: number;
  migrationVersionState: MigrationVersionState;
  migrationIndexSignature: string | null;
  migrationNeedsMigration: boolean;
  migrationOrderKey: string;
  migrationPartition: string;
}

interface LockRecord {
  lockId: string;
  acquiredAt: number;
}

interface MigrationMetadata {
  targetVersion: number;
  versionState: MigrationVersionState;
  indexSignature: string | null;
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

    async create(collection, key, doc, indexes, _options, migrationMetadata) {
      const record = await database.runTransaction(async (transaction) => {
        const ref = documentRef(documentsCollection, collection, key);
        const existingRaw = await transaction.get(ref);
        const existing = parseDocumentSnapshot(existingRaw, "document record");

        if (existing.exists) {
          throw new EngineDocumentAlreadyExistsError(collection, key);
        }

        const createdAt = await nextCreatedAt(transaction, metadataCollection, collection);
        const resolved = resolveWriteMetadata(migrationMetadata, doc);
        const created = createStoredDocumentRecord(
          collection,
          key,
          createdAt,
          1,
          doc,
          indexes,
          resolved.metadata,
          resolved.needsMigrationOverride,
        );

        transaction.create(ref, created);

        return created;
      });

      void record;
    },

    async put(collection, key, doc, indexes, _options, migrationMetadata) {
      await database.runTransaction(async (transaction) => {
        const ref = documentRef(documentsCollection, collection, key);
        const existingRaw = await transaction.get(ref);
        const existing = parseDocumentSnapshot(existingRaw, "document record");
        const resolved = resolveWriteMetadata(migrationMetadata, doc);
        const existingRecord = existing.exists
          ? parseStoredDocumentRecord(snapshotData(existing, "document record"))
          : null;
        const createdAt = existingRecord?.createdAt
          ? existingRecord.createdAt
          : await nextCreatedAt(transaction, metadataCollection, collection);
        const writeVersion = existingRecord ? existingRecord.writeVersion + 1 : 1;
        const updated = createStoredDocumentRecord(
          collection,
          key,
          createdAt,
          writeVersion,
          doc,
          indexes,
          resolved.metadata,
          resolved.needsMigrationOverride,
        );

        transaction.set(ref, updated);
      });
    },

    async update(collection, key, doc, indexes, _options, migrationMetadata) {
      await database.runTransaction(async (transaction) => {
        const ref = documentRef(documentsCollection, collection, key);
        const existingRaw = await transaction.get(ref);
        const existing = parseDocumentSnapshot(existingRaw, "document record");

        if (!existing.exists) {
          throw new EngineDocumentNotFoundError(collection, key);
        }

        const existingRecord = parseStoredDocumentRecord(snapshotData(existing, "document record"));
        const resolved = resolveWriteMetadata(migrationMetadata, doc);
        const updated = createStoredDocumentRecord(
          collection,
          key,
          existingRecord.createdAt,
          existingRecord.writeVersion + 1,
          doc,
          indexes,
          resolved.metadata,
          resolved.needsMigrationOverride,
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
          const existingRecord = existing.exists
            ? parseStoredDocumentRecord(snapshotData(existing, "document record"))
            : null;
          const createdAt = existingRecord
            ? existingRecord.createdAt
            : await nextCreatedAt(transaction, metadataCollection, collection);
          const writeVersion = existingRecord ? existingRecord.writeVersion + 1 : 1;
          const resolved = resolveWriteMetadata(item.migrationMetadata, item.doc);
          const updated = createStoredDocumentRecord(
            collection,
            item.key,
            createdAt,
            writeVersion,
            item.doc,
            item.indexes,
            resolved.metadata,
            resolved.needsMigrationOverride,
          );

          transaction.set(ref, updated);
        });
      }
    },

    async batchSetWithResult(collection, items) {
      const persistedKeys: string[] = [];
      const conflictedKeys: string[] = [];

      for (const item of items) {
        const expectedWriteVersion = parseExpectedWriteVersion(item.expectedWriteToken);
        const resolved = resolveWriteMetadata(item.migrationMetadata, item.doc);

        const result = await database.runTransaction(async (transaction) => {
          const ref = documentRef(documentsCollection, collection, item.key);
          const existingRaw = await transaction.get(ref);
          const existing = parseDocumentSnapshot(existingRaw, "document record");
          const existingRecord = existing.exists
            ? parseStoredDocumentRecord(snapshotData(existing, "document record"))
            : null;

          if (expectedWriteVersion !== undefined) {
            if (!existingRecord || existingRecord.writeVersion !== expectedWriteVersion) {
              return "conflict" as const;
            }

            const updated = createStoredDocumentRecord(
              collection,
              item.key,
              existingRecord.createdAt,
              existingRecord.writeVersion + 1,
              item.doc,
              item.indexes,
              resolved.metadata,
              resolved.needsMigrationOverride,
            );
            transaction.set(ref, updated);
            return "persisted" as const;
          }

          const createdAt = existingRecord
            ? existingRecord.createdAt
            : await nextCreatedAt(transaction, metadataCollection, collection);
          const writeVersion = existingRecord ? existingRecord.writeVersion + 1 : 1;
          const updated = createStoredDocumentRecord(
            collection,
            item.key,
            createdAt,
            writeVersion,
            item.doc,
            item.indexes,
            resolved.metadata,
            resolved.needsMigrationOverride,
          );
          transaction.set(ref, updated);
          return "persisted" as const;
        });

        if (result === "persisted") {
          persistedKeys.push(item.key);
        } else {
          conflictedKeys.push(item.key);
        }
      }

      return {
        persistedKeys,
        conflictedKeys,
      } satisfies BatchSetResult;
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
        if (criteria.skipMetadataSyncHint !== true) {
          await syncMigrationMetadataForCriteria(database, documentsCollection, collection, criteria);
        }
        return queryOutdatedDocuments(documentsCollection, collection, criteria, cursor);
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

async function syncMigrationMetadataForCriteria(
  database: FirestoreLike,
  documentsCollection: FirestoreCollectionLike,
  collection: string,
  criteria: MigrationCriteria,
): Promise<void> {
  const expectedSignature = computeIndexSignature(criteria.indexes);

  await syncMetadataPhase(
    database,
    documentsCollection,
    collection,
    {
      field: "migrationTargetVersion",
      op: "<",
      value: criteria.version,
    },
    criteria,
    expectedSignature,
  );

  await syncMetadataPhase(
    database,
    documentsCollection,
    collection,
    {
      field: "migrationTargetVersion",
      op: ">",
      value: criteria.version,
    },
    criteria,
    expectedSignature,
  );

  await syncMetadataPhase(
    database,
    documentsCollection,
    collection,
    {
      field: "migrationTargetVersion",
      op: "==",
      value: null,
    },
    criteria,
    expectedSignature,
  );

  await syncMetadataPhase(
    database,
    documentsCollection,
    collection,
    {
      field: "migrationTargetVersion",
      op: "==",
      value: criteria.version,
    },
    criteria,
    expectedSignature,
    {
      field: "migrationNeedsMigration",
      op: "==",
      value: null,
    },
  );
}

async function syncMetadataPhase(
  database: FirestoreLike,
  documentsCollection: FirestoreCollectionLike,
  collection: string,
  filter: { field: string; op: string; value: unknown },
  criteria: MigrationCriteria,
  expectedSignature: string,
  extraFilter?: { field: string; op: string; value: unknown },
): Promise<void> {
  while (true) {
    let query = documentsCollection
      .where("collection", "==", collection)
      .where(filter.field, filter.op, filter.value);

    if (extraFilter) {
      query = query.where(extraFilter.field, extraFilter.op, extraFilter.value);
    }

    const raw = await query.limit(OUTDATED_SYNC_CHUNK_SIZE).get();
    const snapshot = parseQuerySnapshot(raw, "document record");

    if (snapshot.docs.length === 0) {
      return;
    }

    for (const doc of snapshot.docs) {
      const record = parseStoredDocumentRecord(snapshotData(doc, "document record"));

      await database.runTransaction(async (transaction) => {
        const ref = documentRef(documentsCollection, collection, record.key);
        const existingRaw = await transaction.get(ref);
        const existing = parseDocumentSnapshot(existingRaw, "document record");

        if (!existing.exists) {
          return;
        }

        const current = parseStoredDocumentRecord(snapshotData(existing, "document record"));
        const nextMetadata = deriveMetadataForCriteria(current.doc, criteria);
        const nextNeedsMigration = computeNeedsMigration(nextMetadata, expectedSignature);

        transaction.set(
          ref,
          {
            migrationTargetVersion: nextMetadata.targetVersion,
            migrationVersionState: nextMetadata.versionState,
            migrationIndexSignature: nextMetadata.indexSignature,
            migrationNeedsMigration: nextNeedsMigration,
            migrationPartition: migrationPartitionKey(
              collection,
              nextMetadata.targetVersion,
              nextNeedsMigration,
            ),
          },
          { merge: true },
        );
      });
    }

    if (snapshot.docs.length < OUTDATED_SYNC_CHUNK_SIZE) {
      return;
    }
  }
}

async function queryOutdatedDocuments(
  documentsCollection: FirestoreCollectionLike,
  collection: string,
  criteria: MigrationCriteria,
  cursor: string | undefined,
): Promise<EngineQueryResult> {
  const pageLimit = normalizeOutdatedPageLimit(criteria.pageSizeHint);
  const partition = migrationPartitionKey(collection, criteria.version, true);
  let query = documentsCollection.where("migrationPartition", "==", partition);

  if (typeof cursor === "string" && cursor.length > 0) {
    query = query.where("migrationOrderKey", ">", cursor);
  }

  const raw = await query.limit(pageLimit + 1).get();
  const snapshot = parseQuerySnapshot(raw, "document record");
  const records = snapshot.docs
    .map((doc) => parseStoredDocumentRecord(snapshotData(doc, "document record")))
    .sort((a, b) => a.migrationOrderKey.localeCompare(b.migrationOrderKey));
  const hasMore = records.length > pageLimit;
  const pageRecords = hasMore ? records.slice(0, pageLimit) : records;
  const documents: KeyedDocument[] = [];

  for (const record of pageRecords) {
    documents.push({
      key: record.key,
      doc: structuredClone(record.doc),
      writeToken: String(record.writeVersion),
    });
  }

  return {
    documents,
    cursor: hasMore ? (pageRecords[pageRecords.length - 1]?.migrationOrderKey ?? null) : null,
  };
}

function normalizeOutdatedPageLimit(value: number | undefined): number {
  if (value === undefined || !Number.isFinite(value)) {
    return OUTDATED_PAGE_LIMIT;
  }

  return Math.max(1, Math.floor(value));
}

function parseStoredDocumentRecord(raw: unknown): StoredDocumentRecord {
  const record = parseRecord(raw, "document record");
  const key = readString(record, "key", "document record");
  const createdAt = readFiniteNumber(record, "createdAt", "document record");
  const writeVersion = readWriteVersion(record);
  const docRaw = record.doc;

  if (!isRecord(docRaw)) {
    throw new Error("Firestore returned an invalid document record");
  }

  const indexes = parseIndexes(record.indexes);
  const migrationMetadata = parseMigrationMetadataFields(record);
  const migrationNeedsMigration = parseMigrationNeedsMigration(record, migrationMetadata);
  const migrationOrder = parseMigrationOrderKey(record, createdAt, key);
  const migrationPartition = parseMigrationPartition(
    record,
    migrationMetadata,
    migrationNeedsMigration,
  );

  return {
    key,
    createdAt,
    writeVersion,
    doc: docRaw,
    indexes,
    migrationTargetVersion: migrationMetadata.targetVersion,
    migrationVersionState: migrationMetadata.versionState,
    migrationIndexSignature: migrationMetadata.indexSignature,
    migrationNeedsMigration,
    migrationOrderKey: migrationOrder,
    migrationPartition,
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

function readWriteVersion(record: Record<string, unknown>): number {
  const raw = record.writeVersion;

  if (raw === undefined || raw === null) {
    return 1;
  }

  const value = readFiniteNumber(record, "writeVersion", "document record");

  if (!Number.isInteger(value) || value <= 0) {
    throw new Error("Firestore returned an invalid document record");
  }

  return value;
}

function parseMigrationMetadataFields(record: Record<string, unknown>): MigrationMetadata {
  const targetVersionRaw = record.migrationTargetVersion;
  const versionStateRaw = record.migrationVersionState;
  const indexSignatureRaw = record.migrationIndexSignature;

  if (
    targetVersionRaw === undefined ||
    versionStateRaw === undefined ||
    indexSignatureRaw === undefined
  ) {
    return deriveLegacyMetadataFromDocument(record.doc);
  }

  if (
    typeof targetVersionRaw !== "number" ||
    !Number.isFinite(targetVersionRaw) ||
    !Number.isInteger(targetVersionRaw) ||
    targetVersionRaw < 0 ||
    !isMigrationVersionState(versionStateRaw) ||
    (indexSignatureRaw !== null && typeof indexSignatureRaw !== "string")
  ) {
    throw new Error("Firestore returned an invalid document record");
  }

  return {
    targetVersion: targetVersionRaw,
    versionState: versionStateRaw,
    indexSignature: indexSignatureRaw,
  };
}

function parseMigrationNeedsMigration(
  record: Record<string, unknown>,
  metadata: MigrationMetadata,
): boolean {
  const raw = record.migrationNeedsMigration;

  if (raw === undefined || raw === null) {
    return metadata.versionState === "stale" || metadata.indexSignature === null;
  }

  if (typeof raw !== "boolean") {
    throw new Error("Firestore returned an invalid document record");
  }

  return raw;
}

function parseMigrationOrderKey(
  record: Record<string, unknown>,
  createdAt: number,
  key: string,
): string {
  const raw = record.migrationOrderKey;

  if (raw === undefined || raw === null) {
    return migrationOrderKey(createdAt, key);
  }

  if (typeof raw !== "string") {
    throw new Error("Firestore returned an invalid document record");
  }

  return raw;
}

function parseMigrationPartition(
  record: Record<string, unknown>,
  metadata: MigrationMetadata,
  needsMigration: boolean,
): string {
  const raw = record.migrationPartition;

  if (raw === undefined || raw === null) {
    const collection = readString(record, "collection", "document record");
    return migrationPartitionKey(collection, metadata.targetVersion, needsMigration);
  }

  if (typeof raw !== "string") {
    throw new Error("Firestore returned an invalid document record");
  }

  return raw;
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
  writeVersion: number,
  doc: unknown,
  indexes: ResolvedIndexKeys,
  metadata: MigrationMetadata,
  needsMigrationOverride?: boolean | null,
): Record<string, unknown> {
  const defaultNeedsMigration =
    metadata.versionState === "stale" || metadata.indexSignature === null;
  const needsMigration =
    needsMigrationOverride === undefined ? defaultNeedsMigration : needsMigrationOverride;
  const partitionNeedsMigration = needsMigration === true;

  return {
    collection,
    key,
    createdAt,
    writeVersion,
    doc: normalizeDocument(doc),
    indexes: normalizeIndexes(indexes),
    migrationTargetVersion: metadata.targetVersion,
    migrationVersionState: metadata.versionState,
    migrationIndexSignature: metadata.indexSignature,
    migrationNeedsMigration: needsMigration,
    migrationOrderKey: migrationOrderKey(createdAt, key),
    migrationPartition: migrationPartitionKey(
      collection,
      metadata.targetVersion,
      partitionNeedsMigration,
    ),
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

function classifyVersionState(
  parsedVersion: ComparableVersion | null,
  latest: number,
  compareVersions: (a: ComparableVersion, b: ComparableVersion) => number,
): MigrationVersionState {
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

function deriveMetadataForCriteria(doc: unknown, criteria: MigrationCriteria): MigrationMetadata {
  if (!isRecord(doc)) {
    return {
      targetVersion: criteria.version,
      versionState: "unknown",
      indexSignature: null,
    };
  }

  const parseVersion = criteria.parseVersion ?? defaultParseVersion;
  const compareVersions = criteria.compareVersions ?? defaultCompareVersions;
  const parsedVersion = parseVersion(doc[criteria.versionField]);
  const versionState = classifyVersionState(parsedVersion, criteria.version, compareVersions);

  return {
    targetVersion: criteria.version,
    versionState,
    indexSignature: computeIndexSignatureFromUnknown(doc[criteria.indexesField]),
  };
}

function normalizeMigrationMetadata(
  raw: MigrationDocumentMetadata | undefined,
): MigrationMetadata | null {
  if (!raw) {
    return null;
  }

  if (
    !Number.isFinite(raw.targetVersion) ||
    Math.floor(raw.targetVersion) !== raw.targetVersion ||
    raw.targetVersion < 0
  ) {
    throw new Error("Firestore received invalid migration metadata target version");
  }

  if (!isMigrationVersionState(raw.versionState)) {
    throw new Error("Firestore received invalid migration metadata state");
  }

  if (raw.indexSignature !== null && typeof raw.indexSignature !== "string") {
    throw new Error("Firestore received invalid migration metadata index signature");
  }

  return {
    targetVersion: raw.targetVersion,
    versionState: raw.versionState,
    indexSignature: raw.indexSignature,
  };
}

function resolveWriteMetadata(
  raw: MigrationDocumentMetadata | undefined,
  doc: unknown,
): { metadata: MigrationMetadata; needsMigrationOverride?: boolean | null } {
  const normalized = normalizeMigrationMetadata(raw);

  if (normalized) {
    return { metadata: normalized };
  }

  return {
    metadata: deriveLegacyMetadataFromDocument(doc),
    // Derived metadata is relative to the stored document itself, not the
    // current migration criteria, so force a one-time sync pass.
    needsMigrationOverride: null,
  };
}

function deriveLegacyMetadataFromDocument(doc: unknown): MigrationMetadata {
  if (!isRecord(doc)) {
    return {
      targetVersion: 0,
      versionState: "unknown",
      indexSignature: null,
    };
  }

  const rawVersion = normalizeNumericVersionFromUnknown(doc.__v);

  return {
    targetVersion: rawVersion ?? 0,
    versionState: rawVersion === null ? "unknown" : "current",
    indexSignature: computeIndexSignatureFromUnknown(doc.__indexes),
  };
}

function isMigrationVersionState(value: unknown): value is MigrationVersionState {
  return value === "current" || value === "stale" || value === "ahead" || value === "unknown";
}

function parseExpectedWriteVersion(raw: string | undefined): number | undefined {
  if (raw === undefined) {
    return undefined;
  }

  if (typeof raw !== "string" || raw.trim().length === 0 || !/^\d+$/.test(raw)) {
    throw new Error("Firestore received an invalid expected write token");
  }

  const parsed = Number(raw);

  if (!Number.isFinite(parsed) || !Number.isInteger(parsed) || parsed <= 0) {
    throw new Error("Firestore received an invalid expected write token");
  }

  return parsed;
}

function computeNeedsMigration(metadata: MigrationMetadata, expectedSignature: string): boolean {
  if (metadata.versionState === "stale") {
    return true;
  }

  if (metadata.versionState !== "current") {
    return false;
  }

  return metadata.indexSignature !== expectedSignature;
}

function computeIndexSignature(indexes: readonly string[]): string {
  return JSON.stringify(indexes);
}

function computeIndexSignatureFromUnknown(raw: unknown): string | null {
  if (!Array.isArray(raw) || raw.some((value) => typeof value !== "string")) {
    return null;
  }

  return computeIndexSignature(raw as string[]);
}

function normalizeNumericVersionFromUnknown(raw: unknown): number | null {
  if (raw === undefined || raw === null) {
    return null;
  }

  if (typeof raw === "number") {
    return Number.isFinite(raw) ? raw : null;
  }

  if (typeof raw !== "string") {
    return null;
  }

  return normalizeNumericVersion(raw);
}

function migrationPartitionKey(
  collection: string,
  targetVersion: number,
  needsMigration: boolean,
): string {
  return `mig:${encodeIdPart(collection)}:${String(targetVersion)}:${needsMigration ? "1" : "0"}`;
}

function migrationOrderKey(createdAt: number, key: string): string {
  return `${String(createdAt).padStart(16, "0")}#${encodeIdPart(key)}`;
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
