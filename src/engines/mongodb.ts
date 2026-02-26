import { DefaultMigrator } from "../migrator";
import { encodeQueryPageCursor, resolveQueryPageStartIndex } from "./query-cursor";
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

const DEFAULT_DOCUMENTS_COLLECTION = "nosql_odm_documents";
const DEFAULT_METADATA_COLLECTION = "nosql_odm_metadata";
const OUTDATED_PAGE_LIMIT = 100;
const OUTDATED_SYNC_CHUNK_SIZE = 100;

interface MongoFindOneAndUpdateOptionsLike {
  upsert?: boolean;
  returnDocument?: "before" | "after";
}

interface MongoUpdateResultLike {
  matchedCount?: unknown;
}

interface MongoCursorLike {
  sort(sort: Record<string, 1 | -1>): MongoCursorLike;
  limit(value: number): MongoCursorLike;
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
  writeVersion: number;
  doc: Record<string, unknown>;
  indexes: ResolvedIndexKeys;
  migrationTargetVersion: number;
  migrationVersionState: MigrationVersionState;
  migrationIndexSignature: string | null;
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

interface OutdatedCursorState {
  phase: "stale" | "current-low" | "current-high";
  stale?: {
    createdAt: number;
    key: string;
  };
  currentLow?: {
    indexSignature: string | null;
    createdAt: number;
    key: string;
  };
  currentHigh?: {
    indexSignature: string | null;
    createdAt: number;
    key: string;
  };
  criteriaVersion: number;
  expectedSignature: string;
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
    capabilities: {
      uniqueConstraints: "atomic",
    },

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

    async getWithMetadata(collection, key) {
      await ready;

      const raw = await documentsCollection.findOne({
        collection,
        key,
      });

      if (!raw) {
        return null;
      }

      const record = parseStoredDocumentRecord(raw);

      return {
        doc: structuredClone(record.doc),
        writeToken: String(record.writeVersion),
      };
    },

    async create(collection, key, doc, indexes, _options, migrationMetadata) {
      await ready;

      const createdAt = await nextCreatedAt(metadataCollection, collection);
      const metadata =
        normalizeMigrationMetadata(migrationMetadata) ?? deriveLegacyMetadataFromDocument(doc);
      const record = createStoredDocumentRecord(
        collection,
        key,
        createdAt,
        1,
        doc,
        indexes,
        metadata,
      );

      try {
        await documentsCollection.insertOne(record);
      } catch (error) {
        if (isMongoDuplicateKeyError(error)) {
          throw new EngineDocumentAlreadyExistsError(collection, key);
        }

        throw error;
      }
    },

    async put(collection, key, doc, indexes, _options, migrationMetadata) {
      await ready;

      const createdAt = await nextCreatedAt(metadataCollection, collection);
      const normalizedDoc = normalizeDocument(doc);
      const normalizedIndexes = normalizeIndexes(indexes);
      const metadata =
        normalizeMigrationMetadata(migrationMetadata) ?? deriveLegacyMetadataFromDocument(doc);

      await documentsCollection.updateOne(
        {
          collection,
          key,
        },
        {
          $set: {
            doc: normalizedDoc,
            indexes: normalizedIndexes,
            migrationTargetVersion: metadata.targetVersion,
            migrationVersionState: metadata.versionState,
            migrationIndexSignature: metadata.indexSignature,
          },
          $setOnInsert: {
            collection,
            key,
            createdAt,
          },
          $inc: {
            writeVersion: 1,
          },
        },
        {
          upsert: true,
        },
      );
    },

    async update(collection, key, doc, indexes, _options, migrationMetadata) {
      await ready;

      const normalizedDoc = normalizeDocument(doc);
      const normalizedIndexes = normalizeIndexes(indexes);
      const metadata =
        normalizeMigrationMetadata(migrationMetadata) ?? deriveLegacyMetadataFromDocument(doc);
      const result = await documentsCollection.updateOne(
        {
          collection,
          key,
        },
        {
          $set: {
            doc: normalizedDoc,
            indexes: normalizedIndexes,
            migrationTargetVersion: metadata.targetVersion,
            migrationVersionState: metadata.versionState,
            migrationIndexSignature: metadata.indexSignature,
          },
          $inc: {
            writeVersion: 1,
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

      return paginate(collection, matched, params);
    },

    async queryWithMetadata(collection, params) {
      await ready;

      const records = await listCollectionDocuments(documentsCollection, collection);
      const matched = matchDocuments(records, params);

      return paginateWithWriteTokens(collection, matched, params);
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

    async batchGetWithMetadata(collection, keys) {
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
          writeToken: String(record.writeVersion),
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
        const metadata =
          normalizeMigrationMetadata(item.migrationMetadata) ??
          deriveLegacyMetadataFromDocument(item.doc);

        await documentsCollection.updateOne(
          {
            collection,
            key: item.key,
          },
          {
            $set: {
              doc: normalizedDoc,
              indexes: normalizedIndexes,
              migrationTargetVersion: metadata.targetVersion,
              migrationVersionState: metadata.versionState,
              migrationIndexSignature: metadata.indexSignature,
            },
            $setOnInsert: {
              collection,
              key: item.key,
              createdAt,
            },
            $inc: {
              writeVersion: 1,
            },
          },
          {
            upsert: true,
          },
        );
      }
    },

    async batchSetWithResult(collection, items) {
      await ready;

      const persistedKeys: string[] = [];
      const conflictedKeys: string[] = [];

      for (const item of items) {
        const normalizedDoc = normalizeDocument(item.doc);
        const normalizedIndexes = normalizeIndexes(item.indexes);
        const metadata =
          normalizeMigrationMetadata(item.migrationMetadata) ??
          deriveLegacyMetadataFromDocument(item.doc);
        const expectedWriteVersion = parseExpectedWriteVersion(item.expectedWriteToken);

        if (expectedWriteVersion !== undefined) {
          const result = await documentsCollection.updateOne(
            {
              collection,
              key: item.key,
              writeVersion: expectedWriteVersion,
            },
            {
              $set: {
                doc: normalizedDoc,
                indexes: normalizedIndexes,
                migrationTargetVersion: metadata.targetVersion,
                migrationVersionState: metadata.versionState,
                migrationIndexSignature: metadata.indexSignature,
              },
              $inc: {
                writeVersion: 1,
              },
            },
          );

          if (
            parseMatchedCount(
              result,
              "MongoDB returned an invalid conditional batch set result",
            ) === 0
          ) {
            conflictedKeys.push(item.key);
            continue;
          }

          persistedKeys.push(item.key);
          continue;
        }

        const createdAt = await nextCreatedAt(metadataCollection, collection);
        await documentsCollection.updateOne(
          {
            collection,
            key: item.key,
          },
          {
            $set: {
              doc: normalizedDoc,
              indexes: normalizedIndexes,
              migrationTargetVersion: metadata.targetVersion,
              migrationVersionState: metadata.versionState,
              migrationIndexSignature: metadata.indexSignature,
            },
            $setOnInsert: {
              collection,
              key: item.key,
              createdAt,
            },
            $inc: {
              writeVersion: 1,
            },
          },
          {
            upsert: true,
          },
        );

        persistedKeys.push(item.key);
      }

      return {
        persistedKeys,
        conflictedKeys,
      } satisfies BatchSetResult;
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
        if (criteria.skipMetadataSyncHint !== true) {
          await syncMigrationMetadataForCriteria(documentsCollection, collection, criteria);
        }
        return getOutdatedDocuments(documentsCollection, collection, criteria, cursor);
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

  await documentsCollection.createIndex({
    collection: 1,
    createdAt: 1,
    key: 1,
  });

  await documentsCollection.createIndex({
    collection: 1,
    migrationTargetVersion: 1,
    migrationVersionState: 1,
    createdAt: 1,
    key: 1,
  });

  await documentsCollection.createIndex({
    collection: 1,
    migrationTargetVersion: 1,
    migrationVersionState: 1,
    migrationIndexSignature: 1,
    createdAt: 1,
    key: 1,
  });

  await documentsCollection.createIndex({
    collection: 1,
    migrationTargetVersion: 1,
    createdAt: 1,
    key: 1,
  });

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

async function syncMigrationMetadataForCriteria(
  documentsCollection: MongoCollectionLike,
  collection: string,
  criteria: MigrationCriteria,
): Promise<void> {
  await syncMetadataPhase(
    documentsCollection,
    collection,
    {
      migrationTargetVersion: {
        $exists: false,
      },
    },
    criteria,
  );

  await syncMetadataPhase(
    documentsCollection,
    collection,
    {
      migrationTargetVersion: {
        $lt: criteria.version,
      },
    },
    criteria,
  );

  await syncMetadataPhase(
    documentsCollection,
    collection,
    {
      migrationTargetVersion: {
        $gt: criteria.version,
      },
    },
    criteria,
  );
}

async function syncMetadataPhase(
  documentsCollection: MongoCollectionLike,
  collection: string,
  filter: Record<string, unknown>,
  criteria: MigrationCriteria,
): Promise<void> {
  while (true) {
    const raws = await documentsCollection
      .find({
        collection,
        ...filter,
      })
      .sort({
        createdAt: 1,
        key: 1,
      })
      .limit(OUTDATED_SYNC_CHUNK_SIZE)
      .toArray();

    if (raws.length === 0) {
      return;
    }

    for (const raw of raws) {
      const record = parseStoredDocumentRecord(raw);
      const metadata = deriveMetadataForCriteria(record.doc, criteria);

      await documentsCollection.updateOne(
        {
          collection,
          key: record.key,
          migrationTargetVersion: {
            $ne: criteria.version,
          },
        },
        {
          $set: {
            migrationTargetVersion: metadata.targetVersion,
            migrationVersionState: metadata.versionState,
            migrationIndexSignature: metadata.indexSignature,
          },
        },
      );
    }

    if (raws.length < OUTDATED_SYNC_CHUNK_SIZE) {
      return;
    }
  }
}

async function getOutdatedDocuments(
  documentsCollection: MongoCollectionLike,
  collection: string,
  criteria: MigrationCriteria,
  cursor: string | undefined,
): Promise<EngineQueryResult> {
  const pageLimit = normalizeOutdatedPageLimit(criteria.pageSizeHint);
  const expectedSignature = computeIndexSignature(criteria.indexes);
  const state = decodeOutdatedCursor(cursor, criteria.version, expectedSignature);
  const documents: StoredDocumentRecord[] = [];
  let remaining = pageLimit;
  let nextCursor: string | null = null;

  if (state.phase === "stale") {
    const stale = await queryOutdatedStalePhase(
      documentsCollection,
      collection,
      criteria.version,
      state.stale,
      remaining,
    );
    documents.push(...stale.records);
    remaining -= stale.records.length;

    if (remaining === 0) {
      if (stale.last) {
        nextCursor = encodeOutdatedCursor({
          phase: stale.hasMore ? "stale" : "current-low",
          stale: stale.hasMore ? stale.last : undefined,
          criteriaVersion: criteria.version,
          expectedSignature,
        });
      }
    } else {
      state.phase = "current-low";
      state.currentLow = undefined;
    }
  }

  if (nextCursor === null && remaining > 0 && state.phase === "current-low") {
    const low = await queryOutdatedCurrentPhase(
      documentsCollection,
      collection,
      criteria.version,
      expectedSignature,
      "$lt",
      state.currentLow,
      remaining,
    );
    documents.push(...low.records);
    remaining -= low.records.length;

    if (remaining === 0) {
      if (low.last) {
        nextCursor = encodeOutdatedCursor({
          phase: low.hasMore ? "current-low" : "current-high",
          currentLow: low.hasMore ? low.last : undefined,
          criteriaVersion: criteria.version,
          expectedSignature,
        });
      }
    } else {
      state.phase = "current-high";
      state.currentHigh = undefined;
    }
  }

  if (nextCursor === null && remaining > 0 && state.phase === "current-high") {
    const high = await queryOutdatedCurrentPhase(
      documentsCollection,
      collection,
      criteria.version,
      expectedSignature,
      "$gt",
      state.currentHigh,
      remaining,
    );
    documents.push(...high.records);
    remaining -= high.records.length;

    if (remaining === 0 && high.hasMore && high.last) {
      nextCursor = encodeOutdatedCursor({
        phase: "current-high",
        currentHigh: high.last,
        criteriaVersion: criteria.version,
        expectedSignature,
      });
    }
  }

  return {
    documents: documents.map((record) => ({
      key: record.key,
      doc: structuredClone(record.doc),
      writeToken: String(record.writeVersion),
    })),
    cursor: nextCursor,
  };
}

function normalizeOutdatedPageLimit(value: number | undefined): number {
  if (value === undefined || !Number.isFinite(value)) {
    return OUTDATED_PAGE_LIMIT;
  }

  return Math.max(1, Math.floor(value));
}

async function queryOutdatedStalePhase(
  documentsCollection: MongoCollectionLike,
  collection: string,
  targetVersion: number,
  cursor: OutdatedCursorState["stale"],
  limit: number,
): Promise<{
  records: StoredDocumentRecord[];
  hasMore: boolean;
  last?: OutdatedCursorState["stale"];
}> {
  if (limit <= 0) {
    return {
      records: [],
      hasMore: false,
    };
  }

  const filter: Record<string, unknown> = {
    collection,
    migrationTargetVersion: targetVersion,
    migrationVersionState: "stale",
  };

  if (cursor) {
    filter.$or = [
      {
        createdAt: {
          $gt: cursor.createdAt,
        },
      },
      {
        createdAt: cursor.createdAt,
        key: {
          $gt: cursor.key,
        },
      },
    ];
  }

  const raws = await documentsCollection
    .find(filter)
    .sort({
      createdAt: 1,
      key: 1,
    })
    .limit(limit + 1)
    .toArray();
  const hasMore = raws.length > limit;
  const pageRaws = hasMore ? raws.slice(0, limit) : raws;
  const records = pageRaws.map(parseStoredDocumentRecord);
  const last = records[records.length - 1];

  return {
    records,
    hasMore,
    last: last
      ? {
          createdAt: last.createdAt,
          key: last.key,
        }
      : undefined,
  };
}

async function queryOutdatedCurrentPhase(
  documentsCollection: MongoCollectionLike,
  collection: string,
  targetVersion: number,
  expectedSignature: string,
  operator: "$lt" | "$gt",
  cursor: OutdatedCursorState["currentLow"] | OutdatedCursorState["currentHigh"],
  limit: number,
): Promise<{
  records: StoredDocumentRecord[];
  hasMore: boolean;
  last?: OutdatedCursorState["currentLow"];
}> {
  if (limit <= 0) {
    return {
      records: [],
      hasMore: false,
    };
  }

  const filter: Record<string, unknown> = {
    collection,
    migrationTargetVersion: targetVersion,
    migrationVersionState: "current",
    migrationIndexSignature: {
      [operator]: expectedSignature,
    },
  };

  if (cursor) {
    filter.$or = [
      {
        migrationIndexSignature: {
          $gt: cursor.indexSignature,
        },
      },
      {
        migrationIndexSignature: cursor.indexSignature,
        createdAt: {
          $gt: cursor.createdAt,
        },
      },
      {
        migrationIndexSignature: cursor.indexSignature,
        createdAt: cursor.createdAt,
        key: {
          $gt: cursor.key,
        },
      },
    ];
  }

  const raws = await documentsCollection
    .find(filter)
    .sort({
      migrationIndexSignature: 1,
      createdAt: 1,
      key: 1,
    })
    .limit(limit + 1)
    .toArray();
  const hasMore = raws.length > limit;
  const pageRaws = hasMore ? raws.slice(0, limit) : raws;
  const records = pageRaws.map(parseStoredDocumentRecord);
  const last = records[records.length - 1];

  return {
    records,
    hasMore,
    last: last
      ? {
          indexSignature: last.migrationIndexSignature,
          createdAt: last.createdAt,
          key: last.key,
        }
      : undefined,
  };
}

function parseStoredDocumentRecord(raw: unknown): StoredDocumentRecord {
  const record = parseRecord(raw, "document record");
  const key = readString(record, "key", "document record");
  const createdAt = readFiniteNumber(record, "createdAt", "document record");
  const writeVersion = readWriteVersion(record);
  const docRaw = record.doc;

  if (!isRecord(docRaw)) {
    throw new Error("MongoDB returned an invalid document record");
  }

  const indexes = parseIndexes(record.indexes);
  const migrationMetadata = parseMigrationMetadataFields(record);

  return {
    key,
    createdAt,
    writeVersion,
    doc: docRaw,
    indexes,
    migrationTargetVersion: migrationMetadata.targetVersion,
    migrationVersionState: migrationMetadata.versionState,
    migrationIndexSignature: migrationMetadata.indexSignature,
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

function readWriteVersion(record: Record<string, unknown>): number {
  const raw = record.writeVersion;

  if (raw === undefined || raw === null) {
    return 1;
  }

  const value = readFiniteNumber(record, "writeVersion", "document record");

  if (!Number.isInteger(value) || value <= 0) {
    throw new Error("MongoDB returned an invalid document record");
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
    throw new Error("MongoDB returned an invalid document record");
  }

  return {
    targetVersion: targetVersionRaw,
    versionState: versionStateRaw,
    indexSignature: indexSignatureRaw,
  };
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
  writeVersion: number,
  doc: unknown,
  indexes: ResolvedIndexKeys,
  metadata: MigrationMetadata,
): Record<string, unknown> {
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

function paginate(
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

function paginateWithWriteTokens(
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
      writeToken: String(record.writeVersion),
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
    throw new Error("MongoDB received invalid migration metadata target version");
  }

  if (!isMigrationVersionState(raw.versionState)) {
    throw new Error("MongoDB received invalid migration metadata state");
  }

  if (raw.indexSignature !== null && typeof raw.indexSignature !== "string") {
    throw new Error("MongoDB received invalid migration metadata index signature");
  }

  return {
    targetVersion: raw.targetVersion,
    versionState: raw.versionState,
    indexSignature: raw.indexSignature,
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
    throw new Error("MongoDB received an invalid expected write token");
  }

  const parsed = Number(raw);

  if (!Number.isFinite(parsed) || !Number.isInteger(parsed) || parsed <= 0) {
    throw new Error("MongoDB received an invalid expected write token");
  }

  return parsed;
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

function encodeOutdatedCursor(state: OutdatedCursorState): string {
  return Buffer.from(JSON.stringify(state), "utf8").toString("base64url");
}

function decodeOutdatedCursor(
  cursor: string | undefined,
  criteriaVersion: number,
  expectedSignature: string,
): OutdatedCursorState {
  const fallback: OutdatedCursorState = {
    phase: "stale",
    criteriaVersion,
    expectedSignature,
  };

  if (!cursor) {
    return fallback;
  }

  try {
    const parsed = JSON.parse(Buffer.from(cursor, "base64url").toString("utf8"));

    if (!isRecord(parsed)) {
      return fallback;
    }

    if (
      (parsed.phase !== "stale" &&
        parsed.phase !== "current-low" &&
        parsed.phase !== "current-high") ||
      parsed.criteriaVersion !== criteriaVersion ||
      parsed.expectedSignature !== expectedSignature
    ) {
      return fallback;
    }

    return {
      phase: parsed.phase,
      stale: parseStaleCursorTuple(parsed.stale),
      currentLow: parseCurrentCursorTuple(parsed.currentLow),
      currentHigh: parseCurrentCursorTuple(parsed.currentHigh),
      criteriaVersion,
      expectedSignature,
    };
  } catch {
    return fallback;
  }
}

function parseStaleCursorTuple(raw: unknown): OutdatedCursorState["stale"] {
  if (!isRecord(raw)) {
    return undefined;
  }

  const createdAt = raw.createdAt;
  const key = raw.key;

  if (
    typeof createdAt !== "number" ||
    !Number.isFinite(createdAt) ||
    typeof key !== "string" ||
    key.length === 0
  ) {
    return undefined;
  }

  return {
    createdAt,
    key,
  };
}

function parseCurrentCursorTuple(raw: unknown): OutdatedCursorState["currentLow"] {
  if (!isRecord(raw)) {
    return undefined;
  }

  const indexSignature = raw.indexSignature;
  const createdAt = raw.createdAt;
  const key = raw.key;

  if (
    (indexSignature !== null && typeof indexSignature !== "string") ||
    typeof createdAt !== "number" ||
    !Number.isFinite(createdAt) ||
    typeof key !== "string" ||
    key.length === 0
  ) {
    return undefined;
  }

  return {
    indexSignature: indexSignature ?? null,
    createdAt,
    key,
  };
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
