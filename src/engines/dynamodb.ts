import {
  BatchGetCommand,
  BatchWriteCommand,
  DeleteCommand,
  GetCommand,
  PutCommand,
  QueryCommand,
  TransactWriteCommand,
  UpdateCommand,
  type BatchGetCommandInput,
  type BatchWriteCommandInput,
  type QueryCommandInput,
} from "@aws-sdk/lib-dynamodb";

import { DefaultMigrator } from "../migrator";
import {
  EngineDocumentAlreadyExistsError,
  EngineDocumentNotFoundError,
  type ComparableVersion,
  type EngineQueryResult,
  type FieldCondition,
  type KeyedDocument,
  type MigrationDocumentMetadata,
  type MigrationCriteria,
  type MigrationVersionState,
  type QueryEngine,
  type QueryParams,
  type ResolvedIndexKeys,
} from "./types";

const MAX_BATCH_WRITE = 25;
const MAX_BATCH_GET = 100;
const BATCH_RETRY_LIMIT = 10;
const META_PARTITION_KEY = "META";
const OUTDATED_PAGE_LIMIT = 100;
const METADATA_SYNC_CHUNK_SIZE = 100;
const ITEM_TYPE_DOC = "doc";
const ITEM_TYPE_LOCK = "lock";
const ITEM_TYPE_CHECKPOINT = "checkpoint";
const ITEM_TYPE_MIGRATION_METADATA = "migration_metadata";
const MIGRATION_OUTDATED_INDEX = "migration_outdated_idx";
const MIGRATION_SYNC_INDEX = "migration_sync_idx";
const MIGRATION_OUTDATED_KEY_PREFIX = "SIG#";
const MIGRATION_OUTDATED_KEY_DELIMITER = "#KEY#";

interface DynamoDbDocumentClientLike {
  send(command: { input: unknown }): Promise<unknown>;
}

export interface DynamoDbEngineOptions {
  client: DynamoDbDocumentClientLike;
  tableName: string;
  hashKeyName?: string;
  sortKeyName?: string;
  migrationOutdatedIndexName?: string;
  migrationSyncIndexName?: string;
}

export interface DynamoDbQueryEngine extends QueryEngine<never> {}

interface DynamoKeyConfig {
  hashKeyName: string;
  sortKeyName: string;
  migrationOutdatedIndexName: string;
  migrationSyncIndexName: string;
}

interface DynamoPrimaryKey {
  pk: string;
  sk: string;
}

interface StoredDocumentItem {
  pk: string;
  sk: string;
  itemType: typeof ITEM_TYPE_DOC;
  collection: string;
  key: string;
  createdAt: number;
  writeVersion: number;
  doc: Record<string, unknown>;
  indexes: ResolvedIndexKeys;
}

interface LockItem {
  pk: string;
  sk: string;
  itemType: typeof ITEM_TYPE_LOCK;
  lockId: string;
  acquiredAt: number;
}

interface CheckpointItem {
  pk: string;
  sk: string;
  itemType: typeof ITEM_TYPE_CHECKPOINT;
  cursor: string;
}

interface MigrationMetadataItem {
  pk: string;
  sk: string;
  itemType: typeof ITEM_TYPE_MIGRATION_METADATA;
  collection: string;
  key: string;
  targetVersion: number;
  versionState: MigrationVersionState;
  indexSignature: string | null;
  migrationOutdatedPk: string;
  migrationOutdatedSk: string;
  migrationSyncPk: string;
  migrationSyncSk: number;
}

interface MigrationMetadata {
  targetVersion: number;
  versionState: MigrationVersionState;
  indexSignature: string | null;
}

interface OutdatedCursorState {
  phase: "stale" | "current-low" | "current-high";
  staleStartKey?: Record<string, unknown>;
  currentLowStartKey?: Record<string, unknown>;
  currentHighStartKey?: Record<string, unknown>;
  criteriaVersion: number;
  expectedSignature: string;
}

type BatchWriteRequest = NonNullable<
  NonNullable<BatchWriteCommandInput["RequestItems"]>[string]
>[number];

function normalizeKeyConfig(options: DynamoDbEngineOptions): DynamoKeyConfig {
  const hashKeyName = normalizeNonEmptyString(options.hashKeyName, "pk", "hashKeyName");
  const sortKeyName = normalizeNonEmptyString(options.sortKeyName, "sk", "sortKeyName");
  const migrationOutdatedIndexName = normalizeNonEmptyString(
    options.migrationOutdatedIndexName,
    MIGRATION_OUTDATED_INDEX,
    "migrationOutdatedIndexName",
  );
  const migrationSyncIndexName = normalizeNonEmptyString(
    options.migrationSyncIndexName,
    MIGRATION_SYNC_INDEX,
    "migrationSyncIndexName",
  );

  if (hashKeyName === sortKeyName) {
    throw new Error("DynamoDB hashKeyName and sortKeyName must be different");
  }

  return {
    hashKeyName,
    sortKeyName,
    migrationOutdatedIndexName,
    migrationSyncIndexName,
  };
}

function normalizeNonEmptyString(
  value: string | undefined,
  fallback: string,
  optionName: string,
): string {
  const candidate = value ?? fallback;

  if (typeof candidate !== "string" || candidate.trim().length === 0) {
    throw new Error(`DynamoDB option "${optionName}" must be a non-empty string`);
  }

  return candidate.trim();
}

function toDynamoPrimaryKey(
  keyConfig: DynamoKeyConfig,
  pk: string,
  sk: string,
): Record<string, string> {
  return {
    [keyConfig.hashKeyName]: pk,
    [keyConfig.sortKeyName]: sk,
  };
}

function parseDynamoPrimaryKey(
  raw: Record<string, unknown>,
  keyConfig: DynamoKeyConfig,
): DynamoPrimaryKey {
  const pk = raw[keyConfig.hashKeyName];
  const sk = raw[keyConfig.sortKeyName];

  if (typeof pk !== "string" || typeof sk !== "string") {
    throw new Error("DynamoDB returned an invalid primary key shape");
  }

  return {
    pk,
    sk,
  };
}

function serializeItemForStorage<T extends { pk: string; sk: string }>(
  item: T,
  keyConfig: DynamoKeyConfig,
): Record<string, unknown> {
  return {
    ...item,
    [keyConfig.hashKeyName]: item.pk,
    [keyConfig.sortKeyName]: item.sk,
  };
}

export function dynamoDbEngine(options: DynamoDbEngineOptions): DynamoDbQueryEngine {
  const { client, tableName } = options;
  const keyConfig = normalizeKeyConfig(options);

  const engine: DynamoDbQueryEngine = {
    capabilities: {
      uniqueConstraints: "atomic",
    },

    async get(collection, key) {
      const item = await getDocumentItem(client, tableName, keyConfig, collection, key);

      if (!item) {
        return null;
      }

      return structuredClone(item.doc);
    },

    async getWithMetadata(collection, key) {
      const item = await getDocumentItem(client, tableName, keyConfig, collection, key);

      if (!item) {
        return null;
      }

      return {
        doc: structuredClone(item.doc),
        writeToken: String(item.writeVersion),
      };
    },

    async create(collection, key, doc, indexes, _options, migrationMetadata) {
      const createdAt = await nextSequence(client, tableName, keyConfig, collection);
      const item = createDocumentItem(collection, key, createdAt, 1, doc, indexes);
      const metadata =
        normalizeMigrationMetadata(migrationMetadata) ?? deriveLegacyMetadataFromDocument(doc);
      const metadataItem = createMigrationMetadataItem(collection, key, metadata);

      try {
        await putDocumentAndMetadata(client, tableName, keyConfig, item, metadataItem, {
          requireDocumentToNotExist: true,
        });
      } catch (error) {
        if (isConditionalCheckFailed(error) || isTransactionConditionalCheckFailed(error)) {
          throw new EngineDocumentAlreadyExistsError(collection, key);
        }

        throw error;
      }
    },

    async put(collection, key, doc, indexes, _options, migrationMetadata) {
      const existing = await getDocumentItem(client, tableName, keyConfig, collection, key);
      const createdAt =
        existing?.createdAt ?? (await nextSequence(client, tableName, keyConfig, collection));
      const writeVersion = (existing?.writeVersion ?? 0) + 1;
      const item = createDocumentItem(collection, key, createdAt, writeVersion, doc, indexes);
      const metadata =
        normalizeMigrationMetadata(migrationMetadata) ?? deriveLegacyMetadataFromDocument(doc);
      const metadataItem = createMigrationMetadataItem(collection, key, metadata);

      await putDocumentAndMetadata(client, tableName, keyConfig, item, metadataItem);
    },

    async update(collection, key, doc, indexes, _options, migrationMetadata) {
      const existing = await getDocumentItem(client, tableName, keyConfig, collection, key);

      if (!existing) {
        throw new EngineDocumentNotFoundError(collection, key);
      }

      const item = createDocumentItem(
        collection,
        key,
        existing.createdAt,
        existing.writeVersion + 1,
        doc,
        indexes,
      );
      const metadata =
        normalizeMigrationMetadata(migrationMetadata) ?? deriveLegacyMetadataFromDocument(doc);
      const metadataItem = createMigrationMetadataItem(collection, key, metadata);

      try {
        await putDocumentAndMetadata(client, tableName, keyConfig, item, metadataItem, {
          requireDocumentToExist: true,
        });
      } catch (error) {
        if (isConditionalCheckFailed(error) || isTransactionConditionalCheckFailed(error)) {
          throw new EngineDocumentNotFoundError(collection, key);
        }

        throw error;
      }
    },

    async delete(collection, key) {
      await deleteDocumentAndMetadata(client, tableName, keyConfig, collection, key);
    },

    async query(collection, params) {
      const records = await listDocuments(client, tableName, keyConfig, collection);
      const matched = matchDocuments(records, params);

      return paginate(matched, params);
    },

    async queryWithMetadata(collection, params) {
      const records = await listDocuments(client, tableName, keyConfig, collection);
      const matched = matchDocuments(records, params);

      return paginateWithWriteTokens(matched, params);
    },

    async batchGet(collection, keys) {
      const results: KeyedDocument[] = [];
      const fetched = await batchGetDocuments(client, tableName, keyConfig, collection, keys);

      for (const key of keys) {
        const record = fetched.get(key);

        if (!record) {
          continue;
        }

        results.push({ key, doc: structuredClone(record.doc) });
      }

      return results;
    },

    async batchGetWithMetadata(collection, keys) {
      const results: KeyedDocument[] = [];
      const fetched = await batchGetDocuments(client, tableName, keyConfig, collection, keys);

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
      const keys = items.map((item) => item.key);
      const existing = await batchGetDocuments(client, tableName, keyConfig, collection, keys);
      const writes: Array<{
        docItem: StoredDocumentItem;
        metadataItem: MigrationMetadataItem;
      }> = [];

      for (const item of items) {
        const existingRecord = existing.get(item.key);
        const createdAt =
          existingRecord?.createdAt ??
          (await nextSequence(client, tableName, keyConfig, collection));
        const writeVersion = (existingRecord?.writeVersion ?? 0) + 1;
        const metadata =
          normalizeMigrationMetadata(item.migrationMetadata) ??
          deriveLegacyMetadataFromDocument(item.doc);

        writes.push({
          docItem: createDocumentItem(
            collection,
            item.key,
            createdAt,
            writeVersion,
            item.doc,
            item.indexes,
          ),
          metadataItem: createMigrationMetadataItem(collection, item.key, metadata),
        });
      }

      await putDocumentAndMetadataBatch(client, tableName, keyConfig, writes);
    },

    async batchSetWithResult(collection, items) {
      const persistedKeys: string[] = [];
      const conflictedKeys: string[] = [];
      const keys = items.map((item) => item.key);
      const existing = await batchGetDocuments(client, tableName, keyConfig, collection, keys);

      for (const item of items) {
        const existingRecord = existing.get(item.key);
        const metadata =
          normalizeMigrationMetadata(item.migrationMetadata) ??
          deriveLegacyMetadataFromDocument(item.doc);
        const metadataItem = createMigrationMetadataItem(collection, item.key, metadata);
        const expectedWriteVersion = parseExpectedWriteVersion(item.expectedWriteToken);

        if (expectedWriteVersion !== undefined) {
          if (!existingRecord || existingRecord.writeVersion !== expectedWriteVersion) {
            conflictedKeys.push(item.key);
            continue;
          }

          const docItem = createDocumentItem(
            collection,
            item.key,
            existingRecord.createdAt,
            expectedWriteVersion + 1,
            item.doc,
            item.indexes,
          );

          try {
            await putDocumentAndMetadata(client, tableName, keyConfig, docItem, metadataItem, {
              expectedWriteVersion,
            });
            persistedKeys.push(item.key);
          } catch (error) {
            if (isConditionalCheckFailed(error) || isTransactionConditionalCheckFailed(error)) {
              conflictedKeys.push(item.key);
              continue;
            }

            throw error;
          }

          continue;
        }

        const createdAt =
          existingRecord?.createdAt ??
          (await nextSequence(client, tableName, keyConfig, collection));
        const writeVersion = (existingRecord?.writeVersion ?? 0) + 1;
        const docItem = createDocumentItem(
          collection,
          item.key,
          createdAt,
          writeVersion,
          item.doc,
          item.indexes,
        );
        await putDocumentAndMetadata(client, tableName, keyConfig, docItem, metadataItem);
        persistedKeys.push(item.key);
      }

      return {
        persistedKeys,
        conflictedKeys,
      };
    },

    async batchDelete(collection, keys) {
      await deleteDocumentAndMetadataBatch(client, tableName, keyConfig, collection, keys);
    },

    migration: {
      async acquireLock(collection, options?) {
        const lockKey = lockSortKey(collection);
        const now = Date.now();
        const ttl = options?.ttl;
        const allowSteal = ttl !== undefined && Number.isFinite(ttl) && ttl >= 0;

        const lock: LockItem = {
          pk: META_PARTITION_KEY,
          sk: lockKey,
          itemType: ITEM_TYPE_LOCK,
          lockId: randomId(),
          acquiredAt: now,
        };

        const putInput: {
          TableName: string;
          Item: Record<string, unknown>;
          ConditionExpression: string;
          ExpressionAttributeNames: Record<string, string>;
          ExpressionAttributeValues?: Record<string, unknown>;
        } = {
          TableName: tableName,
          Item: serializeItemForStorage(lock, keyConfig),
          ConditionExpression: "attribute_not_exists(#pk)",
          ExpressionAttributeNames: {
            "#pk": keyConfig.hashKeyName,
          },
        };

        if (allowSteal) {
          putInput.ConditionExpression = "attribute_not_exists(#pk) OR #acquiredAt <= :staleBefore";
          putInput.ExpressionAttributeNames["#acquiredAt"] = "acquiredAt";
          putInput.ExpressionAttributeValues = {
            ":staleBefore": now - ttl,
          };
        }

        try {
          await client.send(new PutCommand(putInput));
        } catch (error) {
          if (isConditionalCheckFailed(error)) {
            return null;
          }

          throw error;
        }

        return {
          id: lock.lockId,
          collection,
          acquiredAt: lock.acquiredAt,
        };
      },

      async releaseLock(lock) {
        try {
          await client.send(
            new DeleteCommand({
              TableName: tableName,
              Key: toDynamoPrimaryKey(keyConfig, META_PARTITION_KEY, lockSortKey(lock.collection)),
              ConditionExpression: "#lockId = :lockId",
              ExpressionAttributeNames: {
                "#lockId": "lockId",
              },
              ExpressionAttributeValues: {
                ":lockId": lock.id,
              },
            }),
          );
        } catch (error) {
          if (!isConditionalCheckFailed(error)) {
            throw error;
          }
        }
      },

      async getOutdated(collection, criteria, cursor) {
        if (criteria.skipMetadataSyncHint !== true) {
          await syncMigrationMetadataForCriteria(
            client,
            tableName,
            keyConfig,
            collection,
            criteria,
          );
        }
        return getOutdatedDocuments(client, tableName, keyConfig, collection, criteria, cursor);
      },

      async saveCheckpoint(lock, cursor) {
        const checkpoint: CheckpointItem = {
          pk: META_PARTITION_KEY,
          sk: checkpointSortKey(lock.collection),
          itemType: ITEM_TYPE_CHECKPOINT,
          cursor,
        };

        try {
          await client.send(
            new TransactWriteCommand({
              TransactItems: [
                {
                  ConditionCheck: {
                    TableName: tableName,
                    Key: toDynamoPrimaryKey(
                      keyConfig,
                      META_PARTITION_KEY,
                      lockSortKey(lock.collection),
                    ),
                    ConditionExpression: "#lockId = :lockId",
                    ExpressionAttributeNames: {
                      "#lockId": "lockId",
                    },
                    ExpressionAttributeValues: {
                      ":lockId": lock.id,
                    },
                  },
                },
                {
                  Put: {
                    TableName: tableName,
                    Item: serializeItemForStorage(checkpoint, keyConfig),
                  },
                },
              ],
            }),
          );
        } catch (error) {
          if (!isConditionalCheckFailed(error) && !isTransactionConditionalCheckFailed(error)) {
            throw error;
          }
        }
      },

      async loadCheckpoint(collection) {
        const raw = await getMetaItem(client, tableName, keyConfig, checkpointSortKey(collection));

        if (!raw) {
          return null;
        }

        return parseCheckpointItem(raw, keyConfig).cursor;
      },

      async clearCheckpoint(collection) {
        await client.send(
          new DeleteCommand({
            TableName: tableName,
            Key: toDynamoPrimaryKey(keyConfig, META_PARTITION_KEY, checkpointSortKey(collection)),
          }),
        );
      },

      async getStatus(collection) {
        const lockRaw = await getMetaItem(client, tableName, keyConfig, lockSortKey(collection));

        if (!lockRaw) {
          return null;
        }

        const lock = parseLockItem(lockRaw, keyConfig);
        const checkpointRaw = await getMetaItem(
          client,
          tableName,
          keyConfig,
          checkpointSortKey(collection),
        );

        return {
          lock: {
            id: lock.lockId,
            collection,
            acquiredAt: lock.acquiredAt,
          },
          cursor: checkpointRaw ? parseCheckpointItem(checkpointRaw, keyConfig).cursor : null,
        };
      },
    },
  };

  engine.migrator = new DefaultMigrator(engine);

  return engine;
}

function collectionPartitionKey(collection: string): string {
  return `COL#${collection}`;
}

function documentSortKey(key: string): string {
  return `DOC#${key}`;
}

function lockSortKey(collection: string): string {
  return `LOCK#${collection}`;
}

function checkpointSortKey(collection: string): string {
  return `CHECKPOINT#${collection}`;
}

function sequenceSortKey(collection: string): string {
  return `SEQUENCE#${collection}`;
}

function metadataSortKey(key: string): string {
  return `META#${key}`;
}

function migrationOutdatedPartitionKey(
  collection: string,
  targetVersion: number,
  versionState: string,
): string {
  return `MIG#${collection}#V#${String(targetVersion)}#STATE#${versionState}`;
}

function migrationSyncPartitionKey(collection: string): string {
  return `MIGSYNC#${collection}`;
}

function parseDocumentItem(value: unknown, keyConfig: DynamoKeyConfig): StoredDocumentItem {
  if (!isRecord(value)) {
    throw new Error("DynamoDB returned an invalid document item");
  }

  const pk = value[keyConfig.hashKeyName];
  const sk = value[keyConfig.sortKeyName];
  const itemType = value.itemType;
  const collection = value.collection;
  const key = value.key;
  const createdAt = value.createdAt;
  const writeVersion = value.writeVersion;
  const doc = value.doc;
  const indexes = value.indexes;

  if (
    typeof pk !== "string" ||
    typeof sk !== "string" ||
    itemType !== ITEM_TYPE_DOC ||
    typeof collection !== "string" ||
    typeof key !== "string" ||
    typeof createdAt !== "number" ||
    !Number.isFinite(createdAt) ||
    typeof writeVersion !== "number" ||
    !Number.isFinite(writeVersion) ||
    Math.floor(writeVersion) !== writeVersion ||
    writeVersion <= 0 ||
    !isRecord(doc) ||
    !isRecord(indexes)
  ) {
    throw new Error("DynamoDB returned an invalid document item");
  }

  const resolvedIndexes: ResolvedIndexKeys = {};

  for (const [name, indexValue] of Object.entries(indexes)) {
    if (typeof indexValue !== "string") {
      throw new Error("DynamoDB returned an invalid document item");
    }

    resolvedIndexes[name] = indexValue;
  }

  return {
    pk,
    sk,
    itemType,
    collection,
    key,
    createdAt,
    writeVersion,
    doc: doc as Record<string, unknown>,
    indexes: resolvedIndexes,
  };
}

function parseLockItem(value: unknown, keyConfig: DynamoKeyConfig): LockItem {
  if (!isRecord(value)) {
    throw new Error("DynamoDB returned an invalid migration lock item");
  }

  const pk = value[keyConfig.hashKeyName];
  const sk = value[keyConfig.sortKeyName];
  const itemType = value.itemType;
  const lockId = value.lockId;
  const acquiredAt = value.acquiredAt;

  if (
    pk !== META_PARTITION_KEY ||
    typeof sk !== "string" ||
    itemType !== ITEM_TYPE_LOCK ||
    typeof lockId !== "string" ||
    typeof acquiredAt !== "number" ||
    !Number.isFinite(acquiredAt)
  ) {
    throw new Error("DynamoDB returned an invalid migration lock item");
  }

  return { pk, sk, itemType, lockId, acquiredAt };
}

function parseCheckpointItem(value: unknown, keyConfig: DynamoKeyConfig): CheckpointItem {
  if (!isRecord(value)) {
    throw new Error("DynamoDB returned an invalid migration checkpoint item");
  }

  const pk = value[keyConfig.hashKeyName];
  const sk = value[keyConfig.sortKeyName];
  const itemType = value.itemType;
  const cursor = value.cursor;

  if (
    pk !== META_PARTITION_KEY ||
    typeof sk !== "string" ||
    itemType !== ITEM_TYPE_CHECKPOINT ||
    typeof cursor !== "string"
  ) {
    throw new Error("DynamoDB returned an invalid migration checkpoint item");
  }

  return { pk, sk, itemType, cursor };
}

function parseMigrationMetadataItem(
  value: unknown,
  keyConfig: DynamoKeyConfig,
): MigrationMetadataItem {
  if (!isRecord(value)) {
    throw new Error("DynamoDB returned an invalid migration metadata item");
  }

  const pk = value[keyConfig.hashKeyName];
  const sk = value[keyConfig.sortKeyName];
  const itemType = value.itemType;
  const collection = value.collection;
  const key = value.key;
  const targetVersion = value.targetVersion;
  const versionState = value.versionState;
  const indexSignature = value.indexSignature;
  const migrationOutdatedPk = value.migrationOutdatedPk;
  const migrationOutdatedSk = value.migrationOutdatedSk;
  const migrationSyncPk = value.migrationSyncPk;
  const migrationSyncSk = value.migrationSyncSk;

  if (
    typeof pk !== "string" ||
    typeof sk !== "string" ||
    itemType !== ITEM_TYPE_MIGRATION_METADATA ||
    typeof collection !== "string" ||
    typeof key !== "string" ||
    typeof targetVersion !== "number" ||
    !Number.isFinite(targetVersion) ||
    Math.floor(targetVersion) !== targetVersion ||
    targetVersion < 0 ||
    !isMigrationVersionState(versionState) ||
    (indexSignature !== null && typeof indexSignature !== "string") ||
    typeof migrationOutdatedPk !== "string" ||
    typeof migrationOutdatedSk !== "string" ||
    typeof migrationSyncPk !== "string" ||
    typeof migrationSyncSk !== "number" ||
    !Number.isFinite(migrationSyncSk)
  ) {
    throw new Error("DynamoDB returned an invalid migration metadata item");
  }

  return {
    pk,
    sk,
    itemType,
    collection,
    key,
    targetVersion,
    versionState,
    indexSignature,
    migrationOutdatedPk,
    migrationOutdatedSk,
    migrationSyncPk,
    migrationSyncSk,
  };
}

async function nextSequence(
  client: DynamoDbDocumentClientLike,
  tableName: string,
  keyConfig: DynamoKeyConfig,
  collection: string,
): Promise<number> {
  const sk = sequenceSortKey(collection);

  const response = (await client.send(
    new UpdateCommand({
      TableName: tableName,
      Key: toDynamoPrimaryKey(keyConfig, META_PARTITION_KEY, sk),
      UpdateExpression: "SET #itemType = :itemType, #value = if_not_exists(#value, :zero) + :inc",
      ExpressionAttributeNames: {
        "#itemType": "itemType",
        "#value": "value",
      },
      ExpressionAttributeValues: {
        ":itemType": "sequence",
        ":zero": 0,
        ":inc": 1,
      },
      ReturnValues: "UPDATED_NEW",
    }),
  )) as {
    Attributes?: {
      value?: unknown;
    };
  };

  const value = response.Attributes?.value;

  if (typeof value !== "number" || !Number.isFinite(value)) {
    throw new Error("DynamoDB returned an invalid sequence value");
  }

  return value;
}

function createDocumentItem(
  collection: string,
  key: string,
  createdAt: number,
  writeVersion: number,
  doc: unknown,
  indexes: ResolvedIndexKeys,
): StoredDocumentItem {
  const cloned = structuredClone(doc);

  if (!isRecord(cloned)) {
    throw new Error("DynamoDB received a non-object document");
  }

  return {
    pk: collectionPartitionKey(collection),
    sk: documentSortKey(key),
    itemType: ITEM_TYPE_DOC,
    collection,
    key,
    createdAt,
    writeVersion,
    doc: cloned,
    indexes: { ...indexes },
  };
}

function createMigrationMetadataItem(
  collection: string,
  key: string,
  metadata: MigrationMetadata,
): MigrationMetadataItem {
  return {
    pk: collectionPartitionKey(collection),
    sk: metadataSortKey(key),
    itemType: ITEM_TYPE_MIGRATION_METADATA,
    collection,
    key,
    targetVersion: metadata.targetVersion,
    versionState: metadata.versionState,
    indexSignature: metadata.indexSignature,
    migrationOutdatedPk: migrationOutdatedPartitionKey(
      collection,
      metadata.targetVersion,
      metadata.versionState,
    ),
    migrationOutdatedSk: migrationOutdatedSortKey(metadata.indexSignature, key),
    migrationSyncPk: migrationSyncPartitionKey(collection),
    migrationSyncSk: metadata.targetVersion,
  };
}

async function getMetaItem(
  client: DynamoDbDocumentClientLike,
  tableName: string,
  keyConfig: DynamoKeyConfig,
  sk: string,
): Promise<Record<string, unknown> | null> {
  const response = (await client.send(
    new GetCommand({
      TableName: tableName,
      Key: toDynamoPrimaryKey(keyConfig, META_PARTITION_KEY, sk),
    }),
  )) as {
    Item?: unknown;
  };

  if (!response.Item) {
    return null;
  }

  if (!isRecord(response.Item)) {
    throw new Error("DynamoDB returned an invalid meta item");
  }

  return response.Item;
}

async function getDocumentItem(
  client: DynamoDbDocumentClientLike,
  tableName: string,
  keyConfig: DynamoKeyConfig,
  collection: string,
  key: string,
): Promise<StoredDocumentItem | null> {
  const response = (await client.send(
    new GetCommand({
      TableName: tableName,
      Key: toDynamoPrimaryKey(keyConfig, collectionPartitionKey(collection), documentSortKey(key)),
    }),
  )) as {
    Item?: unknown;
  };

  if (!response.Item) {
    return null;
  }

  return parseDocumentItem(response.Item, keyConfig);
}

async function listDocuments(
  client: DynamoDbDocumentClientLike,
  tableName: string,
  keyConfig: DynamoKeyConfig,
  collection: string,
): Promise<StoredDocumentItem[]> {
  const pk = collectionPartitionKey(collection);
  const records: StoredDocumentItem[] = [];
  let cursor: Record<string, unknown> | undefined;

  do {
    const input: QueryCommandInput = {
      TableName: tableName,
      KeyConditionExpression: "#pk = :pk AND begins_with(#sk, :docPrefix)",
      ExpressionAttributeNames: {
        "#pk": keyConfig.hashKeyName,
        "#sk": keyConfig.sortKeyName,
      },
      ExpressionAttributeValues: {
        ":pk": pk,
        ":docPrefix": "DOC#",
      },
      ExclusiveStartKey: cursor,
    };

    const response = (await client.send(new QueryCommand(input))) as {
      Items?: unknown[];
      LastEvaluatedKey?: Record<string, unknown>;
    };

    const items = response.Items ?? [];

    for (const item of items) {
      const parsed = parseDocumentItem(item, keyConfig);
      records.push(parsed);
    }

    cursor = response.LastEvaluatedKey;
  } while (cursor);

  records.sort((a, b) => {
    if (a.createdAt !== b.createdAt) {
      return a.createdAt - b.createdAt;
    }

    return a.key.localeCompare(b.key);
  });

  return records;
}

async function batchGetDocuments(
  client: DynamoDbDocumentClientLike,
  tableName: string,
  keyConfig: DynamoKeyConfig,
  collection: string,
  keys: string[],
): Promise<Map<string, StoredDocumentItem>> {
  const result = new Map<string, StoredDocumentItem>();

  for (const chunk of chunkArray(keys, MAX_BATCH_GET)) {
    let pendingKeys = uniqueDynamoKeys(
      chunk.map((key) => ({
        pk: collectionPartitionKey(collection),
        sk: documentSortKey(key),
      })),
    );

    let attempts = 0;

    while (pendingKeys.length > 0) {
      attempts += 1;

      if (attempts > BATCH_RETRY_LIMIT) {
        throw new Error("DynamoDB batchGet exceeded retry limit due to unprocessed keys");
      }

      const input: BatchGetCommandInput = {
        RequestItems: {
          [tableName]: {
            Keys: pendingKeys.map((key) => toDynamoPrimaryKey(keyConfig, key.pk, key.sk)),
          },
        },
      };

      const response = (await client.send(new BatchGetCommand(input))) as {
        Responses?: Record<string, unknown[]>;
        UnprocessedKeys?: Record<string, { Keys?: Array<Record<string, unknown>> }>;
      };

      const items = response.Responses?.[tableName] ?? [];

      for (const raw of items) {
        const item = parseDocumentItem(raw, keyConfig);
        result.set(item.key, item);
      }

      pendingKeys = (response.UnprocessedKeys?.[tableName]?.Keys ?? []).map((rawKey) =>
        parseDynamoPrimaryKey(rawKey, keyConfig),
      );
    }
  }

  return result;
}

async function batchWriteAll(
  client: DynamoDbDocumentClientLike,
  tableName: string,
  requests: BatchWriteRequest[],
): Promise<void> {
  for (const chunk of chunkArray(requests, MAX_BATCH_WRITE)) {
    let pending = chunk;
    let attempts = 0;

    while (pending.length > 0) {
      attempts += 1;

      if (attempts > BATCH_RETRY_LIMIT) {
        throw new Error("DynamoDB batchWrite exceeded retry limit due to unprocessed items");
      }

      const input: BatchWriteCommandInput = {
        RequestItems: {
          [tableName]: pending,
        },
      };

      const response = (await client.send(new BatchWriteCommand(input))) as {
        UnprocessedItems?: Record<string, BatchWriteRequest[]>;
      };

      pending = response.UnprocessedItems?.[tableName] ?? [];
    }
  }
}

interface PutDocumentAndMetadataOptions {
  requireDocumentToExist?: boolean;
  requireDocumentToNotExist?: boolean;
  expectedWriteVersion?: number;
}

async function putDocumentAndMetadata(
  client: DynamoDbDocumentClientLike,
  tableName: string,
  keyConfig: DynamoKeyConfig,
  docItem: StoredDocumentItem,
  metadataItem: MigrationMetadataItem,
  options?: PutDocumentAndMetadataOptions,
): Promise<void> {
  const expressionAttributeNames: Record<string, string> = {};
  const expressionAttributeValues: Record<string, unknown> = {};
  let conditionExpression: string | undefined;

  if (options?.requireDocumentToNotExist) {
    expressionAttributeNames["#pk"] = keyConfig.hashKeyName;
    expressionAttributeNames["#sk"] = keyConfig.sortKeyName;
    conditionExpression = "attribute_not_exists(#pk) AND attribute_not_exists(#sk)";
  } else if (options?.requireDocumentToExist) {
    expressionAttributeNames["#pk"] = keyConfig.hashKeyName;
    expressionAttributeNames["#sk"] = keyConfig.sortKeyName;
    conditionExpression = "attribute_exists(#pk) AND attribute_exists(#sk)";
  }

  if (options?.expectedWriteVersion !== undefined) {
    expressionAttributeNames["#writeVersion"] = "writeVersion";
    expressionAttributeValues[":expectedWriteVersion"] = options.expectedWriteVersion;
    conditionExpression = `${conditionExpression ? `${conditionExpression} AND ` : ""}#writeVersion = :expectedWriteVersion`;
  }

  await client.send(
    new TransactWriteCommand({
      TransactItems: [
        {
          Put: {
            TableName: tableName,
            Item: serializeItemForStorage(docItem, keyConfig),
            ...(conditionExpression ? { ConditionExpression: conditionExpression } : {}),
            ...(conditionExpression && Object.keys(expressionAttributeNames).length > 0
              ? { ExpressionAttributeNames: expressionAttributeNames }
              : {}),
            ...(conditionExpression && Object.keys(expressionAttributeValues).length > 0
              ? { ExpressionAttributeValues: expressionAttributeValues }
              : {}),
          },
        },
        {
          Put: {
            TableName: tableName,
            Item: serializeItemForStorage(metadataItem, keyConfig),
          },
        },
      ],
    }),
  );
}

async function putDocumentAndMetadataBatch(
  client: DynamoDbDocumentClientLike,
  tableName: string,
  keyConfig: DynamoKeyConfig,
  writes: Array<{
    docItem: StoredDocumentItem;
    metadataItem: MigrationMetadataItem;
  }>,
): Promise<void> {
  for (const chunk of chunkArray(writes, 12)) {
    const transactItems: Array<{
      Put: {
        TableName: string;
        Item: Record<string, unknown>;
      };
    }> = [];

    for (const entry of chunk) {
      transactItems.push({
        Put: {
          TableName: tableName,
          Item: serializeItemForStorage(entry.docItem, keyConfig),
        },
      });
      transactItems.push({
        Put: {
          TableName: tableName,
          Item: serializeItemForStorage(entry.metadataItem, keyConfig),
        },
      });
    }

    await client.send(
      new TransactWriteCommand({
        TransactItems: transactItems,
      }),
    );
  }
}

async function deleteDocumentAndMetadata(
  client: DynamoDbDocumentClientLike,
  tableName: string,
  keyConfig: DynamoKeyConfig,
  collection: string,
  key: string,
): Promise<void> {
  await client.send(
    new TransactWriteCommand({
      TransactItems: [
        {
          Delete: {
            TableName: tableName,
            Key: toDynamoPrimaryKey(
              keyConfig,
              collectionPartitionKey(collection),
              documentSortKey(key),
            ),
          },
        },
        {
          Delete: {
            TableName: tableName,
            Key: toDynamoPrimaryKey(
              keyConfig,
              collectionPartitionKey(collection),
              metadataSortKey(key),
            ),
          },
        },
      ],
    }),
  );
}

async function deleteDocumentAndMetadataBatch(
  client: DynamoDbDocumentClientLike,
  tableName: string,
  keyConfig: DynamoKeyConfig,
  collection: string,
  keys: string[],
): Promise<void> {
  const uniqueKeys = uniqueStrings(keys);

  for (const chunk of chunkArray(uniqueKeys, 12)) {
    const transactItems: Array<{
      Delete: {
        TableName: string;
        Key: {
          [key: string]: string;
        };
      };
    }> = [];

    for (const key of chunk) {
      transactItems.push({
        Delete: {
          TableName: tableName,
          Key: toDynamoPrimaryKey(
            keyConfig,
            collectionPartitionKey(collection),
            documentSortKey(key),
          ),
        },
      });
      transactItems.push({
        Delete: {
          TableName: tableName,
          Key: toDynamoPrimaryKey(
            keyConfig,
            collectionPartitionKey(collection),
            metadataSortKey(key),
          ),
        },
      });
    }

    await client.send(
      new TransactWriteCommand({
        TransactItems: transactItems,
      }),
    );
  }
}

async function syncMigrationMetadataForCriteria(
  client: DynamoDbDocumentClientLike,
  tableName: string,
  keyConfig: DynamoKeyConfig,
  collection: string,
  criteria: MigrationCriteria,
): Promise<void> {
  const queue = await collectMetadataNeedingSync(
    client,
    tableName,
    keyConfig,
    collection,
    criteria.version,
  );

  for (const chunk of chunkArray(queue, METADATA_SYNC_CHUNK_SIZE)) {
    if (chunk.length === 0) {
      continue;
    }

    const docs = await batchGetDocuments(
      client,
      tableName,
      keyConfig,
      collection,
      chunk.map((item) => item.key),
    );
    const requests: BatchWriteRequest[] = [];

    for (const candidate of chunk) {
      const docItem = docs.get(candidate.key);

      if (!docItem) {
        requests.push({
          DeleteRequest: {
            Key: toDynamoPrimaryKey(
              keyConfig,
              collectionPartitionKey(collection),
              metadataSortKey(candidate.key),
            ),
          },
        });
        continue;
      }

      const derived = deriveMetadataForCriteria(docItem.doc, criteria);
      const metadataItem = createMigrationMetadataItem(collection, candidate.key, derived);
      requests.push({
        PutRequest: {
          Item: serializeItemForStorage(metadataItem, keyConfig),
        },
      });
    }

    await batchWriteAll(client, tableName, requests);
  }
}

async function collectMetadataNeedingSync(
  client: DynamoDbDocumentClientLike,
  tableName: string,
  keyConfig: DynamoKeyConfig,
  collection: string,
  targetVersion: number,
): Promise<MigrationMetadataItem[]> {
  const candidates: MigrationMetadataItem[] = [];
  const syncPk = migrationSyncPartitionKey(collection);

  let staleStartKey: Record<string, unknown> | undefined;
  do {
    const response = (await client.send(
      new QueryCommand({
        TableName: tableName,
        IndexName: keyConfig.migrationSyncIndexName,
        KeyConditionExpression: "#pk = :pk AND #sk < :targetVersion",
        ExpressionAttributeNames: {
          "#pk": "migrationSyncPk",
          "#sk": "migrationSyncSk",
        },
        ExpressionAttributeValues: {
          ":pk": syncPk,
          ":targetVersion": targetVersion,
        },
        ExclusiveStartKey: staleStartKey,
      }),
    )) as {
      Items?: unknown[];
      LastEvaluatedKey?: Record<string, unknown>;
    };

    for (const raw of response.Items ?? []) {
      candidates.push(parseMigrationMetadataItem(raw, keyConfig));
    }

    staleStartKey = response.LastEvaluatedKey;
  } while (staleStartKey);

  let aheadStartKey: Record<string, unknown> | undefined;
  do {
    const response = (await client.send(
      new QueryCommand({
        TableName: tableName,
        IndexName: keyConfig.migrationSyncIndexName,
        KeyConditionExpression: "#pk = :pk AND #sk > :targetVersion",
        ExpressionAttributeNames: {
          "#pk": "migrationSyncPk",
          "#sk": "migrationSyncSk",
        },
        ExpressionAttributeValues: {
          ":pk": syncPk,
          ":targetVersion": targetVersion,
        },
        ExclusiveStartKey: aheadStartKey,
      }),
    )) as {
      Items?: unknown[];
      LastEvaluatedKey?: Record<string, unknown>;
    };

    for (const raw of response.Items ?? []) {
      candidates.push(parseMigrationMetadataItem(raw, keyConfig));
    }

    aheadStartKey = response.LastEvaluatedKey;
  } while (aheadStartKey);

  return candidates;
}

async function getOutdatedDocuments(
  client: DynamoDbDocumentClientLike,
  tableName: string,
  keyConfig: DynamoKeyConfig,
  collection: string,
  criteria: MigrationCriteria,
  cursor: string | undefined,
): Promise<EngineQueryResult> {
  const pageLimit = normalizeOutdatedPageLimit(criteria.pageSizeHint);
  const expectedSignature = computeIndexSignature(criteria.indexes);
  // Cursor carries both phase and criteria fingerprint so old cursors cannot
  // be reused after schema/index expectations change.
  const state = decodeOutdatedCursor(cursor, criteria.version, expectedSignature);
  const remaining = pageLimit;
  const metadata: MigrationMetadataItem[] = [];
  let remainingSlots = remaining;
  let nextCursor: string | null = null;

  if (state.phase === "stale") {
    // Phase 1: strictly outdated versions (safe, high-priority migration set).
    const stale = await queryMigrationMetadataPage(client, tableName, keyConfig, {
      indexName: keyConfig.migrationOutdatedIndexName,
      keyConditionExpression: "#pk = :pk",
      expressionAttributeNames: {
        "#pk": "migrationOutdatedPk",
      },
      expressionAttributeValues: {
        ":pk": migrationOutdatedPartitionKey(collection, criteria.version, "stale"),
      },
      exclusiveStartKey: state.staleStartKey,
      limit: remainingSlots,
    });

    metadata.push(...stale.items);
    remainingSlots -= stale.items.length;

    if (remainingSlots === 0) {
      nextCursor = encodeOutdatedCursor({
        phase: stale.lastEvaluatedKey ? "stale" : "current-low",
        staleStartKey: stale.lastEvaluatedKey,
        criteriaVersion: criteria.version,
        expectedSignature,
      });
    } else {
      state.phase = "current-low";
      state.currentLowStartKey = undefined;
    }
  }

  if (nextCursor === null && remainingSlots > 0 && state.phase === "current-low") {
    // Phase 2: "current" docs whose index signature sorts before expected.
    const expectedPrefix = migrationOutdatedSignaturePrefix(expectedSignature);
    const low = await queryMigrationMetadataPage(client, tableName, keyConfig, {
      indexName: keyConfig.migrationOutdatedIndexName,
      keyConditionExpression: "#pk = :pk AND #sk < :upper",
      expressionAttributeNames: {
        "#pk": "migrationOutdatedPk",
        "#sk": "migrationOutdatedSk",
      },
      expressionAttributeValues: {
        ":pk": migrationOutdatedPartitionKey(collection, criteria.version, "current"),
        ":upper": expectedPrefix,
      },
      exclusiveStartKey: state.currentLowStartKey,
      limit: remainingSlots,
    });

    metadata.push(...low.items);
    remainingSlots -= low.items.length;

    if (remainingSlots === 0) {
      nextCursor = encodeOutdatedCursor({
        phase: low.lastEvaluatedKey ? "current-low" : "current-high",
        currentLowStartKey: low.lastEvaluatedKey,
        criteriaVersion: criteria.version,
        expectedSignature,
      });
    } else {
      state.phase = "current-high";
      state.currentHighStartKey = undefined;
    }
  }

  if (nextCursor === null && remainingSlots > 0 && state.phase === "current-high") {
    // Phase 3: "current" docs whose index signature sorts after expected.
    const expectedAfter = migrationOutdatedSignatureAfter(expectedSignature);
    const high = await queryMigrationMetadataPage(client, tableName, keyConfig, {
      indexName: keyConfig.migrationOutdatedIndexName,
      keyConditionExpression: "#pk = :pk AND #sk > :lower",
      expressionAttributeNames: {
        "#pk": "migrationOutdatedPk",
        "#sk": "migrationOutdatedSk",
      },
      expressionAttributeValues: {
        ":pk": migrationOutdatedPartitionKey(collection, criteria.version, "current"),
        ":lower": expectedAfter,
      },
      exclusiveStartKey: state.currentHighStartKey,
      limit: remainingSlots,
    });

    metadata.push(...high.items);
    remainingSlots -= high.items.length;

    if (remainingSlots === 0 && high.lastEvaluatedKey) {
      nextCursor = encodeOutdatedCursor({
        phase: "current-high",
        currentHighStartKey: high.lastEvaluatedKey,
        criteriaVersion: criteria.version,
        expectedSignature,
      });
    }
  }

  const docsByKey = await batchGetDocuments(
    client,
    tableName,
    keyConfig,
    collection,
    metadata.map((item) => item.key),
  );
  const documents: KeyedDocument[] = [];

  for (const item of metadata) {
    const doc = docsByKey.get(item.key);

    if (!doc) {
      continue;
    }

    documents.push({
      key: item.key,
      doc: structuredClone(doc.doc),
      writeToken: String(doc.writeVersion),
    });
  }

  return {
    documents,
    cursor: nextCursor,
  };
}

function normalizeOutdatedPageLimit(value: number | undefined): number {
  if (value === undefined || !Number.isFinite(value)) {
    return OUTDATED_PAGE_LIMIT;
  }

  return Math.max(1, Math.floor(value));
}

async function queryMigrationMetadataPage(
  client: DynamoDbDocumentClientLike,
  tableName: string,
  keyConfig: DynamoKeyConfig,
  args: {
    indexName: string;
    keyConditionExpression: string;
    expressionAttributeNames: Record<string, string>;
    expressionAttributeValues: Record<string, unknown>;
    exclusiveStartKey?: Record<string, unknown>;
    limit: number;
  },
): Promise<{ items: MigrationMetadataItem[]; lastEvaluatedKey?: Record<string, unknown> }> {
  if (args.limit <= 0) {
    return {
      items: [],
    };
  }

  const response = (await client.send(
    new QueryCommand({
      TableName: tableName,
      IndexName: args.indexName,
      KeyConditionExpression: args.keyConditionExpression,
      ExpressionAttributeNames: args.expressionAttributeNames,
      ExpressionAttributeValues: args.expressionAttributeValues,
      ExclusiveStartKey: args.exclusiveStartKey,
      Limit: args.limit,
    }),
  )) as {
    Items?: unknown[];
    LastEvaluatedKey?: Record<string, unknown>;
  };

  return {
    items: (response.Items ?? []).map((raw) => parseMigrationMetadataItem(raw, keyConfig)),
    lastEvaluatedKey: response.LastEvaluatedKey,
  };
}

function matchDocuments(records: StoredDocumentItem[], params: QueryParams): StoredDocumentItem[] {
  const indexName = params.index;
  const results: StoredDocumentItem[] = [];

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

function paginate(records: StoredDocumentItem[], params: QueryParams): EngineQueryResult {
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

function paginateWithWriteTokens(
  records: StoredDocumentItem[],
  params: QueryParams,
): EngineQueryResult {
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
    throw new Error("DynamoDB received invalid migration metadata target version");
  }

  if (!isMigrationVersionState(raw.versionState)) {
    throw new Error("DynamoDB received invalid migration metadata state");
  }

  if (raw.indexSignature !== null && typeof raw.indexSignature !== "string") {
    throw new Error("DynamoDB received invalid migration metadata index signature");
  }

  return {
    targetVersion: raw.targetVersion,
    versionState: raw.versionState,
    indexSignature: raw.indexSignature,
  };
}

function isMigrationVersionState(value: unknown): value is MigrationVersionState {
  return value === "current" || value === "stale" || value === "ahead" || value === "unknown";
}

function parseExpectedWriteVersion(token: string | undefined): number | undefined {
  if (token === undefined) {
    return undefined;
  }

  if (typeof token !== "string" || token.trim().length === 0) {
    throw new Error("DynamoDB received an invalid expected write token");
  }

  if (!/^\d+$/.test(token)) {
    throw new Error("DynamoDB received an invalid expected write token");
  }

  const parsed = Number(token);

  if (!Number.isFinite(parsed) || Math.floor(parsed) !== parsed || parsed <= 0) {
    throw new Error("DynamoDB received an invalid expected write token");
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

function migrationOutdatedSortKey(indexSignature: string | null, key: string): string {
  const signatureToken = encodeMigrationComponent(indexSignature ?? "~");
  const keyToken = encodeMigrationComponent(key);
  return `${MIGRATION_OUTDATED_KEY_PREFIX}${signatureToken}${MIGRATION_OUTDATED_KEY_DELIMITER}${keyToken}`;
}

function migrationOutdatedSignaturePrefix(indexSignature: string): string {
  const signatureToken = encodeMigrationComponent(indexSignature);
  return `${MIGRATION_OUTDATED_KEY_PREFIX}${signatureToken}${MIGRATION_OUTDATED_KEY_DELIMITER}`;
}

function migrationOutdatedSignatureAfter(indexSignature: string): string {
  return `${migrationOutdatedSignaturePrefix(indexSignature)}~`;
}

function encodeMigrationComponent(value: string): string {
  return Buffer.from(value, "utf8").toString("base64url");
}

function encodeOutdatedCursor(state: OutdatedCursorState): string {
  return Buffer.from(JSON.stringify(state), "utf8").toString("base64url");
}

function decodeOutdatedCursor(
  cursor: string | undefined,
  criteriaVersion: number,
  expectedSignature: string,
): OutdatedCursorState {
  const baseState: OutdatedCursorState = {
    phase: "stale",
    criteriaVersion,
    expectedSignature,
  };

  if (!cursor) {
    return baseState;
  }

  try {
    const decoded = Buffer.from(cursor, "base64url").toString("utf8");
    const parsed = JSON.parse(decoded) as Record<string, unknown>;

    if (!isRecord(parsed)) {
      return baseState;
    }

    const phase = parsed.phase;
    const parsedCriteriaVersion = parsed.criteriaVersion;
    const parsedExpectedSignature = parsed.expectedSignature;

    if (
      (phase !== "stale" && phase !== "current-low" && phase !== "current-high") ||
      typeof parsedCriteriaVersion !== "number" ||
      !Number.isFinite(parsedCriteriaVersion) ||
      parsedCriteriaVersion !== criteriaVersion ||
      typeof parsedExpectedSignature !== "string" ||
      parsedExpectedSignature !== expectedSignature
    ) {
      // Any mismatch means the cursor was produced for a different migration
      // window; restart from phase 1 to avoid skipping candidates.
      return baseState;
    }

    return {
      phase,
      criteriaVersion,
      expectedSignature,
      staleStartKey: isRecord(parsed.staleStartKey) ? parsed.staleStartKey : undefined,
      currentLowStartKey: isRecord(parsed.currentLowStartKey)
        ? parsed.currentLowStartKey
        : undefined,
      currentHighStartKey: isRecord(parsed.currentHighStartKey)
        ? parsed.currentHighStartKey
        : undefined,
    };
  } catch {
    return baseState;
  }
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

function isConditionalCheckFailed(error: unknown): boolean {
  if (isRecord(error) && typeof error.name === "string") {
    return error.name === "ConditionalCheckFailedException";
  }

  const message = error instanceof Error ? error.message : String(error);
  return message.includes("ConditionalCheckFailed");
}

function isTransactionConditionalCheckFailed(error: unknown): boolean {
  if (isRecord(error) && typeof error.name === "string") {
    return (
      error.name === "TransactionCanceledException" ||
      error.name === "ConditionalCheckFailedException"
    );
  }

  const message = error instanceof Error ? error.message : String(error);
  return (
    message.includes("TransactionCanceledException") || message.includes("ConditionalCheckFailed")
  );
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function chunkArray<T>(items: T[], size: number): T[][] {
  const chunks: T[][] = [];

  for (let i = 0; i < items.length; i += size) {
    chunks.push(items.slice(i, i + size));
  }

  return chunks;
}

function uniqueDynamoKeys(keys: DynamoPrimaryKey[]): DynamoPrimaryKey[] {
  const unique: DynamoPrimaryKey[] = [];
  const seen = new Set<string>();

  for (const key of keys) {
    const identity = `${key.pk}\u0000${key.sk}`;

    if (seen.has(identity)) {
      continue;
    }

    seen.add(identity);
    unique.push(key);
  }

  return unique;
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
