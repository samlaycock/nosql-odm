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

const MAX_BATCH_WRITE = 25;
const MAX_BATCH_GET = 100;
const BATCH_RETRY_LIMIT = 10;
const META_PARTITION_KEY = "META";
const OUTDATED_PAGE_LIMIT = 100;

interface DynamoDbDocumentClientLike {
  send(command: { input: unknown }): Promise<unknown>;
}

export interface DynamoDbEngineOptions {
  client: DynamoDbDocumentClientLike;
  tableName: string;
}

export interface DynamoDbQueryEngine extends QueryEngine<never> {}

interface StoredDocumentItem {
  pk: string;
  sk: string;
  itemType: "doc";
  collection: string;
  key: string;
  createdAt: number;
  doc: Record<string, unknown>;
  indexes: ResolvedIndexKeys;
}

interface LockItem {
  pk: string;
  sk: string;
  itemType: "lock";
  lockId: string;
  acquiredAt: number;
}

interface CheckpointItem {
  pk: string;
  sk: string;
  itemType: "checkpoint";
  cursor: string;
}

type BatchWriteRequest = NonNullable<
  NonNullable<BatchWriteCommandInput["RequestItems"]>[string]
>[number];

export function dynamoDbEngine(options: DynamoDbEngineOptions): DynamoDbQueryEngine {
  const { client, tableName } = options;

  const engine: DynamoDbQueryEngine = {
    async get(collection, key) {
      const item = await getDocumentItem(client, tableName, collection, key);

      if (!item) {
        return null;
      }

      return structuredClone(item.doc);
    },

    async create(collection, key, doc, indexes) {
      const createdAt = await nextSequence(client, tableName, collection);
      const item = createDocumentItem(collection, key, createdAt, doc, indexes);

      try {
        await client.send(
          new PutCommand({
            TableName: tableName,
            Item: item,
            ConditionExpression: "attribute_not_exists(#pk) AND attribute_not_exists(#sk)",
            ExpressionAttributeNames: {
              "#pk": "pk",
              "#sk": "sk",
            },
          }),
        );
      } catch (error) {
        if (isConditionalCheckFailed(error)) {
          throw new EngineDocumentAlreadyExistsError(collection, key);
        }

        throw error;
      }
    },

    async put(collection, key, doc, indexes) {
      const existing = await getDocumentItem(client, tableName, collection, key);
      const createdAt = existing?.createdAt ?? (await nextSequence(client, tableName, collection));
      const item = createDocumentItem(collection, key, createdAt, doc, indexes);

      await client.send(
        new PutCommand({
          TableName: tableName,
          Item: item,
        }),
      );
    },

    async update(collection, key, doc, indexes) {
      const existing = await getDocumentItem(client, tableName, collection, key);

      if (!existing) {
        throw new EngineDocumentNotFoundError(collection, key);
      }

      const item = createDocumentItem(collection, key, existing.createdAt, doc, indexes);

      try {
        await client.send(
          new PutCommand({
            TableName: tableName,
            Item: item,
            ConditionExpression: "attribute_exists(#pk) AND attribute_exists(#sk)",
            ExpressionAttributeNames: {
              "#pk": "pk",
              "#sk": "sk",
            },
          }),
        );
      } catch (error) {
        if (isConditionalCheckFailed(error)) {
          throw new EngineDocumentNotFoundError(collection, key);
        }

        throw error;
      }
    },

    async delete(collection, key) {
      await client.send(
        new DeleteCommand({
          TableName: tableName,
          Key: {
            pk: collectionPartitionKey(collection),
            sk: documentSortKey(key),
          },
        }),
      );
    },

    async query(collection, params) {
      const records = await listDocuments(client, tableName, collection);
      const matched = matchDocuments(records, params);

      return paginate(matched, params);
    },

    async batchGet(collection, keys) {
      const results: KeyedDocument[] = [];
      const fetched = await batchGetDocuments(client, tableName, collection, keys);

      for (const key of keys) {
        const record = fetched.get(key);

        if (!record) {
          continue;
        }

        results.push({ key, doc: structuredClone(record.doc) });
      }

      return results;
    },

    async batchSet(collection, items) {
      const keys = items.map((item) => item.key);
      const existing = await batchGetDocuments(client, tableName, collection, keys);
      const puts: StoredDocumentItem[] = [];

      for (const item of items) {
        const existingRecord = existing.get(item.key);
        const createdAt =
          existingRecord?.createdAt ?? (await nextSequence(client, tableName, collection));

        puts.push(createDocumentItem(collection, item.key, createdAt, item.doc, item.indexes));
      }

      await batchWriteAll(
        client,
        tableName,
        puts.map((item) => ({ PutRequest: { Item: item } })),
      );
    },

    async batchDelete(collection, keys) {
      const deletes = keys.map((key) => ({
        DeleteRequest: {
          Key: {
            pk: collectionPartitionKey(collection),
            sk: documentSortKey(key),
          },
        },
      }));

      await batchWriteAll(client, tableName, deletes);
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
          itemType: "lock",
          lockId: randomId(),
          acquiredAt: now,
        };

        const putInput: {
          TableName: string;
          Item: LockItem;
          ConditionExpression: string;
          ExpressionAttributeNames: Record<string, string>;
          ExpressionAttributeValues?: Record<string, unknown>;
        } = {
          TableName: tableName,
          Item: lock,
          ConditionExpression: "attribute_not_exists(#pk)",
          ExpressionAttributeNames: {
            "#pk": "pk",
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
              Key: {
                pk: META_PARTITION_KEY,
                sk: lockSortKey(lock.collection),
              },
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
        const records = await listDocuments(client, tableName, collection);
        const parseVersion = criteria.parseVersion ?? defaultParseVersion;
        const compareVersions = criteria.compareVersions ?? defaultCompareVersions;
        const outdated: StoredDocumentItem[] = [];

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
        const checkpoint: CheckpointItem = {
          pk: META_PARTITION_KEY,
          sk: checkpointSortKey(lock.collection),
          itemType: "checkpoint",
          cursor,
        };

        try {
          await client.send(
            new TransactWriteCommand({
              TransactItems: [
                {
                  ConditionCheck: {
                    TableName: tableName,
                    Key: {
                      pk: META_PARTITION_KEY,
                      sk: lockSortKey(lock.collection),
                    },
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
                    Item: checkpoint,
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
        const raw = await getMetaItem(client, tableName, checkpointSortKey(collection));

        if (!raw) {
          return null;
        }

        return parseCheckpointItem(raw).cursor;
      },

      async clearCheckpoint(collection) {
        await client.send(
          new DeleteCommand({
            TableName: tableName,
            Key: {
              pk: META_PARTITION_KEY,
              sk: checkpointSortKey(collection),
            },
          }),
        );
      },

      async getStatus(collection) {
        const lockRaw = await getMetaItem(client, tableName, lockSortKey(collection));

        if (!lockRaw) {
          return null;
        }

        const lock = parseLockItem(lockRaw);
        const checkpointRaw = await getMetaItem(client, tableName, checkpointSortKey(collection));

        return {
          lock: {
            id: lock.lockId,
            collection,
            acquiredAt: lock.acquiredAt,
          },
          cursor: checkpointRaw ? parseCheckpointItem(checkpointRaw).cursor : null,
        };
      },
    },
  };

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

function parseDocumentItem(value: unknown): StoredDocumentItem {
  if (!isRecord(value)) {
    throw new Error("DynamoDB returned an invalid document item");
  }

  const pk = value.pk;
  const sk = value.sk;
  const itemType = value.itemType;
  const collection = value.collection;
  const key = value.key;
  const createdAt = value.createdAt;
  const doc = value.doc;
  const indexes = value.indexes;

  if (
    typeof pk !== "string" ||
    typeof sk !== "string" ||
    itemType !== "doc" ||
    typeof collection !== "string" ||
    typeof key !== "string" ||
    typeof createdAt !== "number" ||
    !Number.isFinite(createdAt) ||
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
    doc: doc as Record<string, unknown>,
    indexes: resolvedIndexes,
  };
}

function parseLockItem(value: unknown): LockItem {
  if (!isRecord(value)) {
    throw new Error("DynamoDB returned an invalid migration lock item");
  }

  const pk = value.pk;
  const sk = value.sk;
  const itemType = value.itemType;
  const lockId = value.lockId;
  const acquiredAt = value.acquiredAt;

  if (
    pk !== META_PARTITION_KEY ||
    typeof sk !== "string" ||
    itemType !== "lock" ||
    typeof lockId !== "string" ||
    typeof acquiredAt !== "number" ||
    !Number.isFinite(acquiredAt)
  ) {
    throw new Error("DynamoDB returned an invalid migration lock item");
  }

  return { pk, sk, itemType, lockId, acquiredAt };
}

function parseCheckpointItem(value: unknown): CheckpointItem {
  if (!isRecord(value)) {
    throw new Error("DynamoDB returned an invalid migration checkpoint item");
  }

  const pk = value.pk;
  const sk = value.sk;
  const itemType = value.itemType;
  const cursor = value.cursor;

  if (
    pk !== META_PARTITION_KEY ||
    typeof sk !== "string" ||
    itemType !== "checkpoint" ||
    typeof cursor !== "string"
  ) {
    throw new Error("DynamoDB returned an invalid migration checkpoint item");
  }

  return { pk, sk, itemType, cursor };
}

async function nextSequence(
  client: DynamoDbDocumentClientLike,
  tableName: string,
  collection: string,
): Promise<number> {
  const sk = sequenceSortKey(collection);

  const response = (await client.send(
    new UpdateCommand({
      TableName: tableName,
      Key: {
        pk: META_PARTITION_KEY,
        sk,
      },
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
    itemType: "doc",
    collection,
    key,
    createdAt,
    doc: cloned,
    indexes: { ...indexes },
  };
}

async function getMetaItem(
  client: DynamoDbDocumentClientLike,
  tableName: string,
  sk: string,
): Promise<Record<string, unknown> | null> {
  const response = (await client.send(
    new GetCommand({
      TableName: tableName,
      Key: {
        pk: META_PARTITION_KEY,
        sk,
      },
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
  collection: string,
  key: string,
): Promise<StoredDocumentItem | null> {
  const response = (await client.send(
    new GetCommand({
      TableName: tableName,
      Key: {
        pk: collectionPartitionKey(collection),
        sk: documentSortKey(key),
      },
    }),
  )) as {
    Item?: unknown;
  };

  if (!response.Item) {
    return null;
  }

  return parseDocumentItem(response.Item);
}

async function listDocuments(
  client: DynamoDbDocumentClientLike,
  tableName: string,
  collection: string,
): Promise<StoredDocumentItem[]> {
  const pk = collectionPartitionKey(collection);
  const records: StoredDocumentItem[] = [];
  let cursor: Record<string, unknown> | undefined;

  do {
    const input: QueryCommandInput = {
      TableName: tableName,
      KeyConditionExpression: "#pk = :pk",
      ExpressionAttributeNames: {
        "#pk": "pk",
      },
      ExpressionAttributeValues: {
        ":pk": pk,
      },
      ExclusiveStartKey: cursor,
    };

    const response = (await client.send(new QueryCommand(input))) as {
      Items?: unknown[];
      LastEvaluatedKey?: Record<string, unknown>;
    };

    const items = response.Items ?? [];

    for (const item of items) {
      const parsed = parseDocumentItem(item);

      if (parsed.itemType === "doc") {
        records.push(parsed);
      }
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
            Keys: pendingKeys,
          },
        },
      };

      const response = (await client.send(new BatchGetCommand(input))) as {
        Responses?: Record<string, unknown[]>;
        UnprocessedKeys?: Record<string, { Keys?: Array<Record<string, unknown>> }>;
      };

      const items = response.Responses?.[tableName] ?? [];

      for (const raw of items) {
        const item = parseDocumentItem(raw);
        result.set(item.key, item);
      }

      pendingKeys = (response.UnprocessedKeys?.[tableName]?.Keys ?? []) as Array<{
        pk: string;
        sk: string;
      }>;
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
  return typeof value === "object" && value !== null;
}

function chunkArray<T>(items: T[], size: number): T[][] {
  const chunks: T[][] = [];

  for (let i = 0; i < items.length; i += size) {
    chunks.push(items.slice(i, i + size));
  }

  return chunks;
}

function uniqueDynamoKeys(
  keys: Array<{ pk: string; sk: string }>,
): Array<{ pk: string; sk: string }> {
  const unique: Array<{ pk: string; sk: string }> = [];
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
