import { afterAll, beforeAll, beforeEach, describe, expect, test } from "bun:test";
import {
  CreateTableCommand,
  DeleteTableCommand,
  DynamoDBClient,
  waitUntilTableExists,
  waitUntilTableNotExists,
} from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, PutCommand } from "@aws-sdk/lib-dynamodb";
import { dynamoDbEngine, type DynamoDbQueryEngine } from "../../src/engines/dynamodb";
import {
  EngineDocumentAlreadyExistsError,
  EngineDocumentNotFoundError,
  type ComparableVersion,
} from "../../src/engines/types";

const endpoint = process.env.DYNAMODB_ENDPOINT ?? "http://127.0.0.1:8000";
const region = process.env.AWS_REGION ?? "us-east-1";
const tableName =
  process.env.DYNAMODB_TEST_TABLE ??
  `nosql_odm_test_${Date.now()}_${Math.random().toString(16).slice(2, 8)}`;

let baseClient: DynamoDBClient;
let documentClient: DynamoDBDocumentClient;
let engine: DynamoDbQueryEngine;
let collection = "";
let collectionCounter = 0;
let createdTable = false;

function nextCollection(prefix: string): string {
  collectionCounter += 1;
  return `${prefix}_${Date.now()}_${String(collectionCounter)}`;
}

async function expectReject(work: Promise<unknown>, pattern: RegExp | string): Promise<void> {
  try {
    await work;
    throw new Error("expected operation to fail");
  } catch (error) {
    const message = String(error);

    if (pattern instanceof RegExp) {
      expect(message).toMatch(pattern);
      return;
    }

    expect(message).toContain(pattern);
  }
}

async function createTableForTests(): Promise<void> {
  try {
    await baseClient.send(
      new CreateTableCommand({
        TableName: tableName,
        AttributeDefinitions: [
          { AttributeName: "pk", AttributeType: "S" },
          { AttributeName: "sk", AttributeType: "S" },
        ],
        KeySchema: [
          { AttributeName: "pk", KeyType: "HASH" },
          { AttributeName: "sk", KeyType: "RANGE" },
        ],
        BillingMode: "PAY_PER_REQUEST",
      }),
    );

    await waitUntilTableExists(
      {
        client: baseClient,
        minDelay: 1,
        maxWaitTime: 20,
      },
      {
        TableName: tableName,
      },
    );

    createdTable = true;
  } catch (error) {
    const msg = String(error);

    throw new Error(
      `Unable to create DynamoDB Local test table "${tableName}" at ${endpoint}. ` +
        `Start it with \`bun run services:up:dynamodb\`. Root error: ${msg}`,
    );
  }
}

describe("dynamoDbEngine integration", () => {
  beforeAll(async () => {
    baseClient = new DynamoDBClient({
      endpoint,
      region,
      credentials: {
        accessKeyId: "local",
        secretAccessKey: "local",
      },
    });

    documentClient = DynamoDBDocumentClient.from(baseClient);
    await createTableForTests();
  });

  beforeEach(() => {
    collection = nextCollection("users");
    engine = dynamoDbEngine({
      client: documentClient,
      tableName,
    });
  });

  afterAll(async () => {
    try {
      if (createdTable) {
        await baseClient.send(
          new DeleteTableCommand({
            TableName: tableName,
          }),
        );

        await waitUntilTableNotExists(
          {
            client: baseClient,
            minDelay: 1,
            maxWaitTime: 20,
          },
          {
            TableName: tableName,
          },
        );
      }
    } finally {
      baseClient.destroy();
    }
  });

  test("get returns null for missing document", async () => {
    expect(await engine.get(collection, "missing")).toBeNull();
  });

  test("create stores and get returns document", async () => {
    await engine.create(collection, "u1", { id: "u1", name: "Sam" }, { primary: "u1" });

    expect(await engine.get(collection, "u1")).toEqual({
      id: "u1",
      name: "Sam",
    });
  });

  test("create throws duplicate-key error when key already exists", async () => {
    await engine.create(collection, "u1", { id: "u1", name: "Sam" }, { primary: "u1" });

    try {
      await engine.create(collection, "u1", { id: "u1", name: "Other" }, { primary: "u1" });
      throw new Error("expected duplicate create to throw");
    } catch (error) {
      expect(error).toBeInstanceOf(EngineDocumentAlreadyExistsError);
    }
  });

  test("put upserts and update replaces existing document", async () => {
    await engine.put(collection, "u1", { id: "u1", name: "Sam" }, { primary: "u1" });
    await engine.update(collection, "u1", { id: "u1", name: "Samuel" }, { primary: "u1" });

    expect(await engine.get(collection, "u1")).toEqual({
      id: "u1",
      name: "Samuel",
    });
  });

  test("update throws not-found error when key does not exist", async () => {
    try {
      await engine.update(collection, "missing", { id: "missing" }, { primary: "missing" });
      throw new Error("expected update on missing key to throw");
    } catch (error) {
      expect(error).toBeInstanceOf(EngineDocumentNotFoundError);
    }
  });

  test("delete removes documents and ignores missing keys", async () => {
    await engine.put(collection, "u1", { id: "u1" }, { primary: "u1" });
    await engine.delete(collection, "u1");
    await engine.delete(collection, "missing");

    expect(await engine.get(collection, "u1")).toBeNull();
  });

  test("get returns deep clones", async () => {
    await engine.put(collection, "u1", { id: "u1", nested: { value: 1 } }, { primary: "u1" });

    const doc1 = (await engine.get(collection, "u1")) as {
      nested: { value: number };
    };
    const doc2 = (await engine.get(collection, "u1")) as {
      nested: { value: number };
    };

    expect(doc1).toEqual(doc2);
    expect(doc1).not.toBe(doc2);
    expect(doc1.nested).not.toBe(doc2.nested);
  });

  test("batchSet, batchGet, batchDelete round trip documents", async () => {
    await engine.batchSet(collection, [
      { key: "u1", doc: { id: "u1", name: "A" }, indexes: { primary: "u1" } },
      { key: "u2", doc: { id: "u2", name: "B" }, indexes: { primary: "u2" } },
      { key: "u3", doc: { id: "u3", name: "C" }, indexes: { primary: "u3" } },
    ]);

    const docs = await engine.batchGet(collection, ["u1", "u2", "missing"]);

    expect(docs).toHaveLength(2);
    expect(docs).toContainEqual({ key: "u1", doc: { id: "u1", name: "A" } });
    expect(docs).toContainEqual({ key: "u2", doc: { id: "u2", name: "B" } });

    await engine.batchDelete(collection, ["u1", "u3"]);

    expect(await engine.get(collection, "u1")).toBeNull();
    expect(await engine.get(collection, "u2")).toEqual({ id: "u2", name: "B" });
    expect(await engine.get(collection, "u3")).toBeNull();
  });

  test("batchGet preserves request order and duplicates", async () => {
    await engine.put(collection, "u1", { id: "u1", nested: { value: 1 } }, { primary: "u1" });

    const docs = await engine.batchGet(collection, ["u1", "u1"]);

    expect(docs).toHaveLength(2);
    expect(docs[0]?.key).toBe("u1");
    expect(docs[1]?.key).toBe("u1");
    expect(docs[0]?.doc).not.toBe(docs[1]?.doc);
  });

  test("batchGet supports large key sets (chunking path)", async () => {
    const items = Array.from({ length: 130 }, (_, i) => {
      const key = `u${String(i).padStart(3, "0")}`;

      return {
        key,
        doc: { id: key },
        indexes: { primary: key },
      };
    });

    await engine.batchSet(collection, items);
    const keys = items.map((item) => item.key);
    const docs = await engine.batchGet(collection, keys);

    expect(docs).toHaveLength(130);
    expect(docs[0]?.key).toBe("u000");
    expect(docs[129]?.key).toBe("u129");
  });

  test("query supports equality, sorting, and cursor pagination", async () => {
    await engine.put(collection, "u1", { id: "u1" }, { byRole: "member#a" });
    await engine.put(collection, "u2", { id: "u2" }, { byRole: "member#c" });
    await engine.put(collection, "u3", { id: "u3" }, { byRole: "member#b" });

    const first = await engine.query(collection, {
      index: "byRole",
      filter: { value: { $begins: "member#" } },
      sort: "asc",
      limit: 2,
    });

    const second = await engine.query(collection, {
      index: "byRole",
      filter: { value: { $begins: "member#" } },
      sort: "asc",
      cursor: first.cursor ?? undefined,
      limit: 2,
    });

    expect(first.documents.map((item) => item.key)).toEqual(["u1", "u3"]);
    expect(first.cursor).toBe("u3");
    expect(second.documents.map((item) => item.key)).toEqual(["u2"]);
    expect(second.cursor).toBeNull();
  });

  test("query supports comparison filters and scan behavior", async () => {
    await engine.put(collection, "a", { id: "a" }, { byDate: "2025-01-01" });
    await engine.put(collection, "b", { id: "b" }, { byDate: "2025-06-15" });
    await engine.put(collection, "c", { id: "c" }, { byDate: "2025-12-31" });

    const between = await engine.query(collection, {
      index: "byDate",
      filter: { value: { $between: ["2025-01-01", "2025-06-15"] } },
    });

    expect(between.documents.map((item) => item.key)).toEqual(["a", "b"]);

    const scan = await engine.query(collection, {});
    expect(scan.documents.map((item) => item.key)).toEqual(["a", "b", "c"]);
  });

  test("migration lock/checkpoint/status semantics", async () => {
    const lock = await engine.migration.acquireLock(collection);

    expect(lock).not.toBeNull();
    expect(await engine.migration.acquireLock(collection)).toBeNull();

    await engine.migration.saveCheckpoint!(lock!, "cursor-a");
    expect(await engine.migration.loadCheckpoint!(collection)).toBe("cursor-a");

    const stolen = await engine.migration.acquireLock(collection, { ttl: 0 });
    expect(stolen).not.toBeNull();
    expect(stolen?.id).not.toBe(lock?.id);

    await engine.migration.saveCheckpoint!(lock!, "cursor-stale");
    expect(await engine.migration.loadCheckpoint!(collection)).toBe("cursor-a");

    await engine.migration.saveCheckpoint!(stolen!, "cursor-b");
    const status = await engine.migration.getStatus!(collection);

    expect(status?.lock.id).toBe(stolen?.id);
    expect(status?.cursor).toBe("cursor-b");

    await engine.migration.releaseLock({
      id: "wrong-id",
      collection,
      acquiredAt: stolen!.acquiredAt,
    });
    expect(await engine.migration.acquireLock(collection)).toBeNull();

    await engine.migration.releaseLock(stolen!);
    expect(await engine.migration.acquireLock(collection)).not.toBeNull();
  });

  test("migration getOutdated returns stale and reindex-needed docs", async () => {
    await engine.put(
      collection,
      "stale",
      { __v: 1, __indexes: ["primary"], id: "stale" },
      { primary: "stale" },
    );
    await engine.put(
      collection,
      "reindex",
      { __v: 2, __indexes: ["primary"], id: "reindex" },
      { primary: "reindex" },
    );
    await engine.put(
      collection,
      "current",
      { __v: 2, __indexes: ["byEmail", "primary"], id: "current" },
      { primary: "current" },
    );

    const page = await engine.migration.getOutdated(collection, {
      version: 2,
      versionField: "__v",
      indexesField: "__indexes",
      indexes: ["byEmail", "primary"],
    });

    expect(page.documents.map((item) => item.key)).toEqual(["stale", "reindex"]);
    expect(page.cursor).toBeNull();
  });

  test("migration getOutdated paginates 100-item pages", async () => {
    for (let i = 0; i < 105; i++) {
      const key = `u${String(i).padStart(3, "0")}`;

      await engine.put(
        collection,
        key,
        { __v: 1, __indexes: ["primary"], id: key },
        { primary: key },
      );
    }

    const first = await engine.migration.getOutdated(collection, {
      version: 2,
      versionField: "__v",
      indexesField: "__indexes",
      indexes: ["primary"],
    });

    expect(first.documents).toHaveLength(100);
    expect(first.cursor).not.toBeNull();

    const second = await engine.migration.getOutdated(
      collection,
      {
        version: 2,
        versionField: "__v",
        indexesField: "__indexes",
        indexes: ["primary"],
      },
      first.cursor ?? undefined,
    );

    expect(second.documents).toHaveLength(5);
    expect(second.cursor).toBeNull();
  });

  test("migration getOutdated honors custom parseVersion/compareVersions", async () => {
    await engine.put(
      collection,
      "old",
      { versionTag: "release-1", indexTags: ["primary"], id: "old" },
      { primary: "old" },
    );

    await engine.put(
      collection,
      "new",
      { versionTag: "release-2", indexTags: ["primary"], id: "new" },
      { primary: "new" },
    );

    const compareVersions = (a: ComparableVersion, b: ComparableVersion): number => {
      const toNum = (value: ComparableVersion) => Number(String(value).replace(/^release-/, ""));
      return toNum(a) - toNum(b);
    };

    const page = await engine.migration.getOutdated(collection, {
      version: 2,
      versionField: "versionTag",
      indexesField: "indexTags",
      indexes: ["primary"],
      parseVersion(raw) {
        if (typeof raw !== "string") {
          return null;
        }

        return raw;
      },
      compareVersions,
    });

    expect(page.documents.map((item) => item.key)).toEqual(["old"]);
  });

  test("get throws for invalid stored document item", async () => {
    await documentClient.send(
      new PutCommand({
        TableName: tableName,
        Item: {
          pk: `COL#${collection}`,
          sk: "DOC#bad",
          itemType: "doc",
          collection,
          key: "bad",
          createdAt: 1,
          doc: "not-an-object",
          indexes: {},
        },
      }),
    );

    await expectReject(engine.get(collection, "bad"), /invalid document item/);
  });

  test("loadCheckpoint throws for invalid checkpoint item", async () => {
    await documentClient.send(
      new PutCommand({
        TableName: tableName,
        Item: {
          pk: "META",
          sk: `CHECKPOINT#${collection}`,
          itemType: "checkpoint",
          cursor: 123,
        },
      }),
    );

    await expectReject(
      engine.migration.loadCheckpoint!(collection),
      /invalid migration checkpoint item/,
    );
  });

  test("getStatus throws for invalid lock item", async () => {
    await documentClient.send(
      new PutCommand({
        TableName: tableName,
        Item: {
          pk: "META",
          sk: `LOCK#${collection}`,
          itemType: "lock",
          lockId: 123,
          acquiredAt: Date.now(),
        },
      }),
    );

    await expectReject(engine.migration.getStatus!(collection), /invalid migration lock item/);
  });
});
