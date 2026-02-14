import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  setDefaultTimeout,
  test,
} from "bun:test";
import {
  CreateTableCommand,
  DeleteTableCommand,
  DynamoDBClient,
  ListTablesCommand,
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
import { createCollectionNameFactory, createTestResourceName, expectReject } from "./helpers";
import { runMigrationIntegrationSuite } from "./migration-suite";

const endpoint = process.env.DYNAMODB_ENDPOINT ?? "http://127.0.0.1:8000";
const region = process.env.AWS_REGION ?? "us-east-1";
const tableName = process.env.DYNAMODB_TEST_TABLE ?? createTestResourceName("nosql_odm_test");
const isCi = process.env.CI === "true";
const migrationTableWaitMaxSeconds = isCi ? 90 : 20;
const connectAttemptsRaw = Number(process.env.DYNAMODB_CONNECT_ATTEMPTS ?? (isCi ? "180" : "60"));
const connectDelayMsRaw = Number(process.env.DYNAMODB_CONNECT_DELAY_MS ?? (isCi ? "1500" : "1000"));

let baseClient: DynamoDBClient;
let documentClient: DynamoDBDocumentClient;
let engine: DynamoDbQueryEngine;
let collection = "";
let createdTable = false;
const nextCollection = createCollectionNameFactory();

setDefaultTimeout(isCi ? 300_000 : 120_000);

function normalizePositiveInteger(value: number, fallback: number): number {
  if (!Number.isFinite(value) || value <= 0) {
    return fallback;
  }

  return Math.floor(value);
}

async function sleep(ms: number): Promise<void> {
  await new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

async function waitForDynamoDbReady(): Promise<void> {
  const attempts = normalizePositiveInteger(connectAttemptsRaw, isCi ? 180 : 60);
  const delayMs = normalizePositiveInteger(connectDelayMsRaw, isCi ? 1500 : 1000);
  let lastError: unknown = null;

  for (let i = 0; i < attempts; i++) {
    try {
      await baseClient.send(new ListTablesCommand({ Limit: 1 }));
      return;
    } catch (error) {
      lastError = error;

      if (i < attempts - 1) {
        await sleep(delayMs);
      }
    }
  }

  throw new Error(
    `Unable to reach DynamoDB Local at ${endpoint}. ` +
      `Start it with \`bun run services:up:dynamodb\`. ` +
      `Retry config: attempts=${String(attempts)}, delayMs=${String(delayMs)}. ` +
      `Root error: ${String(lastError)}`,
  );
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
        maxWaitTime: migrationTableWaitMaxSeconds,
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
    await waitForDynamoDbReady();
    await createTableForTests();
  });

  beforeEach(() => {
    collection = nextCollection("users");
    engine = dynamoDbEngine({
      client: documentClient,
      tableName,
    });
  });

  runMigrationIntegrationSuite({
    engineName: "dynamoDbEngine integration",
    getEngine: () => engine,
    nextCollection,
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
            maxWaitTime: migrationTableWaitMaxSeconds,
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

  test("create rejects non-object documents", async () => {
    await expectReject(
      engine.create(collection, "u1", "not-an-object", { primary: "u1" }),
      /non-object document/,
    );
  });

  test("create rejects circular documents", async () => {
    const doc: { id: string; self?: unknown } = { id: "u1" };
    doc.self = doc;

    await expectReject(
      engine.create(collection, "u1", doc, { primary: "u1" }),
      /circular|cyclic|serialize|call stack/i,
    );
  });

  test("put rejects non-object documents", async () => {
    await expectReject(
      engine.put(collection, "u1", "not-an-object", { primary: "u1" }),
      /non-object document/,
    );
  });

  test("update rejects non-object documents", async () => {
    await engine.put(collection, "u1", { id: "u1" }, { primary: "u1" });

    await expectReject(
      engine.update(collection, "u1", "not-an-object", { primary: "u1" }),
      /non-object document/,
    );
  });

  test("batchSet rejects non-object documents", async () => {
    await expectReject(
      engine.batchSet(collection, [
        { key: "u1", doc: "not-an-object", indexes: { primary: "u1" } },
      ]),
      /non-object document/,
    );
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

  test("batchGet returns deep clones per returned item", async () => {
    await engine.put(collection, "u1", { id: "u1", nested: { value: 1 } }, { primary: "u1" });

    const docs = await engine.batchGet(collection, ["u1", "u1"]);
    const first = docs[0]?.doc as { nested: { value: number } };
    const second = docs[1]?.doc as { nested: { value: number } };

    expect(first).toEqual(second);
    expect(first).not.toBe(second);
    expect(first.nested).not.toBe(second.nested);
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

  test("query pagination edge cases", async () => {
    await engine.put(collection, "u1", { id: "u1" }, { byRole: "member#a" });
    await engine.put(collection, "u2", { id: "u2" }, { byRole: "member#b" });
    await engine.put(collection, "u3", { id: "u3" }, { byRole: "member#c" });

    const limitZero = await engine.query(collection, {
      index: "byRole",
      filter: { value: { $begins: "member#" } },
      sort: "asc",
      limit: 0,
    });

    const noLimit = await engine.query(collection, {
      index: "byRole",
      filter: { value: { $begins: "member#" } },
      sort: "asc",
      limit: Number.POSITIVE_INFINITY,
    });

    const fractional = await engine.query(collection, {
      index: "byRole",
      filter: { value: { $begins: "member#" } },
      sort: "asc",
      limit: 1.9,
    });

    const unknownCursor = await engine.query(collection, {
      index: "byRole",
      filter: { value: { $begins: "member#" } },
      sort: "asc",
      cursor: "unknown",
      limit: 2,
    });

    const lastCursor = await engine.query(collection, {
      index: "byRole",
      filter: { value: { $begins: "member#" } },
      sort: "asc",
      cursor: "u3",
      limit: 2,
    });

    expect(limitZero.documents).toHaveLength(0);
    expect(limitZero.cursor).toBeNull();
    expect(noLimit.documents).toHaveLength(3);
    expect(noLimit.cursor).toBeNull();
    expect(fractional.documents).toHaveLength(1);
    expect(fractional.cursor).toBe("u1");
    expect(unknownCursor.documents.map((item) => item.key)).toEqual(["u1", "u2"]);
    expect(lastCursor.documents).toHaveLength(0);
    expect(lastCursor.cursor).toBeNull();
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

  test("migration lock semantics: invalid ttl does not steal and collection scope is isolated", async () => {
    const lock = await engine.migration.acquireLock(collection);
    const otherCollection = nextCollection("orders");
    const otherLock = await engine.migration.acquireLock(otherCollection);

    expect(lock).not.toBeNull();
    expect(otherLock).not.toBeNull();

    for (const ttl of [Number.NaN, Number.POSITIVE_INFINITY, -1]) {
      expect(
        await engine.migration.acquireLock(collection, {
          ttl,
        }),
      ).toBeNull();
    }

    expect(await engine.migration.acquireLock(collection)).toBeNull();
    expect(await engine.migration.acquireLock(otherCollection)).toBeNull();
  });

  test("migration checkpoint lifecycle and collection scope", async () => {
    const otherCollection = nextCollection("orders");
    const lock = await engine.migration.acquireLock(collection);
    const otherLock = await engine.migration.acquireLock(otherCollection);

    expect(lock).not.toBeNull();
    expect(otherLock).not.toBeNull();
    expect(await engine.migration.loadCheckpoint!(collection)).toBeNull();

    await engine.migration.saveCheckpoint!(lock!, "cursor-a");
    expect(await engine.migration.loadCheckpoint!(collection)).toBe("cursor-a");

    await engine.migration.saveCheckpoint!(lock!, "cursor-b");
    expect(await engine.migration.loadCheckpoint!(collection)).toBe("cursor-b");

    await engine.migration.saveCheckpoint!(otherLock!, "cursor-other");
    expect(await engine.migration.loadCheckpoint!(otherCollection)).toBe("cursor-other");

    await engine.migration.clearCheckpoint!(collection);
    expect(await engine.migration.loadCheckpoint!(collection)).toBeNull();
    expect(await engine.migration.loadCheckpoint!(otherCollection)).toBe("cursor-other");
  });

  test("migration getStatus shape variants", async () => {
    expect(await engine.migration.getStatus!(collection)).toBeNull();

    const lock = await engine.migration.acquireLock(collection);
    expect(lock).not.toBeNull();

    const withoutCursor = await engine.migration.getStatus!(collection);
    expect(withoutCursor?.lock.id).toBe(lock?.id);
    expect(withoutCursor?.cursor).toBeNull();

    await engine.migration.saveCheckpoint!(lock!, "cursor-a");
    const withCursor = await engine.migration.getStatus!(collection);
    expect(withCursor?.lock.id).toBe(lock?.id);
    expect(withCursor?.cursor).toBe("cursor-a");
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

  test("get throws for invalid stored indexes value", async () => {
    await documentClient.send(
      new PutCommand({
        TableName: tableName,
        Item: {
          pk: `COL#${collection}`,
          sk: "DOC#bad-indexes",
          itemType: "doc",
          collection,
          key: "bad-indexes",
          createdAt: 1,
          doc: { id: "bad-indexes" },
          indexes: "not-an-object",
        },
      }),
    );

    await expectReject(engine.get(collection, "bad-indexes"), /invalid document item/);
  });

  test("get throws for non-string stored index values", async () => {
    await documentClient.send(
      new PutCommand({
        TableName: tableName,
        Item: {
          pk: `COL#${collection}`,
          sk: "DOC#bad-index-value",
          itemType: "doc",
          collection,
          key: "bad-index-value",
          createdAt: 1,
          doc: { id: "bad-index-value" },
          indexes: { primary: 1 },
        },
      }),
    );

    await expectReject(engine.get(collection, "bad-index-value"), /invalid document item/);
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

  test("getStatus throws for invalid checkpoint item", async () => {
    await documentClient.send(
      new PutCommand({
        TableName: tableName,
        Item: {
          pk: "META",
          sk: `LOCK#${collection}`,
          itemType: "lock",
          lockId: "ok-lock",
          acquiredAt: Date.now(),
        },
      }),
    );
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
      engine.migration.getStatus!(collection),
      /invalid migration checkpoint item/,
    );
  });
});
