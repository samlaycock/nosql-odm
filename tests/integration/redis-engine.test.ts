import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  setDefaultTimeout,
  test,
} from "bun:test";
import { createClient } from "redis";

import { redisEngine, type RedisQueryEngine } from "../../src/engines/redis";
import {
  EngineDocumentAlreadyExistsError,
  EngineDocumentNotFoundError,
  type ComparableVersion,
} from "../../src/engines/types";
import { createCollectionNameFactory, createTestResourceName, expectReject } from "./helpers";
import { runMigrationIntegrationSuite } from "./migration-suite";

const redisUrl = process.env.REDIS_URL ?? "redis://127.0.0.1:6379";
const isCi = process.env.CI === "true";
const connectAttemptsRaw = Number(process.env.REDIS_CONNECT_ATTEMPTS ?? (isCi ? "180" : "60"));
const connectDelayMsRaw = Number(process.env.REDIS_CONNECT_DELAY_MS ?? (isCi ? "1500" : "1000"));
const keyPrefix = process.env.REDIS_TEST_PREFIX ?? createTestResourceName("nosql_odm_test");

type RedisClient = ReturnType<typeof createClient>;

let client: RedisClient;
let engine: RedisQueryEngine;
let collection = "";
const nextCollection = createCollectionNameFactory();

setDefaultTimeout(isCi ? 300_000 : 120_000);

function normalizePositiveInteger(value: number, fallback: number): number {
  if (!Number.isFinite(value) || value <= 0) {
    return fallback;
  }

  return Math.floor(value);
}

function documentHashKey(prefix: string, collectionName: string, key: string): string {
  return `${prefix}:doc:${collectionName}:${key}`;
}

function collectionOrderKey(prefix: string, collectionName: string): string {
  return `${prefix}:order:${collectionName}`;
}

function migrationLockKey(prefix: string, collectionName: string): string {
  return `${prefix}:migration:lock:${collectionName}`;
}

function migrationCheckpointKey(prefix: string, collectionName: string): string {
  return `${prefix}:migration:checkpoint:${collectionName}`;
}

async function sleep(ms: number): Promise<void> {
  await new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

async function connectWithRetry(): Promise<RedisClient> {
  const attempts = normalizePositiveInteger(connectAttemptsRaw, 60);
  const delayMs = normalizePositiveInteger(connectDelayMsRaw, 1000);

  for (let i = 0; i < attempts; i++) {
    const candidate = createClient({
      url: redisUrl,
    });

    try {
      await candidate.connect();
      await candidate.ping();
      return candidate;
    } catch (error) {
      candidate.destroy();

      if (i === attempts - 1) {
        const msg = String(error);

        throw new Error(
          `Unable to connect to Redis at ${redisUrl}. ` +
            `Start it with \`bun run services:up:redis\`. ` +
            `Retry config: attempts=${String(attempts)}, delayMs=${String(delayMs)}. ` +
            `Root error: ${msg}`,
        );
      }

      await sleep(delayMs);
    }
  }

  throw new Error("Unable to connect to Redis");
}

async function cleanupPrefixKeys(instance: RedisClient): Promise<void> {
  const keys = await instance.keys(`${keyPrefix}:*`);

  if (keys.length > 0) {
    await instance.del(keys);
  }
}

describe("redisEngine integration", () => {
  beforeAll(async () => {
    client = await connectWithRetry();
    await cleanupPrefixKeys(client);
  });

  beforeEach(() => {
    collection = nextCollection("users");
    engine = redisEngine({
      client,
      keyPrefix,
    });
  });

  runMigrationIntegrationSuite({
    engineName: "redisEngine integration",
    getEngine: () => engine,
    nextCollection,
  });

  afterAll(async () => {
    if (!client) {
      return;
    }

    try {
      await cleanupPrefixKeys(client);
    } finally {
      await client.quit();
    }
  });

  test("get returns null for missing document", async () => {
    expect(await engine.get(collection, "missing")).toBeNull();
  });

  test("throws when keyPrefix is empty", () => {
    expect(() =>
      redisEngine({
        client,
        keyPrefix: "   ",
      }),
    ).toThrow(/keyPrefix must not be empty/);
  });

  test("create stores and get returns document", async () => {
    await engine.create(collection, "u1", { id: "u1", name: "Sam" }, { primary: "u1" });

    expect(await engine.get(collection, "u1")).toEqual({ id: "u1", name: "Sam" });
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

    expect(await engine.get(collection, "u1")).toEqual({ id: "u1", name: "Samuel" });
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
      /circular|cyclic|serialize/i,
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

  test("batchSetWithResult skips stale migration writes when a document changed concurrently", async () => {
    await engine.put(
      collection,
      "u1",
      {
        __v: 1,
        __indexes: ["primary"],
        id: "u1",
        name: "Before",
        email: "before@example.com",
      },
      { primary: "u1" },
    );

    const outdated = await engine.migration.getOutdated(collection, {
      version: 2,
      versionField: "__v",
      indexes: ["primary"],
      indexesField: "__indexes",
    });
    const token = outdated.documents[0]?.writeToken;

    expect(typeof token).toBe("string");

    await engine.update(
      collection,
      "u1",
      {
        __v: 1,
        __indexes: ["primary"],
        id: "u1",
        name: "Concurrent",
        email: "concurrent@example.com",
      },
      { primary: "u1" },
    );

    const result = await engine.batchSetWithResult!(collection, [
      {
        key: "u1",
        doc: {
          __v: 2,
          __indexes: ["primary"],
          id: "u1",
          firstName: "Before",
          lastName: "",
          email: "before@example.com",
        },
        indexes: { primary: "u1" },
        expectedWriteToken: token,
      },
    ]);

    expect(result.persistedKeys).toEqual([]);
    expect(result.conflictedKeys).toEqual(["u1"]);
    expect(await engine.get(collection, "u1")).toEqual({
      __v: 1,
      __indexes: ["primary"],
      id: "u1",
      name: "Concurrent",
      email: "concurrent@example.com",
    });
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
    await engine.put(collection, "u1", { id: "u1" }, { byScore: "001" });
    await engine.put(collection, "u2", { id: "u2" }, { byScore: "010" });
    await engine.put(collection, "u3", { id: "u3" }, { byScore: "020" });

    const eq = await engine.query(collection, {
      index: "byScore",
      filter: { value: { $eq: "010" } },
    });
    const range = await engine.query(collection, {
      index: "byScore",
      filter: { value: { $gt: "001", $lt: "020" } },
    });
    const between = await engine.query(collection, {
      index: "byScore",
      filter: { value: { $between: ["005", "020"] } },
    });
    const scan = await engine.query(collection, {});

    expect(eq.documents.map((item) => item.key)).toEqual(["u2"]);
    expect(range.documents.map((item) => item.key)).toEqual(["u2"]);
    expect(between.documents.map((item) => item.key)).toEqual(["u2", "u3"]);
    expect(scan.documents).toHaveLength(3);
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

  test("query tolerates dangling order entries", async () => {
    await client.zAdd(collectionOrderKey(keyPrefix, collection), [{ score: 0, value: "ghost" }]);
    await engine.put(collection, "u1", { id: "u1" }, { primary: "u1" });

    const page = await engine.query(collection, {});

    expect(page.documents.map((item) => item.key)).toEqual(["u1"]);
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

    await engine.migration.releaseLock(stolen!);
    expect(await engine.migration.acquireLock(collection)).not.toBeNull();
  });

  test("migration lock semantics: invalid ttl does not steal, wrong id cannot release, and collection scope", async () => {
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

    await engine.migration.releaseLock({
      id: "wrong-id",
      collection,
      acquiredAt: Date.now(),
    });

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

  test("get throws for invalid stored document record", async () => {
    await client.hSet(documentHashKey(keyPrefix, collection, "bad"), {
      createdAt: "1",
      doc: '"not-an-object"',
      indexes: '{"primary":"bad"}',
    });
    await client.zAdd(collectionOrderKey(keyPrefix, collection), [{ score: 1, value: "bad" }]);

    await expectReject(engine.get(collection, "bad"), /invalid document record/);
  });

  test("get throws for invalid stored index JSON", async () => {
    await client.hSet(documentHashKey(keyPrefix, collection, "bad"), {
      createdAt: "1",
      doc: '{"id":"bad"}',
      indexes: "not-json",
    });
    await client.zAdd(collectionOrderKey(keyPrefix, collection), [{ score: 1, value: "bad" }]);

    await expectReject(engine.get(collection, "bad"), /invalid document record/);
  });

  test("get throws for non-string index values", async () => {
    await client.hSet(documentHashKey(keyPrefix, collection, "bad"), {
      createdAt: "1",
      doc: '{"id":"bad"}',
      indexes: '{"primary":1}',
    });
    await client.zAdd(collectionOrderKey(keyPrefix, collection), [{ score: 1, value: "bad" }]);

    await expectReject(engine.get(collection, "bad"), /invalid document record/);
  });

  test("getStatus throws for invalid lock record", async () => {
    await client.hSet(migrationLockKey(keyPrefix, collection), {
      lockId: "bad-lock",
    });

    await expectReject(engine.migration.getStatus!(collection), /invalid migration lock record/);
  });

  test("loadCheckpoint throws for invalid checkpoint key type", async () => {
    await client.hSet(migrationCheckpointKey(keyPrefix, collection), {
      cursor: "wrong-type",
    });

    await expectReject(
      engine.migration.loadCheckpoint!(collection),
      /WRONGTYPE|invalid migration checkpoint record/,
    );
  });

  test("getStatus throws for invalid checkpoint key type", async () => {
    await engine.migration.acquireLock(collection);
    await client.hSet(migrationCheckpointKey(keyPrefix, collection), {
      cursor: "wrong-type",
    });

    await expectReject(
      engine.migration.getStatus!(collection),
      /WRONGTYPE|invalid migration checkpoint record/,
    );
  });

  test("acquireLock throws for invalid existing lock record", async () => {
    await client.hSet(migrationLockKey(keyPrefix, collection), {
      lockId: "bad-lock",
      acquiredAt: "invalid",
    });

    await expectReject(
      engine.migration.acquireLock(collection, {
        ttl: 0,
      }),
      /invalid migration lock record/,
    );
  });
});
