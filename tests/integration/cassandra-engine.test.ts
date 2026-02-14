import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  setDefaultTimeout,
  test,
} from "bun:test";
import { Client } from "cassandra-driver";
import { cassandraEngine, type CassandraQueryEngine } from "../../src/engines/cassandra";
import {
  EngineDocumentAlreadyExistsError,
  EngineDocumentNotFoundError,
  type ComparableVersion,
} from "../../src/engines/types";
import { createCollectionNameFactory, createTestResourceName, expectReject } from "./helpers";
import { runMigrationIntegrationSuite } from "./migration-suite";

const contactPoints = (process.env.CASSANDRA_CONTACT_POINTS ?? "127.0.0.1")
  .split(",")
  .map((item) => item.trim())
  .filter((item) => item.length > 0);
const port = Number(process.env.CASSANDRA_PORT ?? "9042");
const localDataCenter = process.env.CASSANDRA_LOCAL_DATACENTER ?? "datacenter1";
const connectAttemptsRaw = Number(process.env.CASSANDRA_CONNECT_ATTEMPTS ?? "90");
const connectDelayMsRaw = Number(process.env.CASSANDRA_CONNECT_DELAY_MS ?? "2000");
const keyspace = process.env.CASSANDRA_TEST_KEYSPACE ?? createTestResourceName("nosql_odm_test");

const documentsTable = `${keyspace}.nosql_odm_documents`;
const metadataTable = `${keyspace}.nosql_odm_metadata`;

let client: Client;
let engine: CassandraQueryEngine;
let collection = "";
let keyspaceCreated = false;
const nextCollection = createCollectionNameFactory();

setDefaultTimeout(120_000);

async function sleep(ms: number): Promise<void> {
  await new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

async function connectWithRetry(instance: Client): Promise<void> {
  const attempts =
    Number.isFinite(connectAttemptsRaw) && connectAttemptsRaw > 0
      ? Math.floor(connectAttemptsRaw)
      : 90;
  const delayMs =
    Number.isFinite(connectDelayMsRaw) && connectDelayMsRaw > 0
      ? Math.floor(connectDelayMsRaw)
      : 2000;

  for (let i = 0; i < attempts; i++) {
    try {
      await instance.connect();
      return;
    } catch (error) {
      if (i === attempts - 1) {
        const msg = String(error);

        throw new Error(
          `Unable to connect to Cassandra at ${contactPoints.join(",")}:${String(port)}. ` +
            `Start it with \`bun run services:up:cassandra\`. ` +
            `Retry config: attempts=${String(attempts)}, delayMs=${String(delayMs)}. ` +
            `Root error: ${msg}`,
        );
      }

      await sleep(delayMs);
    }
  }
}

describe("cassandraEngine integration", () => {
  beforeAll(async () => {
    client = new Client({
      contactPoints,
      localDataCenter,
      protocolOptions: {
        port,
      },
    });

    await connectWithRetry(client);

    await client.execute(
      `CREATE KEYSPACE IF NOT EXISTS ${keyspace} WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}`,
    );

    keyspaceCreated = true;
  });

  beforeEach(() => {
    collection = nextCollection("users");
    engine = cassandraEngine({
      client,
      keyspace,
    });
  });

  runMigrationIntegrationSuite({
    engineName: "cassandraEngine integration",
    getEngine: () => engine,
    nextCollection,
  });

  afterAll(async () => {
    try {
      if (keyspaceCreated) {
        await client.execute(`DROP KEYSPACE IF EXISTS ${keyspace}`);
      }
    } finally {
      await client.shutdown();
    }
  });

  test("get returns null for missing document", async () => {
    expect(await engine.get(collection, "missing")).toBeNull();
  });

  test("create stores and get returns document", async () => {
    await engine.create(collection, "u1", { id: "u1", name: "Sam" }, { primary: "u1" });

    expect(await engine.get(collection, "u1")).toEqual({ id: "u1", name: "Sam" });
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

  test("get throws for invalid stored document row", async () => {
    await client.execute(
      `INSERT INTO ${documentsTable} (collection, doc_key, created_at, doc, indexes) VALUES (?, ?, ?, ?, ?)`,
      [collection, "bad", 1, '"not-an-object"', { primary: "bad" }],
      { prepare: true },
    );

    await expectReject(engine.get(collection, "bad"), /invalid document row/);
  });

  test("loadCheckpoint throws for invalid checkpoint row", async () => {
    await client.execute(
      `INSERT INTO ${metadataTable} (collection, kind) VALUES (?, 'checkpoint')`,
      [collection],
      { prepare: true },
    );

    await expectReject(
      engine.migration.loadCheckpoint!(collection),
      /invalid migration checkpoint row/,
    );
  });

  test("getStatus throws for invalid lock row", async () => {
    await client.execute(
      `INSERT INTO ${metadataTable} (collection, kind, lock_id) VALUES (?, 'lock', ?)`,
      [collection, "bad-lock"],
      { prepare: true },
    );

    await expectReject(engine.migration.getStatus!(collection), /invalid migration lock row/);
  });

  test("getStatus throws for invalid checkpoint row", async () => {
    await client.execute(
      `INSERT INTO ${metadataTable} (collection, kind, lock_id, acquired_at) VALUES (?, 'lock', ?, ?)`,
      [collection, "ok-lock", Date.now()],
      { prepare: true },
    );
    await client.execute(
      `INSERT INTO ${metadataTable} (collection, kind) VALUES (?, 'checkpoint')`,
      [collection],
      { prepare: true },
    );

    await expectReject(engine.migration.getStatus!(collection), /invalid migration checkpoint row/);
  });
});
