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

const contactPoints = (process.env.CASSANDRA_CONTACT_POINTS ?? "127.0.0.1")
  .split(",")
  .map((item) => item.trim())
  .filter((item) => item.length > 0);
const port = Number(process.env.CASSANDRA_PORT ?? "9042");
const localDataCenter = process.env.CASSANDRA_LOCAL_DATACENTER ?? "datacenter1";
const connectAttemptsRaw = Number(process.env.CASSANDRA_CONNECT_ATTEMPTS ?? "90");
const connectDelayMsRaw = Number(process.env.CASSANDRA_CONNECT_DELAY_MS ?? "2000");
const keyspace =
  process.env.CASSANDRA_TEST_KEYSPACE ??
  `nosql_odm_test_${Date.now()}_${Math.random().toString(16).slice(2, 8)}`;

const documentsTable = `${keyspace}.nosql_odm_documents`;
const metadataTable = `${keyspace}.nosql_odm_metadata`;

let client: Client;
let engine: CassandraQueryEngine;
let collection = "";
let collectionCounter = 0;
let keyspaceCreated = false;

setDefaultTimeout(120_000);

function nextCollection(prefix: string): string {
  collectionCounter += 1;
  return `${prefix}_${Date.now()}_${String(collectionCounter)}`;
}

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
});
