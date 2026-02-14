import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  setDefaultTimeout,
  test,
} from "bun:test";
import { createPool, type Pool } from "mysql2/promise";
import { mySqlEngine, type MySqlQueryEngine } from "../../src/engines/mysql";
import {
  EngineDocumentAlreadyExistsError,
  EngineDocumentNotFoundError,
  type ComparableVersion,
} from "../../src/engines/types";
import { createCollectionNameFactory, createTestResourceName, expectReject } from "./helpers";
import { runMigrationIntegrationSuite } from "./migration-suite";

const host = process.env.MYSQL_HOST ?? "127.0.0.1";
const port = Number(process.env.MYSQL_PORT ?? "3306");
const user = process.env.MYSQL_USER ?? "root";
const password = process.env.MYSQL_PASSWORD ?? "root";
const adminDatabase = process.env.MYSQL_ADMIN_DATABASE ?? "mysql";
const databaseName =
  process.env.MYSQL_TEST_DATABASE ?? createSqlIdentifier(createTestResourceName("nosql_odm_test"));
const connectAttemptsRaw = Number(process.env.MYSQL_CONNECT_ATTEMPTS ?? "90");
const connectDelayMsRaw = Number(process.env.MYSQL_CONNECT_DELAY_MS ?? "1000");

let adminPool: Pool | null = null;
let pool: Pool | null = null;
let engine: MySqlQueryEngine;
let collection = "";
const nextCollection = createCollectionNameFactory();

setDefaultTimeout(120_000);

function quoteIdentifier(value: string): string {
  return `\`${value.replace(/`/g, "``")}\``;
}

function qualifiedTableName(tableName: string): string {
  return `${quoteIdentifier(databaseName)}.${quoteIdentifier(tableName)}`;
}

function createSqlIdentifier(prefix: string): string {
  const raw = prefix.toLowerCase().replace(/[^a-z0-9_]/g, "_");
  const normalized = /^[a-z_]/.test(raw) ? raw : `t_${raw}`;

  return normalized.slice(0, 63);
}

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

function requirePool(): Pool {
  if (!pool) {
    throw new Error("MySQL test pool is not initialized");
  }

  return pool;
}

function requireAdminPool(): Pool {
  if (!adminPool) {
    throw new Error("MySQL admin pool is not initialized");
  }

  return adminPool;
}

async function connectWithRetry(instance: Pool): Promise<void> {
  const attempts = normalizePositiveInteger(connectAttemptsRaw, 90);
  const delayMs = normalizePositiveInteger(connectDelayMsRaw, 1000);

  for (let i = 0; i < attempts; i++) {
    try {
      await instance.query("SELECT 1");
      return;
    } catch (error) {
      if (i === attempts - 1) {
        const msg = String(error);

        throw new Error(
          `Unable to connect to MySQL at ${host}:${String(port)}. ` +
            `Start it with \`bun run services:up:mysql\`. ` +
            `Retry config: attempts=${String(attempts)}, delayMs=${String(delayMs)}. ` +
            `Root error: ${msg}`,
        );
      }

      await sleep(delayMs);
    }
  }
}

describe("mySqlEngine integration", () => {
  beforeAll(async () => {
    adminPool = createPool({
      host,
      port,
      user,
      password,
      database: adminDatabase,
      connectionLimit: 5,
    });

    await connectWithRetry(requireAdminPool());

    await requireAdminPool().query(
      `CREATE DATABASE IF NOT EXISTS ${quoteIdentifier(databaseName)} CHARACTER SET utf8mb4 COLLATE utf8mb4_bin`,
    );

    pool = createPool({
      host,
      port,
      user,
      password,
      database: databaseName,
      connectionLimit: 10,
    });

    await connectWithRetry(requirePool());
  });

  beforeEach(() => {
    collection = nextCollection("users");
    engine = mySqlEngine({
      client: requirePool(),
      database: databaseName,
    });
  });

  runMigrationIntegrationSuite({
    engineName: "mySqlEngine integration",
    getEngine: () => engine,
    nextCollection,
  });

  afterAll(async () => {
    if (pool) {
      await pool.end();
      pool = null;
    }

    if (adminPool) {
      try {
        await adminPool.query(`DROP DATABASE IF EXISTS ${quoteIdentifier(databaseName)}`);
      } finally {
        await adminPool.end();
      }
    }
  });

  test("get returns null for missing document", async () => {
    expect(await engine.get(collection, "missing")).toBeNull();
  });

  test("create stores and get returns document", async () => {
    await engine.create(collection, "u1", { id: "u1", name: "Sam" }, { primary: "u1" });

    expect(await engine.get(collection, "u1")).toEqual({ id: "u1", name: "Sam" });
  });

  test("supports custom internal table names", async () => {
    const customEngine = mySqlEngine({
      client: requirePool(),
      database: databaseName,
      documentsTable: "custom_documents",
      indexesTable: "custom_indexes",
      migrationLocksTable: "custom_locks",
      migrationCheckpointsTable: "custom_checkpoints",
    });
    const customCollection = nextCollection("custom");

    await customEngine.put(customCollection, "u1", { id: "u1" }, { primary: "u1" });

    const [rows] = await requirePool().query(
      `
        SELECT doc_key
        FROM ${qualifiedTableName("custom_documents")}
        WHERE collection = ? AND doc_key = ?
      `,
      [customCollection, "u1"],
    );
    const lock = await customEngine.migration.acquireLock(customCollection);

    expect(Array.isArray(rows)).toBe(true);
    expect((rows as unknown[]).length).toBe(1);
    expect(lock).not.toBeNull();
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
      /circular|cyclic|serialize|json/i,
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

  test("get throws for invalid stored document payload", async () => {
    await engine.get(collection, "bootstrap");

    await requirePool().query(
      `
        INSERT INTO ${qualifiedTableName("nosql_odm_documents")} (collection, doc_key, doc_json)
        VALUES (?, ?, CAST(? AS JSON))
      `,
      [collection, "bad", JSON.stringify("not-an-object")],
    );

    await expectReject(engine.get(collection, "bad"), /invalid json object/i);
  });

  test("loadCheckpoint throws for invalid checkpoint row", async () => {
    const checkpointsTable = createSqlIdentifier("mysql_checkpoint_invalid");
    const customEngine = mySqlEngine({
      client: requirePool(),
      database: databaseName,
      migrationCheckpointsTable: checkpointsTable,
    });

    await customEngine.get(collection, "bootstrap");

    await requirePool().query(
      `
        ALTER TABLE ${qualifiedTableName(checkpointsTable)}
        MODIFY COLUMN checkpoint_cursor BIGINT NOT NULL
      `,
    );

    await requirePool().query(
      `
        INSERT INTO ${qualifiedTableName(checkpointsTable)} (collection, checkpoint_cursor)
        VALUES (?, ?)
      `,
      [collection, 123],
    );

    await expectReject(
      customEngine.migration.loadCheckpoint!(collection),
      /invalid migration checkpoint row/i,
    );
  });

  test("getStatus throws for invalid lock row", async () => {
    const locksTable = createSqlIdentifier("mysql_lock_status_invalid");
    const checkpointsTable = createSqlIdentifier("mysql_checkpoint_status_valid");
    const customEngine = mySqlEngine({
      client: requirePool(),
      database: databaseName,
      migrationLocksTable: locksTable,
      migrationCheckpointsTable: checkpointsTable,
    });

    await customEngine.get(collection, "bootstrap");

    await requirePool().query(
      `
        ALTER TABLE ${qualifiedTableName(locksTable)}
        MODIFY COLUMN acquired_at VARCHAR(64) NOT NULL
      `,
    );

    await requirePool().query(
      `
        INSERT INTO ${qualifiedTableName(locksTable)} (collection, lock_id, acquired_at)
        VALUES (?, ?, ?)
      `,
      [collection, "bad-lock", "not-a-number"],
    );

    await expectReject(
      customEngine.migration.getStatus!(collection),
      /invalid migration status row/i,
    );
  });

  test("getStatus throws for invalid checkpoint row", async () => {
    const locksTable = createSqlIdentifier("mysql_lock_status_valid");
    const checkpointsTable = createSqlIdentifier("mysql_checkpoint_status_invalid");
    const customEngine = mySqlEngine({
      client: requirePool(),
      database: databaseName,
      migrationLocksTable: locksTable,
      migrationCheckpointsTable: checkpointsTable,
    });

    await customEngine.get(collection, "bootstrap");

    await requirePool().query(
      `
        ALTER TABLE ${qualifiedTableName(checkpointsTable)}
        MODIFY COLUMN checkpoint_cursor BIGINT NOT NULL
      `,
    );

    await requirePool().query(
      `
        INSERT INTO ${qualifiedTableName(locksTable)} (collection, lock_id, acquired_at)
        VALUES (?, ?, ?)
      `,
      [collection, "ok-lock", Date.now()],
    );

    await requirePool().query(
      `
        INSERT INTO ${qualifiedTableName(checkpointsTable)} (collection, checkpoint_cursor)
        VALUES (?, ?)
      `,
      [collection, 123],
    );

    await expectReject(
      customEngine.migration.getStatus!(collection),
      /invalid migration status row/i,
    );
  });

  test("acquireLock throws for invalid existing lock row", async () => {
    const locksTable = createSqlIdentifier("mysql_lock_acquire_invalid");
    const checkpointsTable = createSqlIdentifier("mysql_checkpoint_acquire_valid");
    const customEngine = mySqlEngine({
      client: requirePool(),
      database: databaseName,
      migrationLocksTable: locksTable,
      migrationCheckpointsTable: checkpointsTable,
    });

    await customEngine.get(collection, "bootstrap");

    await requirePool().query(
      `
        ALTER TABLE ${qualifiedTableName(locksTable)}
        MODIFY COLUMN acquired_at VARCHAR(64) NOT NULL
      `,
    );

    await requirePool().query(
      `
        INSERT INTO ${qualifiedTableName(locksTable)} (collection, lock_id, acquired_at)
        VALUES (?, ?, ?)
      `,
      [collection, "bad-lock", "not-a-number"],
    );

    await expectReject(
      customEngine.migration.acquireLock(collection, {
        ttl: 0,
      }),
      /invalid migration lock row/i,
    );
  });
});
