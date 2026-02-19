import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { indexedDB as fakeIndexedDB } from "fake-indexeddb";

import { indexedDbEngine, type IndexedDbQueryEngine } from "../../src/engines/indexeddb";
import {
  EngineDocumentAlreadyExistsError,
  EngineDocumentNotFoundError,
  type ComparableVersion,
} from "../../src/engines/types";
import { createCollectionNameFactory, createTestResourceName, expectReject } from "./helpers";
import { runMigrationIntegrationSuite } from "./migration-suite";

let engine: IndexedDbQueryEngine;
let databaseNameCounter = 0;
let currentDatabaseName = "";
const databaseNameBase = createTestResourceName("nosql_odm_indexeddb_test");
const nextCollection = createCollectionNameFactory();
type IndexedDbFactory = NonNullable<NonNullable<Parameters<typeof indexedDbEngine>[0]>["factory"]>;
type RawHandler = ((event: unknown) => void) | null;

interface RawRequest<T> {
  readonly result: T;
  readonly error: unknown;
  onsuccess: RawHandler;
  onerror: RawHandler;
}

interface RawOpenRequest<TDatabase> extends RawRequest<TDatabase> {
  onupgradeneeded: RawHandler;
}

interface RawObjectStore {
  put(value: unknown): RawRequest<unknown>;
}

interface RawTransaction {
  objectStore(name: string): RawObjectStore;
  oncomplete: RawHandler;
  onerror: RawHandler;
  onabort: RawHandler;
  readonly error: unknown;
}

interface RawDatabase {
  transaction(storeNames: string[], mode: "readwrite" | "readonly"): RawTransaction;
  close(): void;
}

const RAW_STORE_DOCUMENTS = "documents";
const RAW_STORE_META = "meta";
const RAW_STORE_MIGRATION_LOCKS = "migration_locks";
const RAW_STORE_MIGRATION_CHECKPOINTS = "migration_checkpoints";

function createEngine(): IndexedDbQueryEngine {
  databaseNameCounter += 1;
  currentDatabaseName = `${databaseNameBase}_${String(databaseNameCounter)}`;

  return indexedDbEngine({
    databaseName: currentDatabaseName,
    factory: fakeIndexedDB as unknown as IndexedDbFactory,
  });
}

beforeEach(() => {
  engine = createEngine();
});

afterEach(async () => {
  await engine.deleteDatabase();
});

runMigrationIntegrationSuite({
  engineName: "indexedDbEngine integration",
  getEngine: () => engine,
  nextCollection,
});

async function openRawDatabase(databaseName: string): Promise<RawDatabase> {
  return new Promise((resolve, reject) => {
    const request = (fakeIndexedDB as unknown as IndexedDbFactory).open(
      databaseName,
    ) as unknown as RawOpenRequest<RawDatabase>;

    request.onsuccess = () => {
      resolve(request.result);
    };

    request.onerror = () => {
      reject(request.error ?? new Error(`Failed to open raw IndexedDB database "${databaseName}"`));
    };
  });
}

async function putRawRecord(storeName: string, value: unknown): Promise<void> {
  const db = await openRawDatabase(currentDatabaseName);

  try {
    await new Promise<void>((resolve, reject) => {
      const tx = db.transaction([storeName], "readwrite");
      tx.oncomplete = () => resolve();
      tx.onerror = () => reject(tx.error ?? new Error("Raw IndexedDB transaction failed"));
      tx.onabort = () => reject(tx.error ?? new Error("Raw IndexedDB transaction aborted"));

      tx.objectStore(storeName).put(value);
    });
  } finally {
    db.close();
  }
}

describe("indexedDbEngine setup and failure paths", () => {
  test("throws when no IndexedDB factory is available", () => {
    const g = globalThis as { indexedDB?: unknown };
    const original = g.indexedDB;

    try {
      g.indexedDB = undefined;
      expect(() => indexedDbEngine()).toThrow(/No IndexedDB factory found/);
    } finally {
      g.indexedDB = original;
    }
  });

  test("surfaces open failures from factory", async () => {
    const failingFactory: IndexedDbFactory = {
      open() {
        const request = {
          result: undefined,
          error: new Error("open failed"),
          onsuccess: null,
          onerror: null,
          onupgradeneeded: null,
        } as unknown as RawOpenRequest<RawDatabase>;

        queueMicrotask(() => {
          request.onerror?.({});
        });

        return request as unknown as ReturnType<IndexedDbFactory["open"]>;
      },
      deleteDatabase() {
        const request = {
          result: undefined,
          error: null,
          onsuccess: null,
          onerror: null,
        } as unknown as RawRequest<unknown>;

        queueMicrotask(() => {
          request.onsuccess?.({});
        });

        return request as unknown as ReturnType<IndexedDbFactory["deleteDatabase"]>;
      },
    };

    const broken = indexedDbEngine({
      databaseName: `nosql-odm-indexeddb-open-fail-${Date.now()}`,
      factory: failingFactory,
    });

    await expectReject(broken.get("users", "u1"), /open failed/);
  });
});

describe("indexedDbEngine basic CRUD", () => {
  test("get returns null for missing document", async () => {
    expect(await engine.get("users", "missing")).toBeNull();
  });

  test("create stores and get returns document", async () => {
    await engine.create("users", "u1", { id: "u1", name: "Sam" }, { primary: "u1" });

    expect(await engine.get("users", "u1")).toEqual({ id: "u1", name: "Sam" });
  });

  test("create throws duplicate-key error when key already exists", async () => {
    await engine.create("users", "u1", { id: "u1", name: "Sam" }, { primary: "u1" });

    try {
      await engine.create("users", "u1", { id: "u1", name: "Other" }, { primary: "u1" });
      throw new Error("expected duplicate create to throw");
    } catch (error) {
      expect(error).toBeInstanceOf(EngineDocumentAlreadyExistsError);
    }

    expect(await engine.get("users", "u1")).toEqual({ id: "u1", name: "Sam" });
  });

  test("put upserts document", async () => {
    await engine.put("users", "u1", { id: "u1", name: "Sam" }, { primary: "u1" });
    await engine.put("users", "u1", { id: "u1", name: "Samuel" }, { primary: "u1" });

    expect(await engine.get("users", "u1")).toEqual({
      id: "u1",
      name: "Samuel",
    });
  });

  test("update replaces existing document and indexes", async () => {
    await engine.put(
      "users",
      "u1",
      { id: "u1", email: "old@example.com" },
      { byEmail: "old@example.com" },
    );

    await engine.update(
      "users",
      "u1",
      { id: "u1", email: "new@example.com" },
      { byEmail: "new@example.com" },
    );

    const oldResults = await engine.query("users", {
      index: "byEmail",
      filter: { value: "old@example.com" },
    });

    const newResults = await engine.query("users", {
      index: "byEmail",
      filter: { value: "new@example.com" },
    });

    expect(oldResults.documents).toHaveLength(0);
    expect(newResults.documents).toHaveLength(1);
  });

  test("update throws not-found error when key does not exist", async () => {
    try {
      await engine.update("users", "missing", { id: "missing" }, { primary: "missing" });
      throw new Error("expected update on missing key to throw");
    } catch (error) {
      expect(error).toBeInstanceOf(EngineDocumentNotFoundError);
    }
  });

  test("delete removes document", async () => {
    await engine.put("users", "u1", { id: "u1" }, { primary: "u1" });
    await engine.delete("users", "u1");

    expect(await engine.get("users", "u1")).toBeNull();
  });

  test("delete ignores missing key", async () => {
    await engine.delete("users", "missing");
  });

  test("get returns deep clones, not shared references", async () => {
    await engine.put("users", "u1", { id: "u1", nested: { value: 1 } }, { primary: "u1" });

    const doc1 = (await engine.get("users", "u1")) as {
      nested: { value: number };
    };
    const doc2 = (await engine.get("users", "u1")) as {
      nested: { value: number };
    };

    expect(doc1).toEqual(doc2);
    expect(doc1).not.toBe(doc2);
    expect(doc1.nested).not.toBe(doc2.nested);
  });
});

describe("indexedDbEngine batch methods", () => {
  test("batchSet and batchGet round trip documents", async () => {
    await engine.batchSet("users", [
      { key: "u1", doc: { id: "u1", name: "A" }, indexes: { primary: "u1" } },
      { key: "u2", doc: { id: "u2", name: "B" }, indexes: { primary: "u2" } },
    ]);

    const docs = await engine.batchGet("users", ["u1", "u2", "missing"]);

    expect(docs).toHaveLength(2);
    expect(docs).toContainEqual({ key: "u1", doc: { id: "u1", name: "A" } });
    expect(docs).toContainEqual({ key: "u2", doc: { id: "u2", name: "B" } });
  });

  test("batchDelete removes multiple documents", async () => {
    await engine.batchSet("users", [
      { key: "u1", doc: { id: "u1" }, indexes: { primary: "u1" } },
      { key: "u2", doc: { id: "u2" }, indexes: { primary: "u2" } },
      { key: "u3", doc: { id: "u3" }, indexes: { primary: "u3" } },
    ]);

    await engine.batchDelete("users", ["u1", "u3"]);

    expect(await engine.get("users", "u1")).toBeNull();
    expect(await engine.get("users", "u2")).toEqual({ id: "u2" });
    expect(await engine.get("users", "u3")).toBeNull();
  });

  test("batchGet returns deep clones per returned item", async () => {
    await engine.put("users", "u1", { id: "u1", nested: { value: 1 } }, { primary: "u1" });

    const docs = await engine.batchGet("users", ["u1", "u1"]);

    expect(docs).toHaveLength(2);
    expect(docs[0]?.key).toBe("u1");
    expect(docs[1]?.key).toBe("u1");
    expect(docs[0]!.doc).not.toBe(docs[1]!.doc);
  });
});

describe("indexedDbEngine query behavior", () => {
  test("query with index equality", async () => {
    await engine.put("users", "u1", { id: "u1" }, { status: "active" });
    await engine.put("users", "u2", { id: "u2" }, { status: "inactive" });

    const results = await engine.query("users", {
      index: "status",
      filter: { value: "active" },
    });

    expect(results.documents).toEqual([{ key: "u1", doc: { id: "u1" } }]);
  });

  test("query supports comparison filters", async () => {
    await engine.put("items", "a", { id: "a" }, { byDate: "2025-01-01" });
    await engine.put("items", "b", { id: "b" }, { byDate: "2025-06-15" });
    await engine.put("items", "c", { id: "c" }, { byDate: "2025-12-31" });

    const results = await engine.query("items", {
      index: "byDate",
      filter: { value: { $between: ["2025-01-01", "2025-06-15"] } },
    });

    expect(results.documents).toHaveLength(2);
    expect(results.documents.map((item) => item.key)).toEqual(["a", "b"]);
  });

  test("query sort asc/desc for indexed queries", async () => {
    await engine.put("items", "a", { id: "a" }, { byDate: "2025-03-01" });
    await engine.put("items", "b", { id: "b" }, { byDate: "2025-01-01" });
    await engine.put("items", "c", { id: "c" }, { byDate: "2025-02-01" });

    const asc = await engine.query("items", {
      index: "byDate",
      filter: { value: { $begins: "2025-" } },
      sort: "asc",
    });

    const desc = await engine.query("items", {
      index: "byDate",
      filter: { value: { $begins: "2025-" } },
      sort: "desc",
    });

    expect(asc.documents.map((item) => item.key)).toEqual(["b", "c", "a"]);
    expect(desc.documents.map((item) => item.key)).toEqual(["a", "c", "b"]);
  });

  test("query pagination with cursor", async () => {
    await engine.put("users", "u1", { id: "u1" }, { status: "active" });
    await engine.put("users", "u2", { id: "u2" }, { status: "active" });
    await engine.put("users", "u3", { id: "u3" }, { status: "active" });

    const page1 = await engine.query("users", {
      index: "status",
      filter: { value: "active" },
      limit: 2,
    });

    const page2 = await engine.query("users", {
      index: "status",
      filter: { value: "active" },
      cursor: page1.cursor ?? undefined,
      limit: 2,
    });

    expect(page1.documents).toHaveLength(2);
    expect(page1.cursor).not.toBeNull();
    expect(page2.documents).toHaveLength(1);
    expect(page2.cursor).toBeNull();
  });

  test("scan query returns all collection documents", async () => {
    await engine.put("users", "u1", { id: "u1" }, {});
    await engine.put("users", "u2", { id: "u2" }, {});
    await engine.put("posts", "p1", { id: "p1" }, {});

    const results = await engine.query("users", {});

    expect(results.documents.map((item) => item.key)).toEqual(["u1", "u2"]);
  });

  test("query with missing index value excludes document", async () => {
    await engine.put("users", "u1", { id: "u1" }, { byEmail: "a@example.com" });
    await engine.put("users", "u2", { id: "u2" }, {});

    const results = await engine.query("users", {
      index: "byEmail",
      filter: { value: { $begins: "a@" } },
    });

    expect(results.documents).toEqual([{ key: "u1", doc: { id: "u1" } }]);
  });

  test("query limit 0 returns empty terminal page", async () => {
    await engine.put("users", "u1", { id: "u1" }, { primary: "u1" });

    const results = await engine.query("users", { limit: 0 });

    expect(results.documents).toHaveLength(0);
    expect(results.cursor).toBeNull();
  });

  test("query non-finite limit is treated as no limit", async () => {
    await engine.put("users", "u1", { id: "u1" }, { primary: "u1" });
    await engine.put("users", "u2", { id: "u2" }, { primary: "u2" });

    const results = await engine.query("users", {
      limit: Number.POSITIVE_INFINITY,
    });

    expect(results.documents).toHaveLength(2);
    expect(results.cursor).toBeNull();
  });

  test("query fractional limit is floored", async () => {
    await engine.put("users", "u1", { id: "u1" }, { primary: "u1" });
    await engine.put("users", "u2", { id: "u2" }, { primary: "u2" });
    await engine.put("users", "u3", { id: "u3" }, { primary: "u3" });

    const results = await engine.query("users", { limit: 1.9 });

    expect(results.documents).toHaveLength(1);
    expect(results.cursor).toBe("u1");
  });

  test("query with unrecognized cursor starts from beginning", async () => {
    await engine.put("users", "u1", { id: "u1" }, { primary: "u1" });
    await engine.put("users", "u2", { id: "u2" }, { primary: "u2" });

    const results = await engine.query("users", {
      cursor: "does-not-exist",
      limit: 1,
    });

    expect(results.documents.map((item) => item.key)).toEqual(["u1"]);
    expect(results.cursor).toBe("u1");
  });

  test("query cursor at last item returns empty page", async () => {
    await engine.put("users", "u1", { id: "u1" }, { primary: "u1" });

    const results = await engine.query("users", {
      cursor: "u1",
      limit: 10,
    });

    expect(results.documents).toHaveLength(0);
    expect(results.cursor).toBeNull();
  });

  test("query returns deep-cloned documents", async () => {
    await engine.put("users", "u1", { id: "u1", nested: { value: 1 } }, { primary: "u1" });

    const r1 = await engine.query("users", {});
    const r2 = await engine.query("users", {});

    expect(r1.documents[0]!.doc).toEqual(r2.documents[0]!.doc);
    expect(r1.documents[0]!.doc).not.toBe(r2.documents[0]!.doc);
  });
});

describe("indexedDbEngine migration lock/checkpoint/status", () => {
  test("acquireLock and releaseLock with id-check semantics", async () => {
    const lock = await engine.migration.acquireLock("users");

    expect(lock).not.toBeNull();
    expect(await engine.migration.acquireLock("users")).toBeNull();

    // Wrong lock id should not release.
    await engine.migration.releaseLock({
      id: "wrong-id",
      collection: "users",
      acquiredAt: lock!.acquiredAt,
    });

    expect(await engine.migration.acquireLock("users")).toBeNull();

    await engine.migration.releaseLock(lock!);
    expect(await engine.migration.acquireLock("users")).not.toBeNull();
  });

  test("ttl can steal stale lock", async () => {
    const first = await engine.migration.acquireLock("users");

    expect(first).not.toBeNull();
    const second = await engine.migration.acquireLock("users", { ttl: 0 });

    expect(second).not.toBeNull();
    expect(second?.id).not.toBe(first?.id);
  });

  test("invalid ttl values do not steal and collection scope is isolated", async () => {
    const lock = await engine.migration.acquireLock("users");
    const otherLock = await engine.migration.acquireLock("orders");

    expect(lock).not.toBeNull();
    expect(otherLock).not.toBeNull();

    for (const ttl of [Number.NaN, Number.POSITIVE_INFINITY, -1]) {
      expect(
        await engine.migration.acquireLock("users", {
          ttl,
        }),
      ).toBeNull();
    }

    expect(await engine.migration.acquireLock("users")).toBeNull();
    expect(await engine.migration.acquireLock("orders")).toBeNull();
  });

  test("checkpoint writes require current lock owner", async () => {
    const first = await engine.migration.acquireLock("users");

    expect(first).not.toBeNull();
    await engine.migration.saveCheckpoint!(first!, "cursor-a");
    expect(await engine.migration.loadCheckpoint!("users")).toBe("cursor-a");

    const second = await engine.migration.acquireLock("users", { ttl: 0 });
    expect(second).not.toBeNull();

    // stale lock holder should not overwrite
    await engine.migration.saveCheckpoint!(first!, "cursor-stale");
    expect(await engine.migration.loadCheckpoint!("users")).toBe("cursor-a");

    await engine.migration.saveCheckpoint!(second!, "cursor-b");
    expect(await engine.migration.loadCheckpoint!("users")).toBe("cursor-b");

    await engine.migration.clearCheckpoint!("users");
    expect(await engine.migration.loadCheckpoint!("users")).toBeNull();
  });

  test("getStatus returns lock and checkpoint", async () => {
    expect(await engine.migration.getStatus!("users")).toBeNull();

    const lock = await engine.migration.acquireLock("users");
    await engine.migration.saveCheckpoint!(lock!, "cursor-1");

    const status = await engine.migration.getStatus!("users");

    expect(status).not.toBeNull();
    expect(status?.lock.id).toBe(lock?.id);
    expect(status?.cursor).toBe("cursor-1");
  });

  test("getStatus shape variants and collection scope", async () => {
    expect(await engine.migration.getStatus!("users")).toBeNull();

    const lock = await engine.migration.acquireLock("users");
    const otherLock = await engine.migration.acquireLock("orders");
    expect(lock).not.toBeNull();
    expect(otherLock).not.toBeNull();

    const withoutCursor = await engine.migration.getStatus!("users");
    expect(withoutCursor?.lock.id).toBe(lock?.id);
    expect(withoutCursor?.cursor).toBeNull();
    expect(await engine.migration.getStatus!("orders")).not.toBeNull();

    await engine.migration.saveCheckpoint!(lock!, "cursor-a");
    const withCursor = await engine.migration.getStatus!("users");
    expect(withCursor?.lock.id).toBe(lock?.id);
    expect(withCursor?.cursor).toBe("cursor-a");
  });
});

describe("indexedDbEngine migration getOutdated", () => {
  const compareVersions = (a: ComparableVersion, b: ComparableVersion): number => {
    if (typeof a === "number" && typeof b === "number") {
      return a - b;
    }

    return String(a).localeCompare(String(b), undefined, {
      numeric: true,
      sensitivity: "base",
    });
  };

  test("returns stale-version and reindex-needed documents", async () => {
    await engine.put(
      "users",
      "stale",
      { __v: 1, __indexes: ["primary"], id: "stale" },
      { primary: "stale" },
    );
    await engine.put(
      "users",
      "reindex",
      { __v: 2, __indexes: ["primary"], id: "reindex" },
      { primary: "reindex" },
    );
    await engine.put(
      "users",
      "current",
      { __v: 2, __indexes: ["byEmail", "primary"], id: "current" },
      { primary: "current" },
    );
    await engine.put(
      "users",
      "ahead",
      { __v: 3, __indexes: ["byEmail", "primary"], id: "ahead" },
      { primary: "ahead" },
    );

    const page = await engine.migration.getOutdated("users", {
      version: 2,
      versionField: "__v",
      indexesField: "__indexes",
      indexes: ["byEmail", "primary"],
      compareVersions,
    });

    expect(page.documents.map((item) => item.key)).toEqual(["stale", "reindex"]);
    expect(page.cursor).toBeNull();
  });

  test("treats wrong __indexes order as outdated", async () => {
    await engine.put(
      "users",
      "u1",
      { __v: 2, __indexes: ["primary", "byEmail"], id: "u1" },
      { primary: "u1" },
    );

    const page = await engine.migration.getOutdated("users", {
      version: 2,
      versionField: "__v",
      indexesField: "__indexes",
      indexes: ["byEmail", "primary"],
      compareVersions,
    });

    expect(page.documents.map((item) => item.key)).toEqual(["u1"]);
  });

  test("ignores documents with unrecognized versions", async () => {
    await engine.put(
      "users",
      "u1",
      { __v: "release-candidate", __indexes: ["byEmail", "primary"], id: "u1" },
      { primary: "u1" },
    );

    const page = await engine.migration.getOutdated("users", {
      version: 2,
      versionField: "__v",
      indexesField: "__indexes",
      indexes: ["byEmail", "primary"],
    });

    expect(page.documents).toHaveLength(0);
  });

  test("ignores documents when compareVersions throws", async () => {
    await engine.put(
      "users",
      "u1",
      { __v: "bad", __indexes: ["byEmail", "primary"], id: "u1" },
      { primary: "u1" },
    );

    const page = await engine.migration.getOutdated("users", {
      version: 2,
      versionField: "__v",
      indexesField: "__indexes",
      indexes: ["byEmail", "primary"],
      parseVersion(raw) {
        if (typeof raw === "string") {
          return raw;
        }

        return null;
      },
      compareVersions() {
        throw new Error("bad compare");
      },
    });

    expect(page.documents).toHaveLength(0);
  });

  test("paginates outdated results with cursor", async () => {
    for (let i = 0; i < 105; i++) {
      await engine.put(
        "users",
        `u${String(i).padStart(3, "0")}`,
        {
          __v: 1,
          __indexes: ["primary"],
          id: `u${String(i).padStart(3, "0")}`,
        },
        { primary: `u${String(i).padStart(3, "0")}` },
      );
    }

    const first = await engine.migration.getOutdated("users", {
      version: 2,
      versionField: "__v",
      indexesField: "__indexes",
      indexes: ["primary"],
      compareVersions,
    });

    expect(first.documents).toHaveLength(100);
    expect(first.cursor).not.toBeNull();

    const second = await engine.migration.getOutdated(
      "users",
      {
        version: 2,
        versionField: "__v",
        indexesField: "__indexes",
        indexes: ["primary"],
        compareVersions,
      },
      first.cursor ?? undefined,
    );

    expect(second.documents).toHaveLength(5);
    expect(second.cursor).toBeNull();
  });
});

describe("indexedDbEngine corruption handling", () => {
  test("get throws for invalid stored document records", async () => {
    await engine.get("users", "bootstrap");
    await putRawRecord(RAW_STORE_DOCUMENTS, {
      id: "users\u0000bad",
      collection: "users",
      key: "bad",
      createdAt: 1,
      doc: "not-an-object",
      indexes: {},
    });

    await expectReject(engine.get("users", "bad"), /invalid record \(bad document\)/);
  });

  test("create throws for invalid sequence metadata", async () => {
    await engine.get("users", "bootstrap");
    await putRawRecord(RAW_STORE_META, {
      key: "sequence",
      value: "not-a-number",
    });

    await expectReject(
      engine.create("users", "u1", { id: "u1" }, { primary: "u1" }),
      /invalid sequence record/,
    );
  });

  test("acquireLock throws for invalid lock records", async () => {
    await engine.get("users", "bootstrap");
    await putRawRecord(RAW_STORE_MIGRATION_LOCKS, {
      collection: "users",
      lockId: 123,
      acquiredAt: Date.now(),
    });

    await expectReject(
      engine.migration.acquireLock("users"),
      /migration_locks store contains an invalid record/,
    );
  });

  test("loadCheckpoint throws for invalid checkpoint records", async () => {
    await engine.get("users", "bootstrap");
    await putRawRecord(RAW_STORE_MIGRATION_CHECKPOINTS, {
      collection: "users",
      cursor: 123,
    });

    await expectReject(
      engine.migration.loadCheckpoint!("users"),
      /migration_checkpoints store contains an invalid record/,
    );
  });
});
