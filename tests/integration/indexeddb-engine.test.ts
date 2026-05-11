import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { IDBKeyRange, indexedDB as fakeIndexedDB } from "fake-indexeddb";
import * as z from "zod";

import { indexedDbEngine, type IndexedDbQueryEngine } from "../../src/engines/indexeddb";
import {
  EngineDocumentAlreadyExistsError,
  EngineDocumentNotFoundError,
  EngineUniqueConstraintError,
  type ComparableVersion,
  type QueryEngine,
} from "../../src/engines/types";
import { model } from "../../src/model";
import { ConcurrentWriteError, createStore } from "../../src/store";
import { runQueryEngineConformanceSuite } from "./conformance-suite";
import {
  createCollectionNameFactory,
  createTestResourceName,
  expectReject,
  expectRejectInstanceOf,
} from "./helpers";
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
  get(key: string): RawRequest<unknown>;
  getAll(): RawRequest<unknown[]>;
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

function createDocumentGetAllGuardFactory() {
  let blockDocumentGetAll = false;
  let blockQueryIndexEntriesGetAll = false;

  const wrapObjectStore = (storeName: string, store: unknown): unknown => {
    if (storeName !== RAW_STORE_DOCUMENTS && storeName !== "query_index_entries") {
      return store;
    }

    return new Proxy(store as Record<string, unknown>, {
      get(target, property, receiver) {
        if (property !== "getAll") {
          return Reflect.get(target, property, receiver);
        }

        return () => {
          if (storeName === RAW_STORE_DOCUMENTS && blockDocumentGetAll) {
            throw new Error("documents.getAll() should not be used for indexed query execution");
          }

          if (storeName === "query_index_entries" && blockQueryIndexEntriesGetAll) {
            throw new Error(
              "query_index_entries.getAll() should not be used for indexed query execution",
            );
          }

          return (Reflect.get(target, property, receiver) as () => unknown).call(target);
        };
      },
    });
  };

  const wrapTransaction = (transaction: unknown): unknown => {
    return new Proxy(transaction as Record<string, unknown>, {
      get(target, property, receiver) {
        if (property !== "objectStore") {
          return Reflect.get(target, property, receiver);
        }

        return (storeName: string) => {
          const objectStore = (
            Reflect.get(target, property, receiver) as (name: string) => unknown
          ).call(target, storeName);

          return wrapObjectStore(storeName, objectStore);
        };
      },
    });
  };

  const wrapDatabase = (database: unknown): unknown => {
    return new Proxy(database as Record<string, unknown>, {
      get(target, property, receiver) {
        if (property !== "transaction") {
          return Reflect.get(target, property, receiver);
        }

        return (storeNames: string | string[], mode?: "readonly" | "readwrite") => {
          const transaction = (
            Reflect.get(target, property, receiver) as (
              names: string | string[],
              mode?: "readonly" | "readwrite",
            ) => unknown
          ).call(target, storeNames, mode);

          return wrapTransaction(transaction);
        };
      },
    });
  };

  const factory = {
    open(name: string, version?: number) {
      const request = (fakeIndexedDB as unknown as IndexedDbFactory).open(name, version);

      return new Proxy(request as unknown as Record<string, unknown>, {
        get(target, property, receiver) {
          if (property === "result") {
            return wrapDatabase(Reflect.get(target, property, receiver));
          }

          return Reflect.get(target, property, receiver);
        },
        set(target, property, value, receiver) {
          return Reflect.set(target, property, value, receiver);
        },
      }) as unknown as ReturnType<IndexedDbFactory["open"]>;
    },
    deleteDatabase(name: string) {
      return (fakeIndexedDB as unknown as IndexedDbFactory).deleteDatabase(name);
    },
  } satisfies IndexedDbFactory;

  return {
    factory,
    blockDocumentGetAll() {
      blockDocumentGetAll = true;
    },
    blockQueryIndexEntriesGetAll() {
      blockQueryIndexEntriesGetAll = true;
    },
  };
}

function createDocumentGetQueueAssertionFactory(expectedGetCount: number) {
  let documentGetCount = 0;
  let assertGetCount = false;

  const wrapRequest = (request: unknown): unknown => {
    return new Proxy(request as Record<string, unknown>, {
      get(target, property, receiver) {
        return Reflect.get(target, property, receiver);
      },
      set(target, property, value, receiver) {
        if (property !== "onsuccess" || typeof value !== "function") {
          return Reflect.set(target, property, value, receiver);
        }

        const wrappedHandler = (event: unknown) => {
          if (assertGetCount) {
            expect(documentGetCount).toBe(expectedGetCount);
          }

          value(event);
        };

        return Reflect.set(target, property, wrappedHandler, receiver);
      },
    });
  };

  const wrapObjectStore = (storeName: string, store: unknown): unknown => {
    if (storeName !== RAW_STORE_DOCUMENTS) {
      return store;
    }

    return new Proxy(store as Record<string, unknown>, {
      get(target, property, receiver) {
        if (property !== "get") {
          return Reflect.get(target, property, receiver);
        }

        return (key: string) => {
          documentGetCount += 1;

          return wrapRequest(
            (Reflect.get(target, property, receiver) as (documentKey: string) => unknown).call(
              target,
              key,
            ),
          );
        };
      },
    });
  };

  const wrapTransaction = (transaction: unknown): unknown => {
    return new Proxy(transaction as Record<string, unknown>, {
      get(target, property, receiver) {
        if (property !== "objectStore") {
          return Reflect.get(target, property, receiver);
        }

        return (storeName: string) => {
          const objectStore = (
            Reflect.get(target, property, receiver) as (name: string) => unknown
          ).call(target, storeName);

          return wrapObjectStore(storeName, objectStore);
        };
      },
    });
  };

  const wrapDatabase = (database: unknown): unknown => {
    return new Proxy(database as Record<string, unknown>, {
      get(target, property, receiver) {
        if (property !== "transaction") {
          return Reflect.get(target, property, receiver);
        }

        return (storeNames: string | string[], mode?: "readonly" | "readwrite") => {
          const transaction = (
            Reflect.get(target, property, receiver) as (
              names: string | string[],
              mode?: "readonly" | "readwrite",
            ) => unknown
          ).call(target, storeNames, mode);

          return wrapTransaction(transaction);
        };
      },
    });
  };

  const factory = {
    open(name: string, version?: number) {
      const request = (fakeIndexedDB as unknown as IndexedDbFactory).open(name, version);

      return new Proxy(request as unknown as Record<string, unknown>, {
        get(target, property, receiver) {
          if (property === "result") {
            return wrapDatabase(Reflect.get(target, property, receiver));
          }

          return Reflect.get(target, property, receiver);
        },
        set(target, property, value, receiver) {
          return Reflect.set(target, property, value, receiver);
        },
      }) as unknown as ReturnType<IndexedDbFactory["open"]>;
    },
    deleteDatabase(name: string) {
      return (fakeIndexedDB as unknown as IndexedDbFactory).deleteDatabase(name);
    },
  } satisfies IndexedDbFactory;

  return {
    factory,
    startAssertion() {
      documentGetCount = 0;
      assertGetCount = true;
    },
    get documentGetCount() {
      return documentGetCount;
    },
  };
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

runQueryEngineConformanceSuite({
  engineName: "indexedDbEngine integration",
  getEngine: () => engine,
  nextCollection,
  assertEngineUniqueConstraintConformance: true,
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

  test("metadata reads expose changing write tokens", async () => {
    await engine.put("users", "u1", { id: "u1", name: "Sam" }, { primary: "u1" });

    const first = await engine.getWithMetadata!("users", "u1");

    await engine.update("users", "u1", { id: "u1", name: "Samuel" }, { primary: "u1" });

    const second = await engine.getWithMetadata!("users", "u1");

    expect(first?.writeToken).toBe("1");
    expect(second?.writeToken).toBe("2");
    expect(second?.doc).toEqual({ id: "u1", name: "Samuel" });
  });

  test("store.update throws ConcurrentWriteError when an IndexedDB document changes after read", async () => {
    const User = model("user")
      .schema(
        1,
        z.object({
          id: z.string(),
          name: z.string(),
          email: z.email(),
        }),
      )
      .index({ name: "primary", value: "id" })
      .build();
    const conflictingEngine: QueryEngine = {
      ...engine,
      async getWithMetadata(collection, key) {
        const result = await engine.getWithMetadata!(collection, key);

        if (key === "u1") {
          await engine.update(
            collection,
            key,
            {
              __v: 1,
              __indexes: ["primary"],
              id: "u1",
              name: "Concurrent",
              email: "sam@example.com",
            },
            { primary: "u1" },
          );
        }

        return result;
      },
    };
    const store = createStore(conflictingEngine, [User]);

    await store.user.create("u1", {
      id: "u1",
      name: "Sam",
      email: "sam@example.com",
    });

    await expectRejectInstanceOf(store.user.update("u1", { name: "Samuel" }), ConcurrentWriteError);
    expect(await store.user.findByKey("u1")).toEqual({
      id: "u1",
      name: "Concurrent",
      email: "sam@example.com",
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

  test("batchGet queues document reads before awaiting request results", async () => {
    const asserted = createDocumentGetQueueAssertionFactory(4);
    const indexedEngine = indexedDbEngine({
      databaseName: `${databaseNameBase}_batch_get_queue_${Date.now()}`,
      factory: asserted.factory,
    });

    try {
      await indexedEngine.batchSet("users", [
        { key: "u1", doc: { id: "u1", name: "A" }, indexes: { primary: "u1" } },
        { key: "u2", doc: { id: "u2", name: "B" }, indexes: { primary: "u2" } },
      ]);

      asserted.startAssertion();
      const docs = await indexedEngine.batchGet("users", ["u2", "u1", "u2", "missing"]);

      expect(asserted.documentGetCount).toBe(4);
      expect(docs.map((entry) => entry.key)).toEqual(["u2", "u1", "u2"]);
      expect(docs[0]?.doc).toEqual({ id: "u2", name: "B" });
      expect(docs[1]?.doc).toEqual({ id: "u1", name: "A" });
      expect(docs[2]?.doc).toEqual({ id: "u2", name: "B" });
    } finally {
      await indexedEngine.deleteDatabase();
    }
  });

  test("batchGetWithMetadata queues document reads before awaiting request results", async () => {
    const asserted = createDocumentGetQueueAssertionFactory(4);
    const indexedEngine = indexedDbEngine({
      databaseName: `${databaseNameBase}_batch_get_metadata_queue_${Date.now()}`,
      factory: asserted.factory,
    });

    try {
      await indexedEngine.batchSet("users", [
        { key: "u1", doc: { id: "u1", name: "A" }, indexes: { primary: "u1" } },
        { key: "u2", doc: { id: "u2", name: "B" }, indexes: { primary: "u2" } },
      ]);

      asserted.startAssertion();
      const docs = await indexedEngine.batchGetWithMetadata!("users", [
        "u2",
        "u1",
        "u2",
        "missing",
      ]);

      expect(asserted.documentGetCount).toBe(4);
      expect(docs.map((entry) => entry.key)).toEqual(["u2", "u1", "u2"]);
      expect(docs[0]?.doc).toEqual({ id: "u2", name: "B" });
      expect(docs[0]?.writeToken).toBe("1");
      expect(docs[1]?.doc).toEqual({ id: "u1", name: "A" });
      expect(docs[1]?.writeToken).toBe("1");
      expect(docs[2]?.doc).toEqual({ id: "u2", name: "B" });
      expect(docs[2]?.writeToken).toBe("1");
    } finally {
      await indexedEngine.deleteDatabase();
    }
  });

  test("batchSet enforces every unique index field atomically", async () => {
    try {
      await engine.batchSet!("users", [
        {
          key: "u1",
          doc: { id: "u1", email: "sam@example.com", username: "sam" },
          indexes: { byEmail: "sam@example.com", byUsername: "sam" },
          uniqueIndexes: { byEmail: "sam@example.com", byUsername: "sam" },
        },
        {
          key: "u2",
          doc: { id: "u2", email: "jamie@example.com", username: "sam" },
          indexes: { byEmail: "jamie@example.com", byUsername: "sam" },
          uniqueIndexes: { byEmail: "jamie@example.com", byUsername: "sam" },
        },
      ]);
      throw new Error("expected batchSet to fail with unique constraint error");
    } catch (error) {
      expect(error).toBeInstanceOf(EngineUniqueConstraintError);
    }

    expect(await engine.get("users", "u1")).toBeNull();
    expect(await engine.get("users", "u2")).toBeNull();
  });

  test("unique writes avoid collection document scans", async () => {
    const guarded = createDocumentGetAllGuardFactory();
    const indexedEngine = indexedDbEngine({
      databaseName: `${databaseNameBase}_unique_write_guarded_${Date.now()}`,
      factory: guarded.factory,
    });

    try {
      await indexedEngine.batchSet("users", [
        {
          key: "u1",
          doc: { id: "u1", email: "sam@example.com" },
          indexes: { byEmail: "sam@example.com" },
          uniqueIndexes: { byEmail: "sam@example.com" },
        },
        {
          key: "u2",
          doc: { id: "u2", email: "jamie@example.com" },
          indexes: { byEmail: "jamie@example.com" },
          uniqueIndexes: { byEmail: "jamie@example.com" },
        },
      ]);

      guarded.blockDocumentGetAll();

      await indexedEngine.create(
        "users",
        "u3",
        { id: "u3", email: "taylor@example.com" },
        { byEmail: "taylor@example.com" },
        undefined,
        undefined,
        { byEmail: "taylor@example.com" },
      );
      await indexedEngine.update(
        "users",
        "u3",
        { id: "u3", email: "alex@example.com" },
        { byEmail: "alex@example.com" },
        undefined,
        undefined,
        { byEmail: "alex@example.com" },
      );
      await indexedEngine.batchSet("users", [
        {
          key: "u4",
          doc: { id: "u4", email: "casey@example.com" },
          indexes: { byEmail: "casey@example.com" },
          uniqueIndexes: { byEmail: "casey@example.com" },
        },
      ]);

      await expectRejectInstanceOf(
        indexedEngine.create(
          "users",
          "u5",
          { id: "u5", email: "alex@example.com" },
          { byEmail: "alex@example.com" },
          undefined,
          undefined,
          { byEmail: "alex@example.com" },
        ),
        EngineUniqueConstraintError,
      );
    } finally {
      await indexedEngine.deleteDatabase();
    }
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

  test("query with index equality avoids collection document loads", async () => {
    const guarded = createDocumentGetAllGuardFactory();
    const indexedEngine = indexedDbEngine({
      databaseName: `${databaseNameBase}_guarded_${Date.now()}`,
      factory: guarded.factory,
    });

    try {
      await indexedEngine.put("users", "u1", { id: "u1" }, { status: "active" });
      await indexedEngine.put("users", "u2", { id: "u2" }, { status: "inactive" });
      guarded.blockDocumentGetAll();

      const results = await indexedEngine.query("users", {
        index: "status",
        filter: { value: "active" },
      });

      expect(results.documents).toEqual([{ key: "u1", doc: { id: "u1" } }]);
    } finally {
      await indexedEngine.deleteDatabase();
    }
  });

  test("query with index equality queues hydration reads before awaiting request results", async () => {
    const asserted = createDocumentGetQueueAssertionFactory(2);
    const indexedEngine = indexedDbEngine({
      databaseName: `${databaseNameBase}_query_hydration_queue_${Date.now()}`,
      factory: asserted.factory,
    });

    try {
      await indexedEngine.put("users", "u1", { id: "u1", name: "A" }, { status: "active" });
      await indexedEngine.put("users", "u2", { id: "u2", name: "B" }, { status: "active" });
      await indexedEngine.put("users", "u3", { id: "u3", name: "C" }, { status: "inactive" });

      asserted.startAssertion();
      const results = await indexedEngine.query("users", {
        index: "status",
        filter: { value: "active" },
      });

      expect(asserted.documentGetCount).toBe(2);
      expect(results.documents.map((entry) => entry.key)).toEqual(["u1", "u2"]);
    } finally {
      await indexedEngine.deleteDatabase();
    }
  });

  test("query with comparison filters uses native index pushdown", async () => {
    const guarded = createDocumentGetAllGuardFactory();
    const indexedEngine = indexedDbEngine({
      databaseName: `${databaseNameBase}_range_guarded_${Date.now()}`,
      factory: guarded.factory,
    });

    try {
      await indexedEngine.put("items", "a", { id: "a" }, { byDate: "2025-01-01" });
      await indexedEngine.put("items", "b", { id: "b" }, { byDate: "2025-06-15" });
      await indexedEngine.put("items", "c", { id: "c" }, { byDate: "2025-12-31" });
      guarded.blockDocumentGetAll();
      guarded.blockQueryIndexEntriesGetAll();

      const results = await indexedEngine.query("items", {
        index: "byDate",
        filter: { value: { $between: ["2025-01-01", "2025-06-15"] } },
      });

      expect(results.documents.map((item) => item.key)).toEqual(["a", "b"]);
      expect(results.diagnostics).toEqual({
        mode: "native_pushdown",
        reason: "native_pushdown",
        index: "byDate",
      });
    } finally {
      await indexedEngine.deleteDatabase();
    }
  });

  test("query index backfill completion does not depend on existing entry count", async () => {
    const guarded = createDocumentGetAllGuardFactory();
    const databaseName = `${databaseNameBase}_backfill_guarded_${Date.now()}`;
    const firstEngine = indexedDbEngine({
      databaseName,
      factory: guarded.factory,
    });

    try {
      await firstEngine.put("users", "u1", { id: "u1" }, {});
      firstEngine.close();
      guarded.blockDocumentGetAll();

      const reopenedEngine = indexedDbEngine({
        databaseName,
        factory: guarded.factory,
      });

      try {
        expect(await reopenedEngine.get("users", "u1")).toEqual({ id: "u1" });
      } finally {
        await reopenedEngine.deleteDatabase();
      }
    } finally {
      firstEngine.close();
    }
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

  test("native key range pushdown includes inclusive upper-bound documents", async () => {
    const keyRangeGlobal = globalThis as typeof globalThis & {
      IDBKeyRange?: typeof IDBKeyRange;
    };
    const previousKeyRange = keyRangeGlobal.IDBKeyRange;
    keyRangeGlobal.IDBKeyRange = IDBKeyRange;

    try {
      await engine.put("items", "a", { id: "a" }, { byDate: "2025-01-01" });
      await engine.put("items", "b", { id: "b" }, { byDate: "2025-06-15" });
      await engine.put("items", "c", { id: "c" }, { byDate: "2025-12-31" });

      const between = await engine.query("items", {
        index: "byDate",
        filter: { value: { $between: ["2025-01-01", "2025-06-15"] } },
      });
      const lte = await engine.query("items", {
        index: "byDate",
        filter: { value: { $lte: "2025-06-15" } },
      });

      expect(between.documents.map((item) => item.key)).toEqual(["a", "b"]);
      expect(lte.documents.map((item) => item.key)).toEqual(["a", "b"]);
    } finally {
      keyRangeGlobal.IDBKeyRange = previousKeyRange;
    }
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

  test("sorted indexed pagination remains stable when the cursor row is deleted", async () => {
    await engine.put("items", "a", { id: "a" }, { byDate: "2025-01-01" });
    await engine.put("items", "b", { id: "b" }, { byDate: "2025-02-01" });
    await engine.put("items", "c", { id: "c" }, { byDate: "2025-03-01" });

    const page1 = await engine.query("items", {
      index: "byDate",
      filter: { value: { $begins: "2025-" } },
      sort: "asc",
      limit: 2,
    });

    await engine.delete("items", page1.documents[1]!.key);

    const page2 = await engine.query("items", {
      index: "byDate",
      filter: { value: { $begins: "2025-" } },
      sort: "asc",
      cursor: page1.cursor ?? undefined,
      limit: 2,
    });

    expect(page1.documents.map((item) => item.key)).toEqual(["a", "b"]);
    expect(page2.documents.map((item) => item.key)).toEqual(["c"]);
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
    expect(results.cursor).not.toBeNull();
    expect(results.cursor).not.toBe("u1");
  });

  test("query with unrecognized cursor is rejected explicitly", async () => {
    await engine.put("users", "u1", { id: "u1" }, { primary: "u1" });
    await engine.put("users", "u2", { id: "u2" }, { primary: "u2" });

    expect(
      engine.query("users", {
        cursor: "does-not-exist",
        limit: 1,
      }),
    ).rejects.toThrow(/cursor/i);
  });

  test("raw key query cursors are rejected", async () => {
    await engine.put("users", "u1", { id: "u1" }, { primary: "u1" });

    expect(
      engine.query("users", {
        cursor: "u1",
        limit: 10,
      }),
    ).rejects.toThrow(/cursor/i);
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
