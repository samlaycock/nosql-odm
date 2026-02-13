import { describe, expect, test, beforeEach, afterEach } from "bun:test";
import { Database as BunDatabase } from "bun:sqlite";
import type BetterSqlite3 from "better-sqlite3";
import { sqliteEngine } from "../src/engines/sqlite";
import type { QueryEngine } from "../src/engines/types";

// ---------------------------------------------------------------------------
// Setup
// ---------------------------------------------------------------------------

class BunBetterSqliteCompat {
  private readonly db: BunDatabase;

  constructor(filename: string = ":memory:") {
    this.db = new BunDatabase(filename);
  }

  prepare(source: string) {
    const stmt = this.db.prepare(source);

    return {
      run: (...params: unknown[]) => stmt.run(...(params as any[])),
      get: (...params: unknown[]) => {
        const row = stmt.get(...(params as any[]));
        return row === null ? undefined : row;
      },
      all: (...params: unknown[]) => stmt.all(...(params as any[])),
    };
  }

  transaction<F extends (...args: any[]) => any>(fn: F) {
    const tx = this.db.transaction(fn);

    const wrapped = ((...args: Parameters<F>): ReturnType<F> => tx(...args)) as F & {
      default: F;
      deferred: F;
      immediate: F;
      exclusive: F;
    };

    wrapped.default = ((...args: Parameters<F>): ReturnType<F> => tx(...args)) as F;
    wrapped.deferred = ((...args: Parameters<F>): ReturnType<F> => tx.deferred(...args)) as F;
    wrapped.immediate = ((...args: Parameters<F>): ReturnType<F> => tx.immediate(...args)) as F;
    wrapped.exclusive = ((...args: Parameters<F>): ReturnType<F> => tx.exclusive(...args)) as F;

    return wrapped;
  }

  exec(source: string) {
    this.db.exec(source);
    return this;
  }

  close() {
    this.db.close();
    return this;
  }
}

let engine: QueryEngine<never>;
let sqlite: ReturnType<typeof sqliteEngine>;

beforeEach(() => {
  const database = new BunBetterSqliteCompat(":memory:");
  sqlite = sqliteEngine({
    database: database as unknown as BetterSqlite3.Database,
  });
  engine = sqlite;
});

afterEach(() => {
  sqlite.close();
});

// ---------------------------------------------------------------------------
// SQLite-specific setup behavior
// ---------------------------------------------------------------------------

describe("sqlite setup", () => {
  test("creates required internal tables and sets schema version", () => {
    const setupEngine = sqliteEngine({
      database: new BunBetterSqliteCompat(":memory:") as unknown as BetterSqlite3.Database,
    });

    try {
      const versionRow = setupEngine.db.prepare(`PRAGMA user_version`).get() as {
        user_version?: unknown;
      };
      expect(Number(versionRow.user_version)).toBe(1);

      const tables = new Set(
        (
          setupEngine.db
            .prepare(
              `
                SELECT name
                FROM sqlite_master
                WHERE type = 'table'
              `,
            )
            .all() as { name: string }[]
        ).map((table) => table.name),
      );

      expect(tables.has("documents")).toBe(true);
      expect(tables.has("index_entries")).toBe(true);
      expect(tables.has("migration_locks")).toBe(true);
      expect(tables.has("migration_checkpoints")).toBe(true);
    } finally {
      setupEngine.close();
    }
  });

  test("throws when schema version is newer than supported", () => {
    const raw = new BunBetterSqliteCompat(":memory:");
    raw.exec("PRAGMA user_version = 999");

    expect(() =>
      sqliteEngine({
        database: raw as unknown as BetterSqlite3.Database,
      }),
    ).toThrow(/newer than supported/);

    raw.close();
  });

  test("throws when schema version is invalid", () => {
    const raw = new BunBetterSqliteCompat(":memory:");
    raw.exec("PRAGMA user_version = -1");

    expect(() =>
      sqliteEngine({
        database: raw as unknown as BetterSqlite3.Database,
      }),
    ).toThrow(/Invalid SQLite user_version/);

    raw.close();
  });

  test("uses an already-migrated schema at current version", async () => {
    const raw = new BunBetterSqliteCompat(":memory:");
    raw.exec(`
      CREATE TABLE IF NOT EXISTS documents (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        collection TEXT NOT NULL,
        doc_key TEXT NOT NULL,
        doc_json TEXT NOT NULL,
        UNIQUE (collection, doc_key)
      );

      CREATE INDEX IF NOT EXISTS idx_documents_collection_id
        ON documents (collection, id);

      CREATE TABLE IF NOT EXISTS index_entries (
        collection TEXT NOT NULL,
        doc_key TEXT NOT NULL,
        index_name TEXT NOT NULL,
        index_value TEXT NOT NULL,
        PRIMARY KEY (collection, doc_key, index_name),
        FOREIGN KEY (collection, doc_key)
          REFERENCES documents (collection, doc_key)
          ON DELETE CASCADE
      );

      CREATE INDEX IF NOT EXISTS idx_index_entries_lookup
        ON index_entries (collection, index_name, index_value, doc_key);

      CREATE INDEX IF NOT EXISTS idx_index_entries_scan
        ON index_entries (collection, index_name, doc_key);

      CREATE TABLE IF NOT EXISTS migration_locks (
        collection TEXT PRIMARY KEY,
        lock_id TEXT NOT NULL,
        acquired_at INTEGER NOT NULL
      );

      CREATE TABLE IF NOT EXISTS migration_checkpoints (
        collection TEXT PRIMARY KEY,
        cursor TEXT NOT NULL
      );

      PRAGMA user_version = 1;
    `);

    const migratedEngine = sqliteEngine({
      database: raw as unknown as BetterSqlite3.Database,
    });

    try {
      await migratedEngine.put("users", "a", { id: "a" }, { primary: "a" });
      expect(await migratedEngine.get("users", "a")).toEqual({ id: "a" });
    } finally {
      migratedEngine.close();
    }
  });
});

// ---------------------------------------------------------------------------
// SQLite-specific runtime behavior
// ---------------------------------------------------------------------------

describe("sqlite runtime behavior", () => {
  test("deleting a document cascades and removes index_entries rows", async () => {
    await engine.put(
      "users",
      "abc",
      { id: "abc", email: "sam@example.com" },
      { byEmail: "sam@example.com" },
    );

    const before = sqlite.db
      .prepare(`SELECT COUNT(*) AS count FROM index_entries WHERE collection = ? AND doc_key = ?`)
      .get("users", "abc") as { count: number };

    expect(Number(before.count)).toBe(1);

    await engine.delete("users", "abc");

    const after = sqlite.db
      .prepare(`SELECT COUNT(*) AS count FROM index_entries WHERE collection = ? AND doc_key = ?`)
      .get("users", "abc") as { count: number };

    expect(Number(after.count)).toBe(0);
  });

  test("throws a clear error when stored JSON is invalid", async () => {
    sqlite.db
      .prepare(`INSERT INTO documents (collection, doc_key, doc_json) VALUES (?, ?, ?)`)
      .run("users", "broken", "{");

    try {
      await engine.get("users", "broken");
      throw new Error("expected get() to throw for invalid JSON");
    } catch (error) {
      expect(String(error)).toMatch(/invalid JSON/);
    }
  });

  test("throws when putting a non-serializable top-level value", async () => {
    try {
      await engine.put("users", "bad", undefined as unknown, {});
      throw new Error("expected put() to throw for non-serializable value");
    } catch (error) {
      expect(String(error)).toMatch(/cannot be serialized to JSON/);
    }
  });
});

// ---------------------------------------------------------------------------
// get / put / delete basics
// ---------------------------------------------------------------------------

describe("get()", () => {
  test("returns null for non-existent document", async () => {
    const result = await engine.get("users", "nonexistent");

    expect(result).toBeNull();
  });

  test("returns null for non-existent collection", async () => {
    const result = await engine.get("nonexistent", "id1");

    expect(result).toBeNull();
  });

  test("returns stored document", async () => {
    await engine.put("users", "abc", { id: "abc", name: "Sam" }, { primary: "abc" });

    const result = await engine.get("users", "abc");

    expect(result).toEqual({ id: "abc", name: "Sam" });
  });

  test("returns a deep clone, not the original reference", async () => {
    const doc = { id: "abc", nested: { value: 1 } };
    await engine.put("users", "abc", doc, {});

    const result1 = (await engine.get("users", "abc")) as Record<string, unknown>;
    const result2 = (await engine.get("users", "abc")) as Record<string, unknown>;

    expect(result1).toEqual(result2);
    expect(result1).not.toBe(result2);
    expect(result1?.nested).not.toBe(result2?.nested);
  });
});

describe("put()", () => {
  test("stores a document that can be retrieved", async () => {
    await engine.put("users", "abc", { id: "abc", name: "Sam" }, {});

    const result = await engine.get("users", "abc");

    expect(result).toEqual({ id: "abc", name: "Sam" });
  });

  test("overwrites an existing document", async () => {
    await engine.put("users", "abc", { id: "abc", name: "Sam" }, {});
    await engine.put("users", "abc", { id: "abc", name: "Updated" }, {});

    const result = await engine.get("users", "abc");

    expect(result).toEqual({ id: "abc", name: "Updated" });
  });

  test("stores documents in separate collections independently", async () => {
    await engine.put("users", "abc", { id: "abc", type: "user" }, {});
    await engine.put("posts", "abc", { id: "abc", type: "post" }, {});

    const user = await engine.get("users", "abc");
    const post = await engine.get("posts", "abc");

    expect(user).toEqual({ id: "abc", type: "user" });
    expect(post).toEqual({ id: "abc", type: "post" });
  });

  test("deep clones the document on storage", async () => {
    const doc = { id: "abc", nested: { value: 1 } };
    await engine.put("users", "abc", doc, {});

    // Mutate the original — should not affect stored copy
    (doc.nested as { value: number }).value = 999;

    const result = (await engine.get("users", "abc")) as {
      id: string;
      nested: { value: number };
    };

    expect(result?.nested.value).toBe(1);
  });

  test("stores index keys alongside document", async () => {
    await engine.put(
      "users",
      "abc",
      { id: "abc", email: "sam@example.com" },
      { byEmail: "sam@example.com" },
    );

    const results = await engine.query("users", {
      index: "byEmail",
      filter: { value: "sam@example.com" },
    });

    expect(results.documents).toHaveLength(1);
    expect(results.documents[0]).toEqual({
      key: "abc",
      doc: { id: "abc", email: "sam@example.com" },
    });
  });

  test("updates index keys when overwriting a document", async () => {
    await engine.put(
      "users",
      "abc",
      { id: "abc", email: "old@example.com" },
      { byEmail: "old@example.com" },
    );
    await engine.put(
      "users",
      "abc",
      { id: "abc", email: "new@example.com" },
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
});

describe("delete()", () => {
  test("removes a stored document", async () => {
    await engine.put("users", "abc", { id: "abc" }, {});
    await engine.delete("users", "abc");

    const result = await engine.get("users", "abc");

    expect(result).toBeNull();
  });

  test("does not throw when deleting a non-existent document", async () => {
    expect(engine.delete("users", "nonexistent")).resolves.toBeUndefined();
  });

  test("does not affect other documents", async () => {
    await engine.put("users", "a", { id: "a" }, {});
    await engine.put("users", "b", { id: "b" }, {});
    await engine.delete("users", "a");

    expect(await engine.get("users", "a")).toBeNull();
    expect(await engine.get("users", "b")).toEqual({ id: "b" });
  });

  test("does not affect other collections", async () => {
    await engine.put("users", "abc", { id: "abc" }, {});
    await engine.put("posts", "abc", { id: "abc" }, {});
    await engine.delete("users", "abc");

    expect(await engine.get("users", "abc")).toBeNull();
    expect(await engine.get("posts", "abc")).toEqual({ id: "abc" });
  });

  test("removes document from query results", async () => {
    await engine.put("users", "abc", { id: "abc" }, { byId: "abc" });
    await engine.delete("users", "abc");

    const results = await engine.query("users", {
      index: "byId",
      filter: { value: "abc" },
    });

    expect(results.documents).toHaveLength(0);
  });
});

// ---------------------------------------------------------------------------
// batchGet
// ---------------------------------------------------------------------------

describe("batchGet()", () => {
  test("returns multiple documents by IDs", async () => {
    await engine.put("users", "a", { id: "a" }, {});
    await engine.put("users", "b", { id: "b" }, {});
    await engine.put("users", "c", { id: "c" }, {});

    const results = await engine.batchGet!("users", ["a", "b", "c"]);

    expect(results).toHaveLength(3);
    expect(results).toContainEqual({ key: "a", doc: { id: "a" } });
    expect(results).toContainEqual({ key: "b", doc: { id: "b" } });
    expect(results).toContainEqual({ key: "c", doc: { id: "c" } });
  });

  test("skips non-existent IDs", async () => {
    await engine.put("users", "a", { id: "a" }, {});

    const results = await engine.batchGet!("users", ["a", "nonexistent", "also-missing"]);

    expect(results).toHaveLength(1);
    expect(results[0]).toEqual({ key: "a", doc: { id: "a" } });
  });

  test("returns empty array for all non-existent IDs", async () => {
    const results = await engine.batchGet!("users", ["x", "y", "z"]);

    expect(results).toHaveLength(0);
  });

  test("returns empty array for empty ID list", async () => {
    const results = await engine.batchGet!("users", []);

    expect(results).toHaveLength(0);
  });

  test("returns deep clones", async () => {
    await engine.put("users", "a", { id: "a", nested: { v: 1 } }, {});

    const results = await engine.batchGet!("users", ["a", "a"]);

    expect(results).toHaveLength(2);
    expect(results[0]!.doc).not.toBe(results[1]!.doc);
  });
});

// ---------------------------------------------------------------------------
// batchSet
// ---------------------------------------------------------------------------

describe("batchSet()", () => {
  test("stores multiple documents that can be retrieved", async () => {
    await engine.batchSet!("users", [
      { key: "a", doc: { id: "a", name: "Alice" }, indexes: { primary: "a" } },
      { key: "b", doc: { id: "b", name: "Bob" }, indexes: { primary: "b" } },
    ]);

    expect(await engine.get("users", "a")).toEqual({ id: "a", name: "Alice" });
    expect(await engine.get("users", "b")).toEqual({ id: "b", name: "Bob" });
  });

  test("overwrites existing documents", async () => {
    await engine.put("users", "a", { id: "a", name: "Old" }, { primary: "a" });

    await engine.batchSet!("users", [
      { key: "a", doc: { id: "a", name: "New" }, indexes: { primary: "a" } },
    ]);

    expect(await engine.get("users", "a")).toEqual({ id: "a", name: "New" });
  });

  test("stores index keys for query", async () => {
    await engine.batchSet!("users", [
      {
        key: "a",
        doc: { id: "a", email: "sam@example.com" },
        indexes: { byEmail: "sam@example.com" },
      },
      {
        key: "b",
        doc: { id: "b", email: "other@example.com" },
        indexes: { byEmail: "other@example.com" },
      },
    ]);

    const results = await engine.query("users", {
      index: "byEmail",
      filter: { value: "sam@example.com" },
    });

    expect(results.documents).toHaveLength(1);
    expect(results.documents[0]).toEqual({
      key: "a",
      doc: { id: "a", email: "sam@example.com" },
    });
  });

  test("deep clones documents on storage", async () => {
    const doc = { id: "a", nested: { value: 1 } };

    await engine.batchSet!("users", [{ key: "a", doc, indexes: { primary: "a" } }]);

    doc.nested.value = 999;
    const stored = (await engine.get("users", "a")) as {
      id: string;
      nested: { value: number };
    };

    expect(stored.nested.value).toBe(1);
  });
});

// ---------------------------------------------------------------------------
// batchDelete
// ---------------------------------------------------------------------------

describe("batchDelete()", () => {
  test("deletes multiple documents", async () => {
    await engine.put("users", "a", { id: "a" }, { primary: "a" });
    await engine.put("users", "b", { id: "b" }, { primary: "b" });
    await engine.put("users", "c", { id: "c" }, { primary: "c" });

    await engine.batchDelete!("users", ["a", "c"]);

    expect(await engine.get("users", "a")).toBeNull();
    expect(await engine.get("users", "b")).toEqual({ id: "b" });
    expect(await engine.get("users", "c")).toBeNull();
  });

  test("ignores missing IDs", async () => {
    await engine.put("users", "a", { id: "a" }, { primary: "a" });

    await engine.batchDelete!("users", ["missing", "a", "still-missing"]);

    expect(await engine.get("users", "a")).toBeNull();
  });

  test("does not affect other collections", async () => {
    await engine.put("users", "a", { id: "a" }, { primary: "a" });
    await engine.put("posts", "a", { id: "a" }, { primary: "a" });

    await engine.batchDelete!("users", ["a"]);

    expect(await engine.get("users", "a")).toBeNull();
    expect(await engine.get("posts", "a")).toEqual({ id: "a" });
  });
});

// ---------------------------------------------------------------------------
// query() — equality filters
// ---------------------------------------------------------------------------

describe("query() equality", () => {
  test("finds documents by string equality on index", async () => {
    await engine.put(
      "users",
      "a",
      { id: "a", email: "sam@example.com" },
      { byEmail: "sam@example.com" },
    );
    await engine.put(
      "users",
      "b",
      { id: "b", email: "other@example.com" },
      { byEmail: "other@example.com" },
    );

    const results = await engine.query("users", {
      index: "byEmail",
      filter: { value: "sam@example.com" },
    });

    expect(results.documents).toHaveLength(1);
    expect(results.documents[0]).toEqual({
      key: "a",
      doc: { id: "a", email: "sam@example.com" },
    });
  });

  test("finds documents by numeric equality on index", async () => {
    await engine.put("items", "a", { id: "a" }, { priority: "1" });
    await engine.put("items", "b", { id: "b" }, { priority: "2" });

    const results = await engine.query("items", {
      index: "priority",
      filter: { value: "1" },
    });

    expect(results.documents).toHaveLength(1);
  });

  test("finds documents using $eq operator", async () => {
    await engine.put("users", "a", { id: "a" }, { status: "active" });
    await engine.put("users", "b", { id: "b" }, { status: "inactive" });

    const results = await engine.query("users", {
      index: "status",
      filter: { value: { $eq: "active" } },
    });

    expect(results.documents).toHaveLength(1);
    expect(results.documents[0]).toEqual({ key: "a", doc: { id: "a" } });
  });

  test("returns empty results when no documents match", async () => {
    await engine.put("users", "a", { id: "a" }, { status: "active" });

    const results = await engine.query("users", {
      index: "status",
      filter: { value: "deleted" },
    });

    expect(results.documents).toHaveLength(0);
    expect(results.cursor).toBeNull();
  });

  test("returns empty results for non-existent collection", async () => {
    const results = await engine.query("nonexistent", {
      index: "byId",
      filter: { value: "anything" },
    });

    expect(results.documents).toHaveLength(0);
  });

  test("returns multiple matching documents", async () => {
    await engine.put("users", "a", { id: "a" }, { status: "active" });
    await engine.put("users", "b", { id: "b" }, { status: "active" });
    await engine.put("users", "c", { id: "c" }, { status: "inactive" });

    const results = await engine.query("users", {
      index: "status",
      filter: { value: "active" },
    });

    expect(results.documents).toHaveLength(2);
  });

  test("only matches against the specified index", async () => {
    await engine.put("users", "a", { id: "a" }, { byEmail: "sam@example.com", status: "active" });

    const emailResults = await engine.query("users", {
      index: "byEmail",
      filter: { value: "sam@example.com" },
    });

    const statusResults = await engine.query("users", {
      index: "status",
      filter: { value: "sam@example.com" },
    });

    expect(emailResults.documents).toHaveLength(1);
    expect(statusResults.documents).toHaveLength(0);
  });
});

// ---------------------------------------------------------------------------
// query() — comparison operators
// ---------------------------------------------------------------------------

describe("query() comparison operators", () => {
  beforeEach(async () => {
    await engine.put("items", "a", { id: "a", date: "2025-01-01" }, { byDate: "2025-01-01" });
    await engine.put("items", "b", { id: "b", date: "2025-06-15" }, { byDate: "2025-06-15" });
    await engine.put("items", "c", { id: "c", date: "2025-12-31" }, { byDate: "2025-12-31" });
  });

  test("$gt filters documents with index value greater than", async () => {
    const results = await engine.query("items", {
      index: "byDate",
      filter: { value: { $gt: "2025-06-15" } },
    });

    expect(results.documents).toHaveLength(1);
    expect(results.documents[0]).toEqual({
      key: "c",
      doc: { id: "c", date: "2025-12-31" },
    });
  });

  test("$gte filters documents with index value greater than or equal", async () => {
    const results = await engine.query("items", {
      index: "byDate",
      filter: { value: { $gte: "2025-06-15" } },
    });

    expect(results.documents).toHaveLength(2);
  });

  test("$lt filters documents with index value less than", async () => {
    const results = await engine.query("items", {
      index: "byDate",
      filter: { value: { $lt: "2025-06-15" } },
    });

    expect(results.documents).toHaveLength(1);
    expect(results.documents[0]).toEqual({
      key: "a",
      doc: { id: "a", date: "2025-01-01" },
    });
  });

  test("$lte filters documents with index value less than or equal", async () => {
    const results = await engine.query("items", {
      index: "byDate",
      filter: { value: { $lte: "2025-06-15" } },
    });

    expect(results.documents).toHaveLength(2);
  });

  test("$begins filters by prefix", async () => {
    await engine.put("items", "d", { id: "d" }, { byDate: "2026-01-01" });

    const results = await engine.query("items", {
      index: "byDate",
      filter: { value: { $begins: "2025" } },
    });

    expect(results.documents).toHaveLength(3);
  });

  test("$between filters documents within range (inclusive)", async () => {
    const results = await engine.query("items", {
      index: "byDate",
      filter: { value: { $between: ["2025-01-01", "2025-06-15"] } },
    });

    expect(results.documents).toHaveLength(2);
  });

  test("$between with tight range matches exactly", async () => {
    const results = await engine.query("items", {
      index: "byDate",
      filter: { value: { $between: ["2025-06-15", "2025-06-15"] } },
    });

    expect(results.documents).toHaveLength(1);
    expect(results.documents[0]).toEqual({
      key: "b",
      doc: { id: "b", date: "2025-06-15" },
    });
  });

  test("combined conditions are ANDed together", async () => {
    const results = await engine.query("items", {
      index: "byDate",
      filter: { value: { $gte: "2025-01-01", $lt: "2025-12-31" } },
    });

    expect(results.documents).toHaveLength(2);
  });

  test("$begins with no matches returns empty", async () => {
    const results = await engine.query("items", {
      index: "byDate",
      filter: { value: { $begins: "2030" } },
    });

    expect(results.documents).toHaveLength(0);
  });

  test("empty condition object matches everything", async () => {
    const results = await engine.query("items", {
      index: "byDate",
      filter: { value: {} },
    });

    expect(results.documents).toHaveLength(3);
  });

  test("$begins with empty string matches all documents", async () => {
    const results = await engine.query("items", {
      index: "byDate",
      filter: { value: { $begins: "" } },
    });

    expect(results.documents).toHaveLength(3);
  });

  test("$between with reversed bounds returns no results", async () => {
    const results = await engine.query("items", {
      index: "byDate",
      filter: { value: { $between: ["2025-12-31", "2025-01-01"] } },
    });

    expect(results.documents).toHaveLength(0);
  });

  test("$eq combined with $begins narrows results", async () => {
    const results = await engine.query("items", {
      index: "byDate",
      filter: { value: { $eq: "2025-06-15", $begins: "2025" } },
    });

    expect(results.documents).toHaveLength(1);
    expect(results.documents[0]).toEqual({
      key: "b",
      doc: { id: "b", date: "2025-06-15" },
    });
  });

  test("$gt combined with $lt narrows to open range", async () => {
    const results = await engine.query("items", {
      index: "byDate",
      filter: { value: { $gt: "2025-01-01", $lt: "2025-12-31" } },
    });

    expect(results.documents).toHaveLength(1);
    expect(results.documents[0]).toEqual({
      key: "b",
      doc: { id: "b", date: "2025-06-15" },
    });
  });

  test("$begins combined with $lte narrows results", async () => {
    const results = await engine.query("items", {
      index: "byDate",
      filter: { value: { $begins: "2025", $lte: "2025-06-15" } },
    });

    expect(results.documents).toHaveLength(2);
  });

  test("contradictory conditions return empty results", async () => {
    const results = await engine.query("items", {
      index: "byDate",
      filter: { value: { $gt: "2025-12-31", $lt: "2025-01-01" } },
    });

    expect(results.documents).toHaveLength(0);
  });
});

// ---------------------------------------------------------------------------
// query() — composite index values
// ---------------------------------------------------------------------------

describe("query() composite indexes", () => {
  beforeEach(async () => {
    await engine.put(
      "users",
      "a",
      { id: "a", tenant: "acme", name: "Alice" },
      { byTenant: "TENANT#acme#USER#Alice" },
    );
    await engine.put(
      "users",
      "b",
      { id: "b", tenant: "acme", name: "Bob" },
      { byTenant: "TENANT#acme#USER#Bob" },
    );
    await engine.put(
      "users",
      "c",
      { id: "c", tenant: "globex", name: "Charlie" },
      { byTenant: "TENANT#globex#USER#Charlie" },
    );
  });

  test("exact match on composite index value", async () => {
    const results = await engine.query("users", {
      index: "byTenant",
      filter: { value: "TENANT#acme#USER#Alice" },
    });

    expect(results.documents).toHaveLength(1);
    expect(results.documents[0]).toEqual({
      key: "a",
      doc: { id: "a", tenant: "acme", name: "Alice" },
    });
  });

  test("$begins on composite index value", async () => {
    const results = await engine.query("users", {
      index: "byTenant",
      filter: { value: { $begins: "TENANT#acme" } },
    });

    expect(results.documents).toHaveLength(2);
  });

  test("$begins distinguishes between tenant prefixes", async () => {
    const results = await engine.query("users", {
      index: "byTenant",
      filter: { value: { $begins: "TENANT#globex" } },
    });

    expect(results.documents).toHaveLength(1);
    expect(results.documents[0]).toEqual({
      key: "c",
      doc: { id: "c", tenant: "globex", name: "Charlie" },
    });
  });
});

// ---------------------------------------------------------------------------
// query() — sorting
// ---------------------------------------------------------------------------

describe("query() sorting", () => {
  beforeEach(async () => {
    await engine.put("items", "a", { id: "a" }, { byDate: "2025-03-01" });
    await engine.put("items", "b", { id: "b" }, { byDate: "2025-01-01" });
    await engine.put("items", "c", { id: "c" }, { byDate: "2025-02-01" });
  });

  test("sorts ascending by index value", async () => {
    const results = await engine.query("items", {
      index: "byDate",
      filter: { value: { $begins: "2025" } },
      sort: "asc",
    });

    expect(results.documents.map((d: any) => d.doc.id)).toEqual(["b", "c", "a"]);
  });

  test("sorts descending by index value", async () => {
    const results = await engine.query("items", {
      index: "byDate",
      filter: { value: { $begins: "2025" } },
      sort: "desc",
    });

    expect(results.documents.map((d: any) => d.doc.id)).toEqual(["a", "c", "b"]);
  });

  test("no sort preserves insertion order", async () => {
    const results = await engine.query("items", {
      index: "byDate",
      filter: { value: { $begins: "2025" } },
    });

    expect(results.documents.map((d: any) => d.doc.id)).toEqual(["a", "b", "c"]);
  });
});

// ---------------------------------------------------------------------------
// query() — pagination
// ---------------------------------------------------------------------------

describe("query() pagination", () => {
  beforeEach(async () => {
    for (let i = 0; i < 10; i++) {
      await engine.put(
        "items",
        `item-${String(i).padStart(2, "0")}`,
        { id: `item-${String(i).padStart(2, "0")}` },
        { all: "yes" },
      );
    }
  });

  test("limit restricts number of returned documents", async () => {
    const results = await engine.query("items", {
      index: "all",
      filter: { value: "yes" },
      limit: 3,
    });

    expect(results.documents).toHaveLength(3);
    expect(results.cursor).not.toBeNull();
  });

  test("cursor continues from previous page", async () => {
    const page1 = await engine.query("items", {
      index: "all",
      filter: { value: "yes" },
      limit: 3,
    });

    const page2 = await engine.query("items", {
      index: "all",
      filter: { value: "yes" },
      limit: 3,
      cursor: page1.cursor!,
    });

    expect(page2.documents).toHaveLength(3);

    // Pages should not overlap
    const ids1 = page1.documents.map((d: any) => d.doc.id);
    const ids2 = page2.documents.map((d: any) => d.doc.id);

    for (const id of ids2) {
      expect(ids1).not.toContain(id);
    }
  });

  test("sorted asc pagination uses cursor correctly for indexed queries", async () => {
    await engine.put("sorted-items", "a", { id: "a" }, { byDate: "2025-01-01" });
    await engine.put("sorted-items", "b", { id: "b" }, { byDate: "2025-01-02" });
    await engine.put("sorted-items", "c", { id: "c" }, { byDate: "2025-01-03" });
    await engine.put("sorted-items", "d", { id: "d" }, { byDate: "2025-01-04" });

    const page1 = await engine.query("sorted-items", {
      index: "byDate",
      filter: { value: { $begins: "2025-" } },
      sort: "asc",
      limit: 2,
    });

    expect(page1.documents.map((d: any) => d.doc.id)).toEqual(["a", "b"]);
    expect(page1.cursor).toBe("b");

    const page2 = await engine.query("sorted-items", {
      index: "byDate",
      filter: { value: { $begins: "2025-" } },
      sort: "asc",
      limit: 2,
      cursor: page1.cursor!,
    });

    expect(page2.documents.map((d: any) => d.doc.id)).toEqual(["c", "d"]);
    expect(page2.cursor).toBeNull();
  });

  test("sorted desc pagination uses cursor correctly for indexed queries", async () => {
    await engine.put("sorted-items", "a", { id: "a" }, { byDate: "2025-01-01" });
    await engine.put("sorted-items", "b", { id: "b" }, { byDate: "2025-01-02" });
    await engine.put("sorted-items", "c", { id: "c" }, { byDate: "2025-01-03" });
    await engine.put("sorted-items", "d", { id: "d" }, { byDate: "2025-01-04" });

    const page1 = await engine.query("sorted-items", {
      index: "byDate",
      filter: { value: { $begins: "2025-" } },
      sort: "desc",
      limit: 2,
    });

    expect(page1.documents.map((d: any) => d.doc.id)).toEqual(["d", "c"]);
    expect(page1.cursor).toBe("c");

    const page2 = await engine.query("sorted-items", {
      index: "byDate",
      filter: { value: { $begins: "2025-" } },
      sort: "desc",
      limit: 2,
      cursor: page1.cursor!,
    });

    expect(page2.documents.map((d: any) => d.doc.id)).toEqual(["b", "a"]);
    expect(page2.cursor).toBeNull();
  });

  test("paginating through all results collects every document", async () => {
    const allDocs: unknown[] = [];
    let cursor: string | null = null;

    do {
      const page = await engine.query("items", {
        index: "all",
        filter: { value: "yes" },
        limit: 3,
        ...(cursor ? { cursor } : {}),
      });

      allDocs.push(...page.documents);
      cursor = page.cursor;
    } while (cursor !== null);

    expect(allDocs).toHaveLength(10);
  });

  test("last page returns null cursor", async () => {
    const results = await engine.query("items", {
      index: "all",
      filter: { value: "yes" },
      limit: 100,
    });

    expect(results.documents).toHaveLength(10);
    expect(results.cursor).toBeNull();
  });

  test("no limit returns all matching documents", async () => {
    const results = await engine.query("items", {
      index: "all",
      filter: { value: "yes" },
    });

    expect(results.documents).toHaveLength(10);
    expect(results.cursor).toBeNull();
  });

  test("invalid cursor starts from beginning", async () => {
    const results = await engine.query("items", {
      index: "all",
      filter: { value: "yes" },
      limit: 3,
      cursor: "not-a-number",
    });

    expect(results.documents).toHaveLength(3);
  });
});

// ---------------------------------------------------------------------------
// query() — returns deep clones
// ---------------------------------------------------------------------------

describe("query() cloning", () => {
  test("returns deep clones of documents", async () => {
    await engine.put("users", "a", { id: "a", nested: { v: 1 } }, { all: "yes" });

    const r1 = await engine.query("users", {
      index: "all",
      filter: { value: "yes" },
    });
    const r2 = await engine.query("users", {
      index: "all",
      filter: { value: "yes" },
    });

    expect(r1.documents[0]!.doc).toEqual(r2.documents[0]!.doc);
    expect(r1.documents[0]!.doc).not.toBe(r2.documents[0]!.doc);
  });
});

// ---------------------------------------------------------------------------
// Migration — locking
// ---------------------------------------------------------------------------

describe("migration locking", () => {
  test("acquireLock returns a lock object", async () => {
    const lock = await engine.migration.acquireLock("users");

    expect(lock).not.toBeNull();
    expect(lock!.id).toBeDefined();
    expect(lock!.collection).toBe("users");
  });

  test("acquireLock returns null if already locked", async () => {
    await engine.migration.acquireLock("users");
    const second = await engine.migration.acquireLock("users");

    expect(second).toBeNull();
  });

  test("invalid ttl values do not steal an existing lock", async () => {
    const first = await engine.migration.acquireLock("users");
    expect(first).not.toBeNull();

    expect(await engine.migration.acquireLock("users", { ttl: Number.NaN })).toBeNull();
    expect(await engine.migration.acquireLock("users", { ttl: -1 })).toBeNull();
    expect(
      await engine.migration.acquireLock("users", { ttl: Number.POSITIVE_INFINITY }),
    ).toBeNull();
  });

  test("falls back to non-crypto lock ids when crypto.randomUUID is unavailable", async () => {
    const originalCrypto = Object.getOwnPropertyDescriptor(globalThis, "crypto");

    Object.defineProperty(globalThis, "crypto", {
      value: {},
      configurable: true,
      writable: true,
    });

    try {
      const lock = await engine.migration.acquireLock("users");
      expect(lock).not.toBeNull();
      expect(lock!.id).toMatch(/^[0-9a-f]+-[0-9a-f]+$/);
    } finally {
      if (originalCrypto) {
        Object.defineProperty(globalThis, "crypto", originalCrypto);
      } else {
        delete (globalThis as { crypto?: unknown }).crypto;
      }
    }
  });

  test("different collections can be locked independently", async () => {
    const lock1 = await engine.migration.acquireLock("users");
    const lock2 = await engine.migration.acquireLock("posts");

    expect(lock1).not.toBeNull();
    expect(lock2).not.toBeNull();
  });

  test("releaseLock allows re-acquiring the lock", async () => {
    const lock = await engine.migration.acquireLock("users");
    await engine.migration.releaseLock(lock!);

    const newLock = await engine.migration.acquireLock("users");

    expect(newLock).not.toBeNull();
    expect(newLock!.id).not.toBe(lock!.id);
  });

  test("releaseLock with wrong ID does not release the lock", async () => {
    await engine.migration.acquireLock("users");

    const fakeLock = {
      id: "wrong-id",
      collection: "users",
      acquiredAt: Date.now(),
    };
    await engine.migration.releaseLock(fakeLock);

    const result = await engine.migration.acquireLock("users");

    expect(result).toBeNull();
  });

  test("releaseLock for different collection does not affect others", async () => {
    const usersLock = await engine.migration.acquireLock("users");
    await engine.migration.acquireLock("posts");

    await engine.migration.releaseLock(usersLock!);

    // Users can be re-locked, posts cannot
    expect(await engine.migration.acquireLock("users")).not.toBeNull();
    expect(await engine.migration.acquireLock("posts")).toBeNull();
  });

  test("each lock has a unique ID", async () => {
    const lock1 = await engine.migration.acquireLock("users");
    await engine.migration.releaseLock(lock1!);
    const lock2 = await engine.migration.acquireLock("users");

    expect(lock1!.id).not.toBe(lock2!.id);
  });
});

// ---------------------------------------------------------------------------
// query() — no filter (scan all documents)
// ---------------------------------------------------------------------------

describe("query() with no filter", () => {
  test("returns empty page for empty collection", async () => {
    const page = await engine.query("users", {});

    expect(page.documents).toHaveLength(0);
    expect(page.cursor).toBeNull();
  });

  test("returns all documents", async () => {
    for (let i = 0; i < 5; i++) {
      await engine.put("users", `user-${i}`, { id: `user-${i}` }, {});
    }

    const page = await engine.query("users", {});

    expect(page.documents).toHaveLength(5);
    expect(page.cursor).toBeNull();
  });

  test("paginates with limit", async () => {
    for (let i = 0; i < 15; i++) {
      await engine.put("users", `user-${String(i).padStart(4, "0")}`, { id: i }, {});
    }

    const page1 = await engine.query("users", { limit: 10 });

    expect(page1.documents).toHaveLength(10);
    expect(page1.cursor).not.toBeNull();

    const page2 = await engine.query("users", {
      limit: 10,
      cursor: page1.cursor!,
    });

    expect(page2.documents).toHaveLength(5);
    expect(page2.cursor).toBeNull();
  });

  test("limit 0 returns an empty terminal page", async () => {
    await engine.put("users", "a", { id: "a" }, {});
    await engine.put("users", "b", { id: "b" }, {});

    const page = await engine.query("users", { limit: 0 });

    expect(page.documents).toHaveLength(0);
    expect(page.cursor).toBeNull();
  });

  test("cursor-based pagination walks all documents", async () => {
    for (let i = 0; i < 25; i++) {
      await engine.put("users", `user-${String(i).padStart(4, "0")}`, { id: i }, {});
    }

    const allDocs: unknown[] = [];
    let cursor: string | undefined;

    do {
      const page = await engine.query("users", { limit: 7, cursor });
      allDocs.push(...page.documents);
      cursor = page.cursor ?? undefined;
    } while (cursor);

    expect(allDocs).toHaveLength(25);
  });

  test("returns deep clones", async () => {
    await engine.put("users", "a", { id: "a", nested: { v: 1 } }, {});

    const page = await engine.query("users", {});
    const doc = page.documents[0]!.doc as Record<string, unknown>;

    // Mutate the returned document
    (doc.nested as { v: number }).v = 999;

    // Re-query should return original
    const page2 = await engine.query("users", {});
    const doc2 = page2.documents[0]!.doc as {
      id: string;
      nested: { v: number };
    };

    expect(doc2.nested.v).toBe(1);
  });

  test("invalid cursor starts from beginning", async () => {
    for (let i = 0; i < 5; i++) {
      await engine.put("users", `user-${i}`, { id: `user-${i}` }, {});
    }

    const page = await engine.query("users", { cursor: "nonexistent-cursor" });

    expect(page.documents).toHaveLength(5);
  });
});

// ---------------------------------------------------------------------------
// Migration — checkpoints
// ---------------------------------------------------------------------------

describe("migration checkpoints", () => {
  test("loadCheckpoint returns null when no checkpoint exists", async () => {
    const checkpoint = await engine.migration.loadCheckpoint!("users");

    expect(checkpoint).toBeNull();
  });

  test("saveCheckpoint and loadCheckpoint round-trip", async () => {
    const lock = await engine.migration.acquireLock("users");
    await engine.migration.saveCheckpoint!(lock!, "cursor-abc");

    const checkpoint = await engine.migration.loadCheckpoint!("users");

    expect(checkpoint).toBe("cursor-abc");
  });

  test("saveCheckpoint overwrites previous checkpoint", async () => {
    const lock = await engine.migration.acquireLock("users");
    await engine.migration.saveCheckpoint!(lock!, "cursor-1");
    await engine.migration.saveCheckpoint!(lock!, "cursor-2");

    const checkpoint = await engine.migration.loadCheckpoint!("users");

    expect(checkpoint).toBe("cursor-2");
  });

  test("stale lock holder cannot overwrite checkpoint after TTL lock steal", async () => {
    const lock1 = await engine.migration.acquireLock("users");
    expect(lock1).not.toBeNull();

    await engine.migration.saveCheckpoint!(lock1!, "cursor-1");

    // Steal/replace the lock.
    const lock2 = await engine.migration.acquireLock("users", { ttl: 0 });
    expect(lock2).not.toBeNull();
    expect(lock2!.id).not.toBe(lock1!.id);

    // Current lock updates checkpoint.
    await engine.migration.saveCheckpoint!(lock2!, "cursor-2");

    // Old lock must not be able to overwrite.
    await engine.migration.saveCheckpoint!(lock1!, "stale-overwrite");

    const checkpoint = await engine.migration.loadCheckpoint!("users");
    expect(checkpoint).toBe("cursor-2");
  });

  test("checkpoints are scoped to collections", async () => {
    const lock1 = await engine.migration.acquireLock("users");
    const lock2 = await engine.migration.acquireLock("posts");

    await engine.migration.saveCheckpoint!(lock1!, "users-cursor");
    await engine.migration.saveCheckpoint!(lock2!, "posts-cursor");

    expect(await engine.migration.loadCheckpoint!("users")).toBe("users-cursor");
    expect(await engine.migration.loadCheckpoint!("posts")).toBe("posts-cursor");
  });
});

// ---------------------------------------------------------------------------
// Migration — getOutdated
// ---------------------------------------------------------------------------

describe("migration getOutdated()", () => {
  const criteria = {
    version: 2,
    versionField: "__v",
    indexes: ["byEmail", "primary"],
    indexesField: "__indexes",
  };

  test("returns documents with stale version", async () => {
    await engine.put("users", "a", { __v: 1, id: "a" }, {});
    await engine.put("users", "b", { __v: 2, id: "b", __indexes: ["byEmail", "primary"] }, {});

    const result = await engine.migration.getOutdated("users", criteria);

    expect(result.documents).toHaveLength(1);
    expect(result.documents[0]!.key).toBe("a");
  });

  test("returns documents missing __indexes field", async () => {
    await engine.put("users", "a", { __v: 2, id: "a" }, {});

    const result = await engine.migration.getOutdated("users", criteria);

    expect(result.documents).toHaveLength(1);
    expect(result.documents[0]!.key).toBe("a");
  });

  test("returns documents with wrong __indexes", async () => {
    await engine.put("users", "a", { __v: 2, id: "a", __indexes: ["primary"] }, {});

    const result = await engine.migration.getOutdated("users", criteria);

    expect(result.documents).toHaveLength(1);
    expect(result.documents[0]!.key).toBe("a");
  });

  test("returns documents with __indexes in wrong order", async () => {
    await engine.put("users", "a", { __v: 2, id: "a", __indexes: ["primary", "byEmail"] }, {});

    const result = await engine.migration.getOutdated("users", criteria);

    expect(result.documents).toHaveLength(1);
  });

  test("skips documents that are fully up to date", async () => {
    await engine.put("users", "a", { __v: 2, id: "a", __indexes: ["byEmail", "primary"] }, {});

    const result = await engine.migration.getOutdated("users", criteria);

    expect(result.documents).toHaveLength(0);
  });

  test("returns empty for empty collection", async () => {
    const result = await engine.migration.getOutdated("users", criteria);

    expect(result.documents).toHaveLength(0);
    expect(result.cursor).toBeNull();
  });

  test("assumes version 1 when __v is missing", async () => {
    await engine.put("users", "a", { id: "a" }, {});

    const result = await engine.migration.getOutdated("users", criteria);

    expect(result.documents).toHaveLength(1);
  });

  test("paginates results with cursor", async () => {
    // Seed 150 stale documents
    for (let i = 0; i < 150; i++) {
      const id = `u${String(i).padStart(4, "0")}`;
      await engine.put("users", id, { __v: 1, id }, {});
    }

    const page1 = await engine.migration.getOutdated("users", criteria);

    expect(page1.documents).toHaveLength(100);
    expect(page1.cursor).not.toBeNull();

    const page2 = await engine.migration.getOutdated("users", criteria, page1.cursor!);

    expect(page2.documents).toHaveLength(50);
    expect(page2.cursor).toBeNull();
  });

  test("returns null cursor when exactly 100 outdated docs exist and no additional rows remain", async () => {
    for (let i = 0; i < 100; i++) {
      const id = `u${String(i).padStart(4, "0")}`;
      await engine.put("users", id, { __v: 1, id }, {});
    }

    const page = await engine.migration.getOutdated("users", criteria);

    expect(page.documents).toHaveLength(100);
    expect(page.cursor).toBeNull();
  });

  test("throws when malformed JSON is encountered during the first outdated scan", async () => {
    sqlite.db
      .prepare(`INSERT INTO documents (collection, doc_key, doc_json) VALUES (?, ?, ?)`)
      .run("users", "broken-first", "{");

    try {
      await engine.migration.getOutdated("users", criteria);
      throw new Error("expected getOutdated() to throw for malformed JSON");
    } catch (error) {
      expect(String(error)).toMatch(/invalid JSON|JSON/);
    }
  });

  test("throws when malformed JSON is encountered during lookahead scan", async () => {
    for (let i = 0; i < 100; i++) {
      const id = `u${String(i).padStart(4, "0")}`;
      await engine.put("users", id, { __v: 1, id }, {});
    }

    sqlite.db
      .prepare(`INSERT INTO documents (collection, doc_key, doc_json) VALUES (?, ?, ?)`)
      .run("users", "broken-lookahead", "{");

    try {
      await engine.migration.getOutdated("users", criteria);
      throw new Error("expected getOutdated() to throw for malformed lookahead JSON");
    } catch (error) {
      expect(String(error)).toMatch(/JSON|parse|Unexpected|invalid/i);
    }
  });

  test("returns mix of stale-version and wrong-indexes documents", async () => {
    await engine.put("users", "a", { __v: 1, id: "a" }, {});
    await engine.put("users", "b", { __v: 2, id: "b", __indexes: ["primary"] }, {});
    await engine.put("users", "c", { __v: 2, id: "c", __indexes: ["byEmail", "primary"] }, {});

    const result = await engine.migration.getOutdated("users", criteria);

    expect(result.documents).toHaveLength(2);
    const keys = result.documents.map((d) => d.key);
    expect(keys).toContain("a");
    expect(keys).toContain("b");
    expect(keys).not.toContain("c");
  });

  test("scoped to specified collection", async () => {
    await engine.put("users", "a", { __v: 1, id: "a" }, {});
    await engine.put("posts", "b", { __v: 1, id: "b" }, {});

    const result = await engine.migration.getOutdated("users", criteria);

    expect(result.documents).toHaveLength(1);
    expect(result.documents[0]!.key).toBe("a");
  });

  test("documents with version higher than criteria are not outdated", async () => {
    await engine.put("users", "a", { __v: 3, id: "a", __indexes: ["byEmail", "primary"] }, {});

    const result = await engine.migration.getOutdated("users", criteria);

    expect(result.documents).toHaveLength(0);
  });

  test('default parser treats string versions like "v1" as numeric', async () => {
    await engine.put("users", "a", { __v: "v1", id: "a", __indexes: ["byEmail", "primary"] }, {});
    await engine.put("users", "b", { __v: "v2", id: "b", __indexes: ["byEmail", "primary"] }, {});

    const result = await engine.migration.getOutdated("users", criteria);
    const keys = result.documents.map((d) => d.key);

    expect(keys).toContain("a");
    expect(keys).not.toContain("b");
  });

  test("documents with unparseable default version strings are ignored", async () => {
    await engine.put(
      "users",
      "a",
      { __v: "release-candidate", id: "a", __indexes: ["byEmail", "primary"] },
      {},
    );
    await engine.put("users", "b", { __v: "   ", id: "b", __indexes: ["byEmail", "primary"] }, {});

    const result = await engine.migration.getOutdated("users", criteria);

    expect(result.documents).toHaveLength(0);
  });

  test("returns null cursor when exactly one page of outdated documents exists", async () => {
    for (let i = 0; i < 100; i++) {
      const id = `stale-${String(i).padStart(3, "0")}`;
      await engine.put("users", id, { __v: 1, id }, {});
    }

    for (let i = 0; i < 30; i++) {
      const id = `fresh-${String(i).padStart(3, "0")}`;
      await engine.put("users", id, { __v: 2, id, __indexes: ["byEmail", "primary"] }, {});
    }

    const page = await engine.migration.getOutdated("users", criteria);

    expect(page.documents).toHaveLength(100);
    expect(page.cursor).toBeNull();
  });

  test("supports custom parseVersion/compareVersions for non-standard version fields", async () => {
    await engine.put(
      "users",
      "a",
      { __v: { major: 1 }, id: "a", __indexes: ["byEmail", "primary"] },
      {},
    );
    await engine.put(
      "users",
      "b",
      { __v: { major: 2 }, id: "b", __indexes: ["byEmail", "primary"] },
      {},
    );

    const result = await engine.migration.getOutdated("users", {
      ...criteria,
      parseVersion: (raw) =>
        typeof raw === "object" && raw !== null && "major" in raw
          ? Number((raw as { major: unknown }).major)
          : null,
      compareVersions: (a, b) => Number(a) - Number(b),
    });

    expect(result.documents).toHaveLength(1);
    expect(result.documents[0]!.key).toBe("a");
  });

  test("ignores documents when custom compareVersions cannot safely compare", async () => {
    await engine.put(
      "users",
      "a",
      { __v: { major: 1 }, id: "a", __indexes: ["byEmail", "primary"] },
      {},
    );

    const nanCompare = await engine.migration.getOutdated("users", {
      ...criteria,
      parseVersion: (raw) =>
        typeof raw === "object" && raw !== null && "major" in raw
          ? Number((raw as { major: unknown }).major)
          : null,
      compareVersions: () => Number.NaN,
    });

    expect(nanCompare.documents).toHaveLength(0);

    const throwingCompare = await engine.migration.getOutdated("users", {
      ...criteria,
      parseVersion: (raw) =>
        typeof raw === "object" && raw !== null && "major" in raw
          ? Number((raw as { major: unknown }).major)
          : null,
      compareVersions: () => {
        throw new Error("no-compare");
      },
    });

    expect(throwingCompare.documents).toHaveLength(0);
  });
});

// ---------------------------------------------------------------------------
// Migration — clearCheckpoint
// ---------------------------------------------------------------------------

describe("migration clearCheckpoint()", () => {
  test("removes an existing checkpoint", async () => {
    const lock = await engine.migration.acquireLock("users");
    await engine.migration.saveCheckpoint!(lock!, "cursor-abc");

    await engine.migration.clearCheckpoint!("users");

    const checkpoint = await engine.migration.loadCheckpoint!("users");
    expect(checkpoint).toBeNull();
  });

  test("does not throw when clearing a non-existent checkpoint", async () => {
    await engine.migration.clearCheckpoint!("users");

    const checkpoint = await engine.migration.loadCheckpoint!("users");
    expect(checkpoint).toBeNull();
  });

  test("only clears the specified collection checkpoint", async () => {
    const lock1 = await engine.migration.acquireLock("users");
    const lock2 = await engine.migration.acquireLock("posts");

    await engine.migration.saveCheckpoint!(lock1!, "users-cursor");
    await engine.migration.saveCheckpoint!(lock2!, "posts-cursor");

    await engine.migration.clearCheckpoint!("users");

    expect(await engine.migration.loadCheckpoint!("users")).toBeNull();
    expect(await engine.migration.loadCheckpoint!("posts")).toBe("posts-cursor");
  });
});

// ---------------------------------------------------------------------------
// Migration — lock acquiredAt
// ---------------------------------------------------------------------------

describe("migration lock acquiredAt", () => {
  test("lock includes acquiredAt timestamp", async () => {
    const before = Date.now();
    const lock = await engine.migration.acquireLock("users");
    const after = Date.now();

    expect(lock).not.toBeNull();
    expect(lock!.acquiredAt).toBeGreaterThanOrEqual(before);
    expect(lock!.acquiredAt).toBeLessThanOrEqual(after);
  });
});

// ---------------------------------------------------------------------------
// Migration — getStatus
// ---------------------------------------------------------------------------

describe("migration getStatus()", () => {
  test("returns null when no lock is held", async () => {
    const status = await engine.migration.getStatus!("users");

    expect(status).toBeNull();
  });

  test("returns lock info when lock is held", async () => {
    const lock = await engine.migration.acquireLock("users");

    const status = await engine.migration.getStatus!("users");

    expect(status).not.toBeNull();
    expect(status!.lock.id).toBe(lock!.id);
    expect(status!.lock.collection).toBe("users");
    expect(status!.cursor).toBeNull();

    await engine.migration.releaseLock(lock!);
  });

  test("returns cursor when checkpoint is set", async () => {
    const lock = await engine.migration.acquireLock("users");
    await engine.migration.saveCheckpoint!(lock!, "some-cursor");

    const status = await engine.migration.getStatus!("users");

    expect(status!.cursor).toBe("some-cursor");

    await engine.migration.releaseLock(lock!);
  });

  test("returns null after lock is released", async () => {
    const lock = await engine.migration.acquireLock("users");
    await engine.migration.releaseLock(lock!);

    const status = await engine.migration.getStatus!("users");

    expect(status).toBeNull();
  });

  test("is scoped to collection", async () => {
    await engine.migration.acquireLock("users");

    const usersStatus = await engine.migration.getStatus!("users");
    const postsStatus = await engine.migration.getStatus!("posts");

    expect(usersStatus).not.toBeNull();
    expect(postsStatus).toBeNull();
  });
});

// ---------------------------------------------------------------------------
// Collection isolation
// ---------------------------------------------------------------------------

describe("collection isolation", () => {
  test("operations on one collection do not affect another", async () => {
    await engine.put("users", "a", { id: "a", type: "user" }, {});
    await engine.put("posts", "a", { id: "a", type: "post" }, {});

    await engine.delete("users", "a");

    expect(await engine.get("users", "a")).toBeNull();
    expect(await engine.get("posts", "a")).toEqual({ id: "a", type: "post" });
  });

  test("queries on one collection do not return documents from another", async () => {
    await engine.put("users", "a", { id: "a" }, { status: "active" });
    await engine.put("posts", "b", { id: "b" }, { status: "active" });

    const results = await engine.query("users", {
      index: "status",
      filter: { value: "active" },
    });

    expect(results.documents).toHaveLength(1);
    expect(results.documents[0]).toEqual({ key: "a", doc: { id: "a" } });
  });

  test("migration locks are per-collection", async () => {
    const userLock = await engine.migration.acquireLock("users");
    const postLock = await engine.migration.acquireLock("posts");

    expect(userLock).not.toBeNull();
    expect(postLock).not.toBeNull();

    // Can't double-lock same collection
    expect(await engine.migration.acquireLock("users")).toBeNull();
    expect(await engine.migration.acquireLock("posts")).toBeNull();
  });

  test("no-filter query only returns documents from specified collection", async () => {
    await engine.put("users", "a", { id: "a" }, {});
    await engine.put("posts", "b", { id: "b" }, {});

    const page = await engine.query("users", {});

    expect(page.documents).toHaveLength(1);
    expect(page.documents[0]).toEqual({ key: "a", doc: { id: "a" } });
  });
});

// ---------------------------------------------------------------------------
// query() — undefined/missing index values
// ---------------------------------------------------------------------------

describe("query() with missing index values", () => {
  test("documents without the queried index are excluded", async () => {
    await engine.put("users", "a", { id: "a" }, { byEmail: "sam@example.com" });
    await engine.put("users", "b", { id: "b" }, { byName: "Sam" }); // no byEmail index

    const results = await engine.query("users", {
      index: "byEmail",
      filter: { value: "sam@example.com" },
    });

    expect(results.documents).toHaveLength(1);
    expect(results.documents[0]).toEqual({ key: "a", doc: { id: "a" } });
  });

  test("documents with empty indexes object are excluded from all queries", async () => {
    await engine.put("users", "a", { id: "a" }, {});

    const results = await engine.query("users", {
      index: "byEmail",
      filter: { value: "anything" },
    });

    expect(results.documents).toHaveLength(0);
  });

  test("sorting handles documents with undefined index values by excluding them", async () => {
    // Only documents matching the filter are included in sorted results,
    // and undefined index values fail the filter check, so they're never sorted.
    await engine.put("items", "a", { id: "a" }, { byDate: "2025-01-01" });
    await engine.put("items", "b", { id: "b" }, {}); // no byDate
    await engine.put("items", "c", { id: "c" }, { byDate: "2025-03-01" });

    const results = await engine.query("items", {
      index: "byDate",
      filter: { value: { $begins: "2025" } },
      sort: "desc",
    });

    expect(results.documents).toHaveLength(2);
    expect(results.documents.map((d: any) => d.doc.id)).toEqual(["c", "a"]);
  });
});

// ---------------------------------------------------------------------------
// query() — pagination edge cases
// ---------------------------------------------------------------------------

describe("query() pagination edge cases", () => {
  beforeEach(async () => {
    for (let i = 0; i < 5; i++) {
      await engine.put("items", `item-${i}`, { id: `item-${i}` }, { all: "yes" });
    }
  });

  test("limit 0 returns an empty terminal page", async () => {
    const page1 = await engine.query("items", {
      index: "all",
      filter: { value: "yes" },
      limit: 0,
    });

    expect(page1.documents).toHaveLength(0);
    expect(page1.cursor).toBeNull();

    // Repeat request should stay terminal (no looping cursor).
    const page2 = await engine.query("items", {
      index: "all",
      filter: { value: "yes" },
      limit: 0,
      cursor: page1.cursor ?? undefined,
    });

    expect(page2.documents).toHaveLength(0);
    expect(page2.cursor).toBeNull();
  });

  test("non-finite limits are treated as no limit", async () => {
    const nanLimit = await engine.query("items", {
      index: "all",
      filter: { value: "yes" },
      limit: Number.NaN,
    });
    const infinityLimit = await engine.query("items", {
      index: "all",
      filter: { value: "yes" },
      limit: Number.POSITIVE_INFINITY,
    });

    expect(nanLimit.documents).toHaveLength(5);
    expect(nanLimit.cursor).toBeNull();
    expect(infinityLimit.documents).toHaveLength(5);
    expect(infinityLimit.cursor).toBeNull();
  });

  test("fractional limits are floored", async () => {
    const page = await engine.query("items", {
      index: "all",
      filter: { value: "yes" },
      limit: 2.9,
    });

    expect(page.documents).toHaveLength(2);
    expect(page.cursor).not.toBeNull();
  });

  test("cursor of last item returns empty page", async () => {
    // item-4 is the last item — nothing comes after it
    const results = await engine.query("items", {
      index: "all",
      filter: { value: "yes" },
      limit: 3,
      cursor: "item-4",
    });

    expect(results.documents).toHaveLength(0);
    expect(results.cursor).toBeNull();
  });

  test("unrecognised cursor starts from the beginning", async () => {
    const results = await engine.query("items", {
      index: "all",
      filter: { value: "yes" },
      limit: 3,
      cursor: "nonexistent-key",
    });

    expect(results.documents).toHaveLength(3);
  });

  test("cursor at exact boundary returns remaining documents", async () => {
    // Get cursor from first page of 3
    const page1 = await engine.query("items", {
      index: "all",
      filter: { value: "yes" },
      limit: 3,
    });

    // Second page should have exactly 2
    const page2 = await engine.query("items", {
      index: "all",
      filter: { value: "yes" },
      limit: 3,
      cursor: page1.cursor!,
    });

    expect(page2.documents).toHaveLength(2);
    expect(page2.cursor).toBeNull();
  });

  test("limit 1 paginates one document at a time", async () => {
    const allDocs: unknown[] = [];
    let cursor: string | null = null;

    do {
      const page = await engine.query("items", {
        index: "all",
        filter: { value: "yes" },
        limit: 1,
        ...(cursor ? { cursor } : {}),
      });

      expect(page.documents).toHaveLength(cursor !== null || page.cursor !== null ? 1 : 0);
      allDocs.push(...page.documents);
      cursor = page.cursor;
    } while (cursor !== null);

    expect(allDocs).toHaveLength(5);
  });
});

// ---------------------------------------------------------------------------
// Lexicographic sorting edge cases
// ---------------------------------------------------------------------------

describe("lexicographic sort edge cases", () => {
  test("numeric strings sort lexicographically, not numerically", async () => {
    await engine.put("items", "a", { id: "a" }, { byNum: "2" });
    await engine.put("items", "b", { id: "b" }, { byNum: "10" });
    await engine.put("items", "c", { id: "c" }, { byNum: "1" });

    const results = await engine.query("items", {
      index: "byNum",
      filter: { value: { $gte: "0" } },
      sort: "asc",
    });

    // Lexicographic: "1" < "10" < "2"
    expect(results.documents.map((d: any) => d.doc.id)).toEqual(["c", "b", "a"]);
  });

  test("special characters in index values are handled", async () => {
    await engine.put("items", "a", { id: "a" }, { byKey: "TENANT#acme#USER#alice" });
    await engine.put("items", "b", { id: "b" }, { byKey: "TENANT#acme#USER#bob" });
    await engine.put("items", "c", { id: "c" }, { byKey: "TENANT#zebra#USER#carol" });

    const results = await engine.query("items", {
      index: "byKey",
      filter: { value: { $begins: "TENANT#acme" } },
      sort: "asc",
    });

    expect(results.documents).toHaveLength(2);
    expect(results.documents.map((d: any) => d.doc.id)).toEqual(["a", "b"]);
  });
});
