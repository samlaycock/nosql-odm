import type BetterSqlite3 from "better-sqlite3";

import { Database as BunDatabase } from "bun:sqlite";
import { afterEach, beforeEach, describe, expect, test } from "bun:test";

import { sqliteEngine, type SqliteQueryEngine } from "../../src/engines/sqlite";
import { runQueryEngineConformanceSuite } from "./conformance-suite";
import { createCollectionNameFactory } from "./helpers";
import { runMigrationIntegrationSuite } from "./migration-suite";

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

let engine: SqliteQueryEngine;
const nextCollection = createCollectionNameFactory();

beforeEach(() => {
  engine = sqliteEngine({
    database: new BunBetterSqliteCompat(":memory:") as unknown as BetterSqlite3.Database,
  });
});

afterEach(() => {
  engine.close();
});

runMigrationIntegrationSuite({
  engineName: "sqliteEngine integration",
  getEngine: () => engine,
  nextCollection,
});

runQueryEngineConformanceSuite({
  engineName: "sqliteEngine integration",
  getEngine: () => engine,
  nextCollection,
  assertEngineUniqueConstraintConformance: true,
});

describe("sqliteEngine integration", () => {
  test("batchGet and batchSet support configured chunking for large batches", async () => {
    const customEngine = sqliteEngine({
      database: new BunBetterSqliteCompat(":memory:") as unknown as BetterSqlite3.Database,
      batchGetChunkSize: 11,
      batchSetChunkSize: 13,
    });
    const collection = nextCollection("chunked_users");
    const items = Array.from({ length: 130 }, (_, i) => {
      const key = `u${String(i).padStart(3, "0")}`;

      return {
        key,
        doc: { id: key, order: i },
        indexes: { primary: key },
      };
    });

    try {
      await customEngine.batchSet(collection, items);

      const docs = await customEngine.batchGet(
        collection,
        items.map((item) => item.key),
      );

      expect(docs).toHaveLength(items.length);
      expect(docs.map((item) => item.key)).toEqual(items.map((item) => item.key));
      expect(await customEngine.get(collection, "u064")).toEqual({
        id: "u064",
        order: 64,
      });
    } finally {
      customEngine.close();
    }
  });
});
