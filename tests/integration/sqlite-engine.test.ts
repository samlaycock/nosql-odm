import type BetterSqlite3 from "better-sqlite3";

import { Database as BunDatabase } from "bun:sqlite";
import { afterEach, beforeEach } from "bun:test";

import { sqliteEngine, type SqliteQueryEngine } from "../../src/engines/sqlite";
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
