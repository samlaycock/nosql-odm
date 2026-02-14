import { beforeEach, describe, expect, test } from "bun:test";
import * as z from "zod";
import { createStore, MigrationScopeConflictError, MissingMigratorError } from "../../src/store";
import { memoryEngine, type MemoryQueryEngine } from "../../src/engines/memory";

function buildUserV2() {
  return model("user")
    .schema(
      1,
      z.object({
        id: z.string(),
        name: z.string(),
        email: z.email(),
      }),
    )
    .schema(
      2,
      z.object({
        id: z.string(),
        firstName: z.string(),
        lastName: z.string(),
        email: z.email(),
      }),
      {
        migrate(old) {
          const [firstName, ...rest] = old.name.split(" ");
          return {
            id: old.id,
            firstName: firstName ?? "",
            lastName: rest.join(" ") || "",
            email: old.email,
          };
        },
      },
    )
    .index({ name: "primary", value: "id" })
    .build();
}

function buildPostV2() {
  return model("post")
    .schema(
      1,
      z.object({
        id: z.string(),
        title: z.string(),
      }),
    )
    .schema(
      2,
      z.object({
        id: z.string(),
        title: z.string(),
        published: z.boolean(),
      }),
      {
        migrate(old) {
          return { ...old, published: false };
        },
      },
    )
    .index({ name: "primary", value: "id" })
    .build();
}

import { model } from "../../src/model";

async function expectReject(
  work: Promise<unknown>,
  expected: RegExp | string | (new (...args: never[]) => Error),
): Promise<void> {
  let error: unknown = null;

  try {
    await work;
  } catch (caught) {
    error = caught;
  }

  if (error === null) {
    throw new Error("Expected promise rejection");
  }

  const message =
    error instanceof Error
      ? (error.stack ?? error.message)
      : typeof error === "string"
        ? error
        : JSON.stringify(error);

  if (expected instanceof RegExp) {
    expect(message).toMatch(expected);
    return;
  }

  if (typeof expected === "string") {
    expect(message).toContain(expected);
    return;
  }

  expect(error).toBeInstanceOf(expected);
}

async function seedUsers(engine: MemoryQueryEngine, count: number) {
  for (let i = 0; i < count; i++) {
    const id = `u${String(i).padStart(4, "0")}`;
    await engine.put(
      "user",
      id,
      { __v: 1, id, name: `User ${i}`, email: `u${i}@example.com` },
      { primary: id },
    );
  }
}

let engine: MemoryQueryEngine;

beforeEach(() => {
  engine = memoryEngine();
});

describe("migrator scope conflicts", () => {
  test("model migration cannot start while a store migration run exists", async () => {
    const store = createStore(engine, [buildUserV2(), buildPostV2()]);
    await store.getOrCreateMigration();

    expect(store.user.migrateAll()).rejects.toThrow(MigrationScopeConflictError);
  });

  test("store migration cannot start while a model migration run exists", async () => {
    const store = createStore(engine, [buildUserV2(), buildPostV2()]);
    await store.user.getOrCreateMigration();

    expect(store.migrateAll()).rejects.toThrow(MigrationScopeConflictError);
  });

  test("model migrateNextPage cannot start while a store migration run exists", async () => {
    const store = createStore(engine, [buildUserV2(), buildPostV2()]);
    await store.getOrCreateMigration();

    await expectReject(store.user.migrateNextPage(), MigrationScopeConflictError);
  });

  test("store migrateNextPage cannot start while a model migration run exists", async () => {
    const store = createStore(engine, [buildUserV2(), buildPostV2()]);
    await store.user.getOrCreateMigration();

    await expectReject(store.migrateNextPage(), MigrationScopeConflictError);
  });
});

describe("paged migration API", () => {
  test("migrateNextPage processes one migration page per call", async () => {
    await seedUsers(engine, 150);
    const store = createStore(engine, [buildUserV2()]);

    const first = await store.user.migrateNextPage();
    expect(first.model).toBe("user");
    expect(first.completed).toBe(false);
    expect(first.migrated).toBe(100);

    const second = await store.user.migrateNextPage();
    expect(second.model).toBe("user");
    expect(second.completed).toBe(true);
    expect(second.migrated).toBe(50);

    const progress = await store.user.getMigrationProgress();
    expect(progress).toBeNull();
  });
});

describe("migration progress", () => {
  test("can introspect model and store migration progress", async () => {
    const store = createStore(engine, [buildUserV2(), buildPostV2()]);

    await store.user.getOrCreateMigration();
    const modelProgress = await store.user.getMigrationProgress();
    expect(modelProgress).not.toBeNull();
    expect(modelProgress?.scope).toBe("model");
    expect(modelProgress?.models).toEqual(["user"]);

    const otherEngine = memoryEngine();
    const otherStore = createStore(otherEngine, [buildUserV2(), buildPostV2()]);
    await otherStore.getOrCreateMigration();
    const storeProgress = await otherStore.getMigrationProgress();
    expect(storeProgress).not.toBeNull();
    expect(storeProgress?.scope).toBe("store");
    expect(storeProgress?.models).toEqual(["post", "user"]);
  });
});

describe("missing migrator", () => {
  test("migration APIs throw a dedicated error when no migrator is configured", async () => {
    const noMigratorEngine = memoryEngine();
    delete (noMigratorEngine as { migrator?: unknown }).migrator;
    const store = createStore(noMigratorEngine, [buildUserV2()]);

    await expectReject(store.user.migrateAll(), MissingMigratorError);
    await expectReject(store.user.migrateNextPage(), MissingMigratorError);
  });
});

describe("persisted migration run validation", () => {
  test("rejects array-shaped progressByModel in persisted run state", async () => {
    const lock = await engine.migration.acquireLock("user");
    expect(lock).not.toBeNull();

    await engine.migration.saveCheckpoint!(
      lock!,
      JSON.stringify({
        id: "run-1",
        scope: "model",
        models: ["user"],
        modelIndex: 0,
        cursor: null,
        startedAt: Date.now(),
        updatedAt: Date.now(),
        progressByModel: [],
      }),
    );
    await engine.migration.releaseLock(lock!);

    const store = createStore(engine, [buildUserV2()]);
    await expectReject(store.user.getMigrationProgress(), /invalid model progress/i);
  });

  test("rejects array-shaped skipReasons in persisted run state", async () => {
    const lock = await engine.migration.acquireLock("user");
    expect(lock).not.toBeNull();

    await engine.migration.saveCheckpoint!(
      lock!,
      JSON.stringify({
        id: "run-2",
        scope: "model",
        models: ["user"],
        modelIndex: 0,
        cursor: null,
        startedAt: Date.now(),
        updatedAt: Date.now(),
        progressByModel: {
          user: {
            migrated: 0,
            skipped: 0,
            pages: 0,
            skipReasons: [],
          },
        },
      }),
    );
    await engine.migration.releaseLock(lock!);

    const store = createStore(engine, [buildUserV2()]);
    await expectReject(store.user.getMigrationProgress(), /invalid skip reasons/i);
  });
});

describe("concurrent write conflicts", () => {
  test("migrateNextPage skips documents when conditional migration writes conflict", async () => {
    await engine.put(
      "user",
      "u1",
      {
        __v: 1,
        __indexes: ["primary"],
        id: "u1",
        name: "Sam User",
        email: "sam@example.com",
      },
      { primary: "u1" },
    );

    const originalGetOutdated = engine.migration.getOutdated.bind(engine.migration);
    engine.migration.getOutdated = async (collection, criteria, cursor) => {
      const page = await originalGetOutdated(collection, criteria, cursor);

      return {
        ...page,
        documents: page.documents.map((entry) => ({
          ...entry,
          writeToken: "1",
        })),
      };
    };

    engine.batchSetWithResult = async (_collection, items) => ({
      persistedKeys: [],
      conflictedKeys: items.map((item) => item.key),
    });

    const store = createStore(engine, [buildUserV2()]);
    const page = await store.user.migrateNextPage();

    expect(page.completed).toBe(true);
    expect(page.migrated).toBe(0);
    expect(page.skipped).toBe(1);
    expect(page.skipReasons).toEqual({ concurrent_write: 1 });

    const stored = (await engine.get("user", "u1")) as Record<string, unknown>;
    expect(stored.__v).toBe(1);
    expect(stored.name).toBe("Sam User");
  });
});
