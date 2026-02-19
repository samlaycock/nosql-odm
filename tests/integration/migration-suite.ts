import { describe, expect, test } from "bun:test";
import * as z from "zod";

import type { QueryEngine } from "../../src/engines/types";

import { model } from "../../src/model";
import {
  createStore,
  type MigrationHooks,
  type MigrationNextPageResult,
  type MigrationResult,
  type MigrationRunOptions,
  type MigrationRunProgress,
} from "../../src/store";
import { expectReject } from "./helpers";

interface MigrationIntegrationSuiteOptions<TOptions = Record<string, unknown>> {
  engineName: string;
  getEngine: () => QueryEngine<TOptions>;
  nextCollection: (prefix: string) => string;
}

interface MigrationBoundModelApi {
  getOrCreateMigration(options?: MigrationRunOptions): Promise<MigrationRunProgress>;
  migrateNextPage(options?: MigrationRunOptions): Promise<MigrationNextPageResult>;
  migrateAll(options?: MigrationRunOptions): Promise<MigrationResult>;
  getMigrationProgress(): Promise<MigrationRunProgress | null>;
  getMigrationStatus(): Promise<{ lock: { id: string }; cursor: string | null } | null>;
}

interface MigrationStoreApi {
  getOrCreateMigration(options?: MigrationRunOptions): Promise<MigrationRunProgress>;
  migrateNextPage(options?: MigrationRunOptions): Promise<MigrationNextPageResult>;
  getMigrationProgress(): Promise<MigrationRunProgress | null>;
}

function buildUserModel(collection: string) {
  return model(collection)
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
    .index({ name: "byEmail", value: "email" })
    .build();
}

function buildPostModel(collection: string) {
  return model(collection)
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
        slug: z.string(),
      }),
      {
        migrate(old) {
          return {
            id: old.id,
            title: old.title,
            slug: String(old.title).toLowerCase().replace(/\s+/g, "-"),
          };
        },
      },
    )
    .index({ name: "primary", value: "id" })
    .build();
}

function buildFlakyModel(collection: string) {
  return model(collection)
    .schema(
      1,
      z.object({
        id: z.string(),
        name: z.string(),
      }),
    )
    .schema(
      2,
      z.object({
        id: z.string(),
        firstName: z.string(),
      }),
      {
        migrate(old) {
          if (old.name === "bad-user") {
            throw new Error("intentional migration failure");
          }

          return {
            id: old.id,
            firstName: old.name,
          };
        },
      },
    )
    .index({ name: "primary", value: "id" })
    .build();
}

function getBoundModel(store: object, modelName: string): MigrationBoundModelApi {
  const candidate = (store as Record<string, unknown>)[modelName];

  if (!candidate || typeof candidate !== "object") {
    throw new Error(`Store does not expose bound model "${modelName}"`);
  }

  return candidate as MigrationBoundModelApi;
}

async function drainModelMigration(modelApi: MigrationBoundModelApi): Promise<void> {
  for (let i = 0; i < 64; i++) {
    const page = await modelApi.migrateNextPage();

    if (page.status === "busy") {
      throw new Error("Model migration page was unexpectedly busy while draining");
    }

    if (page.completed) {
      return;
    }
  }

  throw new Error("Model migration drain exceeded safety iteration limit");
}

async function drainStoreMigration(store: MigrationStoreApi): Promise<void> {
  for (let i = 0; i < 64; i++) {
    const page = await store.migrateNextPage();

    if (page.status === "busy") {
      throw new Error("Store migration page was unexpectedly busy while draining");
    }

    if (page.completed) {
      return;
    }
  }

  throw new Error("Store migration drain exceeded safety iteration limit");
}

async function seedLegacyUsers(
  engine: QueryEngine<unknown>,
  collection: string,
  count: number,
): Promise<void> {
  for (let i = 0; i < count; i++) {
    const id = `u${String(i).padStart(3, "0")}`;
    await engine.put(
      collection,
      id,
      {
        __v: 1,
        __indexes: ["primary"],
        id,
        name: `User ${String(i)}`,
        email: `${id}@example.com`,
      },
      { primary: id },
    );
  }
}

export function runMigrationIntegrationSuite<TOptions>(
  options: MigrationIntegrationSuiteOptions<TOptions>,
): void {
  describe(`${options.engineName} migration workflow suite`, () => {
    test("engine exposes a default migrator", () => {
      const engine = options.getEngine();
      expect(engine.migrator).toBeDefined();
    });

    test("model migrateAll migrates stale versions and reindexes latest docs", async () => {
      const engine = options.getEngine();
      const collection = options.nextCollection("users_model_all");
      const User = buildUserModel(collection);
      const store = createStore(engine, [User]);
      const users = getBoundModel(store, collection);

      await engine.put(
        collection,
        "u-old-1",
        {
          __v: 1,
          __indexes: ["primary"],
          id: "u-old-1",
          name: "Old One",
          email: "old.one@example.com",
        },
        { primary: "u-old-1" },
      );
      await engine.put(
        collection,
        "u-old-2",
        {
          __v: 1,
          __indexes: ["primary"],
          id: "u-old-2",
          name: "Old Two",
          email: "old.two@example.com",
        },
        { primary: "u-old-2" },
      );
      await engine.put(
        collection,
        "u-reindex",
        {
          __v: 2,
          __indexes: ["primary"],
          id: "u-reindex",
          firstName: "Re",
          lastName: "Index",
          email: "reindex@example.com",
        },
        { primary: "u-reindex" },
      );

      const result = await users.migrateAll();

      expect(result.status).toBe("completed");
      expect(result.migrated).toBe(2);
      expect(result.skipped).toBe(0);

      const migrated = (await engine.get(collection, "u-old-1")) as Record<string, unknown>;
      const reindexed = (await engine.get(collection, "u-reindex")) as Record<string, unknown>;

      expect(migrated.__v).toBe(2);
      expect(migrated.firstName).toBe("Old");
      expect(migrated.lastName).toBe("One");
      expect(reindexed.__indexes).toEqual(["byEmail", "primary"]);

      const byEmail = await engine.query(collection, {
        index: "byEmail",
        filter: { value: "reindex@example.com" },
      });
      expect(byEmail.documents.map((item) => item.key)).toEqual(["u-reindex"]);
    });

    test("model paged migration reports progress and clears it on completion", async () => {
      const engine = options.getEngine();
      const collection = options.nextCollection("users_model_paged");
      const User = buildUserModel(collection);
      const store = createStore(engine, [User]);
      const users = getBoundModel(store, collection);

      await seedLegacyUsers(engine as QueryEngine<unknown>, collection, 101);

      const run = await users.getOrCreateMigration();
      expect(run.scope).toBe("model");
      expect(run.models).toEqual([collection]);

      const first = await users.migrateNextPage();
      expect(first.status).toBe("processed");
      expect(first.migrated).toBe(100);
      expect(first.completed).toBe(false);

      const inFlight = await users.getMigrationProgress();
      expect(inFlight).not.toBeNull();
      expect(inFlight?.running).toBe(true);
      expect(inFlight?.totals.migrated).toBe(100);

      const second = await users.migrateNextPage();
      expect(second.status).toBe("completed");
      expect(second.migrated).toBe(1);
      expect(second.completed).toBe(true);

      expect(await users.getMigrationProgress()).toBeNull();
    });

    test("store-level migrations enforce scope conflict rules", async () => {
      const engine = options.getEngine();
      const usersCollection = options.nextCollection("users_scope");
      const postsCollection = options.nextCollection("posts_scope");
      const User = buildUserModel(usersCollection);
      const Post = buildPostModel(postsCollection);
      const store = createStore(engine, [User, Post]);
      const users = getBoundModel(store, usersCollection);

      await users.getOrCreateMigration();
      await expectReject(
        store.getOrCreateMigration(),
        /Cannot start store migration because model .* is already being migrated/,
      );
      await drainModelMigration(users);

      await store.getOrCreateMigration();
      await expectReject(
        users.getOrCreateMigration(),
        /Cannot start model migration for .* while a store migration is in progress/,
      );
      await drainStoreMigration(store);

      expect(await store.getMigrationProgress()).toBeNull();
    });

    test("migrateNextPage returns busy when a lock is held externally", async () => {
      const engine = options.getEngine();
      const collection = options.nextCollection("users_busy");
      const User = buildUserModel(collection);
      const store = createStore(engine, [User]);
      const users = getBoundModel(store, collection);

      await engine.put(
        collection,
        "u1",
        {
          __v: 1,
          __indexes: ["primary"],
          id: "u1",
          name: "Busy User",
          email: "busy@example.com",
        },
        { primary: "u1" },
      );

      const lock = await engine.migration.acquireLock(collection);
      expect(lock).not.toBeNull();

      const page = await users.migrateNextPage();
      expect(page.status).toBe("busy");
      expect(page.completed).toBe(false);
      expect(page.hasMore).toBe(true);

      await engine.migration.releaseLock(lock!);
    });

    test("lockTtlMs enables stale lock takeover for migrateAll", async () => {
      const engine = options.getEngine();
      const collection = options.nextCollection("users_stale_lock");
      const User = buildUserModel(collection);
      const store = createStore(engine, [User]);
      const users = getBoundModel(store, collection);

      await engine.put(
        collection,
        "u1",
        {
          __v: 1,
          __indexes: ["primary"],
          id: "u1",
          name: "Stale Lock",
          email: "stale@example.com",
        },
        { primary: "u1" },
      );

      await engine.migration.acquireLock(collection);

      await expectReject(users.migrateAll(), /Migration is already running for collection/);

      const retried = await users.migrateAll({ lockTtlMs: 0 });
      expect(retried.status).toBe("completed");
      expect(retried.migrated).toBe(1);
    });

    test("migration hooks emit lifecycle and per-document events", async () => {
      const engine = options.getEngine();
      const collection = options.nextCollection("users_hooks");
      const Flaky = buildFlakyModel(collection);

      await engine.put(
        collection,
        "good",
        { __v: 1, __indexes: ["primary"], id: "good", name: "good-user" },
        { primary: "good" },
      );
      await engine.put(
        collection,
        "bad",
        { __v: 1, __indexes: ["primary"], id: "bad", name: "bad-user" },
        { primary: "bad" },
      );

      const counters = {
        created: 0,
        resumed: 0,
        pageClaimed: 0,
        migrated: 0,
        skipped: 0,
        completed: 0,
        failed: 0,
      };

      const hooks: MigrationHooks = {
        onMigrationCreated() {
          counters.created += 1;
        },
        onMigrationResumed() {
          counters.resumed += 1;
        },
        onPageClaimed() {
          counters.pageClaimed += 1;
        },
        onDocumentMigrated() {
          counters.migrated += 1;
        },
        onDocumentSkipped() {
          counters.skipped += 1;
        },
        onMigrationCompleted() {
          counters.completed += 1;
        },
        onMigrationFailed() {
          counters.failed += 1;
        },
      };

      const store = createStore(engine, [Flaky], { migrationHooks: hooks });
      const users = getBoundModel(store, collection);

      await users.getOrCreateMigration();
      await users.getOrCreateMigration();
      const result = await users.migrateAll();

      expect(result.status).toBe("completed");
      expect(result.migrated).toBe(1);
      expect(result.skipped).toBe(1);
      expect(result.skipReasons).toEqual({ migration_error: 1 });

      expect(counters.created).toBeGreaterThanOrEqual(1);
      expect(counters.resumed).toBeGreaterThanOrEqual(1);
      expect(counters.pageClaimed).toBeGreaterThanOrEqual(1);
      expect(counters.migrated).toBeGreaterThanOrEqual(1);
      expect(counters.skipped).toBeGreaterThanOrEqual(1);
      expect(counters.completed).toBeGreaterThanOrEqual(1);
      expect(counters.failed).toBe(0);
    });

    test("bound-model getMigrationStatus surfaces engine lock/checkpoint state", async () => {
      const engine = options.getEngine();
      const collection = options.nextCollection("users_status");
      const User = buildUserModel(collection);
      const store = createStore(engine, [User]);
      const users = getBoundModel(store, collection);

      const lock = await engine.migration.acquireLock(collection);
      expect(lock).not.toBeNull();
      await engine.migration.saveCheckpoint?.(lock!, "cursor-status");

      const status = await users.getMigrationStatus();
      expect(status).not.toBeNull();
      expect(status?.lock.id).toBe(lock?.id);
      expect(status?.cursor).toBe("cursor-status");

      await engine.migration.releaseLock(lock!);
    });
  });
}
