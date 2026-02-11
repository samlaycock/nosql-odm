import { describe, expect, test, beforeEach } from "bun:test";
import * as z from "zod";
import { model } from "../src/model";
import { createStore } from "../src/store";
import { memoryEngine, type MemoryQueryEngine } from "../src/engines/memory";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function buildUserV2() {
  return model("user")
    .schema(
      1,
      z.object({
        id: z.string(),
        name: z.string(),
        email: z.string().email(),
      }),
    )
    .schema(
      2,
      z.object({
        id: z.string(),
        firstName: z.string(),
        lastName: z.string(),
        email: z.string().email(),
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

class SimulatedCrashError extends Error {
  constructor() {
    super("Simulated crash");
    this.name = "SimulatedCrashError";
  }
}

function seedV1Users(engine: MemoryQueryEngine, count: number) {
  const promises: Promise<void>[] = [];

  for (let i = 0; i < count; i++) {
    const id = `u${String(i).padStart(4, "0")}`;
    promises.push(
      engine.put(
        "user",
        id,
        { __v: 1, id, name: `User ${i}`, email: `user${i}@example.com` },
        { primary: id, byEmail: `user${i}@example.com` },
      ),
    );
  }

  return Promise.all(promises);
}

let engine: MemoryQueryEngine;

beforeEach(() => {
  engine = memoryEngine();
});

// ---------------------------------------------------------------------------
// Crash during migrateAll — within a single page
// ---------------------------------------------------------------------------

describe("migration crash recovery — single page", () => {
  test("crash after N writes, then successful retry migrates remaining docs", async () => {
    await seedV1Users(engine, 10);

    let putCount = 0;

    // Crash after 3 successful migration writes
    engine.setOptions({
      onBeforePut(collection, _id, _doc) {
        if (collection === "user") {
          putCount++;
          if (putCount > 3) {
            throw new SimulatedCrashError();
          }
        }
      },
    });

    const store1 = createStore(engine, [buildUserV2()]);

    // First attempt crashes mid-page
    expect(store1.user.migrateAll()).rejects.toThrow(SimulatedCrashError);

    // 3 documents were migrated before the crash
    let migratedCount = 0;
    let staleCount = 0;

    for (let i = 0; i < 10; i++) {
      const id = `u${String(i).padStart(4, "0")}`;
      const raw = (await engine.get("user", id)) as Record<string, unknown>;

      if (raw.__v === 2) {
        migratedCount++;
      } else {
        staleCount++;
      }
    }

    expect(migratedCount).toBe(3);
    expect(staleCount).toBe(7);

    // Remove the failure hook
    engine.setOptions({});

    // Retry — should succeed and migrate the remaining 7
    const store2 = createStore(engine, [buildUserV2()]);
    const result = await store2.user.migrateAll();

    expect(result.status).toBe("completed");
    expect(result.migrated).toBe(7);

    // All documents should now be v2
    for (let i = 0; i < 10; i++) {
      const id = `u${String(i).padStart(4, "0")}`;
      const raw = (await engine.get("user", id)) as Record<string, unknown>;

      expect(raw.__v).toBe(2);
      expect(raw.firstName).toBeDefined();
    }
  });

  test("crash on very first write, no documents migrated", async () => {
    await seedV1Users(engine, 5);

    // Crash immediately
    engine.setOptions({
      onBeforePut() {
        throw new SimulatedCrashError();
      },
    });

    const store = createStore(engine, [buildUserV2()]);
    expect(store.user.migrateAll()).rejects.toThrow(SimulatedCrashError);

    // No documents migrated
    for (let i = 0; i < 5; i++) {
      const id = `u${String(i).padStart(4, "0")}`;
      const raw = (await engine.get("user", id)) as Record<string, unknown>;
      expect(raw.__v).toBe(1);
    }

    // Remove failure, retry
    engine.setOptions({});

    const store2 = createStore(engine, [buildUserV2()]);
    const result = await store2.user.migrateAll();

    expect(result.status).toBe("completed");
    expect(result.migrated).toBe(5);
  });
});

// ---------------------------------------------------------------------------
// Crash during migrateAll — across multiple pages
// ---------------------------------------------------------------------------

describe("migration crash recovery — multi-page", () => {
  test("crash on second page, checkpoint resumes from page boundary", async () => {
    // 150 documents = 2 pages (100 + 50) in the memory engine
    await seedV1Users(engine, 150);

    let putCount = 0;

    // Let the first page (100 docs) complete, crash on the second page after 10 writes
    engine.setOptions({
      onBeforePut(collection, _id, _doc) {
        if (collection === "user") {
          putCount++;
          if (putCount > 110) {
            throw new SimulatedCrashError();
          }
        }
      },
    });

    const store1 = createStore(engine, [buildUserV2()]);
    expect(store1.user.migrateAll()).rejects.toThrow(SimulatedCrashError);

    // Count migrated vs stale
    let migratedCount = 0;

    for (let i = 0; i < 150; i++) {
      const id = `u${String(i).padStart(4, "0")}`;
      const raw = (await engine.get("user", id)) as Record<string, unknown>;

      if (raw.__v === 2) {
        migratedCount++;
      }
    }

    // 110 docs migrated (100 from first page + 10 from second)
    expect(migratedCount).toBe(110);

    // Checkpoint should have been saved after first page
    const checkpoint = await engine.migration.loadCheckpoint!("user");
    expect(checkpoint).not.toBeNull();

    // Remove failure, retry
    engine.setOptions({});

    const store2 = createStore(engine, [buildUserV2()]);
    const result = await store2.user.migrateAll();

    expect(result.status).toBe("completed");
    // Should only migrate the remaining 40 (50 on page 2, minus 10 already done)
    // But since checkpoint resumes at page boundary, it re-scans page 2 (50 docs)
    // and skips the 10 already migrated, migrating the remaining 40
    expect(result.migrated).toBe(40);

    // All should be v2 now
    for (let i = 0; i < 150; i++) {
      const id = `u${String(i).padStart(4, "0")}`;
      const raw = (await engine.get("user", id)) as Record<string, unknown>;
      expect(raw.__v).toBe(2);
    }
  });

  test("crash on third page of a three-page migration", async () => {
    // 250 documents = 3 pages (100 + 100 + 50)
    await seedV1Users(engine, 250);

    let putCount = 0;

    // Let first two pages complete (200 docs), crash after 5 on third page
    engine.setOptions({
      onBeforePut(collection, _id, _doc) {
        if (collection === "user") {
          putCount++;
          if (putCount > 205) {
            throw new SimulatedCrashError();
          }
        }
      },
    });

    const store1 = createStore(engine, [buildUserV2()]);
    expect(store1.user.migrateAll()).rejects.toThrow(SimulatedCrashError);

    // Verify checkpoint exists (should be after page 2)
    const checkpoint = await engine.migration.loadCheckpoint!("user");
    expect(checkpoint).not.toBeNull();

    // Count migrated
    let migratedCount = 0;

    for (let i = 0; i < 250; i++) {
      const id = `u${String(i).padStart(4, "0")}`;
      const raw = (await engine.get("user", id)) as Record<string, unknown>;

      if (raw.__v === 2) {
        migratedCount++;
      }
    }

    expect(migratedCount).toBe(205);

    // Retry
    engine.setOptions({});

    const store2 = createStore(engine, [buildUserV2()]);
    const result = await store2.user.migrateAll();

    expect(result.status).toBe("completed");
    expect(result.migrated).toBe(45); // 50 on page 3, minus 5 already done

    // All migrated
    for (let i = 0; i < 250; i++) {
      const id = `u${String(i).padStart(4, "0")}`;
      const raw = (await engine.get("user", id)) as Record<string, unknown>;
      expect(raw.__v).toBe(2);
    }
  });
});

// ---------------------------------------------------------------------------
// Multiple crashes with recovery
// ---------------------------------------------------------------------------

describe("migration recovery — multiple sequential crashes", () => {
  test("survives multiple crashes before completing", async () => {
    await seedV1Users(engine, 20);

    // First attempt: crash after 5
    let putCount = 0;
    engine.setOptions({
      onBeforePut(collection) {
        if (collection === "user") {
          putCount++;
          if (putCount > 5) throw new SimulatedCrashError();
        }
      },
    });

    const store1 = createStore(engine, [buildUserV2()]);
    expect(store1.user.migrateAll()).rejects.toThrow(SimulatedCrashError);

    // Second attempt: crash after 5 more (10 total)
    putCount = 0;
    engine.setOptions({
      onBeforePut(collection) {
        if (collection === "user") {
          putCount++;
          if (putCount > 5) throw new SimulatedCrashError();
        }
      },
    });

    const store2 = createStore(engine, [buildUserV2()]);
    expect(store2.user.migrateAll()).rejects.toThrow(SimulatedCrashError);

    // Third attempt: crash after 5 more (15 total)
    putCount = 0;
    engine.setOptions({
      onBeforePut(collection) {
        if (collection === "user") {
          putCount++;
          if (putCount > 5) throw new SimulatedCrashError();
        }
      },
    });

    const store3 = createStore(engine, [buildUserV2()]);
    expect(store3.user.migrateAll()).rejects.toThrow(SimulatedCrashError);

    // Some progress has been made — at least some docs should be migrated
    let migratedSoFar = 0;

    for (let i = 0; i < 20; i++) {
      const id = `u${String(i).padStart(4, "0")}`;
      const raw = (await engine.get("user", id)) as Record<string, unknown>;

      if (raw.__v === 2) migratedSoFar++;
    }

    expect(migratedSoFar).toBeGreaterThan(0);
    expect(migratedSoFar).toBeLessThan(20);

    // Final attempt: no crash
    engine.setOptions({});

    const store4 = createStore(engine, [buildUserV2()]);
    const result = await store4.user.migrateAll();

    expect(result.status).toBe("completed");

    // All migrated
    for (let i = 0; i < 20; i++) {
      const id = `u${String(i).padStart(4, "0")}`;
      const raw = (await engine.get("user", id)) as Record<string, unknown>;
      expect(raw.__v).toBe(2);
    }
  });
});

// ---------------------------------------------------------------------------
// Idempotency — re-running on already-migrated data
// ---------------------------------------------------------------------------

describe("migration idempotency", () => {
  test("running migrateAll twice produces same result", async () => {
    await seedV1Users(engine, 10);

    const store = createStore(engine, [buildUserV2()]);

    const result1 = await store.user.migrateAll();
    expect(result1.status).toBe("completed");
    expect(result1.migrated).toBe(10);

    const result2 = await store.user.migrateAll();
    expect(result2.status).toBe("completed");
    expect(result2.migrated).toBe(0);

    // All still v2
    for (let i = 0; i < 10; i++) {
      const id = `u${String(i).padStart(4, "0")}`;
      const raw = (await engine.get("user", id)) as Record<string, unknown>;
      expect(raw.__v).toBe(2);
    }
  });

  test("partially migrated data is not double-migrated on retry", async () => {
    await seedV1Users(engine, 10);

    let putCount = 0;
    const migratedIds: string[] = [];

    engine.setOptions({
      onBeforePut(collection, id) {
        if (collection === "user") {
          putCount++;
          migratedIds.push(id);
          if (putCount > 5) throw new SimulatedCrashError();
        }
      },
    });

    const store1 = createStore(engine, [buildUserV2()]);
    expect(store1.user.migrateAll()).rejects.toThrow(SimulatedCrashError);

    // Track which IDs get written on retry and whether they were stale
    const retryMigratedIds: string[] = [];
    const retryReindexedIds: string[] = [];
    engine.setOptions({
      onBeforePut(collection, id, doc) {
        if (collection === "user") {
          const d = doc as Record<string, unknown>;
          // If __v < 2 before being written, it was a migration; otherwise index recompute
          if ((d as any).__v === 2) {
            retryReindexedIds.push(id);
          } else {
            retryMigratedIds.push(id);
          }
        }
      },
    });

    const store2 = createStore(engine, [buildUserV2()]);
    await store2.user.migrateAll();

    // Already-migrated docs from the first run should be reindexed (not
    // re-migrated) on retry — their schema version stays the same.
    const firstRunMigrated = migratedIds.slice(0, 5);

    for (const id of firstRunMigrated) {
      expect(retryMigratedIds).not.toContain(id);
    }
  });
});

// ---------------------------------------------------------------------------
// Lock recovery after crash
// ---------------------------------------------------------------------------

describe("lock recovery after crash", () => {
  test("lock is released after crash so retry can proceed", async () => {
    await seedV1Users(engine, 5);

    engine.setOptions({
      onBeforePut() {
        throw new SimulatedCrashError();
      },
    });

    const store = createStore(engine, [buildUserV2()]);
    expect(store.user.migrateAll()).rejects.toThrow(SimulatedCrashError);

    // Lock should be released (finally block in migrateAll)
    engine.setOptions({});

    const store2 = createStore(engine, [buildUserV2()]);
    const result = await store2.user.migrateAll();

    expect(result.status).toBe("completed");
    expect(result.migrated).toBe(5);
  });

  test("lock is released after crash so same model can retry immediately", async () => {
    await seedV1Users(engine, 5);

    engine.setOptions({
      onBeforePut() {
        throw new SimulatedCrashError();
      },
    });

    const store = createStore(engine, [buildUserV2()]);

    // First attempt crashes
    expect(store.user.migrateAll()).rejects.toThrow(SimulatedCrashError);

    // Remove failure
    engine.setOptions({});

    // Should NOT throw MigrationAlreadyRunningError — lock was released in finally
    const result = await store.user.migrateAll();
    expect(result.status).toBe("completed");
    expect(result.migrated).toBe(5);
  });

  test("lock from crashed migration does not block other models", async () => {
    await seedV1Users(engine, 5);
    await engine.put("post", "p1", { __v: 1, id: "p1", title: "Hello", authorId: "u1" }, {});

    engine.setOptions({
      onBeforePut(collection) {
        if (collection === "user") throw new SimulatedCrashError();
      },
    });

    const postModel = model("post")
      .schema(
        1,
        z.object({
          id: z.string(),
          title: z.string(),
          authorId: z.string(),
        }),
      )
      .build();

    const store = createStore(engine, [buildUserV2(), postModel]);
    const results = await store.migrateAll();

    const userResult = results.find((r: any) => r.model === "user");
    const postResult = results.find((r: any) => r.model === "post");

    // User failed but post should succeed
    expect(userResult?.status).toBe("failed");
    expect(postResult?.status).toBe("completed");
  });
});

// ---------------------------------------------------------------------------
// Multi-version crash recovery (v1 → v3)
// ---------------------------------------------------------------------------

describe("migration crash recovery — multi-version chain", () => {
  function buildUserV3() {
    return model("user")
      .schema(
        1,
        z.object({
          id: z.string(),
          name: z.string(),
          email: z.string().email(),
        }),
      )
      .schema(
        2,
        z.object({
          id: z.string(),
          firstName: z.string(),
          lastName: z.string(),
          email: z.string().email(),
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
      .schema(
        3,
        z.object({
          id: z.string(),
          firstName: z.string(),
          lastName: z.string(),
          email: z.string().email(),
          role: z.enum(["admin", "member", "guest"]),
        }),
        {
          migrate(old) {
            return { ...old, role: "member" as const };
          },
        },
      )
      .index({ name: "primary", value: "id" })
      .index({ name: "byEmail", value: "email" })
      .build();
  }

  test("crash during v1→v3 migration, retry completes remaining", async () => {
    await seedV1Users(engine, 10);

    let putCount = 0;
    engine.setOptions({
      onBeforePut(collection) {
        if (collection === "user") {
          putCount++;
          if (putCount > 4) throw new SimulatedCrashError();
        }
      },
    });

    const store1 = createStore(engine, [buildUserV3()]);
    expect(store1.user.migrateAll()).rejects.toThrow(SimulatedCrashError);

    // 4 docs migrated to v3
    let migratedCount = 0;
    for (let i = 0; i < 10; i++) {
      const id = `u${String(i).padStart(4, "0")}`;
      const raw = (await engine.get("user", id)) as Record<string, unknown>;
      if (raw.__v === 3) migratedCount++;
    }
    expect(migratedCount).toBe(4);

    engine.setOptions({});

    const store2 = createStore(engine, [buildUserV3()]);
    const result = await store2.user.migrateAll();

    expect(result.status).toBe("completed");
    expect(result.migrated).toBe(6);

    // All docs should be v3 with role field
    for (let i = 0; i < 10; i++) {
      const id = `u${String(i).padStart(4, "0")}`;
      const raw = (await engine.get("user", id)) as Record<string, unknown>;
      expect(raw.__v).toBe(3);
      expect(raw.role).toBe("member");
      expect(raw.firstName).toBeDefined();
    }
  });

  test("mixed v1 and v2 documents, crash and recovery to v3", async () => {
    // 5 v1 docs
    await seedV1Users(engine, 5);

    // 5 v2 docs
    for (let i = 5; i < 10; i++) {
      const id = `u${String(i).padStart(4, "0")}`;
      await engine.put(
        "user",
        id,
        {
          __v: 2,
          id,
          firstName: `User`,
          lastName: `${i}`,
          email: `user${i}@example.com`,
        },
        { primary: id, byEmail: `user${i}@example.com` },
      );
    }

    let putCount = 0;
    engine.setOptions({
      onBeforePut(collection) {
        if (collection === "user") {
          putCount++;
          if (putCount > 3) throw new SimulatedCrashError();
        }
      },
    });

    const store1 = createStore(engine, [buildUserV3()]);
    expect(store1.user.migrateAll()).rejects.toThrow(SimulatedCrashError);

    engine.setOptions({});

    const store2 = createStore(engine, [buildUserV3()]);
    const result = await store2.user.migrateAll();

    expect(result.status).toBe("completed");

    // All should be v3
    for (let i = 0; i < 10; i++) {
      const id = `u${String(i).padStart(4, "0")}`;
      const raw = (await engine.get("user", id)) as Record<string, unknown>;
      expect(raw.__v).toBe(3);
      expect(raw.role).toBe("member");
    }
  });
});

// ---------------------------------------------------------------------------
// Checkpoint lifecycle
// ---------------------------------------------------------------------------

describe("checkpoint lifecycle", () => {
  test("checkpoint is cleared after a successful run", async () => {
    // 150 docs = 2 pages, checkpoint will be saved after page 1
    await seedV1Users(engine, 150);

    const store = createStore(engine, [buildUserV2()]);
    const result1 = await store.user.migrateAll();

    expect(result1.status).toBe("completed");
    expect(result1.migrated).toBe(150);
    expect(await engine.migration.loadCheckpoint!("user")).toBeNull();

    // Seed 5 more v1 docs
    for (let i = 150; i < 155; i++) {
      const id = `u${String(i).padStart(4, "0")}`;
      await engine.put(
        "user",
        id,
        { __v: 1, id, name: `User ${i}`, email: `user${i}@example.com` },
        { primary: id, byEmail: `user${i}@example.com` },
      );
    }

    // Second run should still find and migrate newly added stale docs.
    const result2 = await store.user.migrateAll();

    expect(result2.status).toBe("completed");
    expect(result2.migrated).toBe(5);
    expect(await engine.migration.loadCheckpoint!("user")).toBeNull();
  });

  test("next run can migrate stale documents that appear before a previous checkpoint", async () => {
    await seedV1Users(engine, 150);

    const store = createStore(engine, [buildUserV2()]);
    const result1 = await store.user.migrateAll();

    expect(result1.status).toBe("completed");
    expect(result1.migrated).toBe(150);

    // Downgrade an existing early document back to v1.
    // Without checkpoint clearing, a later run could resume mid-collection and skip this.
    await engine.put(
      "user",
      "u0001",
      {
        __v: 1,
        id: "u0001",
        name: "Downgraded User",
        email: "downgraded@example.com",
      },
      { primary: "u0001", byEmail: "downgraded@example.com" },
    );

    const result2 = await store.user.migrateAll();

    expect(result2.status).toBe("completed");
    expect(result2.migrated).toBe(1);

    const raw = (await engine.get("user", "u0001")) as Record<string, unknown>;
    expect(raw.__v).toBe(2);
    expect(raw.firstName).toBe("Downgraded");
    expect(raw.lastName).toBe("User");
  });

  test("crash on first page leaves no checkpoint, retry starts from beginning", async () => {
    await seedV1Users(engine, 50);

    let putCount = 0;
    engine.setOptions({
      onBeforePut(collection) {
        if (collection === "user") {
          putCount++;
          if (putCount > 10) throw new SimulatedCrashError();
        }
      },
    });

    const store1 = createStore(engine, [buildUserV2()]);
    expect(store1.user.migrateAll()).rejects.toThrow(SimulatedCrashError);

    // No checkpoint should exist (checkpoint is only saved between pages)
    const checkpoint = await engine.migration.loadCheckpoint!("user");
    expect(checkpoint).toBeNull();

    engine.setOptions({});

    // Retry starts from the very beginning
    const store2 = createStore(engine, [buildUserV2()]);
    const result = await store2.user.migrateAll();

    expect(result.status).toBe("completed");
    // 10 were already migrated, so only 40 remain
    expect(result.migrated).toBe(40);
  });
});

// ---------------------------------------------------------------------------
// Lock TTL / stale lock recovery
// ---------------------------------------------------------------------------

describe("lock TTL — stale lock recovery", () => {
  test("acquireLock with ttl steals a stale lock", async () => {
    await seedV1Users(engine, 5);

    // Acquire a lock manually
    const staleLock = await engine.migration.acquireLock("user");
    expect(staleLock).not.toBeNull();

    // Without TTL, a second acquire fails
    const blocked = await engine.migration.acquireLock("user");
    expect(blocked).toBeNull();

    // Backdate the lock's acquiredAt to simulate staleness
    (staleLock as any).acquiredAt = Date.now() - 60_000;
    // Re-store the backdated lock in the engine
    await engine.migration.releaseLock(staleLock!);
    const reLock = await engine.migration.acquireLock("user");
    expect(reLock).not.toBeNull();
    await engine.migration.releaseLock(reLock!);

    // Now acquire with a fresh timestamp and try TTL steal
    const freshLock = await engine.migration.acquireLock("user");
    expect(freshLock).not.toBeNull();

    // TTL of 1ms — but lock was JUST acquired so it's not stale yet
    // We need to actually make it stale. Manually poke the engine's internal state.
    // Instead, just verify that a non-stale lock is NOT stolen.
    const notStolen = await engine.migration.acquireLock("user", { ttl: 60_000 });
    expect(notStolen).toBeNull();

    // With a very short TTL (0ms), any existing lock is stale
    const stolen = await engine.migration.acquireLock("user", { ttl: 0 });
    expect(stolen).not.toBeNull();
    expect(stolen!.id).not.toBe(freshLock!.id);

    await engine.migration.releaseLock(stolen!);
  });

  test("migrateAll can recover from a permanently stuck lock via TTL", async () => {
    await seedV1Users(engine, 5);

    // Simulate a stuck lock (process died without releasing)
    const stuckLock = await engine.migration.acquireLock("user");
    expect(stuckLock).not.toBeNull();

    // Without TTL, migrateAll fails
    const store1 = createStore(engine, [buildUserV2()]);
    expect(store1.user.migrateAll()).rejects.toThrow("already running");

    // Steal the lock with TTL=0 (treat any lock as stale)
    const newLock = await engine.migration.acquireLock("user", { ttl: 0 });
    expect(newLock).not.toBeNull();
    await engine.migration.releaseLock(newLock!);

    // Now migrateAll works
    const store2 = createStore(engine, [buildUserV2()]);
    const result = await store2.user.migrateAll();
    expect(result.status).toBe("completed");
    expect(result.migrated).toBe(5);
  });
});

// ---------------------------------------------------------------------------
// Migration status observability
// ---------------------------------------------------------------------------

describe("migration status observability", () => {
  test("getMigrationStatus returns null when no migration is running", async () => {
    const store = createStore(engine, [buildUserV2()]);
    const status = await store.user.getMigrationStatus();
    expect(status).toBeNull();
  });

  test("getMigrationStatus returns lock and checkpoint during migration", async () => {
    // 150 docs = 2 pages, so a checkpoint is saved after page 1
    await seedV1Users(engine, 150);

    let putCount = 0;

    // Let first page complete, then crash on page 2
    engine.setOptions({
      onBeforePut(collection) {
        if (collection === "user") {
          putCount++;
          if (putCount > 105) throw new SimulatedCrashError();
        }
      },
    });

    const store = createStore(engine, [buildUserV2()]);
    expect(store.user.migrateAll()).rejects.toThrow(SimulatedCrashError);

    // Lock should be released (finally block), but checkpoint should exist
    const status = await store.user.getMigrationStatus();

    // Lock is released after crash, so status should be null
    expect(status).toBeNull();

    // But the checkpoint persists — verify via engine directly
    const checkpoint = await engine.migration.loadCheckpoint!("user");
    expect(checkpoint).not.toBeNull();
  });

  test("getMigrationStatus shows lock while migration is in progress", async () => {
    await seedV1Users(engine, 5);

    // Acquire lock manually to simulate an in-progress migration
    const lock = await engine.migration.acquireLock("user");
    expect(lock).not.toBeNull();

    const store = createStore(engine, [buildUserV2()]);
    const status = await store.user.getMigrationStatus();

    expect(status).not.toBeNull();
    expect(status!.lock.collection).toBe("user");
    expect(status!.lock.acquiredAt).toBeGreaterThan(0);
    expect(status!.cursor).toBeNull();

    await engine.migration.releaseLock(lock!);
  });

  test("getMigrationStatus shows checkpoint cursor when set", async () => {
    const lock = await engine.migration.acquireLock("user");
    expect(lock).not.toBeNull();

    await engine.migration.saveCheckpoint!(lock!, "u0050");

    const store = createStore(engine, [buildUserV2()]);
    const status = await store.user.getMigrationStatus();

    expect(status).not.toBeNull();
    expect(status!.cursor).toBe("u0050");

    await engine.migration.releaseLock(lock!);
    await engine.migration.clearCheckpoint!("user");
  });
});

// ---------------------------------------------------------------------------
// Concurrent lazy migration during eager migration
// ---------------------------------------------------------------------------

describe("lazy and eager migration interaction", () => {
  test("lazy read during eager migrateAll does not corrupt data", async () => {
    await seedV1Users(engine, 10);

    const store = createStore(engine, [buildUserV2()]);

    // Start eager migration
    const migratePromise = store.user.migrateAll();

    // Immediately do a lazy read (may or may not hit a migrated doc)
    const user = await store.user.findByKey("u0000");

    expect(user).toBeDefined();
    expect(user!.firstName).toBeDefined();
    expect(user!.email).toBeDefined();

    // Wait for eager migration to finish
    const result = await migratePromise;
    expect(result.status).toBe("completed");

    // All docs should be v2 and consistent
    for (let i = 0; i < 10; i++) {
      const id = `u${String(i).padStart(4, "0")}`;
      const raw = (await engine.get("user", id)) as Record<string, unknown>;
      expect(raw.__v).toBe(2);
      expect(raw.firstName).toBeDefined();
    }
  });
});
