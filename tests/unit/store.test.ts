import { describe, expect, test, beforeEach } from "bun:test";
import * as z from "zod";

import { memoryEngine, type MemoryQueryEngine } from "../../src/engines/memory";
import {
  EngineDocumentAlreadyExistsError,
  EngineDocumentNotFoundError,
  type QueryEngine,
} from "../../src/engines/types";
import { DefaultMigrator } from "../../src/migrator";
import { model, ValidationError } from "../../src/model";
import {
  createStore,
  DocumentAlreadyExistsError,
  MigrationProjectionError,
  MigrationAlreadyRunningError,
  MigrationScopeConflictError,
  UniqueConstraintError,
} from "../../src/store";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function buildUserV1() {
  return model("user")
    .schema(
      1,
      z.object({
        id: z.string(),
        name: z.string(),
        email: z.email(),
      }),
    )
    .index({ name: "primary", value: "id" })
    .index({ name: "byEmail", value: "email" })
    .build();
}

function buildUserV1WithUniqueEmail() {
  return model("user")
    .schema(
      1,
      z.object({
        id: z.string(),
        name: z.string(),
        email: z.email(),
      }),
    )
    .index({ name: "primary", value: "id" })
    .index({ name: "byEmail", value: "email", unique: true })
    .build();
}

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
    .index({ name: "byEmail", value: "email" })
    .build();
}

function buildUserV3() {
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
    .schema(
      3,
      z.object({
        id: z.string(),
        firstName: z.string(),
        lastName: z.string(),
        email: z.email(),
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
    .index({
      name: "byRole",
      value: (data) => `${data.role}#${data.lastName}`,
    })
    .build();
}

function buildPost() {
  return model("post")
    .schema(
      1,
      z.object({
        id: z.string(),
        title: z.string(),
        authorId: z.string(),
      }),
    )
    .index({ name: "primary", value: "id" })
    .index({ name: "byAuthor", value: "authorId" })
    .build();
}

function buildBlobV1() {
  return model("blob")
    .schema(
      1,
      z.object({
        id: z.string(),
        payload: z.any(),
      }),
    )
    .index({ name: "primary", value: "id" })
    .build();
}

function buildBlobV2WithBigIntMigration() {
  return model("blob")
    .schema(
      1,
      z.object({
        id: z.string(),
        payload: z.any(),
      }),
    )
    .schema(
      2,
      z.object({
        id: z.string(),
        payload: z.any(),
      }),
      {
        migrate(old) {
          return {
            ...old,
            payload: BigInt(1),
          };
        },
      },
    )
    .index({ name: "primary", value: "id" })
    .build();
}

let engine: MemoryQueryEngine;

beforeEach(() => {
  engine = memoryEngine();
});

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

// ---------------------------------------------------------------------------
// createStore basics
// ---------------------------------------------------------------------------

describe("createStore()", () => {
  test("creates a store with model accessors", () => {
    const store = createStore(engine, [buildUserV1(), buildPost()]);

    expect(store.user).toBeDefined();
    expect(store.post).toBeDefined();
    expect(typeof store.migrateAll).toBe("function");
  });

  test("throws on duplicate model names", () => {
    expect(() => {
      createStore(engine, [buildUserV1(), buildUserV1()]);
    }).toThrow('Duplicate model name: "user"');
  });

  test("creates a store with a single model", () => {
    const store = createStore(engine, [buildUserV1()]);

    expect(store.user).toBeDefined();
  });

  test("throws when both migrator and migrationHooks are provided", () => {
    expect(() => {
      createStore(engine, [buildUserV1()], {
        migrator: new DefaultMigrator(engine),
        migrationHooks: {
          onMigrationCreated() {
            return;
          },
        },
      });
    }).toThrow(/migrator.*migrationHooks.*cannot be provided together/i);
  });
});

// ---------------------------------------------------------------------------
// Unique indexes
// ---------------------------------------------------------------------------

describe("unique indexes", () => {
  test("createStore rejects unique indexes when the engine lacks atomic uniqueness support", () => {
    const noUniqueEngine = memoryEngine() as MemoryQueryEngine & {
      capabilities?: { uniqueConstraints: "none" };
    };
    noUniqueEngine.capabilities = { uniqueConstraints: "none" };

    expect(() => createStore(noUniqueEngine, [buildUserV1WithUniqueEmail()])).toThrow(
      /does not support atomic unique constraints/i,
    );
  });

  test("create enforces unique index values", async () => {
    const store = createStore(engine, [buildUserV1WithUniqueEmail()]);

    await store.user.create("u1", {
      id: "u1",
      name: "Sam",
      email: "sam@example.com",
    });

    await expectReject(
      store.user.create("u2", {
        id: "u2",
        name: "Other",
        email: "sam@example.com",
      }),
      UniqueConstraintError,
    );
  });

  test("update enforces unique index values", async () => {
    const store = createStore(engine, [buildUserV1WithUniqueEmail()]);

    await store.user.create("u1", {
      id: "u1",
      name: "Sam",
      email: "sam@example.com",
    });
    await store.user.create("u2", {
      id: "u2",
      name: "Jamie",
      email: "jamie@example.com",
    });

    await expectReject(
      store.user.update("u2", {
        email: "sam@example.com",
      }),
      UniqueConstraintError,
    );

    expect(await store.user.findByKey("u2")).toEqual({
      id: "u2",
      name: "Jamie",
      email: "jamie@example.com",
    });
  });

  test("batchSet rejects duplicate unique values atomically", async () => {
    const store = createStore(engine, [buildUserV1WithUniqueEmail()]);

    await expectReject(
      store.user.batchSet([
        {
          key: "u1",
          data: { id: "u1", name: "Sam", email: "sam@example.com" },
        },
        {
          key: "u2",
          data: { id: "u2", name: "Other", email: "sam@example.com" },
        },
      ]),
      UniqueConstraintError,
    );

    expect(await store.user.findByKey("u1")).toBeNull();
    expect(await store.user.findByKey("u2")).toBeNull();
  });
});

// ---------------------------------------------------------------------------
// CRUD — create
// ---------------------------------------------------------------------------

describe("store.create()", () => {
  test("creates and retrieves a document", async () => {
    const store = createStore(engine, [buildUserV1()]);

    const created = await store.user.create("u1", {
      id: "u1",
      name: "Sam Laycock",
      email: "sam@example.com",
    });

    expect(created).toEqual({
      id: "u1",
      name: "Sam Laycock",
      email: "sam@example.com",
    });

    const fetched = await store.user.findByKey("u1");
    expect(fetched).toEqual(created);
  });

  test("stamps document with version field", async () => {
    const store = createStore(engine, [buildUserV1()]);

    await store.user.create("u1", {
      id: "u1",
      name: "Sam",
      email: "sam@example.com",
    });

    // Read raw from engine to verify __v is set
    const raw = (await engine.get("user", "u1")) as Record<string, unknown>;

    expect(raw.__v).toBe(1);
  });

  test("stamps document with __indexes field", async () => {
    const store = createStore(engine, [buildUserV1()]);

    await store.user.create("u1", {
      id: "u1",
      name: "Sam",
      email: "sam@example.com",
    });

    const raw = (await engine.get("user", "u1")) as Record<string, unknown>;

    // Sorted index names
    expect(raw.__indexes).toEqual(["byEmail", "primary"]);
  });

  test("stamps document with latest version", async () => {
    const store = createStore(engine, [buildUserV2()]);

    await store.user.create("u1", {
      id: "u1",
      firstName: "Sam",
      lastName: "Laycock",
      email: "sam@example.com",
    });

    const raw = (await engine.get("user", "u1")) as Record<string, unknown>;

    expect(raw.__v).toBe(2);
  });

  test("validates data on create", async () => {
    const store = createStore(engine, [buildUserV1()]);

    expect(
      store.user.create("u1", { id: "u1", name: "Sam", email: "not-an-email" }),
    ).rejects.toThrow();
  });

  test("throws DocumentAlreadyExistsError when key exists", async () => {
    const store = createStore(engine, [buildUserV1()]);

    await store.user.create("u1", {
      id: "u1",
      name: "Sam",
      email: "sam@example.com",
    });

    expect(
      store.user.create("u1", {
        id: "u1",
        name: "Other",
        email: "other@example.com",
      }),
    ).rejects.toThrow(DocumentAlreadyExistsError);
  });

  test("maps EngineDocumentAlreadyExistsError from engine.create", async () => {
    const duplicateEngine: QueryEngine<never> = {
      async get() {
        return null;
      },
      async create() {
        throw new EngineDocumentAlreadyExistsError("user", "u1");
      },
      async put() {},
      async update() {},
      async delete() {},
      async query() {
        return { documents: [], cursor: null };
      },
      async batchGet() {
        return [];
      },
      async batchSet() {},
      async batchDelete() {},
      migration: {
        async acquireLock() {
          return null;
        },
        async releaseLock() {},
        async getOutdated() {
          return { documents: [], cursor: null };
        },
      },
    };

    const store = createStore(duplicateEngine, [buildUserV1()]);

    try {
      await store.user.create("u1", {
        id: "u1",
        name: "Sam",
        email: "sam@example.com",
      });
      throw new Error("expected create to fail with duplicate error");
    } catch (error) {
      expect(error).toBeInstanceOf(DocumentAlreadyExistsError);
    }
  });

  test("rethrows non-duplicate engine.create errors unchanged", async () => {
    const failingEngine: QueryEngine<never> = {
      async get() {
        return null;
      },
      async create() {
        throw new Error("write timeout");
      },
      async put() {},
      async update() {},
      async delete() {},
      async query() {
        return { documents: [], cursor: null };
      },
      async batchGet() {
        return [];
      },
      async batchSet() {},
      async batchDelete() {},
      migration: {
        async acquireLock() {
          return null;
        },
        async releaseLock() {},
        async getOutdated() {
          return { documents: [], cursor: null };
        },
      },
    };

    const store = createStore(failingEngine, [buildUserV1()]);

    try {
      await store.user.create("u1", {
        id: "u1",
        name: "Sam",
        email: "sam@example.com",
      });
      throw new Error("expected create to fail");
    } catch (error) {
      expect(String(error)).toContain("write timeout");
    }
  });

  test("does not pre-read existence; delegates create race handling to engine.create", async () => {
    let getCalls = 0;
    let createCalls = 0;
    const trackingEngine: QueryEngine<never> = {
      async get() {
        getCalls++;
        throw new Error("store.create should not call engine.get");
      },
      async create() {
        createCalls++;
      },
      async put() {},
      async update() {},
      async delete() {},
      async query() {
        return { documents: [], cursor: null };
      },
      async batchGet() {
        return [];
      },
      async batchSet() {},
      async batchDelete() {},
      migration: {
        async acquireLock() {
          return null;
        },
        async releaseLock() {},
        async getOutdated() {
          return { documents: [], cursor: null };
        },
      },
    };

    const store = createStore(trackingEngine, [buildUserV1()]);

    await store.user.create("u1", {
      id: "u1",
      name: "Sam",
      email: "sam@example.com",
    });

    expect(getCalls).toBe(0);
    expect(createCalls).toBe(1);
  });

  test("computes and stores index keys", async () => {
    const store = createStore(engine, [buildUserV1()]);

    await store.user.create("u1", {
      id: "u1",
      name: "Sam",
      email: "sam@example.com",
    });

    const results = await engine.query("user", {
      index: "byEmail",
      filter: { value: "sam@example.com" },
    });

    expect(results.documents).toHaveLength(1);
  });
});

// ---------------------------------------------------------------------------
// CRUD — findByKey
// ---------------------------------------------------------------------------

describe("store.findByKey()", () => {
  test("returns null for non-existent document", async () => {
    const store = createStore(engine, [buildUserV1()]);

    const result = await store.user.findByKey("nonexistent");

    expect(result).toBeNull();
  });

  test("returns the document at current version", async () => {
    const store = createStore(engine, [buildUserV1()]);

    await store.user.create("u1", {
      id: "u1",
      name: "Sam",
      email: "sam@example.com",
    });

    const result = await store.user.findByKey("u1");

    expect(result).toEqual({
      id: "u1",
      name: "Sam",
      email: "sam@example.com",
    });
  });
});

// ---------------------------------------------------------------------------
// CRUD — query
// ---------------------------------------------------------------------------

describe("store.query()", () => {
  test("queries by index value", async () => {
    const store = createStore(engine, [buildUserV1()]);

    await store.user.create("u1", {
      id: "u1",
      name: "Sam",
      email: "sam@example.com",
    });
    await store.user.create("u2", {
      id: "u2",
      name: "Other",
      email: "other@example.com",
    });

    const results = await store.user.query({
      index: "byEmail",
      filter: { value: "sam@example.com" },
    });

    expect(results.documents).toHaveLength(1);
    expect(results.documents[0]).toEqual({
      id: "u1",
      name: "Sam",
      email: "sam@example.com",
    });
  });

  test("returns empty results for no matches", async () => {
    const store = createStore(engine, [buildUserV1()]);

    const results = await store.user.query({
      index: "byEmail",
      filter: { value: "nobody@example.com" },
    });

    expect(results.documents).toHaveLength(0);
    expect(results.cursor).toBeNull();
  });

  test("returns all documents when no filter is provided", async () => {
    const store = createStore(engine, [buildUserV1()]);

    await store.user.create("u1", {
      id: "u1",
      name: "Sam",
      email: "sam@example.com",
    });
    await store.user.create("u2", {
      id: "u2",
      name: "Other",
      email: "other@example.com",
    });

    const results = await store.user.query({});

    expect(results.documents).toHaveLength(2);
  });

  test("paginates all documents with no filter", async () => {
    const store = createStore(engine, [buildUserV1()]);

    await store.user.create("u1", {
      id: "u1",
      name: "A",
      email: "a@example.com",
    });
    await store.user.create("u2", {
      id: "u2",
      name: "B",
      email: "b@example.com",
    });
    await store.user.create("u3", {
      id: "u3",
      name: "C",
      email: "c@example.com",
    });

    const page1 = await store.user.query({ limit: 2 });

    expect(page1.documents).toHaveLength(2);
    expect(page1.cursor).not.toBeNull();

    const page2 = await store.user.query({ limit: 2, cursor: page1.cursor! });

    expect(page2.documents).toHaveLength(1);
    expect(page2.cursor).toBeNull();
  });
});

// ---------------------------------------------------------------------------
// CRUD — query with `where`
// ---------------------------------------------------------------------------

describe("store.query() with where", () => {
  test("queries by field name", async () => {
    const store = createStore(engine, [buildUserV1()]);

    await store.user.create("u1", {
      id: "u1",
      name: "Sam",
      email: "sam@example.com",
    });
    await store.user.create("u2", {
      id: "u2",
      name: "Other",
      email: "other@example.com",
    });

    const results = await store.user.query({
      where: { email: "sam@example.com" },
    });

    expect(results.documents).toHaveLength(1);
    expect(results.documents[0]).toEqual({
      id: "u1",
      name: "Sam",
      email: "sam@example.com",
    });
  });

  test("works with FieldCondition operators", async () => {
    const store = createStore(engine, [buildUserV1()]);

    await store.user.create("u1", {
      id: "u1",
      name: "Sam",
      email: "a@example.com",
    });
    await store.user.create("u2", {
      id: "u2",
      name: "Other",
      email: "b@example.com",
    });
    await store.user.create("u3", {
      id: "u3",
      name: "Third",
      email: "c@example.com",
    });

    const results = await store.user.query({
      where: { email: { $begins: "a@" } },
    });

    expect(results.documents).toHaveLength(1);
    expect(results.documents[0]!.email).toBe("a@example.com");
  });

  test("supports limit, cursor, and sort", async () => {
    const store = createStore(engine, [buildUserV1()]);

    await store.user.create("u1", {
      id: "u1",
      name: "A",
      email: "a@example.com",
    });
    await store.user.create("u2", {
      id: "u2",
      name: "B",
      email: "b@example.com",
    });
    await store.user.create("u3", {
      id: "u3",
      name: "C",
      email: "c@example.com",
    });

    const page1 = await store.user.query({
      where: { id: { $gte: "u1" } },
      limit: 2,
      sort: "asc",
    });

    expect(page1.documents).toHaveLength(2);
    expect(page1.cursor).not.toBeNull();

    const page2 = await store.user.query({
      where: { id: { $gte: "u1" } },
      limit: 2,
      cursor: page1.cursor!,
    });

    expect(page2.documents).toHaveLength(1);
  });

  test("throws when field has no index", async () => {
    const store = createStore(engine, [buildUserV1()]);

    expect(store.user.query({ where: { nonexistent: "value" } })).rejects.toThrow(
      'No index found for field "nonexistent"',
    );
  });

  test("throws when combined with index/filter", async () => {
    const store = createStore(engine, [buildUserV1()]);

    expect(
      store.user.query({
        index: "byEmail",
        filter: { value: "sam@example.com" },
        where: { email: "sam@example.com" },
      }),
    ).rejects.toThrow('Cannot use both "index"/"filter" and "where"');
  });

  test("throws when where has multiple fields", async () => {
    const store = createStore(engine, [buildUserV1()]);

    expect(store.user.query({ where: { email: "a", id: "b" } })).rejects.toThrow(
      '"where" must contain exactly one field',
    );
  });

  test("throws when where has zero fields", async () => {
    const store = createStore(engine, [buildUserV1()]);

    expect(store.user.query({ where: {} })).rejects.toThrow(
      '"where" must contain exactly one field',
    );
  });
});

// ---------------------------------------------------------------------------
// CRUD — update
// ---------------------------------------------------------------------------

describe("store.update()", () => {
  test("updates an existing document", async () => {
    const store = createStore(engine, [buildUserV1()]);

    await store.user.create("u1", {
      id: "u1",
      name: "Sam",
      email: "sam@example.com",
    });

    const updated = await store.user.update("u1", { name: "Samuel" });

    expect(updated).toEqual({
      id: "u1",
      name: "Samuel",
      email: "sam@example.com",
    });

    const fetched = await store.user.findByKey("u1");
    expect(fetched).toEqual(updated);
  });

  test("throws for non-existent document", async () => {
    const store = createStore(engine, [buildUserV1()]);

    expect(store.user.update("nonexistent", { name: "Sam" })).rejects.toThrow(
      'Document "nonexistent" not found',
    );
  });

  test("validates merged data", async () => {
    const store = createStore(engine, [buildUserV1()]);

    await store.user.create("u1", {
      id: "u1",
      name: "Sam",
      email: "sam@example.com",
    });

    expect(store.user.update("u1", { email: "not-an-email" })).rejects.toThrow();
  });

  test("recomputes index keys after update", async () => {
    const store = createStore(engine, [buildUserV1()]);

    await store.user.create("u1", {
      id: "u1",
      name: "Sam",
      email: "old@example.com",
    });
    await store.user.update("u1", { email: "new@example.com" });

    const oldResults = await store.user.query({
      index: "byEmail",
      filter: { value: "old@example.com" },
    });
    const newResults = await store.user.query({
      index: "byEmail",
      filter: { value: "new@example.com" },
    });

    expect(oldResults.documents).toHaveLength(0);
    expect(newResults.documents).toHaveLength(1);
  });

  test("throws not found when engine.update reports concurrent delete", async () => {
    const raceEngine: QueryEngine<never> = {
      async get() {
        return {
          __v: 1,
          __indexes: ["byEmail", "primary"],
          id: "u1",
          name: "Sam",
          email: "sam@example.com",
        };
      },
      async create() {},
      async put() {},
      async update() {
        throw new EngineDocumentNotFoundError("user", "u1");
      },
      async delete() {},
      async query() {
        return { documents: [], cursor: null };
      },
      async batchGet() {
        return [];
      },
      async batchSet() {},
      async batchDelete() {},
      migration: {
        async acquireLock() {
          return null;
        },
        async releaseLock() {},
        async getOutdated() {
          return { documents: [], cursor: null };
        },
      },
    };

    const store = createStore(raceEngine, [buildUserV1()]);

    try {
      await store.user.update("u1", { name: "Samuel" });
      throw new Error("expected update to fail with not found");
    } catch (error) {
      expect(String(error)).toContain('Document "u1" not found in model "user"');
    }
  });

  test("rethrows non-not-found engine.update errors unchanged", async () => {
    const raceEngine: QueryEngine<never> = {
      async get() {
        return {
          __v: 1,
          __indexes: ["byEmail", "primary"],
          id: "u1",
          name: "Sam",
          email: "sam@example.com",
        };
      },
      async create() {},
      async put() {},
      async update() {
        throw new Error("write timeout");
      },
      async delete() {},
      async query() {
        return { documents: [], cursor: null };
      },
      async batchGet() {
        return [];
      },
      async batchSet() {},
      async batchDelete() {},
      migration: {
        async acquireLock() {
          return null;
        },
        async releaseLock() {},
        async getOutdated() {
          return { documents: [], cursor: null };
        },
      },
    };

    const store = createStore(raceEngine, [buildUserV1()]);

    try {
      await store.user.update("u1", { name: "Samuel" });
      throw new Error("expected update to fail");
    } catch (error) {
      expect(String(error)).toContain("write timeout");
    }
  });
});

// ---------------------------------------------------------------------------
// CRUD — delete
// ---------------------------------------------------------------------------

describe("store.delete()", () => {
  test("deletes a document", async () => {
    const store = createStore(engine, [buildUserV1()]);

    await store.user.create("u1", {
      id: "u1",
      name: "Sam",
      email: "sam@example.com",
    });
    await store.user.delete("u1");

    const result = await store.user.findByKey("u1");
    expect(result).toBeNull();
  });

  test("does not throw when deleting non-existent document", async () => {
    const store = createStore(engine, [buildUserV1()]);

    expect(store.user.delete("nonexistent")).resolves.toBeUndefined();
  });
});

// ---------------------------------------------------------------------------
// CRUD — batchGet
// ---------------------------------------------------------------------------

describe("store.batchGet()", () => {
  test("retrieves multiple documents", async () => {
    const store = createStore(engine, [buildUserV1()]);

    await store.user.create("u1", {
      id: "u1",
      name: "Sam",
      email: "sam@example.com",
    });
    await store.user.create("u2", {
      id: "u2",
      name: "Other",
      email: "other@example.com",
    });

    const results = await store.user.batchGet(["u1", "u2"]);

    expect(results).toHaveLength(2);
    expect(results).toContainEqual({
      id: "u1",
      name: "Sam",
      email: "sam@example.com",
    });
    expect(results).toContainEqual({
      id: "u2",
      name: "Other",
      email: "other@example.com",
    });
  });

  test("skips non-existent IDs", async () => {
    const store = createStore(engine, [buildUserV1()]);

    await store.user.create("u1", {
      id: "u1",
      name: "Sam",
      email: "sam@example.com",
    });

    const results = await store.user.batchGet(["u1", "missing"]);

    expect(results).toHaveLength(1);
  });

  test("delegates to engine.batchGet and does not fall back to per-key engine.get", async () => {
    const calls: string[] = [];
    const trackingEngine: QueryEngine<never> = {
      async get() {
        calls.push("get");
        throw new Error("batchGet should not call engine.get");
      },
      async create() {},
      async put() {},
      async update() {},
      async delete() {},
      async query() {
        return { documents: [], cursor: null };
      },
      async batchGet() {
        calls.push("batchGet");
        return [
          {
            key: "u1",
            doc: {
              __v: 1,
              __indexes: ["byEmail", "primary"],
              id: "u1",
              name: "Sam",
              email: "sam@example.com",
            },
          },
        ];
      },
      async batchSet() {},
      async batchDelete() {},
      migration: {
        async acquireLock() {
          return null;
        },
        async releaseLock() {},
        async getOutdated() {
          return { documents: [], cursor: null };
        },
      },
    };

    const store = createStore(trackingEngine, [buildUserV1()]);
    const results = await store.user.batchGet(["u1"]);

    expect(results).toEqual([{ id: "u1", name: "Sam", email: "sam@example.com" }]);
    expect(calls).toEqual(["batchGet"]);
  });
});

// ---------------------------------------------------------------------------
// CRUD — batchSet
// ---------------------------------------------------------------------------

describe("store.batchSet()", () => {
  test("creates multiple documents and returns validated values", async () => {
    const store = createStore(engine, [buildUserV1()]);

    const results = await store.user.batchSet([
      { key: "u1", data: { id: "u1", name: "Sam", email: "sam@example.com" } },
      {
        key: "u2",
        data: { id: "u2", name: "Jane", email: "jane@example.com" },
      },
    ]);

    expect(results).toHaveLength(2);
    expect(results).toContainEqual({
      id: "u1",
      name: "Sam",
      email: "sam@example.com",
    });
    expect(results).toContainEqual({
      id: "u2",
      name: "Jane",
      email: "jane@example.com",
    });

    expect(await store.user.findByKey("u1")).toEqual({
      id: "u1",
      name: "Sam",
      email: "sam@example.com",
    });
    expect(await store.user.findByKey("u2")).toEqual({
      id: "u2",
      name: "Jane",
      email: "jane@example.com",
    });
  });

  test("stamps documents with latest version", async () => {
    const store = createStore(engine, [buildUserV2()]);

    await store.user.batchSet([
      {
        key: "u1",
        data: {
          id: "u1",
          firstName: "Sam",
          lastName: "Laycock",
          email: "sam@example.com",
        },
      },
      {
        key: "u2",
        data: {
          id: "u2",
          firstName: "Jane",
          lastName: "Doe",
          email: "jane@example.com",
        },
      },
    ]);

    const raw1 = (await engine.get("user", "u1")) as Record<string, unknown>;
    const raw2 = (await engine.get("user", "u2")) as Record<string, unknown>;

    expect(raw1.__v).toBe(2);
    expect(raw2.__v).toBe(2);
  });

  test("computes index keys for all batch documents", async () => {
    const store = createStore(engine, [buildUserV1()]);

    await store.user.batchSet([
      { key: "u1", data: { id: "u1", name: "Sam", email: "sam@example.com" } },
      {
        key: "u2",
        data: { id: "u2", name: "Jane", email: "jane@example.com" },
      },
    ]);

    const results = await store.user.query({
      index: "byEmail",
      filter: { value: "jane@example.com" },
    });

    expect(results.documents).toHaveLength(1);
    expect(results.documents[0]).toEqual({
      id: "u2",
      name: "Jane",
      email: "jane@example.com",
    });
  });

  test("validates all documents before writing any", async () => {
    const store = createStore(engine, [buildUserV1()]);

    expect(
      store.user.batchSet([
        {
          key: "u1",
          data: { id: "u1", name: "Sam", email: "sam@example.com" },
        },
        {
          key: "u2",
          data: { id: "u2", name: "Broken", email: "not-an-email" },
        },
      ]),
    ).rejects.toThrow(ValidationError);

    expect(await store.user.findByKey("u1")).toBeNull();
    expect(await store.user.findByKey("u2")).toBeNull();
  });

  test("throws when a batch item is missing an explicit key", async () => {
    const noIdModel = model("thing")
      .schema(1, z.object({ key: z.string(), value: z.string() }))
      .build();

    const store = createStore(engine, [noIdModel]);

    expect(store.thing.batchSet([{ data: { key: "a", value: "b" } } as any])).rejects.toThrow(
      "Invalid document key",
    );
  });

  test("delegates to engine.batchSet and does not fall back to per-item engine.put", async () => {
    const calls: string[] = [];
    const trackingEngine: QueryEngine<never> = {
      async get() {
        return null;
      },
      async create() {},
      async put() {
        calls.push("put");
        throw new Error("batchSet should not call engine.put");
      },
      async update() {},
      async delete() {},
      async query() {
        return { documents: [], cursor: null };
      },
      async batchGet() {
        return [];
      },
      async batchSet() {
        calls.push("batchSet");
      },
      async batchDelete() {},
      migration: {
        async acquireLock() {
          return null;
        },
        async releaseLock() {},
        async getOutdated() {
          return { documents: [], cursor: null };
        },
      },
    };

    const store = createStore(trackingEngine, [buildUserV1()]);
    await store.user.batchSet([
      {
        key: "u1",
        data: { id: "u1", name: "Sam", email: "sam@example.com" },
      },
    ]);

    expect(calls).toEqual(["batchSet"]);
  });
});

// ---------------------------------------------------------------------------
// JSON compatibility enforcement on writes
// ---------------------------------------------------------------------------

describe("JSON compatibility on engine writes", () => {
  const expectReject = async (work: Promise<unknown>, pattern: RegExp) => {
    try {
      await work;
      throw new Error("expected operation to fail");
    } catch (error) {
      expect(String(error)).toMatch(pattern);
    }
  };

  test("create rejects non-JSON values before writing to engine", async () => {
    const store = createStore(engine, [buildBlobV1()]);

    await expectReject(
      store.blob.create("b1", {
        id: "b1",
        payload: BigInt(1),
      }),
      /not JSON-compatible/,
    );

    expect(await store.blob.findByKey("b1")).toBeNull();
  });

  test("update rejects non-JSON values before writing to engine", async () => {
    const store = createStore(engine, [buildBlobV1()]);

    await store.blob.create("b1", {
      id: "b1",
      payload: { safe: "ok" },
    });

    await expectReject(
      store.blob.update("b1", {
        payload: undefined,
      }),
      /not JSON-compatible/,
    );
  });

  test("batchSet fails atomically when any document is not JSON-compatible", async () => {
    const store = createStore(engine, [buildBlobV1()]);

    await expectReject(
      store.blob.batchSet([
        {
          key: "b1",
          data: {
            id: "b1",
            payload: { safe: true },
          },
        },
        {
          key: "b2",
          data: {
            id: "b2",
            payload: BigInt(2),
          },
        },
      ]),
      /not JSON-compatible/,
    );

    expect(await store.blob.findByKey("b1")).toBeNull();
    expect(await store.blob.findByKey("b2")).toBeNull();
  });

  test("lazy migration writeback rejects non-JSON migrated documents", async () => {
    await engine.put(
      "blob",
      "b1",
      { __v: 1, __indexes: ["primary"], id: "b1", payload: "old" },
      { primary: "b1" },
    );

    const store = createStore(engine, [buildBlobV2WithBigIntMigration()]);

    await expectReject(store.blob.findByKey("b1"), /not JSON-compatible/);
  });

  test("migrateAll rejects non-JSON migrated documents", async () => {
    await engine.put(
      "blob",
      "b1",
      { __v: 1, __indexes: ["primary"], id: "b1", payload: "old" },
      { primary: "b1" },
    );

    const store = createStore(engine, [buildBlobV2WithBigIntMigration()]);

    await expectReject(store.blob.migrateAll(), /not JSON-compatible/);
  });

  test("create rejects circular references", async () => {
    const store = createStore(engine, [buildBlobV1()]);
    const circular: Record<string, unknown> = {};
    circular.self = circular;

    await expectReject(
      store.blob.create("b1", {
        id: "b1",
        payload: circular,
      }),
      /circular references are not allowed/,
    );
  });

  test("create rejects unsupported nested value types", async () => {
    const store = createStore(engine, [buildBlobV1()]);

    class CustomPayload {}

    const cases: Array<{ payload: unknown; pattern: RegExp }> = [
      { payload: { v: Symbol("x") }, pattern: /symbol is not allowed/ },
      { payload: { v: () => "x" }, pattern: /function is not allowed/ },
      { payload: { v: new Date() }, pattern: /unsupported object type "Date"/ },
      {
        payload: { v: new Map([["a", 1]]) },
        pattern: /unsupported object type "Map"/,
      },
      {
        payload: { v: new Set([1, 2]) },
        pattern: /unsupported object type "Set"/,
      },
      {
        payload: { v: new CustomPayload() },
        pattern: /unsupported object type "CustomPayload"/,
      },
    ];

    for (let i = 0; i < cases.length; i++) {
      await expectReject(
        store.blob.create(`type-${String(i)}`, {
          id: `type-${String(i)}`,
          payload: cases[i]!.payload,
        }),
        cases[i]!.pattern,
      );
    }
  });

  test("error includes the precise nested JSON path", async () => {
    const store = createStore(engine, [buildBlobV1()]);

    await expectReject(
      store.blob.create("path-test", {
        id: "path-test",
        payload: {
          deep: [{ ok: true }, { bad: BigInt(1) }],
        },
      }),
      /\$\.payload\.deep\[1\]\.bad/,
    );
  });
});

// ---------------------------------------------------------------------------
// CRUD — batchDelete
// ---------------------------------------------------------------------------

describe("store.batchDelete()", () => {
  test("deletes multiple documents", async () => {
    const store = createStore(engine, [buildUserV1()]);

    await store.user.batchSet([
      { key: "u1", data: { id: "u1", name: "Sam", email: "sam@example.com" } },
      {
        key: "u2",
        data: { id: "u2", name: "Jane", email: "jane@example.com" },
      },
      {
        key: "u3",
        data: { id: "u3", name: "Alex", email: "alex@example.com" },
      },
    ]);

    await store.user.batchDelete(["u1", "u3"]);

    expect(await store.user.findByKey("u1")).toBeNull();
    expect(await store.user.findByKey("u2")).toEqual({
      id: "u2",
      name: "Jane",
      email: "jane@example.com",
    });
    expect(await store.user.findByKey("u3")).toBeNull();
  });

  test("does not throw for missing IDs", async () => {
    const store = createStore(engine, [buildUserV1()]);

    await store.user.batchSet([
      { key: "u1", data: { id: "u1", name: "Sam", email: "sam@example.com" } },
    ]);

    expect(store.user.batchDelete(["missing", "u1", "still-missing"])).resolves.toBeUndefined();
    expect(await store.user.findByKey("u1")).toBeNull();
  });

  test("delegates to engine.batchDelete and does not fall back to per-key engine.delete", async () => {
    const calls: string[] = [];
    const trackingEngine: QueryEngine<never> = {
      async get() {
        return null;
      },
      async create() {},
      async put() {},
      async update() {},
      async delete() {
        calls.push("delete");
        throw new Error("batchDelete should not call engine.delete");
      },
      async query() {
        return { documents: [], cursor: null };
      },
      async batchGet() {
        return [];
      },
      async batchSet() {},
      async batchDelete() {
        calls.push("batchDelete");
      },
      migration: {
        async acquireLock() {
          return null;
        },
        async releaseLock() {},
        async getOutdated() {
          return { documents: [], cursor: null };
        },
      },
    };

    const store = createStore(trackingEngine, [buildUserV1()]);
    await store.user.batchDelete(["u1", "u2"]);

    expect(calls).toEqual(["batchDelete"]);
  });
});

// ---------------------------------------------------------------------------
// Lazy migration on read — findByKey
// ---------------------------------------------------------------------------

describe("lazy migration on findByKey", () => {
  test("auto-migrates a stale v1 document to v2 on read", async () => {
    // Seed a v1 document directly into the engine
    await engine.put(
      "user",
      "u1",
      { __v: 1, id: "u1", name: "Sam Laycock", email: "sam@example.com" },
      { primary: "u1", byEmail: "sam@example.com" },
    );

    const store = createStore(engine, [buildUserV2()]);
    const result = await store.user.findByKey("u1");

    expect(result).toEqual({
      id: "u1",
      firstName: "Sam",
      lastName: "Laycock",
      email: "sam@example.com",
    });
  });

  test("writes back migrated document to engine (lazy mode)", async () => {
    await engine.put(
      "user",
      "u1",
      { __v: 1, id: "u1", name: "Sam Laycock", email: "sam@example.com" },
      { primary: "u1", byEmail: "sam@example.com" },
    );

    const store = createStore(engine, [buildUserV2()]);
    await store.user.findByKey("u1");

    // Check engine has the migrated version
    const raw = (await engine.get("user", "u1")) as Record<string, unknown>;

    expect(raw.__v).toBe(2);
    expect(raw.firstName).toBe("Sam");
    expect(raw.lastName).toBe("Laycock");
    expect(raw.name).toBeUndefined();
  });

  test("uses engine.batchSet for lazy writeback", async () => {
    const calls: { collection: string; items: unknown[] }[] = [];
    const trackingEngine: QueryEngine<never> = {
      async get() {
        return {
          __v: 1,
          id: "u1",
          name: "Sam Laycock",
          email: "sam@example.com",
        };
      },
      async create() {},
      async put() {},
      async update() {},
      async delete() {},
      async query() {
        return { documents: [], cursor: null };
      },
      async batchGet() {
        return [];
      },
      async batchSet(collection, items) {
        calls.push({ collection, items });
      },
      async batchDelete() {},
      migration: {
        async acquireLock() {
          return null;
        },
        async releaseLock() {},
        async getOutdated() {
          return { documents: [], cursor: null };
        },
      },
    };
    trackingEngine.migrator = new DefaultMigrator(trackingEngine);

    const store = createStore(trackingEngine, [buildUserV2()]);
    await store.user.findByKey("u1");

    expect(calls).toHaveLength(1);
    expect(calls[0]!.collection).toBe("user");
    expect((calls[0]!.items[0] as { key: string }).key).toBe("u1");
  });

  test("auto-migrates v1 to v3 through entire chain", async () => {
    await engine.put(
      "user",
      "u1",
      { __v: 1, id: "u1", name: "Sam Laycock", email: "sam@example.com" },
      { primary: "u1", byEmail: "sam@example.com" },
    );

    const store = createStore(engine, [buildUserV3()]);
    const result = await store.user.findByKey("u1");

    expect(result).toEqual({
      id: "u1",
      firstName: "Sam",
      lastName: "Laycock",
      email: "sam@example.com",
      role: "member",
    });

    // Verify write-back
    const raw = (await engine.get("user", "u1")) as Record<string, unknown>;
    expect(raw.__v).toBe(3);
    expect(raw.role).toBe("member");
  });

  test("auto-migrates v2 to v3", async () => {
    await engine.put(
      "user",
      "u1",
      {
        __v: 2,
        id: "u1",
        firstName: "Sam",
        lastName: "Laycock",
        email: "sam@example.com",
      },
      { primary: "u1", byEmail: "sam@example.com" },
    );

    const store = createStore(engine, [buildUserV3()]);
    const result = await store.user.findByKey("u1");

    expect(result).toEqual({
      id: "u1",
      firstName: "Sam",
      lastName: "Laycock",
      email: "sam@example.com",
      role: "member",
    });
  });

  test("does not migrate document already at latest version", async () => {
    const store = createStore(engine, [buildUserV1()]);

    await store.user.create("u1", {
      id: "u1",
      name: "Sam",
      email: "sam@example.com",
    });

    // findByKey should not trigger migration
    const result = await store.user.findByKey("u1");

    expect(result).toEqual({ id: "u1", name: "Sam", email: "sam@example.com" });
  });

  test("assumes version 1 when __v is missing", async () => {
    await engine.put(
      "user",
      "u1",
      { id: "u1", name: "Sam Laycock", email: "sam@example.com" },
      { primary: "u1", byEmail: "sam@example.com" },
    );

    const store = createStore(engine, [buildUserV2()]);
    const result = await store.user.findByKey("u1");

    expect(result).toEqual({
      id: "u1",
      firstName: "Sam",
      lastName: "Laycock",
      email: "sam@example.com",
    });
  });

  test("recomputes index keys after lazy migration", async () => {
    await engine.put(
      "user",
      "u1",
      { __v: 1, id: "u1", name: "Sam Laycock", email: "sam@example.com" },
      { primary: "u1", byEmail: "sam@example.com" },
    );

    const store = createStore(engine, [buildUserV3()]);
    await store.user.findByKey("u1");

    // The byRole index should now be computed
    const results = await engine.query("user", {
      index: "byRole",
      filter: { value: { $begins: "member#" } },
    });

    expect(results.documents).toHaveLength(1);
  });
});

// ---------------------------------------------------------------------------
// Lazy migration on read — query
// ---------------------------------------------------------------------------

describe("lazy migration on query", () => {
  test("auto-migrates stale documents returned by query", async () => {
    await engine.put(
      "user",
      "u1",
      { __v: 1, id: "u1", name: "Sam Laycock", email: "sam@example.com" },
      { primary: "u1", byEmail: "sam@example.com" },
    );

    const store = createStore(engine, [buildUserV2()]);

    const results = await store.user.query({
      index: "byEmail",
      filter: { value: "sam@example.com" },
    });

    expect(results.documents).toHaveLength(1);
    expect(results.documents[0]).toEqual({
      id: "u1",
      firstName: "Sam",
      lastName: "Laycock",
      email: "sam@example.com",
    });

    // Verify write-back happened
    const raw = (await engine.get("user", "u1")) as Record<string, unknown>;
    expect(raw.__v).toBe(2);
  });

  test("handles mix of current and stale documents in query results", async () => {
    // u1 is v1 (stale)
    await engine.put(
      "user",
      "u1",
      { __v: 1, id: "u1", name: "Sam Laycock", email: "sam@example.com" },
      { primary: "u1", byEmail: "sam@example.com" },
    );

    // u2 is v2 (current)
    await engine.put(
      "user",
      "u2",
      {
        __v: 2,
        id: "u2",
        firstName: "Jane",
        lastName: "Doe",
        email: "jane@example.com",
      },
      { primary: "u2", byEmail: "jane@example.com" },
    );

    const store = createStore(engine, [buildUserV2()]);

    // Scan all documents (no index filter)
    const results = await store.user.query({});

    expect(results.documents).toHaveLength(2);
    expect(results.documents).toContainEqual({
      id: "u1",
      firstName: "Sam",
      lastName: "Laycock",
      email: "sam@example.com",
    });
    expect(results.documents).toContainEqual({
      id: "u2",
      firstName: "Jane",
      lastName: "Doe",
      email: "jane@example.com",
    });
  });

  test("uses engine.batchSet for lazy writeback", async () => {
    const calls: { collection: string; items: unknown[] }[] = [];
    const trackingEngine: QueryEngine<never> = {
      async get() {
        return null;
      },
      async create() {},
      async put() {},
      async update() {},
      async delete() {},
      async query() {
        return {
          documents: [
            {
              key: "u1",
              doc: {
                __v: 1,
                id: "u1",
                name: "Sam Laycock",
                email: "sam@example.com",
              },
            },
          ],
          cursor: null,
        };
      },
      async batchGet() {
        return [];
      },
      async batchSet(collection, items) {
        calls.push({ collection, items });
      },
      async batchDelete() {},
      migration: {
        async acquireLock() {
          return null;
        },
        async releaseLock() {},
        async getOutdated() {
          return { documents: [], cursor: null };
        },
      },
    };

    const store = createStore(trackingEngine, [buildUserV2()]);
    await store.user.query({
      index: "byEmail",
      filter: { value: "sam@example.com" },
    });

    expect(calls).toHaveLength(1);
    expect(calls[0]!.collection).toBe("user");
    expect((calls[0]!.items[0] as { key: string }).key).toBe("u1");
  });
});

// ---------------------------------------------------------------------------
// Lazy migration on read — batchGet
// ---------------------------------------------------------------------------

describe("lazy migration on batchGet", () => {
  test("auto-migrates stale documents in batch", async () => {
    await engine.put(
      "user",
      "u1",
      { __v: 1, id: "u1", name: "Sam Laycock", email: "sam@example.com" },
      {},
    );
    await engine.put(
      "user",
      "u2",
      { __v: 1, id: "u2", name: "Jane Doe", email: "jane@example.com" },
      {},
    );

    const store = createStore(engine, [buildUserV2()]);
    const results = await store.user.batchGet(["u1", "u2"]);

    expect(results).toHaveLength(2);
    expect(results).toContainEqual({
      id: "u1",
      firstName: "Sam",
      lastName: "Laycock",
      email: "sam@example.com",
    });
    expect(results).toContainEqual({
      id: "u2",
      firstName: "Jane",
      lastName: "Doe",
      email: "jane@example.com",
    });

    // Verify write-back
    const raw1 = (await engine.get("user", "u1")) as Record<string, unknown>;
    const raw2 = (await engine.get("user", "u2")) as Record<string, unknown>;
    expect(raw1.__v).toBe(2);
    expect(raw2.__v).toBe(2);
  });

  test("uses engine.batchSet for lazy writeback", async () => {
    const calls: { collection: string; items: unknown[] }[] = [];
    const trackingEngine: QueryEngine<never> = {
      async get() {
        return null;
      },
      async create() {},
      async put() {},
      async update() {},
      async delete() {},
      async query() {
        return { documents: [], cursor: null };
      },
      async batchGet() {
        return [
          {
            key: "u1",
            doc: {
              __v: 1,
              id: "u1",
              name: "Sam Laycock",
              email: "sam@example.com",
            },
          },
        ];
      },
      async batchSet(collection, items) {
        calls.push({ collection, items });
      },
      async batchDelete() {},
      migration: {
        async acquireLock() {
          return null;
        },
        async releaseLock() {},
        async getOutdated() {
          return { documents: [], cursor: null };
        },
      },
    };

    const store = createStore(trackingEngine, [buildUserV2()]);
    await store.user.batchGet(["u1"]);

    expect(calls).toHaveLength(1);
    expect(calls[0]!.collection).toBe("user");
    expect((calls[0]!.items[0] as { key: string }).key).toBe("u1");
  });
});

// ---------------------------------------------------------------------------
// Lazy migration on update
// ---------------------------------------------------------------------------

describe("lazy migration on update", () => {
  test("migrates stale document before applying update", async () => {
    await engine.put(
      "user",
      "u1",
      { __v: 1, id: "u1", name: "Sam Laycock", email: "sam@example.com" },
      { primary: "u1", byEmail: "sam@example.com" },
    );

    const store = createStore(engine, [buildUserV2()]);

    const updated = await store.user.update("u1", { firstName: "Samuel" });

    expect(updated).toEqual({
      id: "u1",
      firstName: "Samuel",
      lastName: "Laycock",
      email: "sam@example.com",
    });

    const raw = (await engine.get("user", "u1")) as Record<string, unknown>;
    expect(raw.__v).toBe(2);
    expect(raw.firstName).toBe("Samuel");
  });
});

// ---------------------------------------------------------------------------
// Readonly migration mode
// ---------------------------------------------------------------------------

describe("readonly migration mode", () => {
  test("does not write back migrated document", async () => {
    const userModel = model("user", { migration: "readonly" })
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

    await engine.put(
      "user",
      "u1",
      { __v: 1, id: "u1", name: "Sam Laycock", email: "sam@example.com" },
      { primary: "u1" },
    );

    const store = createStore(engine, [userModel]);
    const result = await store.user.findByKey("u1");

    // Returns migrated data
    expect(result).toEqual({
      id: "u1",
      firstName: "Sam",
      lastName: "Laycock",
      email: "sam@example.com",
    });

    // But engine still has the v1 document
    const raw = (await engine.get("user", "u1")) as Record<string, unknown>;
    expect(raw.__v).toBe(1);
    expect(raw.name).toBe("Sam Laycock");
  });
});

// ---------------------------------------------------------------------------
// Eager migration mode
// ---------------------------------------------------------------------------

describe("eager migration mode", () => {
  test("does not write back migrated document on read", async () => {
    const userModel = model("user", { migration: "eager" })
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

    await engine.put(
      "user",
      "u1",
      { __v: 1, id: "u1", name: "Sam Laycock", email: "sam@example.com" },
      { primary: "u1" },
    );

    const store = createStore(engine, [userModel]);
    const result = await store.user.findByKey("u1");

    // Returns migrated data in memory
    expect(result).toEqual({
      id: "u1",
      firstName: "Sam",
      lastName: "Laycock",
      email: "sam@example.com",
    });

    // But engine still has the v1 document (no lazy writeback)
    const raw = (await engine.get("user", "u1")) as Record<string, unknown>;
    expect(raw.__v).toBe(1);
    expect(raw.name).toBe("Sam Laycock");
  });

  test("migrateAll still works for eager models", async () => {
    const userModel = model("user", { migration: "eager" })
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
      .build();

    await engine.put(
      "user",
      "u1",
      { __v: 1, id: "u1", name: "Sam Laycock", email: "sam@example.com" },
      {},
    );

    const store = createStore(engine, [userModel]);
    const migrationResult = await store.user.migrateAll();

    expect(migrationResult.status).toBe("completed");
    expect(migrationResult.migrated).toBe(1);

    // After explicit migrateAll, engine has v2
    const raw = (await engine.get("user", "u1")) as Record<string, unknown>;
    expect(raw.__v).toBe(2);
    expect(raw.firstName).toBe("Sam");
  });
});

// ---------------------------------------------------------------------------
// migrateAll() — single model
// ---------------------------------------------------------------------------

describe("store.model.migrateAll()", () => {
  test("migrates all stale documents", async () => {
    // Seed 5 v1 documents directly
    for (let i = 0; i < 5; i++) {
      await engine.put(
        "user",
        `u${i}`,
        {
          __v: 1,
          id: `u${i}`,
          name: `User ${i}`,
          email: `user${i}@example.com`,
        },
        { primary: `u${i}`, byEmail: `user${i}@example.com` },
      );
    }

    const store = createStore(engine, [buildUserV2()]);
    const result = await store.user.migrateAll();

    expect(result.status).toBe("completed");
    expect(result.migrated).toBe(5);

    // Verify all are now v2
    for (let i = 0; i < 5; i++) {
      const raw = (await engine.get("user", `u${i}`)) as Record<string, unknown>;
      expect(raw.__v).toBe(2);
      expect(raw.firstName).toBeDefined();
      expect(raw.name).toBeUndefined();
    }
  });

  test("uses engine.batchSet to persist migrated pages", async () => {
    const batchSetCalls: { collection: string; items: unknown[] }[] = [];
    const lock = { id: "lock-1", collection: "user", acquiredAt: Date.now() };
    let checkpoint: string | null = null;
    let lockHeld = false;
    const trackingEngine: QueryEngine<never> = {
      async get() {
        return null;
      },
      async create() {},
      async put() {},
      async update() {},
      async delete() {},
      async query() {
        return { documents: [], cursor: null };
      },
      async batchGet() {
        return [];
      },
      async batchSet(collection, items) {
        batchSetCalls.push({ collection, items });
      },
      async batchDelete() {},
      migration: {
        async acquireLock() {
          if (lockHeld) {
            return null;
          }

          lockHeld = true;
          return lock;
        },
        async releaseLock() {
          lockHeld = false;
        },
        async getOutdated(_collection, _criteria, cursor) {
          if (cursor) {
            return { documents: [], cursor: null };
          }

          return {
            documents: [
              {
                key: "u1",
                doc: {
                  __v: 1,
                  id: "u1",
                  name: "Sam Laycock",
                  email: "sam@example.com",
                },
              },
            ],
            cursor: null,
          };
        },
        async saveCheckpoint(_lock, cursor) {
          checkpoint = cursor;
        },
        async loadCheckpoint() {
          return checkpoint;
        },
        async clearCheckpoint() {
          checkpoint = null;
        },
        async getStatus() {
          if (!lockHeld) {
            return null;
          }

          return {
            lock,
            cursor: checkpoint,
          };
        },
      },
    };
    trackingEngine.migrator = new DefaultMigrator(trackingEngine);

    const store = createStore(trackingEngine, [buildUserV2()]);
    const result = await store.user.migrateAll();

    expect(result.status).toBe("completed");
    expect(result.migrated).toBe(1);
    expect(batchSetCalls).toHaveLength(1);
    expect(batchSetCalls[0]!.collection).toBe("user");
    expect((batchSetCalls[0]!.items[0] as { key: string }).key).toBe("u1");
  });

  test("skips documents already at latest version", async () => {
    // Seed 2 v1 and 3 v2 documents
    for (let i = 0; i < 2; i++) {
      await engine.put(
        "user",
        `stale${i}`,
        {
          __v: 1,
          id: `stale${i}`,
          name: `Stale ${i}`,
          email: `stale${i}@example.com`,
        },
        {},
      );
    }
    for (let i = 0; i < 3; i++) {
      await engine.put(
        "user",
        `current${i}`,
        {
          __v: 2,
          id: `current${i}`,
          firstName: "Current",
          lastName: `${i}`,
          email: `current${i}@example.com`,
        },
        {},
      );
    }

    const store = createStore(engine, [buildUserV2()]);
    const result = await store.user.migrateAll();

    expect(result.status).toBe("completed");
    expect(result.migrated).toBe(2);
  });

  test("returns migrated: 0 when all documents are current", async () => {
    const store = createStore(engine, [buildUserV1()]);

    await store.user.create("u1", {
      id: "u1",
      name: "Sam",
      email: "sam@example.com",
    });

    const result = await store.user.migrateAll();

    expect(result.status).toBe("completed");
    expect(result.migrated).toBe(0);
  });

  test("returns migrated: 0 for empty collection", async () => {
    const store = createStore(engine, [buildUserV2()]);
    const result = await store.user.migrateAll();

    expect(result.status).toBe("completed");
    expect(result.migrated).toBe(0);
  });

  test("recomputes index keys for migrated documents", async () => {
    await engine.put(
      "user",
      "u1",
      { __v: 1, id: "u1", name: "Sam Laycock", email: "sam@example.com" },
      { primary: "u1", byEmail: "sam@example.com" },
    );

    const store = createStore(engine, [buildUserV3()]);
    await store.user.migrateAll();

    // The dynamic byRole index should be computed
    const results = await engine.query("user", {
      index: "byRole",
      filter: { value: "member#Laycock" },
    });

    expect(results.documents).toHaveLength(1);
  });

  test("recomputes index keys for current-version documents", async () => {
    // Seed a v1 document with no byEmail index value
    await engine.put(
      "user",
      "u1",
      { __v: 1, id: "u1", name: "Sam", email: "sam@example.com" },
      { primary: "u1" }, // missing byEmail
    );

    const store = createStore(engine, [buildUserV1()]);
    await store.user.migrateAll();

    // Even though the document is already at v1 (latest), migrateAll should
    // have recomputed indexes — byEmail should now be set.
    const results = await engine.query("user", {
      index: "byEmail",
      filter: { value: "sam@example.com" },
    });

    expect(results.documents).toHaveLength(1);
  });

  test("skips write for current-version documents with correct indexes", async () => {
    const store = createStore(engine, [buildUserV1()]);

    // Create via store — stamps __v and __indexes correctly
    await store.user.create("u1", {
      id: "u1",
      name: "Sam",
      email: "sam@example.com",
    });

    // Track writes during migrateAll
    let putCount = 0;
    engine.setOptions({
      onBeforePut() {
        putCount++;
      },
    });

    await store.user.migrateAll();

    // No writes needed — version and indexes already match
    expect(putCount).toBe(0);
  });

  test("re-indexes documents missing __indexes field", async () => {
    // Seed directly without __indexes
    await engine.put(
      "user",
      "u1",
      { __v: 1, id: "u1", name: "Sam", email: "sam@example.com" },
      { primary: "u1", byEmail: "sam@example.com" },
    );

    const store = createStore(engine, [buildUserV1()]);
    await store.user.migrateAll();

    const raw = (await engine.get("user", "u1")) as Record<string, unknown>;

    // __indexes should now be stamped
    expect(raw.__indexes).toEqual(["byEmail", "primary"]);
  });

  test("re-indexes documents with outdated __indexes", async () => {
    // Seed with old set of indexes (missing byEmail)
    await engine.put(
      "user",
      "u1",
      {
        __v: 1,
        __indexes: ["primary"],
        id: "u1",
        name: "Sam",
        email: "sam@example.com",
      },
      { primary: "u1" },
    );

    const store = createStore(engine, [buildUserV1()]);
    await store.user.migrateAll();

    const raw = (await engine.get("user", "u1")) as Record<string, unknown>;

    expect(raw.__indexes).toEqual(["byEmail", "primary"]);

    // byEmail index should now be queryable
    const results = await engine.query("user", {
      index: "byEmail",
      filter: { value: "sam@example.com" },
    });

    expect(results.documents).toHaveLength(1);
  });

  test("migrates v1 through v2 to v3 in batch", async () => {
    await engine.put(
      "user",
      "u1",
      { __v: 1, id: "u1", name: "Sam Laycock", email: "sam@example.com" },
      {},
    );

    const store = createStore(engine, [buildUserV3()]);
    const result = await store.user.migrateAll();

    expect(result.migrated).toBe(1);

    const raw = (await engine.get("user", "u1")) as Record<string, unknown>;
    expect(raw.__v).toBe(3);
    expect(raw.firstName).toBe("Sam");
    expect(raw.role).toBe("member");
  });
});

// ---------------------------------------------------------------------------
// migrateAll() — concurrent locking
// ---------------------------------------------------------------------------

describe("migrateAll() concurrent locking", () => {
  test("second migrateAll() throws MigrationAlreadyRunningError", async () => {
    // Seed many documents so migration takes a while
    for (let i = 0; i < 50; i++) {
      await engine.put(
        "user",
        `u${i}`,
        {
          __v: 1,
          id: `u${i}`,
          name: `User ${i}`,
          email: `user${i}@example.com`,
        },
        {},
      );
    }

    await engine.migration.acquireLock("user");

    const store = createStore(engine, [buildUserV2()]);

    await expectReject(store.user.migrateAll(), MigrationAlreadyRunningError);
  });

  test("lock is released after migration completes", async () => {
    await engine.put("user", "u1", { __v: 1, id: "u1", name: "Sam", email: "sam@example.com" }, {});

    const store = createStore(engine, [buildUserV2()]);

    await store.user.migrateAll();

    // Second migration should succeed (lock released)
    const result = await store.user.migrateAll();

    expect(result.status).toBe("completed");
    expect(result.migrated).toBe(0); // already migrated
  });

  test("lock is released even when invalid documents are skipped", async () => {
    // Create a model where migration produces invalid data for this document.
    const brokenModel = model("broken")
      .schema(1, z.object({ id: z.string(), value: z.string() }))
      .schema(
        2,
        z.object({
          id: z.string(),
          value: z.string(),
          count: z.number().min(0),
        }),
        { migrate: (old) => ({ ...old, count: -1 }) }, // will fail validation
      )
      .build();

    await engine.put("broken", "b1", { __v: 1, id: "b1", value: "test" }, {});

    const store = createStore(engine, [brokenModel]);

    const first = await store.broken.migrateAll();
    expect(first.status).toBe("completed");
    expect(first.migrated).toBe(0);
    expect(first.skipped).toBe(1);
    expect(first.skipReasons).toEqual({
      validation_error: 1,
    });

    // Lock should be released — rerun still completes (doc remains skipped).
    const second = await store.broken.migrateAll();
    expect(second.status).toBe("completed");
    expect(second.migrated).toBe(0);
    expect(second.skipped).toBe(1);
    expect(second.skipReasons).toEqual({
      validation_error: 1,
    });
  });

  test("different models can migrate concurrently", async () => {
    await engine.put("user", "u1", { __v: 1, id: "u1", name: "Sam", email: "sam@example.com" }, {});
    await engine.put("post", "p1", { __v: 1, id: "p1", title: "Hello", authorId: "u1" }, {});

    const store = createStore(engine, [buildUserV2(), buildPost()]);

    // Both should succeed concurrently since they lock different collections
    const [userResult, postResult] = await Promise.all([
      store.user.migrateAll(),
      store.post.migrateAll(),
    ]);

    expect(userResult.status).toBe("completed");
    expect(userResult.migrated).toBe(1);
    expect(postResult.status).toBe("completed");
    expect(postResult.migrated).toBe(0); // post is still v1, latest is v1
  });
});

// ---------------------------------------------------------------------------
// migrateAll() — checkpoints
// ---------------------------------------------------------------------------

describe("migrateAll() checkpoints", () => {
  test("saves checkpoints during migration", async () => {
    // Seed 150+ documents to force multiple scan pages
    for (let i = 0; i < 150; i++) {
      await engine.put(
        "user",
        `u${String(i).padStart(4, "0")}`,
        {
          __v: 1,
          id: `u${String(i).padStart(4, "0")}`,
          name: `User ${i}`,
          email: `user${i}@example.com`,
        },
        {},
      );
    }

    const store = createStore(engine, [buildUserV2()]);
    const result = await store.user.migrateAll();

    expect(result.status).toBe("completed");
    expect(result.migrated).toBe(150);

    // All documents should be v2
    for (let i = 0; i < 150; i++) {
      const raw = (await engine.get("user", `u${String(i).padStart(4, "0")}`)) as Record<
        string,
        unknown
      >;
      expect(raw.__v).toBe(2);
    }
  });
});

// ---------------------------------------------------------------------------
// store.migrateAll() — all models
// ---------------------------------------------------------------------------

describe("store.migrateAll()", () => {
  test("migrates all registered models", async () => {
    await engine.put(
      "user",
      "u1",
      { __v: 1, id: "u1", name: "Sam Laycock", email: "sam@example.com" },
      {},
    );
    await engine.put("post", "p1", { __v: 1, id: "p1", title: "Hello", authorId: "u1" }, {});

    const store = createStore(engine, [buildUserV2(), buildPost()]);
    const results = await store.migrateAll();

    expect(results).toHaveLength(2);

    const userResult = results.find((r: any) => r.model === "user");
    const postResult = results.find((r: any) => r.model === "post");

    expect(userResult?.status).toBe("completed");
    expect(userResult?.migrated).toBe(1);
    expect(postResult?.status).toBe("completed");
    expect(postResult?.migrated).toBe(0);
  });

  test("fails store migration when the scope is covered by an active model migration", async () => {
    await engine.put("user", "u1", { __v: 1, id: "u1", name: "Sam", email: "sam@example.com" }, {});

    // Acquire lock externally
    await engine.migration.acquireLock("user");

    const store = createStore(engine, [buildUserV2(), buildPost()]);
    await expectReject(store.migrateAll(), MigrationScopeConflictError);
  });

  test("uses lockTtlMs to recover from stale locks", async () => {
    await engine.put("user", "u1", { __v: 1, id: "u1", name: "Sam", email: "sam@example.com" }, {});

    // Simulate a stuck lock.
    await engine.migration.acquireLock("user");

    const store = createStore(engine, [buildUserV2()]);

    await expectReject(store.migrateAll(), MigrationScopeConflictError);

    const second = await store.migrateAll({ lockTtlMs: 0 });
    expect(second[0]?.status).toBe("completed");
    expect(second[0]?.migrated).toBe(1);
  });

  test("continues migrating other models when one has skipped documents", async () => {
    const brokenModel = model("broken")
      .schema(1, z.object({ id: z.string(), value: z.string() }))
      .schema(
        2,
        z.object({
          id: z.string(),
          value: z.string(),
          count: z.number().min(0),
        }),
        { migrate: (old) => ({ ...old, count: -1 }) },
      )
      .build();

    await engine.put("broken", "b1", { __v: 1, id: "b1", value: "test" }, {});
    await engine.put("post", "p1", { __v: 1, id: "p1", title: "Hello", authorId: "u1" }, {});

    const store = createStore(engine, [brokenModel, buildPost()]);
    const results = await store.migrateAll();

    const brokenResult = results.find((r: any) => r.model === "broken");
    const postResult = results.find((r: any) => r.model === "post");

    expect(brokenResult?.status).toBe("completed");
    expect(brokenResult?.migrated).toBe(0);
    expect(brokenResult?.skipped).toBe(1);
    expect(brokenResult?.skipReasons).toEqual({
      validation_error: 1,
    });
    expect(postResult?.status).toBe("completed");
  });

  test("returns empty array for store with no models", async () => {
    const store = createStore(engine, []);
    const results = await store.migrateAll();

    expect(results).toHaveLength(0);
  });
});

// ---------------------------------------------------------------------------
// Multi-model isolation
// ---------------------------------------------------------------------------

describe("multi-model isolation", () => {
  test("operations on one model do not affect another", async () => {
    const store = createStore(engine, [buildUserV1(), buildPost()]);

    await store.user.create("u1", {
      id: "u1",
      name: "Sam",
      email: "sam@example.com",
    });
    await store.post.create("p1", { id: "p1", title: "Hello", authorId: "u1" });

    await store.user.delete("u1");

    expect(await store.user.findByKey("u1")).toBeNull();
    expect(await store.post.findByKey("p1")).toEqual({
      id: "p1",
      title: "Hello",
      authorId: "u1",
    });
  });

  test("querying one model does not return data from another", async () => {
    const store = createStore(engine, [buildUserV1(), buildPost()]);

    await store.user.create("shared-id", {
      id: "shared-id",
      name: "Sam",
      email: "sam@example.com",
    });
    await store.post.create("shared-id", {
      id: "shared-id",
      title: "Hello",
      authorId: "u1",
    });

    const user = await store.user.findByKey("shared-id");
    const post = await store.post.findByKey("shared-id");

    expect(user).toEqual({
      id: "shared-id",
      name: "Sam",
      email: "sam@example.com",
    });
    expect(post).toEqual({ id: "shared-id", title: "Hello", authorId: "u1" });
  });
});

// ---------------------------------------------------------------------------
// Dynamic indexes through migration
// ---------------------------------------------------------------------------

describe("dynamic indexes after migration", () => {
  test("dynamic index is correctly computed after v1→v3 migration", async () => {
    await engine.put(
      "user",
      "u1",
      { __v: 1, id: "u1", name: "Sam Laycock", email: "sam@example.com" },
      { primary: "u1" },
    );

    const store = createStore(engine, [buildUserV3()]);
    await store.user.migrateAll();

    // Query using the dynamic byRole index
    const results = await engine.query("user", {
      index: "byRole",
      filter: { value: "member#Laycock" },
    });

    expect(results.documents).toHaveLength(1);
    expect((results.documents[0]!.doc as any).firstName).toBe("Sam");
  });

  test("dynamic index computed on create", async () => {
    const store = createStore(engine, [buildUserV3()]);

    await store.user.create("u1", {
      id: "u1",
      firstName: "Sam",
      lastName: "Laycock",
      email: "sam@example.com",
      role: "admin",
    });

    const results = await engine.query("user", {
      index: "byRole",
      filter: { value: "admin#Laycock" },
    });

    expect(results.documents).toHaveLength(1);
  });

  test("dynamic index recomputed on update", async () => {
    const store = createStore(engine, [buildUserV3()]);

    await store.user.create("u1", {
      id: "u1",
      firstName: "Sam",
      lastName: "Laycock",
      email: "sam@example.com",
      role: "member",
    });

    await store.user.update("u1", { role: "admin" });

    // Old index value should not match
    const oldResults = await engine.query("user", {
      index: "byRole",
      filter: { value: "member#Laycock" },
    });
    expect(oldResults.documents).toHaveLength(0);

    // New index value should match
    const newResults = await engine.query("user", {
      index: "byRole",
      filter: { value: "admin#Laycock" },
    });
    expect(newResults.documents).toHaveLength(1);
  });
});

// ---------------------------------------------------------------------------
// Readonly migration mode — full coverage
// ---------------------------------------------------------------------------

describe("readonly migration mode — all read paths", () => {
  function buildReadonlyUserV2() {
    return model("user", { migration: "readonly" })
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
      .index({ name: "all", value: () => "yes" })
      .build();
  }

  test("query does not write back in readonly mode", async () => {
    await engine.put(
      "user",
      "u1",
      { __v: 1, id: "u1", name: "Sam Laycock", email: "sam@example.com" },
      { primary: "u1", byEmail: "sam@example.com", all: "yes" },
    );

    const store = createStore(engine, [buildReadonlyUserV2()]);
    const results = await store.user.query({
      index: "all",
      filter: { value: "yes" },
    });

    expect(results.documents).toHaveLength(1);
    expect(results.documents[0]).toEqual({
      id: "u1",
      firstName: "Sam",
      lastName: "Laycock",
      email: "sam@example.com",
    });

    // Engine still has v1
    const raw = (await engine.get("user", "u1")) as Record<string, unknown>;
    expect(raw.__v).toBe(1);
    expect(raw.name).toBe("Sam Laycock");
  });

  test("batchGet does not write back in readonly mode", async () => {
    await engine.put(
      "user",
      "u1",
      { __v: 1, id: "u1", name: "Sam Laycock", email: "sam@example.com" },
      {},
    );

    const store = createStore(engine, [buildReadonlyUserV2()]);
    const results = await store.user.batchGet(["u1"]);

    expect(results).toHaveLength(1);
    expect(results[0]).toEqual({
      id: "u1",
      firstName: "Sam",
      lastName: "Laycock",
      email: "sam@example.com",
    });

    // Engine still has v1
    const raw = (await engine.get("user", "u1")) as Record<string, unknown>;
    expect(raw.__v).toBe(1);
  });

  test("update still writes (it must persist the merged result)", async () => {
    await engine.put(
      "user",
      "u1",
      { __v: 1, id: "u1", name: "Sam Laycock", email: "sam@example.com" },
      { primary: "u1", byEmail: "sam@example.com" },
    );

    const store = createStore(engine, [buildReadonlyUserV2()]);
    const updated = await store.user.update("u1", { firstName: "Samuel" });

    expect(updated).toEqual({
      id: "u1",
      firstName: "Samuel",
      lastName: "Laycock",
      email: "sam@example.com",
    });

    // Update always persists (even in readonly mode, the update itself must write)
    const raw = (await engine.get("user", "u1")) as Record<string, unknown>;
    expect(raw.__v).toBe(2);
    expect(raw.firstName).toBe("Samuel");
  });
});

// ---------------------------------------------------------------------------
// Lazy migration error handling
// ---------------------------------------------------------------------------

describe("lazy migration error handling", () => {
  function buildBrokenMigrationModel() {
    return model("broken")
      .schema(1, z.object({ id: z.string(), value: z.string() }))
      .schema(
        2,
        z.object({
          id: z.string(),
          value: z.string(),
          count: z.number().min(0),
        }),
        { migrate: (old) => ({ ...old, count: -1 }) }, // produces invalid data
      )
      .index({ name: "primary", value: "id" })
      .index({ name: "all", value: () => "yes" })
      .build();
  }

  test("findByKey returns null when lazy migration produces invalid data", async () => {
    await engine.put("broken", "b1", { __v: 1, id: "b1", value: "test" }, { primary: "b1" });

    const store = createStore(engine, [buildBrokenMigrationModel()]);

    const result = await store.broken.findByKey("b1");
    expect(result).toBeNull();
  });

  test("query skips documents when lazy migration produces invalid data", async () => {
    await engine.put(
      "broken",
      "b1",
      { __v: 1, id: "b1", value: "test" },
      { primary: "b1", all: "yes" },
    );

    const store = createStore(engine, [buildBrokenMigrationModel()]);

    const result = await store.broken.query({
      index: "all",
      filter: { value: "yes" },
    });
    expect(result.documents).toEqual([]);
  });

  test("batchGet skips documents when lazy migration produces invalid data", async () => {
    await engine.put("broken", "b1", { __v: 1, id: "b1", value: "test" }, {});

    const store = createStore(engine, [buildBrokenMigrationModel()]);

    const result = await store.broken.batchGet(["b1"]);
    expect(result).toEqual([]);
  });

  test("update throws when existing doc migration produces invalid data", async () => {
    await engine.put("broken", "b1", { __v: 1, id: "b1", value: "test" }, { primary: "b1" });

    const store = createStore(engine, [buildBrokenMigrationModel()]);

    expect(store.broken.update("b1", { value: "new" })).rejects.toThrow(ValidationError);
  });

  test("findByKey does not write back if migration is skipped", async () => {
    await engine.put("broken", "b1", { __v: 1, id: "b1", value: "test" }, { primary: "b1" });

    const store = createStore(engine, [buildBrokenMigrationModel()]);

    const result = await store.broken.findByKey("b1");
    expect(result).toBeNull();

    // Engine should still have v1 document (writeback never reached)
    const raw = (await engine.get("broken", "b1")) as Record<string, unknown>;
    expect(raw.__v).toBe(1);
    expect(raw.value).toBe("test");
  });
});

// ---------------------------------------------------------------------------
// Strict migration error mode
// ---------------------------------------------------------------------------

describe("strict migration error mode", () => {
  function buildStrictBrokenValidationModel() {
    return model("broken", { migrationErrors: "throw" })
      .schema(1, z.object({ id: z.string(), value: z.string() }))
      .schema(
        2,
        z.object({
          id: z.string(),
          value: z.string(),
          count: z.number().min(0),
        }),
        { migrate: (old) => ({ ...old, count: -1 }) },
      )
      .index({ name: "primary", value: "id" })
      .index({ name: "all", value: () => "yes" })
      .build();
  }

  function buildStrictThrowingMigrationModel() {
    return model("broken", { migrationErrors: "throw" })
      .schema(1, z.object({ id: z.string(), value: z.string() }))
      .schema(
        2,
        z.object({
          id: z.string(),
          value: z.string(),
          active: z.boolean(),
        }),
        {
          migrate: () => {
            throw new Error("migration exploded");
          },
        },
      )
      .index({ name: "primary", value: "id" })
      .build();
  }

  test("findByKey throws when lazy projection fails", async () => {
    await engine.put("broken", "b1", { __v: 1, id: "b1", value: "test" }, { primary: "b1" });

    const store = createStore(engine, [buildStrictBrokenValidationModel()]);

    await expectReject(store.broken.findByKey("b1"), MigrationProjectionError);
  });

  test("query throws when lazy projection fails", async () => {
    await engine.put(
      "broken",
      "b1",
      { __v: 1, id: "b1", value: "test" },
      { primary: "b1", all: "yes" },
    );

    const store = createStore(engine, [buildStrictBrokenValidationModel()]);

    await expectReject(
      store.broken.query({
        index: "all",
        filter: { value: "yes" },
      }),
      MigrationProjectionError,
    );
  });

  test("batchGet throws when lazy projection fails", async () => {
    await engine.put("broken", "b1", { __v: 1, id: "b1", value: "test" }, { primary: "b1" });

    const store = createStore(engine, [buildStrictBrokenValidationModel()]);

    await expectReject(store.broken.batchGet(["b1"]), MigrationProjectionError);
  });

  test("update throws when source document projection fails", async () => {
    await engine.put("broken", "b1", { __v: 1, id: "b1", value: "test" }, { primary: "b1" });

    const store = createStore(engine, [buildStrictThrowingMigrationModel()]);

    await expectReject(
      store.broken.update("b1", {
        value: "new",
        active: true,
      }),
      MigrationProjectionError,
    );
  });

  test("migrateAll throws when projection fails", async () => {
    await engine.put("broken", "b1", { __v: 1, id: "b1", value: "test" }, {});

    const store = createStore(engine, [buildStrictBrokenValidationModel()]);

    await expectReject(store.broken.migrateAll(), MigrationProjectionError);
  });
});

// ---------------------------------------------------------------------------
// Migration chain error (mid-chain throw)
// ---------------------------------------------------------------------------

describe("migration chain errors", () => {
  test("findByKey returns null when migration function throws", async () => {
    const badModel = model("user")
      .schema(1, z.object({ id: z.string(), name: z.string() }))
      .schema(2, z.object({ id: z.string(), name: z.string(), email: z.string() }), {
        migrate: () => {
          throw new Error("migration exploded");
        },
      })
      .index({ name: "primary", value: "id" })
      .build();

    await engine.put("user", "u1", { __v: 1, id: "u1", name: "Sam" }, { primary: "u1" });

    const store = createStore(engine, [badModel]);

    const result = await store.user.findByKey("u1");
    expect(result).toBeNull();
  });

  test("migrateAll skips document when migration function throws", async () => {
    const badModel = model("user")
      .schema(1, z.object({ id: z.string(), name: z.string() }))
      .schema(2, z.object({ id: z.string(), name: z.string(), email: z.string() }), {
        migrate: () => {
          throw new Error("migration exploded");
        },
      })
      .build();

    await engine.put("user", "u1", { __v: 1, id: "u1", name: "Sam" }, {});

    const store = createStore(engine, [badModel]);

    const result = await store.user.migrateAll();
    expect(result.status).toBe("completed");
    expect(result.migrated).toBe(0);
    expect(result.skipped).toBe(1);
    expect(result.skipReasons).toEqual({
      migration_error: 1,
    });

    // Lock should still be released
    const lock = await engine.migration.acquireLock("user");
    expect(lock).not.toBeNull();
  });

  test("v1→v2 succeeds but v2→v3 throw is skipped", async () => {
    const badV3Model = model("user")
      .schema(1, z.object({ id: z.string(), name: z.string() }))
      .schema(
        2,
        z.object({
          id: z.string(),
          firstName: z.string(),
          lastName: z.string(),
        }),
        {
          migrate(old) {
            const [firstName, ...rest] = old.name.split(" ");
            return {
              id: old.id,
              firstName: firstName ?? "",
              lastName: rest.join(" ") || "",
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
          role: z.string(),
        }),
        {
          migrate: () => {
            throw new Error("v3 migration failed");
          },
        },
      )
      .build();

    await engine.put("user", "u1", { __v: 1, id: "u1", name: "Sam Laycock" }, {});

    const store = createStore(engine, [badV3Model]);

    const result = await store.user.findByKey("u1");
    expect(result).toBeNull();
  });
});

// ---------------------------------------------------------------------------
// Custom version field through store operations
// ---------------------------------------------------------------------------

describe("custom version field in store context", () => {
  function buildCustomVersionModel() {
    return model("user", { versionField: "_v" })
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
          name: z.string(),
          active: z.boolean(),
        }),
        { migrate: (old) => ({ ...old, active: true }) },
      )
      .index({ name: "primary", value: "id" })
      .index({ name: "all", value: () => "yes" })
      .build();
  }

  test("create stamps custom version field", async () => {
    const store = createStore(engine, [buildCustomVersionModel()]);

    await store.user.create("u1", { id: "u1", name: "Sam", active: true });

    const raw = (await engine.get("user", "u1")) as Record<string, unknown>;
    expect(raw._v).toBe(2);
    expect(raw.__v).toBeUndefined();
  });

  test("findByKey lazy-migrates using custom version field", async () => {
    await engine.put("user", "u1", { _v: 1, id: "u1", name: "Sam" }, { primary: "u1" });

    const store = createStore(engine, [buildCustomVersionModel()]);
    const result = await store.user.findByKey("u1");

    expect(result).toEqual({ id: "u1", name: "Sam", active: true });

    const raw = (await engine.get("user", "u1")) as Record<string, unknown>;
    expect(raw._v).toBe(2);
  });

  test("query lazy-migrates using custom version field", async () => {
    await engine.put("user", "u1", { _v: 1, id: "u1", name: "Sam" }, { primary: "u1", all: "yes" });

    const store = createStore(engine, [buildCustomVersionModel()]);
    const results = await store.user.query({
      index: "all",
      filter: { value: "yes" },
    });

    expect(results.documents[0]).toEqual({
      id: "u1",
      name: "Sam",
      active: true,
    });
  });

  test("update lazy-migrates using custom version field", async () => {
    await engine.put("user", "u1", { _v: 1, id: "u1", name: "Sam" }, { primary: "u1" });

    const store = createStore(engine, [buildCustomVersionModel()]);
    const updated = await store.user.update("u1", { name: "Samuel" });

    expect(updated).toEqual({ id: "u1", name: "Samuel", active: true });
  });

  test("migrateAll uses custom version field", async () => {
    for (let i = 0; i < 3; i++) {
      await engine.put("user", `u${i}`, { _v: 1, id: `u${i}`, name: `User ${i}` }, {});
    }

    const store = createStore(engine, [buildCustomVersionModel()]);
    const result = await store.user.migrateAll();

    expect(result.status).toBe("completed");
    expect(result.migrated).toBe(3);

    for (let i = 0; i < 3; i++) {
      const raw = (await engine.get("user", `u${i}`)) as Record<string, unknown>;
      expect(raw._v).toBe(2);
      expect(raw.active).toBe(true);
    }
  });
});

// ---------------------------------------------------------------------------
// Custom indexes field in store context
// ---------------------------------------------------------------------------

describe("custom indexes field in store context", () => {
  function buildCustomIndexesModel() {
    return model("user", { indexesField: "_idx" })
      .schema(
        1,
        z.object({
          id: z.string(),
          name: z.string(),
          email: z.string(),
        }),
      )
      .index({ name: "primary", value: "id" })
      .index({ name: "byEmail", value: "email" })
      .build();
  }

  test("create stamps custom indexes field", async () => {
    const store = createStore(engine, [buildCustomIndexesModel()]);

    await store.user.create("u1", {
      id: "u1",
      name: "Sam",
      email: "sam@example.com",
    });

    const raw = (await engine.get("user", "u1")) as Record<string, unknown>;
    expect(raw._idx).toEqual(["byEmail", "primary"]);
    expect(raw.__indexes).toBeUndefined();
  });

  test("update stamps custom indexes field", async () => {
    const store = createStore(engine, [buildCustomIndexesModel()]);

    await store.user.create("u1", {
      id: "u1",
      name: "Sam",
      email: "sam@example.com",
    });
    await store.user.update("u1", { name: "Samuel" });

    const raw = (await engine.get("user", "u1")) as Record<string, unknown>;
    expect(raw._idx).toEqual(["byEmail", "primary"]);
  });

  test("migrateAll uses custom indexes field for outdated detection", async () => {
    // Seed a document with correct version but missing custom indexes field
    await engine.put(
      "user",
      "u1",
      { __v: 1, id: "u1", name: "Sam", email: "sam@example.com" },
      { primary: "u1", byEmail: "sam@example.com" },
    );

    const store = createStore(engine, [buildCustomIndexesModel()]);
    const result = await store.user.migrateAll();

    expect(result.status).toBe("completed");

    const raw = (await engine.get("user", "u1")) as Record<string, unknown>;
    expect(raw._idx).toEqual(["byEmail", "primary"]);
    expect(raw.__indexes).toBeUndefined();
  });

  test("migrateAll skips documents with correct custom indexes field", async () => {
    await engine.put(
      "user",
      "u1",
      {
        __v: 1,
        id: "u1",
        name: "Sam",
        email: "sam@example.com",
        _idx: ["byEmail", "primary"],
      },
      { primary: "u1", byEmail: "sam@example.com" },
    );

    const store = createStore(engine, [buildCustomIndexesModel()]);
    const result = await store.user.migrateAll();

    expect(result.status).toBe("completed");
  });
});

// ---------------------------------------------------------------------------
// Documents without id or _id field
// ---------------------------------------------------------------------------

describe("documents without id or _id field", () => {
  test("create supports documents without id or _id when key is explicit", async () => {
    const noIdModel = model("thing")
      .schema(1, z.object({ key: z.string(), value: z.string() }))
      .build();

    const store = createStore(engine, [noIdModel]);

    const created = await store.thing.create("thing-1", {
      key: "a",
      value: "b",
    });
    expect(created).toEqual({ key: "a", value: "b" });
    expect(await store.thing.findByKey("thing-1")).toEqual({
      key: "a",
      value: "b",
    });
  });

  test("query writes back documents without id or _id (key from engine)", async () => {
    const noIdModel = model("thing")
      .schema(1, z.object({ key: z.string(), value: z.string() }))
      .schema(2, z.object({ key: z.string(), value: z.string(), extra: z.boolean() }), {
        migrate: (old) => ({ ...old, extra: true }),
      })
      .index({ name: "all", value: () => "yes" })
      .build();

    await engine.put("thing", "manual-key", { __v: 1, key: "a", value: "b" }, { all: "yes" });

    const store = createStore(engine, [noIdModel]);
    const results = await store.thing.query({
      index: "all",
      filter: { value: "yes" },
    });

    expect(results.documents[0]).toEqual({ key: "a", value: "b", extra: true });

    // Engine should now have v2 since key is always available from engine
    const raw = (await engine.get("thing", "manual-key")) as Record<string, unknown>;
    expect(raw.__v).toBe(2);
  });

  test("migrateAll migrates documents without id or _id (key from engine)", async () => {
    const noIdModel = model("thing")
      .schema(1, z.object({ key: z.string(), value: z.string() }))
      .schema(2, z.object({ key: z.string(), value: z.string(), extra: z.boolean() }), {
        migrate: (old) => ({ ...old, extra: true }),
      })
      .build();

    await engine.put("thing", "manual-key", { __v: 1, key: "a", value: "b" }, {});

    const store = createStore(engine, [noIdModel]);
    const result = await store.thing.migrateAll();

    expect(result.status).toBe("completed");
    expect(result.migrated).toBe(1);

    const raw = (await engine.get("thing", "manual-key")) as Record<string, unknown>;
    expect(raw.__v).toBe(2);
    expect(raw.extra).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// Edge case version values
// ---------------------------------------------------------------------------

describe("edge case version values in store", () => {
  test("document with version higher than latest is ignored", async () => {
    // Seed a doc claiming to be v5, but model only has v1
    await engine.put(
      "user",
      "u1",
      { __v: 5, id: "u1", name: "Sam", email: "sam@example.com" },
      { primary: "u1" },
    );

    const store = createStore(engine, [buildUserV1()]);

    const result = await store.user.findByKey("u1");
    expect(result).toBeNull();
  });

  test("document with unrecognized version string is ignored", async () => {
    await engine.put(
      "user",
      "u1",
      {
        __v: "not-a-number",
        id: "u1",
        name: "Sam Laycock",
        email: "sam@example.com",
      },
      { primary: "u1" },
    );

    const store = createStore(engine, [buildUserV2()]);
    const result = await store.user.findByKey("u1");
    expect(result).toBeNull();
  });

  test('default parser migrates string versions like "v1"', async () => {
    await engine.put(
      "user",
      "u1",
      { __v: "v1", id: "u1", name: "Sam Laycock", email: "sam@example.com" },
      { primary: "u1", byEmail: "sam@example.com" },
    );

    const store = createStore(engine, [buildUserV2()]);
    const result = await store.user.findByKey("u1");

    expect(result).toEqual({
      id: "u1",
      firstName: "Sam",
      lastName: "Laycock",
      email: "sam@example.com",
    });
  });

  test("custom version parser/comparator are used for migration", async () => {
    const customVersionModel = model("user", {
      parseVersion(raw) {
        if (typeof raw === "string" || typeof raw === "number") {
          return raw;
        }

        return null;
      },
      compareVersions(a, b) {
        const toNumber = (v: string | number) =>
          typeof v === "number" ? v : Number(String(v).replace(/^release-/i, ""));

        return toNumber(a) - toNumber(b);
      },
    })
      .schema(1, z.object({ id: z.string(), name: z.string() }))
      .schema(2, z.object({ id: z.string(), name: z.string(), active: z.boolean() }), {
        migrate: (old) => ({ ...old, active: true }),
      })
      .index({ name: "primary", value: "id" })
      .build();

    await engine.put("user", "u1", { __v: "release-1", id: "u1", name: "Sam" }, { primary: "u1" });

    const store = createStore(engine, [customVersionModel]);
    const result = await store.user.findByKey("u1");

    expect(result).toEqual({ id: "u1", name: "Sam", active: true });
  });

  test("document with null version is treated as v1", async () => {
    await engine.put(
      "user",
      "u1",
      { __v: null, id: "u1", name: "Sam Laycock", email: "sam@example.com" },
      { primary: "u1" },
    );

    const store = createStore(engine, [buildUserV2()]);
    const result = await store.user.findByKey("u1");

    expect(result).toEqual({
      id: "u1",
      firstName: "Sam",
      lastName: "Laycock",
      email: "sam@example.com",
    });
  });

  test("migrateAll tracks unknown_source_version skips", async () => {
    await engine.put(
      "user",
      "u1",
      { __v: 0, id: "u1", name: "Sam Laycock", email: "sam@example.com" },
      { primary: "u1", byEmail: "sam@example.com" },
    );

    const store = createStore(engine, [buildUserV2()]);
    const result = await store.user.migrateAll();

    expect(result.status).toBe("completed");
    expect(result.migrated).toBe(0);
    expect(result.skipped).toBe(1);
    expect(result.skipReasons).toEqual({
      unknown_source_version: 1,
    });
  });
});

// ---------------------------------------------------------------------------
// Various data types as document fields (key is always explicit)
// ---------------------------------------------------------------------------

describe("various data types with explicit keys", () => {
  test("document with numeric id field is supported", async () => {
    const numIdModel = model("item")
      .schema(1, z.object({ id: z.number(), value: z.string() }))
      .build();

    const store = createStore(engine, [numIdModel]);

    await store.item.create("0", { id: 0, value: "zero" });
    const result = await store.item.findByKey("0");

    expect(result).toEqual({ id: 0, value: "zero" });
  });

  test("document with boolean field is supported", async () => {
    const boolModel = model("item")
      .schema(1, z.object({ id: z.boolean(), value: z.string() }))
      .build();

    const store = createStore(engine, [boolModel]);

    await store.item.create("bool-1", { id: true, value: "test" });
    const result = await store.item.findByKey("bool-1");

    expect(result).toEqual({ id: true, value: "test" });
  });

  test("document with _id field is supported", async () => {
    const underscoreIdModel = model("item")
      .schema(1, z.object({ _id: z.string(), value: z.string() }))
      .index({ name: "primary", value: "_id" })
      .build();

    const store = createStore(engine, [underscoreIdModel]);

    await store.item.create("abc", { _id: "abc", value: "test" });
    const result = await store.item.findByKey("abc");

    expect(result).toEqual({ _id: "abc", value: "test" });
  });
});

// ---------------------------------------------------------------------------
// Options passthrough
// ---------------------------------------------------------------------------

describe("engine options passthrough", () => {
  function buildTrackingEngine(
    calls: unknown[],
    methods: {
      get?: (_collection: string, _id: string, options?: { trace: string }) => Promise<unknown>;
      create?: (
        _collection: string,
        _id: string,
        _doc: unknown,
        _indexes: Record<string, string>,
        options?: { trace: string },
      ) => Promise<void>;
      update?: (
        _collection: string,
        _id: string,
        _doc: unknown,
        _indexes: Record<string, string>,
        options?: { trace: string },
      ) => Promise<void>;
      delete?: (_collection: string, _id: string, options?: { trace: string }) => Promise<void>;
      query?: (
        _collection: string,
        _params: unknown,
        options?: { trace: string },
      ) => Promise<{
        documents: [];
        cursor: null;
      }>;
      batchGet?: (_collection: string, _ids: string[], options?: { trace: string }) => Promise<[]>;
      batchSet?: (
        _collection: string,
        _items: unknown[],
        options?: { trace: string },
      ) => Promise<void>;
      batchDelete?: (
        _collection: string,
        _ids: string[],
        options?: { trace: string },
      ) => Promise<void>;
    } = {},
  ): QueryEngine<{ trace: string }> {
    return {
      async get(collection, id, options) {
        if (methods.get) {
          return methods.get(collection, id, options);
        }

        return null;
      },
      async create(collection, id, doc, indexes, options) {
        if (methods.create) {
          await methods.create(collection, id, doc, indexes, options);
        }
      },
      async put() {},
      async update(collection, id, doc, indexes, options) {
        if (methods.update) {
          await methods.update(collection, id, doc, indexes, options);
        }
      },
      async delete(collection, id, options) {
        if (methods.delete) {
          await methods.delete(collection, id, options);
        }
      },
      async query(collection, params, options) {
        if (methods.query) {
          return methods.query(collection, params, options);
        }

        return { documents: [], cursor: null };
      },
      async batchGet(collection, ids, options) {
        if (methods.batchGet) {
          return methods.batchGet(collection, ids, options);
        }

        return [];
      },
      async batchSet(collection, items, options) {
        if (methods.batchSet) {
          await methods.batchSet(collection, items, options);
        }
      },
      async batchDelete(collection, ids, options) {
        if (methods.batchDelete) {
          await methods.batchDelete(collection, ids, options);
        }
      },
      migration: {
        async acquireLock() {
          return null;
        },
        async releaseLock() {},
        async getOutdated() {
          return { documents: [], cursor: null };
        },
      },
    };
  }

  test("findByKey passes options to engine.get", async () => {
    const calls: unknown[] = [];
    const trackingEngine = buildTrackingEngine(calls, {
      async get(_collection, _id, options) {
        calls.push({ method: "get", options });
        return null;
      },
    });

    const store = createStore(trackingEngine, [buildUserV1()]);
    await store.user.findByKey("u1", { trace: "test-trace" });

    expect(calls).toHaveLength(1);
    expect((calls[0] as any).options).toEqual({ trace: "test-trace" });
  });

  test("findByKey lazy writeback passes options to engine.batchSet", async () => {
    const calls: unknown[] = [];
    const trackingEngine = buildTrackingEngine(calls, {
      async get(_collection, _id, options) {
        calls.push({ method: "get", options });
        return {
          __v: 1,
          __indexes: ["byEmail", "primary"],
          id: "u1",
          name: "Sam Laycock",
          email: "sam@example.com",
        };
      },
      async batchSet(_collection, items, options) {
        calls.push({ method: "batchSet", options, items: items.length });
      },
    });

    const store = createStore(trackingEngine, [buildUserV2()]);
    await store.user.findByKey("u1", { trace: "lazy-writeback-trace" });

    expect(calls).toHaveLength(2);
    expect((calls[0] as any).options).toEqual({
      trace: "lazy-writeback-trace",
    });
    expect((calls[1] as any).options).toEqual({
      trace: "lazy-writeback-trace",
    });
    expect((calls[1] as any).items).toBe(1);
  });

  test("create passes options to engine.create", async () => {
    const calls: unknown[] = [];
    const trackingEngine = buildTrackingEngine(calls, {
      async create(_collection, _id, _doc, _indexes, options) {
        calls.push({ method: "create", options });
      },
    });

    const store = createStore(trackingEngine, [buildUserV1()]);
    await store.user.create(
      "u1",
      { id: "u1", name: "Sam", email: "sam@example.com" },
      { trace: "create-trace" },
    );

    expect(calls).toHaveLength(1);
    expect((calls[0] as any).options).toEqual({ trace: "create-trace" });
  });

  test("update passes options to engine.update", async () => {
    const calls: unknown[] = [];
    const trackingEngine = buildTrackingEngine(calls, {
      async get() {
        return {
          __v: 1,
          __indexes: ["primary", "byEmail"],
          id: "u1",
          name: "Sam",
          email: "sam@example.com",
        };
      },
      async update(_collection, _id, _doc, _indexes, options) {
        calls.push({ method: "update", options });
      },
    });

    const store = createStore(trackingEngine, [buildUserV1()]);
    await store.user.update("u1", { name: "Sam Updated" }, { trace: "update-trace" });

    expect(calls).toHaveLength(1);
    expect((calls[0] as any).options).toEqual({ trace: "update-trace" });
  });

  test("update passes options to both engine.get and engine.update", async () => {
    const calls: unknown[] = [];
    const trackingEngine = buildTrackingEngine(calls, {
      async get(_collection, _id, options) {
        calls.push({ method: "get", options });
        return {
          __v: 1,
          __indexes: ["primary", "byEmail"],
          id: "u1",
          name: "Sam",
          email: "sam@example.com",
        };
      },
      async update(_collection, _id, _doc, _indexes, options) {
        calls.push({ method: "update", options });
      },
    });

    const store = createStore(trackingEngine, [buildUserV1()]);
    await store.user.update("u1", { name: "Sam Updated" }, { trace: "update-trace-2" });

    expect(calls).toHaveLength(2);
    expect((calls[0] as any).options).toEqual({ trace: "update-trace-2" });
    expect((calls[1] as any).options).toEqual({ trace: "update-trace-2" });
  });

  test("delete passes options to engine.delete", async () => {
    const calls: unknown[] = [];
    const trackingEngine = buildTrackingEngine(calls, {
      async delete(_collection, _id, options) {
        calls.push({ method: "delete", options });
      },
    });

    const store = createStore(trackingEngine, [buildUserV1()]);
    await store.user.delete("u1", { trace: "delete-trace" });

    expect(calls).toHaveLength(1);
    expect((calls[0] as any).options).toEqual({ trace: "delete-trace" });
  });

  test("query passes options to engine.query", async () => {
    const calls: unknown[] = [];
    const trackingEngine = buildTrackingEngine(calls, {
      async query(_collection, _params, options) {
        calls.push({ method: "query", options });
        return { documents: [], cursor: null };
      },
    });

    const store = createStore(trackingEngine, [buildUserV1()]);
    await store.user.query(
      { index: "byEmail", filter: { value: "sam@example.com" } },
      { trace: "query-trace" },
    );

    expect(calls).toHaveLength(1);
    expect((calls[0] as any).options).toEqual({ trace: "query-trace" });
  });

  test("batchSet passes options to engine.batchSet when available", async () => {
    const calls: unknown[] = [];
    const trackingEngine = buildTrackingEngine(calls, {
      async batchSet(_collection, items, options) {
        calls.push({ method: "batchSet", options, items: items.length });
      },
    });

    const store = createStore(trackingEngine, [buildUserV1()]);
    await store.user.batchSet(
      [
        {
          key: "u1",
          data: { id: "u1", name: "Sam", email: "sam@example.com" },
        },
        {
          key: "u2",
          data: { id: "u2", name: "Jane", email: "jane@example.com" },
        },
      ],
      { trace: "batch-set-trace" },
    );

    expect(calls).toHaveLength(1);
    expect((calls[0] as any).options).toEqual({ trace: "batch-set-trace" });
    expect((calls[0] as any).items).toBe(2);
  });

  test("batchGet passes options to engine.batchGet", async () => {
    const calls: unknown[] = [];
    const trackingEngine = buildTrackingEngine(calls, {
      async batchGet(_collection, _ids, options) {
        calls.push({ method: "batchGet", options });
        return [];
      },
    });

    const store = createStore(trackingEngine, [buildUserV1()]);
    await store.user.batchGet(["u1", "u2"], { trace: "batch-get-trace" });

    expect(calls).toHaveLength(1);
    expect((calls[0] as any).options).toEqual({ trace: "batch-get-trace" });
  });

  test("batchDelete passes options to engine.batchDelete when available", async () => {
    const calls: unknown[] = [];
    const trackingEngine = buildTrackingEngine(calls, {
      async batchDelete(_collection, ids, options) {
        calls.push({ method: "batchDelete", options, ids: ids.length });
      },
    });

    const store = createStore(trackingEngine, [buildUserV1()]);
    await store.user.batchDelete(["u1", "u2"], { trace: "batch-delete-trace" });

    expect(calls).toHaveLength(1);
    expect((calls[0] as any).options).toEqual({ trace: "batch-delete-trace" });
    expect((calls[0] as any).ids).toBe(2);
  });
});

// ---------------------------------------------------------------------------
// Dynamic index names
// ---------------------------------------------------------------------------

describe("dynamic index names", () => {
  function buildTenantResource() {
    return model("resource")
      .schema(
        1,
        z.object({
          id: z.string(),
          tenant: z.string(),
          userId: z.string(),
          data: z.string(),
        }),
      )
      .index({ name: "primary", value: "id" })
      .index("tenantUser_v1", {
        name: (d) => `${d.tenant}#user`,
        value: (d) => `${d.tenant}#${d.userId}`,
      })
      .build();
  }

  test("create stores dynamic index name in engine", async () => {
    const store = createStore(engine, [buildTenantResource()]);

    await store.resource.create("r1", {
      id: "r1",
      tenant: "acme",
      userId: "u1",
      data: "hello",
    });

    // The engine should store the resolved dynamic name "acme#user" as the index key
    const raw = (await engine.get("resource", "r1")) as Record<string, unknown>;
    expect(raw.__v).toBe(1);
    // __indexes tracks the static keys
    expect(raw.__indexes).toEqual(["primary", "tenantUser_v1"]);

    // Query the engine directly with the resolved dynamic index name
    const results = await engine.query("resource", {
      index: "acme#user",
      filter: { value: "acme#u1" },
    });
    expect(results.documents).toHaveLength(1);
  });

  test("query by resolved dynamic index name", async () => {
    const store = createStore(engine, [buildTenantResource()]);

    await store.resource.create("r1", {
      id: "r1",
      tenant: "acme",
      userId: "u1",
      data: "hello",
    });
    await store.resource.create("r2", {
      id: "r2",
      tenant: "globex",
      userId: "u2",
      data: "world",
    });

    // Query by the resolved dynamic name — each tenant gets its own index partition
    const acmeResults = await store.resource.query({
      index: "acme#user",
      filter: { value: "acme#u1" },
    });
    expect(acmeResults.documents).toHaveLength(1);
    expect(acmeResults.documents[0]!.tenant).toBe("acme");

    const globexResults = await store.resource.query({
      index: "globex#user",
      filter: { value: "globex#u2" },
    });
    expect(globexResults.documents).toHaveLength(1);
    expect(globexResults.documents[0]!.tenant).toBe("globex");
  });

  test("update recomputes dynamic index name", async () => {
    const store = createStore(engine, [buildTenantResource()]);

    await store.resource.create("r1", {
      id: "r1",
      tenant: "acme",
      userId: "u1",
      data: "hello",
    });

    // Update the tenant — dynamic index name changes
    await store.resource.update("r1", { tenant: "globex" });

    // Old dynamic name should no longer match
    const oldResults = await engine.query("resource", {
      index: "acme#user",
      filter: { value: "acme#u1" },
    });
    expect(oldResults.documents).toHaveLength(0);

    // New dynamic name should match
    const newResults = await engine.query("resource", {
      index: "globex#user",
      filter: { value: "globex#u1" },
    });
    expect(newResults.documents).toHaveLength(1);
  });

  test("static index query still works alongside dynamic indexes", async () => {
    const store = createStore(engine, [buildTenantResource()]);

    await store.resource.create("r1", {
      id: "r1",
      tenant: "acme",
      userId: "u1",
      data: "hello",
    });

    // Query by the static "primary" index
    const results = await store.resource.query({
      index: "primary",
      filter: { value: "r1" },
    });
    expect(results.documents).toHaveLength(1);
    expect(results.documents[0]!.id).toBe("r1");
  });

  test("batchSet stores dynamic index names correctly", async () => {
    const store = createStore(engine, [buildTenantResource()]);

    await store.resource.batchSet([
      {
        key: "r1",
        data: { id: "r1", tenant: "acme", userId: "u1", data: "a" },
      },
      {
        key: "r2",
        data: { id: "r2", tenant: "globex", userId: "u2", data: "b" },
      },
    ]);

    const acmeResults = await engine.query("resource", {
      index: "acme#user",
      filter: { value: "acme#u1" },
    });
    expect(acmeResults.documents).toHaveLength(1);

    const globexResults = await engine.query("resource", {
      index: "globex#user",
      filter: { value: "globex#u2" },
    });
    expect(globexResults.documents).toHaveLength(1);
  });

  test("migrateAll recomputes dynamic index names", async () => {
    // Seed a document without the dynamic index applied
    await engine.put(
      "resource",
      "r1",
      {
        __v: 1,
        id: "r1",
        tenant: "acme",
        userId: "u1",
        data: "hello",
      },
      { primary: "r1" },
    );

    const store = createStore(engine, [buildTenantResource()]);
    const result = await store.resource.migrateAll();

    expect(result.status).toBe("completed");

    // After migration, the dynamic index should be queryable
    const results = await engine.query("resource", {
      index: "acme#user",
      filter: { value: "acme#u1" },
    });
    expect(results.documents).toHaveLength(1);

    // __indexes should track the static keys
    const raw = (await engine.get("resource", "r1")) as Record<string, unknown>;
    expect(raw.__indexes).toEqual(["primary", "tenantUser_v1"]);
  });

  test("unknown index name throws when no dynamic indexes exist", async () => {
    const staticOnly = model("item")
      .schema(1, z.object({ id: z.string() }))
      .index({ name: "primary", value: "id" })
      .build();

    const store = createStore(engine, [staticOnly]);
    await store.item.create("i1", { id: "i1" });

    expect(
      store.item.query({ index: "nonexistent" as any, filter: { value: "x" } }),
    ).rejects.toThrow('No index named "nonexistent"');
  });

  test("findByKey lazy-migrates and recomputes dynamic index on writeback", async () => {
    // Seed a v1 doc without the dynamic index in the engine
    await engine.put(
      "resource",
      "r1",
      { __v: 1, id: "r1", tenant: "acme", userId: "u1", data: "hello" },
      { primary: "r1" },
    );

    // Build a v2 model that adds a field and has the dynamic index
    const resourceV2 = model("resource")
      .schema(
        1,
        z.object({
          id: z.string(),
          tenant: z.string(),
          userId: z.string(),
          data: z.string(),
        }),
      )
      .schema(
        2,
        z.object({
          id: z.string(),
          tenant: z.string(),
          userId: z.string(),
          data: z.string(),
          active: z.boolean(),
        }),
        { migrate: (old) => ({ ...old, active: true }) },
      )
      .index({ name: "primary", value: "id" })
      .index("tenantUser_v1", {
        name: (d) => `${d.tenant}#user`,
        value: (d) => `${d.tenant}#${d.userId}`,
      })
      .build();

    const store = createStore(engine, [resourceV2]);
    const result = await store.resource.findByKey("r1");

    expect(result).toEqual({
      id: "r1",
      tenant: "acme",
      userId: "u1",
      data: "hello",
      active: true,
    });

    // Lazy writeback should have stored the dynamic index
    const engineResults = await engine.query("resource", {
      index: "acme#user",
      filter: { value: "acme#u1" },
    });
    expect(engineResults.documents).toHaveLength(1);

    // __indexes should track static keys
    const raw = (await engine.get("resource", "r1")) as Record<string, unknown>;
    expect(raw.__indexes).toEqual(["primary", "tenantUser_v1"]);
    expect(raw.__v).toBe(2);
  });

  test("batchGet with dynamic indexes returns correct data", async () => {
    const store = createStore(engine, [buildTenantResource()]);

    // Create documents through the store so indexes are properly set
    await store.resource.create("r1", {
      id: "r1",
      tenant: "acme",
      userId: "u1",
      data: "a",
    });
    await store.resource.create("r2", {
      id: "r2",
      tenant: "globex",
      userId: "u2",
      data: "b",
    });

    const results = await store.resource.batchGet(["r1", "r2"]);

    expect(results).toHaveLength(2);
    expect(results[0]!.tenant).toBe("acme");
    expect(results[1]!.tenant).toBe("globex");
  });

  test("batchGet lazy-migrates and writes back dynamic indexes", async () => {
    // Seed v1 docs that will need migration to v2
    await engine.put(
      "resource",
      "r1",
      { __v: 1, id: "r1", tenant: "acme", userId: "u1", data: "a" },
      { primary: "r1" },
    );

    // Build a v2 model with dynamic index
    const resourceV2 = model("resource")
      .schema(
        1,
        z.object({
          id: z.string(),
          tenant: z.string(),
          userId: z.string(),
          data: z.string(),
        }),
      )
      .schema(
        2,
        z.object({
          id: z.string(),
          tenant: z.string(),
          userId: z.string(),
          data: z.string(),
          active: z.boolean(),
        }),
        { migrate: (old) => ({ ...old, active: true }) },
      )
      .index({ name: "primary", value: "id" })
      .index("tenantUser_v1", {
        name: (d) => `${d.tenant}#user`,
        value: (d) => `${d.tenant}#${d.userId}`,
      })
      .build();

    const store = createStore(engine, [resourceV2]);
    const results = await store.resource.batchGet(["r1"]);

    expect(results).toHaveLength(1);
    expect(results[0]!.active).toBe(true);

    // Lazy writeback should have stored the dynamic index
    const acme = await engine.query("resource", {
      index: "acme#user",
      filter: { value: "acme#u1" },
    });
    expect(acme.documents).toHaveLength(1);
  });

  test("query with filter operators on dynamic index", async () => {
    const store = createStore(engine, [buildTenantResource()]);

    await store.resource.create("r1", {
      id: "r1",
      tenant: "acme",
      userId: "u1",
      data: "a",
    });
    await store.resource.create("r2", {
      id: "r2",
      tenant: "acme",
      userId: "u2",
      data: "b",
    });
    await store.resource.create("r3", {
      id: "r3",
      tenant: "acme",
      userId: "u3",
      data: "c",
    });

    // $begins on the dynamic index value
    const results = await store.resource.query({
      index: "acme#user",
      filter: { value: { $begins: "acme#u" } },
    });
    expect(results.documents).toHaveLength(3);
  });

  test("query with pagination on dynamic index", async () => {
    const store = createStore(engine, [buildTenantResource()]);

    for (let i = 0; i < 5; i++) {
      await store.resource.create(`r${i}`, {
        id: `r${i}`,
        tenant: "acme",
        userId: `u${i}`,
        data: `data${i}`,
      });
    }

    // First page
    const page1 = await store.resource.query({
      index: "acme#user",
      filter: { value: { $begins: "acme#" } },
      limit: 3,
    });
    expect(page1.documents).toHaveLength(3);
    expect(page1.cursor).not.toBeNull();

    // Second page
    const page2 = await store.resource.query({
      index: "acme#user",
      filter: { value: { $begins: "acme#" } },
      limit: 3,
      cursor: page1.cursor!,
    });
    expect(page2.documents).toHaveLength(2);
    expect(page2.cursor).toBeNull();
  });

  test("query returns empty for non-matching resolved dynamic name", async () => {
    const store = createStore(engine, [buildTenantResource()]);

    await store.resource.create("r1", {
      id: "r1",
      tenant: "acme",
      userId: "u1",
      data: "hello",
    });

    // Query a dynamic name that no document produces
    const results = await store.resource.query({
      index: "unknown#user",
      filter: { value: "unknown#u1" },
    });
    expect(results.documents).toHaveLength(0);
  });

  test("dynamic-only model (no static indexes)", async () => {
    const dynamicOnly = model("resource")
      .schema(
        1,
        z.object({
          id: z.string(),
          tenant: z.string(),
          type: z.string(),
        }),
      )
      .index("tenantType_v1", {
        name: (d) => `${d.tenant}#${d.type}`,
        value: (d) => `${d.tenant}#${d.type}#${d.id}`,
      })
      .build();

    const store = createStore(engine, [dynamicOnly]);

    await store.resource.create("r1", {
      id: "r1",
      tenant: "acme",
      type: "user",
    });
    await store.resource.create("r2", {
      id: "r2",
      tenant: "acme",
      type: "order",
    });

    const userResults = await store.resource.query({
      index: "acme#user",
      filter: { value: { $begins: "acme#user" } },
    });
    expect(userResults.documents).toHaveLength(1);
    expect(userResults.documents[0]!.id).toBe("r1");

    const orderResults = await store.resource.query({
      index: "acme#order",
      filter: { value: { $begins: "acme#order" } },
    });
    expect(orderResults.documents).toHaveLength(1);
    expect(orderResults.documents[0]!.id).toBe("r2");

    // __indexes tracks static keys
    const raw = (await engine.get("resource", "r1")) as Record<string, unknown>;
    expect(raw.__indexes).toEqual(["tenantType_v1"]);
  });

  test("multiple dynamic indexes on same model", async () => {
    const multiDynamic = model("resource")
      .schema(
        1,
        z.object({
          id: z.string(),
          tenant: z.string(),
          region: z.string(),
          type: z.string(),
        }),
      )
      .index("tenantType_v1", {
        name: (d) => `${d.tenant}#${d.type}`,
        value: (d) => `${d.tenant}#${d.type}#${d.id}`,
      })
      .index("regionType_v1", {
        name: (d) => `${d.region}#${d.type}`,
        value: (d) => `${d.region}#${d.type}#${d.id}`,
      })
      .build();

    const store = createStore(engine, [multiDynamic]);

    await store.resource.create("r1", {
      id: "r1",
      tenant: "acme",
      region: "us-east",
      type: "user",
    });

    // Query by tenant dynamic index
    const tenantResults = await store.resource.query({
      index: "acme#user",
      filter: { value: { $begins: "acme#user" } },
    });
    expect(tenantResults.documents).toHaveLength(1);

    // Query by region dynamic index
    const regionResults = await store.resource.query({
      index: "us-east#user",
      filter: { value: { $begins: "us-east#user" } },
    });
    expect(regionResults.documents).toHaveLength(1);

    // __indexes tracks both static keys
    const raw = (await engine.get("resource", "r1")) as Record<string, unknown>;
    expect(raw.__indexes).toEqual(["regionType_v1", "tenantType_v1"]);
  });

  test("where query is not supported for dynamic-name indexes", async () => {
    const store = createStore(engine, [buildTenantResource()]);

    await store.resource.create("r1", {
      id: "r1",
      tenant: "acme",
      userId: "u1",
      data: "hello",
    });

    // `where` resolves field → index via string-based value indexes.
    // "tenant" is not a string-value index, so it should fail.
    expect(store.resource.query({ where: { tenant: "acme" } })).rejects.toThrow(
      'No index found for field "tenant"',
    );
  });

  test("readonly mode does not writeback dynamic indexes", async () => {
    const readonlyResource = model("resource", { migration: "readonly" })
      .schema(
        1,
        z.object({
          id: z.string(),
          tenant: z.string(),
          userId: z.string(),
          data: z.string(),
        }),
      )
      .index({ name: "primary", value: "id" })
      .index("tenantUser_v1", {
        name: (d) => `${d.tenant}#user`,
        value: (d) => `${d.tenant}#${d.userId}`,
      })
      .build();

    // Seed a doc as v1 without dynamic index in engine
    await engine.put(
      "resource",
      "r1",
      { __v: 1, id: "r1", tenant: "acme", userId: "u1", data: "hello" },
      { primary: "r1" },
    );

    const store = createStore(engine, [readonlyResource]);
    const result = await store.resource.findByKey("r1");

    // Data is returned correctly
    expect(result).toEqual({
      id: "r1",
      tenant: "acme",
      userId: "u1",
      data: "hello",
    });

    // Engine should NOT have been updated (readonly mode)
    const raw = (await engine.get("resource", "r1")) as Record<string, unknown>;
    expect(raw.__v).toBe(1);
    // Dynamic index should NOT be stored
    const engineResults = await engine.query("resource", {
      index: "acme#user",
      filter: { value: "acme#u1" },
    });
    expect(engineResults.documents).toHaveLength(0);
  });

  test("scan query returns all docs regardless of dynamic indexes", async () => {
    const store = createStore(engine, [buildTenantResource()]);

    await store.resource.create("r1", {
      id: "r1",
      tenant: "acme",
      userId: "u1",
      data: "a",
    });
    await store.resource.create("r2", {
      id: "r2",
      tenant: "globex",
      userId: "u2",
      data: "b",
    });

    // Scan (no index/filter) returns all docs
    const results = await store.resource.query({});
    expect(results.documents).toHaveLength(2);
  });

  test("migrateAll with multiple dynamic indexes", async () => {
    const multiDynamic = model("resource")
      .schema(
        1,
        z.object({
          id: z.string(),
          tenant: z.string(),
          region: z.string(),
          type: z.string(),
        }),
      )
      .index("tenantType_v1", {
        name: (d) => `${d.tenant}#${d.type}`,
        value: (d) => `${d.tenant}#${d.type}#${d.id}`,
      })
      .index("regionType_v1", {
        name: (d) => `${d.region}#${d.type}`,
        value: (d) => `${d.region}#${d.type}#${d.id}`,
      })
      .build();

    // Seed docs without any indexes applied
    await engine.put(
      "resource",
      "r1",
      { __v: 1, id: "r1", tenant: "acme", region: "us-east", type: "user" },
      {},
    );
    await engine.put(
      "resource",
      "r2",
      { __v: 1, id: "r2", tenant: "globex", region: "eu-west", type: "order" },
      {},
    );

    const store = createStore(engine, [multiDynamic]);
    const result = await store.resource.migrateAll();

    expect(result.status).toBe("completed");

    // Both dynamic indexes should be queryable after migration
    const tenantResult = await engine.query("resource", {
      index: "acme#user",
      filter: { value: { $begins: "acme#user" } },
    });
    expect(tenantResult.documents).toHaveLength(1);

    const regionResult = await engine.query("resource", {
      index: "eu-west#order",
      filter: { value: { $begins: "eu-west#order" } },
    });
    expect(regionResult.documents).toHaveLength(1);

    // __indexes should track both static keys
    const raw = (await engine.get("resource", "r1")) as Record<string, unknown>;
    expect(raw.__indexes).toEqual(["regionType_v1", "tenantType_v1"]);
  });

  test("where query works for static field indexes alongside dynamic indexes", async () => {
    const store = createStore(engine, [buildTenantResource()]);

    await store.resource.create("r1", {
      id: "r1",
      tenant: "acme",
      userId: "u1",
      data: "a",
    });
    await store.resource.create("r2", {
      id: "r2",
      tenant: "globex",
      userId: "u2",
      data: "b",
    });

    // `where` on the static "id" field should still work
    const results = await store.resource.query({ where: { id: "r1" } });
    expect(results.documents).toHaveLength(1);
    expect(results.documents[0]!.id).toBe("r1");
  });

  test("delete removes doc including dynamic index entries", async () => {
    const store = createStore(engine, [buildTenantResource()]);

    await store.resource.create("r1", {
      id: "r1",
      tenant: "acme",
      userId: "u1",
      data: "hello",
    });

    // Verify it's queryable
    const before = await engine.query("resource", {
      index: "acme#user",
      filter: { value: "acme#u1" },
    });
    expect(before.documents).toHaveLength(1);

    await store.resource.delete("r1");

    // After delete, the document and its indexes should be gone
    const after = await engine.query("resource", {
      index: "acme#user",
      filter: { value: "acme#u1" },
    });
    expect(after.documents).toHaveLength(0);
  });

  test("batchDelete removes docs with dynamic indexes", async () => {
    const store = createStore(engine, [buildTenantResource()]);

    await store.resource.create("r1", {
      id: "r1",
      tenant: "acme",
      userId: "u1",
      data: "a",
    });
    await store.resource.create("r2", {
      id: "r2",
      tenant: "acme",
      userId: "u2",
      data: "b",
    });

    await store.resource.batchDelete(["r1", "r2"]);

    const results = await engine.query("resource", {
      index: "acme#user",
      filter: { value: { $begins: "acme#" } },
    });
    expect(results.documents).toHaveLength(0);
  });
});
