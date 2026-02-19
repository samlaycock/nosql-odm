import { describe, expect, test } from "bun:test";
import * as z from "zod";

import { model, ModelDefinition, ValidationError, VersionError } from "../../src/model";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function buildSimpleModel() {
  return model("user")
    .schema(
      1,
      z.object({
        id: z.string(),
        name: z.string(),
        email: z.email(),
      }),
    )
    .build();
}

function buildVersionedModel() {
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
    .build();
}

function buildThreeVersionModel() {
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
          return {
            ...old,
            role: "member" as const,
          };
        },
      },
    )
    .build();
}

// ---------------------------------------------------------------------------
// model() factory & builder
// ---------------------------------------------------------------------------

describe("model() factory", () => {
  test("creates a model with a single schema version", () => {
    const m = buildSimpleModel();

    expect(m).toBeInstanceOf(ModelDefinition);
    expect(m.name).toBe("user");
    expect(m.latestVersion).toBe(1);
  });

  test("creates a model with multiple schema versions", () => {
    const m = buildVersionedModel();

    expect(m.name).toBe("user");
    expect(m.latestVersion).toBe(2);
  });

  test("creates a model with three schema versions", () => {
    const m = buildThreeVersionModel();

    expect(m.latestVersion).toBe(3);
  });

  test("applies default options", () => {
    const m = buildSimpleModel();

    expect(m.options.migration).toBe("lazy");
    expect(m.options.versionField).toBe("__v");
  });

  test("allows overriding migration strategy", () => {
    const m = model("post", { migration: "readonly" })
      .schema(1, z.object({ id: z.string() }))
      .build();

    expect(m.options.migration).toBe("readonly");
  });

  test("allows overriding version field", () => {
    const m = model("post", { versionField: "_version" })
      .schema(1, z.object({ id: z.string() }))
      .build();

    expect(m.options.versionField).toBe("_version");
  });

  test("allows overriding both options", () => {
    const m = model("post", { migration: "eager", versionField: "v" })
      .schema(1, z.object({ id: z.string() }))
      .build();

    expect(m.options.migration).toBe("eager");
    expect(m.options.versionField).toBe("v");
  });
});

// ---------------------------------------------------------------------------
// Builder validation
// ---------------------------------------------------------------------------

describe("builder validation", () => {
  test("throws if building a model with no schemas", () => {
    // We have to bypass the type system since InitialModelBuilder doesn't expose build()
    const builder = model("user") as unknown as { build(): unknown };

    // InitialModelBuilderImpl doesn't have a build method, so this should throw
    expect(() => builder.build()).toThrow();
  });

  test("throws if versioned schema is missing migrate function", () => {
    expect(() => {
      model("user")
        .schema(1, z.object({ id: z.string() }))
        .schema(2, z.object({ id: z.string(), name: z.string() }), {} as any);
    }).toThrow("must have a migrate function");
  });

  test("throws if first schema version is not 1", () => {
    expect(() => {
      model("user")
        .schema(2 as unknown as 1, z.object({ id: z.string() }))
        .build();
    }).toThrow("First schema version must be 1");
  });

  test("throws if schema versions are not sequential", () => {
    expect(() => {
      model("user")
        .schema(1, z.object({ id: z.string() }))
        .schema(3, z.object({ id: z.string(), name: z.string() }), {
          migrate: (old) => ({ ...old, name: "" }),
        });
    }).toThrow("Schema version must be 2, got 3");
  });
});

// ---------------------------------------------------------------------------
// Indexes
// ---------------------------------------------------------------------------

describe("indexes", () => {
  test("registers static field indexes", () => {
    const m = model("user")
      .schema(
        1,
        z.object({
          id: z.string(),
          email: z.string(),
        }),
      )
      .index({ name: "primary", value: "id" })
      .index({ name: "byEmail", value: "email", unique: true })
      .build();

    expect(m.indexes).toHaveLength(2);
    expect(m.indexes[0]?.name).toBe("primary");
    expect(m.indexes[0]?.value).toBe("id");
    expect(m.indexes[1]?.name).toBe("byEmail");
    expect(m.indexes[1]?.unique).toBe(true);
  });

  test("registers dynamic function indexes", () => {
    const m = model("user")
      .schema(
        1,
        z.object({
          id: z.string(),
          tenantId: z.string(),
          name: z.string(),
        }),
      )
      .index({
        name: "byTenant",
        value: (data) => `TENANT#${data.tenantId}#USER#${data.name}`,
      })
      .build();

    expect(m.indexes).toHaveLength(1);
    expect(typeof m.indexes[0]?.value).toBe("function");
  });

  test("resolves static index keys from data", () => {
    const m = model("user")
      .schema(
        1,
        z.object({
          id: z.string(),
          email: z.string(),
        }),
      )
      .index({ name: "primary", value: "id" })
      .index({ name: "byEmail", value: "email" })
      .build();

    const keys = m.resolveIndexKeys({ id: "abc123", email: "sam@example.com" });

    expect(keys).toEqual({
      primary: "abc123",
      byEmail: "sam@example.com",
    });
  });

  test("resolves dynamic index keys from data", () => {
    const m = model("order")
      .schema(
        1,
        z.object({
          id: z.string(),
          tenantId: z.string(),
          status: z.string(),
          createdAt: z.string(),
        }),
      )
      .index({
        name: "byTenantStatus",
        value: (data) => `TENANT#${data.tenantId}#${data.status}#${data.createdAt}`,
      })
      .build();

    const keys = m.resolveIndexKeys({
      id: "order-1",
      tenantId: "acme",
      status: "active",
      createdAt: "2025-01-15",
    });

    expect(keys).toEqual({
      byTenantStatus: "TENANT#acme#active#2025-01-15",
    });
  });

  test("resolves mixed static and dynamic indexes", () => {
    const m = model("user")
      .schema(
        1,
        z.object({
          id: z.string(),
          email: z.string(),
          tenantId: z.string(),
        }),
      )
      .index({ name: "primary", value: "id" })
      .index({ name: "byTenant", value: (data) => `TENANT#${data.tenantId}` })
      .build();

    const keys = m.resolveIndexKeys({
      id: "abc",
      email: "a@b.com",
      tenantId: "t1",
    });

    expect(keys).toEqual({
      primary: "abc",
      byTenant: "TENANT#t1",
    });
  });

  test("model with no indexes returns empty resolved keys", () => {
    const m = buildSimpleModel();
    const keys = m.resolveIndexKeys({
      id: "abc",
      name: "Sam",
      email: "s@e.com",
    });

    expect(keys).toEqual({});
  });

  test("indexes are reset when adding a new schema version", () => {
    const m = model("user")
      .schema(1, z.object({ id: z.string(), name: z.string() }))
      .index({ name: "byName", value: "name" })
      .schema(2, z.object({ id: z.string(), name: z.string(), email: z.string() }), {
        migrate: (old) => ({ ...old, email: "" }),
      })
      // indexes from v1 are cleared, need to re-add for v2
      .index({ name: "byEmail", value: "email" })
      .build();

    expect(m.indexes).toHaveLength(1);
    expect(m.indexes[0]?.name).toBe("byEmail");
  });
});

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

describe("validate()", () => {
  test("validates data against the latest schema", async () => {
    const m = buildSimpleModel();
    const data = { id: "abc", name: "Sam", email: "sam@example.com" };

    const result = await m.validate(data);

    expect(result).toEqual(data);
  });

  test("throws ValidationError for invalid data", async () => {
    const m = buildSimpleModel();

    expect(m.validate({ id: "abc", name: "Sam", email: "not-an-email" })).rejects.toThrow(
      ValidationError,
    );
  });

  test("throws ValidationError for missing required fields", async () => {
    const m = buildSimpleModel();

    expect(m.validate({ id: "abc" })).rejects.toThrow(ValidationError);
  });

  test("throws ValidationError for wrong types", async () => {
    const m = buildSimpleModel();

    expect(m.validate({ id: 123, name: "Sam", email: "sam@example.com" })).rejects.toThrow(
      ValidationError,
    );
  });

  test("throws ValidationError for empty object", async () => {
    const m = buildSimpleModel();

    expect(m.validate({})).rejects.toThrow(ValidationError);
  });

  test("validates against latest version schema, not earlier versions", async () => {
    const m = buildVersionedModel();

    // v2 shape requires firstName/lastName, not name
    expect(
      m.validate({
        id: "abc",
        firstName: "Sam",
        lastName: "Laycock",
        email: "sam@example.com",
      }),
    ).resolves.toEqual({
      id: "abc",
      firstName: "Sam",
      lastName: "Laycock",
      email: "sam@example.com",
    });

    // v1 shape should fail against v2 schema
    expect(
      m.validate({ id: "abc", name: "Sam Laycock", email: "sam@example.com" }),
    ).rejects.toThrow(ValidationError);
  });

  test("ValidationError includes issues", async () => {
    const m = buildSimpleModel();

    try {
      await m.validate({ id: "abc" });
      expect.unreachable("should have thrown");
    } catch (err) {
      expect(err).toBeInstanceOf(ValidationError);
      expect((err as ValidationError).issues.length).toBeGreaterThan(0);
    }
  });
});

// ---------------------------------------------------------------------------
// Migration
// ---------------------------------------------------------------------------

describe("migrate()", () => {
  test("returns data as-is when already at latest version", async () => {
    const m = buildSimpleModel();
    const doc = { __v: 1, id: "abc", name: "Sam", email: "sam@example.com" };

    const result = await m.migrate(doc);

    expect(result).toEqual({
      id: "abc",
      name: "Sam",
      email: "sam@example.com",
    });
  });

  test("migrates from v1 to v2", async () => {
    const m = buildVersionedModel();
    const doc = {
      __v: 1,
      id: "abc",
      name: "Sam Laycock",
      email: "sam@example.com",
    };

    const result = await m.migrate(doc);

    expect(result).toEqual({
      id: "abc",
      firstName: "Sam",
      lastName: "Laycock",
      email: "sam@example.com",
    });
  });

  test("migrates from v1 to v3 through entire chain", async () => {
    const m = buildThreeVersionModel();
    const doc = {
      __v: 1,
      id: "abc",
      name: "Sam Laycock",
      email: "sam@example.com",
    };

    const result = await m.migrate(doc);

    expect(result).toEqual({
      id: "abc",
      firstName: "Sam",
      lastName: "Laycock",
      email: "sam@example.com",
      role: "member",
    });
  });

  test("migrates from v2 to v3", async () => {
    const m = buildThreeVersionModel();
    const doc = {
      __v: 2,
      id: "abc",
      firstName: "Sam",
      lastName: "Laycock",
      email: "sam@example.com",
    };

    const result = await m.migrate(doc);

    expect(result).toEqual({
      id: "abc",
      firstName: "Sam",
      lastName: "Laycock",
      email: "sam@example.com",
      role: "member",
    });
  });

  test("assumes version 1 when __v is missing", async () => {
    const m = buildVersionedModel();
    const doc = { id: "abc", name: "Sam Laycock", email: "sam@example.com" };

    const result = await m.migrate(doc as Record<string, unknown>);

    expect(result).toEqual({
      id: "abc",
      firstName: "Sam",
      lastName: "Laycock",
      email: "sam@example.com",
    });
  });

  test("uses custom version field", async () => {
    const m = model("user", { versionField: "_v" })
      .schema(1, z.object({ id: z.string(), name: z.string() }))
      .schema(2, z.object({ id: z.string(), name: z.string(), active: z.boolean() }), {
        migrate: (old) => ({ ...old, active: true }),
      })
      .build();

    const doc = { _v: 1, id: "abc", name: "Sam" };
    const result = await m.migrate(doc);

    expect(result).toEqual({ id: "abc", name: "Sam", active: true });
  });

  test("throws VersionError when doc version exceeds latest", async () => {
    const m = buildSimpleModel();
    const doc = { __v: 5, id: "abc", name: "Sam", email: "sam@example.com" };

    expect(m.migrate(doc)).rejects.toThrow(VersionError);
  });

  test("validates migrated data against latest schema", async () => {
    // Build a model where the migration produces invalid data
    const m = model("broken")
      .schema(1, z.object({ id: z.string(), value: z.string() }))
      .schema(
        2,
        z.object({
          id: z.string(),
          value: z.string(),
          count: z.number().min(0),
        }),
        { migrate: (old) => ({ ...old, count: -1 }) }, // invalid: count must be >= 0
      )
      .build();

    const doc = { __v: 1, id: "abc", value: "test" };

    expect(m.migrate(doc)).rejects.toThrow(ValidationError);
  });

  test("handles single-word name in v1 to v2 migration", async () => {
    const m = buildVersionedModel();
    const doc = { __v: 1, id: "abc", name: "Cher", email: "cher@example.com" };

    const result = await m.migrate(doc);

    expect(result).toEqual({
      id: "abc",
      firstName: "Cher",
      lastName: "",
      email: "cher@example.com",
    });
  });

  test("handles multi-word name in v1 to v2 migration", async () => {
    const m = buildVersionedModel();
    const doc = {
      __v: 1,
      id: "abc",
      name: "Jean Claude Van Damme",
      email: "jcvd@example.com",
    };

    const result = await m.migrate(doc);

    expect(result).toEqual({
      id: "abc",
      firstName: "Jean",
      lastName: "Claude Van Damme",
      email: "jcvd@example.com",
    });
  });

  test("does not mutate the original document", async () => {
    const m = buildVersionedModel();
    const doc = {
      __v: 1,
      id: "abc",
      name: "Sam Laycock",
      email: "sam@example.com",
    };
    const original = { ...doc };

    await m.migrate(doc);

    expect(doc).toEqual(original);
  });
});

// ---------------------------------------------------------------------------
// Builder immutability
// ---------------------------------------------------------------------------

describe("builder immutability", () => {
  test("schema() returns a new builder instance", () => {
    const b1 = model("user").schema(1, z.object({ id: z.string() }));

    const b2 = b1.index({ name: "primary", value: "id" });

    // b1 and b2 should be different builders
    const m1 = b1.build();
    const m2 = b2.build();

    expect(m1.indexes).toHaveLength(0);
    expect(m2.indexes).toHaveLength(1);
  });

  test("index() returns a new builder instance", () => {
    const b1 = model("user").schema(1, z.object({ id: z.string(), email: z.string() }));

    const b2 = b1.index({ name: "primary", value: "id" });
    const b3 = b2.index({ name: "byEmail", value: "email" });

    expect(b1.build().indexes).toHaveLength(0);
    expect(b2.build().indexes).toHaveLength(1);
    expect(b3.build().indexes).toHaveLength(2);
  });

  test("building multiple times returns equivalent but separate instances", () => {
    const builder = model("user").schema(1, z.object({ id: z.string() }));

    const m1 = builder.build();
    const m2 = builder.build();

    expect(m1).not.toBe(m2);
    expect(m1.name).toBe(m2.name);
    expect(m1.latestVersion).toBe(m2.latestVersion);
  });
});

// ---------------------------------------------------------------------------
// Edge cases
// ---------------------------------------------------------------------------

describe("edge cases", () => {
  test("model with empty string name", () => {
    const m = model("")
      .schema(1, z.object({ id: z.string() }))
      .build();

    expect(m.name).toBe("");
  });

  test("resolveIndexKeys handles numeric field values", () => {
    const m = model("item")
      .schema(1, z.object({ id: z.string(), priority: z.number() }))
      .index({ name: "byPriority", value: "priority" })
      .build();

    const keys = m.resolveIndexKeys({ id: "abc", priority: 42 });

    expect(keys).toEqual({ byPriority: "42" });
  });

  test("resolveIndexKeys handles boolean field values", () => {
    const m = model("item")
      .schema(1, z.object({ id: z.string(), active: z.boolean() }))
      .index({ name: "byActive", value: "active" })
      .build();

    const keys = m.resolveIndexKeys({ id: "abc", active: true });

    expect(keys).toEqual({ byActive: "true" });
  });

  test("dynamic index returning empty string", () => {
    const m = model("item")
      .schema(1, z.object({ id: z.string() }))
      .index({ name: "empty", value: () => "" })
      .build();

    const keys = m.resolveIndexKeys({ id: "abc" });

    expect(keys).toEqual({ empty: "" });
  });

  test("resolves dynamic index name from data", () => {
    const m = model("resource")
      .schema(
        1,
        z.object({
          id: z.string(),
          tenant: z.string(),
          userId: z.string(),
        }),
      )
      .index("tenantUser_v1", {
        name: (data) => `${data.tenant}#user`,
        value: (data) => `${data.tenant}#${data.userId}`,
      })
      .build();

    const keys = m.resolveIndexKeys({
      id: "r1",
      tenant: "acme",
      userId: "u1",
    });

    // The resolved name "acme#user" is the key in ResolvedIndexKeys,
    // while "tenantUser_v1" is the static key in __indexes.
    expect(keys).toEqual({ "acme#user": "acme#u1" });
  });

  test("dynamic index name: different documents produce different index names", () => {
    const m = model("resource")
      .schema(
        1,
        z.object({
          id: z.string(),
          tenant: z.string(),
          type: z.string(),
        }),
      )
      .index("tenantType_v1", {
        name: (data) => `${data.tenant}#${data.type}`,
        value: (data) => `${data.tenant}#${data.type}#${data.id}`,
      })
      .build();

    const keys1 = m.resolveIndexKeys({
      id: "r1",
      tenant: "acme",
      type: "user",
    });
    const keys2 = m.resolveIndexKeys({
      id: "r2",
      tenant: "globex",
      type: "order",
    });

    expect(keys1).toEqual({ "acme#user": "acme#user#r1" });
    expect(keys2).toEqual({ "globex#order": "globex#order#r2" });
  });

  test("dynamic index name: indexNames returns static keys for __indexes", () => {
    const m = model("resource")
      .schema(
        1,
        z.object({
          id: z.string(),
          tenant: z.string(),
        }),
      )
      .index("tenantUser_v1", {
        name: (data) => `${data.tenant}#user`,
        value: (data) => `${data.tenant}#${data.id}`,
      })
      .index({ name: "primary", value: "id" })
      .build();

    // indexNames returns static keys (for __indexes tracking), not resolved names.
    expect(m.indexNames).toEqual(["primary", "tenantUser_v1"]);
  });

  test("dynamic index name mixed with static indexes", () => {
    const m = model("resource")
      .schema(
        1,
        z.object({
          id: z.string(),
          tenant: z.string(),
          email: z.string(),
        }),
      )
      .index({ name: "byEmail", value: "email" })
      .index("tenantIdx_v1", {
        name: (data) => `${data.tenant}#resource`,
        value: (data) => `${data.tenant}#${data.id}`,
      })
      .build();

    const keys = m.resolveIndexKeys({
      id: "r1",
      tenant: "acme",
      email: "a@b.com",
    });

    expect(keys).toEqual({
      byEmail: "a@b.com",
      "acme#resource": "acme#r1",
    });
  });

  test("dynamic index name: builder requires key and definition", () => {
    expect(() => {
      model("resource")
        .schema(1, z.object({ id: z.string() }))
        .index("myKey")
        .build();
    }).toThrow('index("myKey", ...) requires a definition object');
  });

  test("dynamic index name: multiple dynamic indexes on same model", () => {
    const m = model("resource")
      .schema(
        1,
        z.object({
          id: z.string(),
          tenant: z.string(),
          type: z.string(),
          region: z.string(),
        }),
      )
      .index("tenantType_v1", {
        name: (data) => `${data.tenant}#${data.type}`,
        value: (data) => `${data.tenant}#${data.type}#${data.id}`,
      })
      .index("regionType_v1", {
        name: (data) => `${data.region}#${data.type}`,
        value: (data) => `${data.region}#${data.type}#${data.id}`,
      })
      .build();

    const keys = m.resolveIndexKeys({
      id: "r1",
      tenant: "acme",
      type: "user",
      region: "us-east",
    });

    expect(keys).toEqual({
      "acme#user": "acme#user#r1",
      "us-east#user": "us-east#user#r1",
    });
    expect(m.indexNames).toEqual(["regionType_v1", "tenantType_v1"]);
  });

  test("dynamic index name returning empty string", () => {
    const m = model("item")
      .schema(1, z.object({ id: z.string() }))
      .index("empty_v1", {
        name: () => "",
        value: () => "val",
      })
      .build();

    const keys = m.resolveIndexKeys({ id: "abc" });

    expect(keys).toEqual({ "": "val" });
  });

  test("dynamic index name: unique flag is preserved", () => {
    const m = model("resource")
      .schema(
        1,
        z.object({
          id: z.string(),
          tenant: z.string(),
        }),
      )
      .index("tenantPrimary_v1", {
        name: (data) => `${data.tenant}#primary`,
        value: (data) => data.id,
        unique: true,
      })
      .build();

    expect(m.indexes[0]!.unique).toBe(true);
    expect(m.indexes[0]!.key).toBe("tenantPrimary_v1");
  });

  test("dynamic indexes are reset when adding a new schema version", () => {
    const m = model("resource")
      .schema(1, z.object({ id: z.string(), tenant: z.string() }))
      .index("tenantIdx_v1", {
        name: (data) => `${data.tenant}#resource`,
        value: (data) => `${data.tenant}#${data.id}`,
      })
      .schema(2, z.object({ id: z.string(), tenant: z.string(), region: z.string() }), {
        migrate: (old) => ({ ...old, region: "default" }),
      })
      .build();

    // Indexes should have been cleared by schema v2 (no indexes on the built model)
    expect(m.indexes).toHaveLength(0);
    expect(m.indexNames).toEqual([]);
  });

  test("model with only dynamic indexes has no static index names for querying", () => {
    const m = model("resource")
      .schema(1, z.object({ id: z.string(), tenant: z.string() }))
      .index("tenantIdx_v1", {
        name: (data) => `${data.tenant}#resource`,
        value: (data) => `${data.tenant}#${data.id}`,
      })
      .build();

    // indexNames returns the static keys (for __indexes tracking)
    expect(m.indexNames).toEqual(["tenantIdx_v1"]);

    // But the resolved keys use the dynamic name
    const keys = m.resolveIndexKeys({ id: "r1", tenant: "acme" });
    expect(Object.keys(keys)).toEqual(["acme#resource"]);
  });

  test("latestShape returns the most recent schema shape", () => {
    const m = buildVersionedModel();

    // Should be the v2 shape, not v1
    expect(m.latestShape).toBeDefined();
    expect(m.latestVersion).toBe(2);
  });

  test("migrate with version 0 throws VersionError (unknown source version)", async () => {
    const m = buildVersionedModel();

    const doc = { __v: 0, id: "abc", name: "Sam", email: "sam@example.com" };

    expect(m.migrate(doc)).rejects.toThrow(VersionError);
  });

  test("migrate with negative version throws VersionError", async () => {
    const m = buildVersionedModel();
    const doc = { __v: -1, id: "abc", name: "Sam", email: "sam@example.com" };

    expect(m.migrate(doc)).rejects.toThrow(VersionError);
  });

  test("migrate with fractional version triggers migration (truncated by comparison)", async () => {
    const m = buildVersionedModel();
    // 1.5 < 2 (latest), so migration runs from v1.5+1=v2.5 — but versions[1] is v2
    // The loop does v = docVersion + 1 = 2.5, and versions[2.5 - 1] = versions[1.5]
    // which is undefined, so it throws MigrationError
    const doc = { __v: 1.5, id: "abc", name: "Sam", email: "sam@example.com" };

    expect(m.migrate(doc)).rejects.toThrow();
  });

  test('migrate accepts string version "v1" with default parser', async () => {
    const m = buildVersionedModel();
    const doc = {
      __v: "v1",
      id: "abc",
      name: "Sam Laycock",
      email: "sam@example.com",
    };

    const result = await m.migrate(doc as Record<string, unknown>);

    expect(result).toEqual({
      id: "abc",
      firstName: "Sam",
      lastName: "Laycock",
      email: "sam@example.com",
    });
  });

  test("migrate supports custom parser/comparator", async () => {
    const m = model("user", {
      parseVersion(raw) {
        if (typeof raw === "number" || typeof raw === "string") {
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
      .build();

    const result = await m.migrate({
      __v: "release-1",
      id: "abc",
      name: "Sam",
    });

    expect(result).toEqual({
      id: "abc",
      name: "Sam",
      active: true,
    });
  });

  test("migration function that throws is propagated", async () => {
    const m = model("user")
      .schema(1, z.object({ id: z.string() }))
      .schema(2, z.object({ id: z.string(), name: z.string() }), {
        migrate: () => {
          throw new TypeError("custom migration error");
        },
      })
      .build();

    const doc = { __v: 1, id: "abc" };

    expect(m.migrate(doc)).rejects.toThrow("custom migration error");
  });

  test("v1→v2 succeeds but v2→v3 migration throws mid-chain", async () => {
    const m = model("user")
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
            throw new Error("v3 broke");
          },
        },
      )
      .build();

    const doc = { __v: 1, id: "abc", name: "Sam Laycock" };

    expect(m.migrate(doc)).rejects.toThrow("v3 broke");
  });

  test("migrate preserves extra fields from migration functions", async () => {
    const m = model("item")
      .schema(1, z.object({ id: z.string() }))
      .schema(
        2,
        z.object({
          id: z.string(),
          createdAt: z.string(),
          updatedAt: z.string(),
        }),
        {
          migrate: (old) => ({
            ...old,
            createdAt: "2025-01-01",
            updatedAt: "2025-01-01",
          }),
        },
      )
      .build();

    const doc = { __v: 1, id: "abc" };
    const result = await m.migrate(doc);

    expect(result).toEqual({
      id: "abc",
      createdAt: "2025-01-01",
      updatedAt: "2025-01-01",
    });
  });
});
