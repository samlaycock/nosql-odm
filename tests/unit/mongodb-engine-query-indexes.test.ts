import { describe, expect, test } from "bun:test";

import { mongoDbEngine } from "../../src/engines/mongodb";

type AnyRecord = Record<string, unknown>;

class FakeMongoCollection {
  readonly createIndexCalls: Array<{ keys: Record<string, 1 | -1>; options?: AnyRecord }> = [];

  async createIndex(keys: Record<string, 1 | -1>, options?: AnyRecord) {
    this.createIndexCalls.push({ keys, options });
    return "ok";
  }

  find(_filter: AnyRecord) {
    return {
      sort(_sort: AnyRecord) {
        return this;
      },
      limit(_value: number) {
        return this;
      },
      async toArray(): Promise<unknown[]> {
        return [];
      },
    };
  }

  async findOne(_filter: AnyRecord) {
    return null;
  }

  async insertOne(_document: AnyRecord) {
    return { acknowledged: true };
  }

  async updateOne(_filter: AnyRecord, _update: AnyRecord, _options?: AnyRecord) {
    return { matchedCount: 1 };
  }

  async deleteOne(_filter: AnyRecord) {
    return { acknowledged: true };
  }

  async bulkWrite(_operations: AnyRecord[], _options?: AnyRecord) {
    return { matchedCount: 0 };
  }

  async findOneAndUpdate(_filter: AnyRecord, update: AnyRecord, _options?: AnyRecord) {
    const inc = (update as { $inc?: { value?: number } }).$inc?.value ?? 0;
    return { kind: "sequence", value: inc };
  }
}

class FakeMongoDatabase {
  readonly documents = new FakeMongoCollection();
  readonly metadata = new FakeMongoCollection();

  collection(name: string) {
    if (name === "nosql_odm_documents") {
      return this.documents;
    }

    if (name === "nosql_odm_metadata") {
      return this.metadata;
    }

    throw new Error(`Unexpected collection: ${name}`);
  }
}

describe("mongodb engine query index provisioning", () => {
  test("creates no query indexes when queryIndexes is not provided", async () => {
    const db = new FakeMongoDatabase();
    const engine = mongoDbEngine({ database: db });

    await engine.get("users", "u1");

    const queryIndexCalls = db.documents.createIndexCalls.filter(
      (call) =>
        "indexes" in call.keys || Object.keys(call.keys).some((k) => k.startsWith("indexes.")),
    );

    expect(queryIndexCalls).toHaveLength(0);
  });

  test("creates no query indexes when queryIndexes is an empty array", async () => {
    const db = new FakeMongoDatabase();
    const engine = mongoDbEngine({ database: db, queryIndexes: [] });

    await engine.get("users", "u1");

    const queryIndexCalls = db.documents.createIndexCalls.filter(({ keys }) =>
      Object.keys(keys).some((k) => k.startsWith("indexes.")),
    );

    expect(queryIndexCalls).toHaveLength(0);
  });

  test("creates ascending and descending compound indexes for a single query index", async () => {
    const db = new FakeMongoDatabase();
    const engine = mongoDbEngine({ database: db, queryIndexes: ["byEmail"] });

    await engine.get("users", "u1");

    const queryIndexCalls = db.documents.createIndexCalls.filter(({ keys }) =>
      Object.keys(keys).some((k) => k.startsWith("indexes.")),
    );

    expect(queryIndexCalls).toHaveLength(2);
    expect(queryIndexCalls).toContainEqual({
      keys: { collection: 1, "indexes.byEmail": 1, createdAt: 1, key: 1 },
      options: undefined,
    });
    expect(queryIndexCalls).toContainEqual({
      keys: { collection: 1, "indexes.byEmail": -1, createdAt: 1, key: 1 },
      options: undefined,
    });
  });

  test("creates ascending and descending compound indexes for multiple query indexes", async () => {
    const db = new FakeMongoDatabase();
    const engine = mongoDbEngine({
      database: db,
      queryIndexes: ["byEmail", "byStatus", "byCreatedAt"],
    });

    await engine.get("users", "u1");

    const queryIndexCalls = db.documents.createIndexCalls.filter(({ keys }) =>
      Object.keys(keys).some((k) => k.startsWith("indexes.")),
    );

    expect(queryIndexCalls).toHaveLength(6);

    for (const name of ["byEmail", "byStatus", "byCreatedAt"]) {
      expect(queryIndexCalls).toContainEqual({
        keys: { collection: 1, [`indexes.${name}`]: 1, createdAt: 1, key: 1 },
        options: undefined,
      });
      expect(queryIndexCalls).toContainEqual({
        keys: { collection: 1, [`indexes.${name}`]: -1, createdAt: 1, key: 1 },
        options: undefined,
      });
    }
  });

  test("query index provisioning completes before the first operation resolves", async () => {
    const db = new FakeMongoDatabase();
    const engine = mongoDbEngine({ database: db, queryIndexes: ["byEmail"] });

    await engine.get("users", "u1");

    const queryIndexCalls = db.documents.createIndexCalls.filter(({ keys }) =>
      Object.keys(keys).some((k) => k.startsWith("indexes.")),
    );

    expect(queryIndexCalls).toHaveLength(2);
  });
});
