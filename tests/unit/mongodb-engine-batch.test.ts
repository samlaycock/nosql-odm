import { describe, expect, test } from "bun:test";

import { mongoDbEngine } from "../../src/engines/mongodb";

type AnyRecord = Record<string, unknown>;

interface BulkWriteCall {
  operations: AnyRecord[];
  options: Record<string, unknown> | undefined;
}

class EmptyMongoCursor {
  sort(_sort: Record<string, 1 | -1>) {
    return this;
  }

  limit(_value: number) {
    return this;
  }

  async toArray(): Promise<unknown[]> {
    return [];
  }
}

class FakeMongoDocumentsCollection {
  readonly createIndexCalls: Array<Record<string, 1 | -1>> = [];
  readonly updateOneCalls: Array<{
    filter: Record<string, unknown>;
    update: Record<string, unknown>;
    options: Record<string, unknown> | undefined;
  }> = [];
  readonly deleteOneCalls: Array<Record<string, unknown>> = [];
  readonly bulkWriteCalls: BulkWriteCall[] = [];
  private readonly writeVersions = new Map<string, number>();

  async createIndex(keys: Record<string, 1 | -1>, _options?: Record<string, unknown>) {
    this.createIndexCalls.push(keys);
    return "ok";
  }

  find(_filter: Record<string, unknown>) {
    return new EmptyMongoCursor();
  }

  async findOne(_filter: Record<string, unknown>) {
    return null;
  }

  async insertOne(_document: Record<string, unknown>) {
    return { acknowledged: true };
  }

  async updateOne(
    filter: Record<string, unknown>,
    update: Record<string, unknown>,
    options?: Record<string, unknown>,
  ) {
    this.updateOneCalls.push({ filter, update, options });

    const rawKey = filter.key;

    if (typeof rawKey !== "string") {
      throw new Error("expected string key in fake updateOne filter");
    }

    const key = rawKey;
    const expectedWriteVersion = filter.writeVersion;

    if (typeof expectedWriteVersion === "number") {
      const current = this.writeVersions.get(key) ?? 1;

      if (current !== expectedWriteVersion) {
        return {
          matchedCount: 0,
        };
      }

      this.writeVersions.set(key, current + 1);

      return {
        matchedCount: 1,
      };
    }

    const current = this.writeVersions.get(key) ?? 0;
    this.writeVersions.set(key, current + 1);

    return {
      matchedCount: 1,
    };
  }

  async deleteOne(filter: Record<string, unknown>) {
    this.deleteOneCalls.push(filter);
    return {
      acknowledged: true,
    };
  }

  async bulkWrite(operations: AnyRecord[], options?: Record<string, unknown>) {
    this.bulkWriteCalls.push({
      operations,
      options,
    });

    for (const operation of operations) {
      if ("updateOne" in operation) {
        const updateSpec = operation.updateOne as Record<string, unknown>;
        const filter = updateSpec.filter as Record<string, unknown>;
        const rawKey = filter.key;

        if (typeof rawKey !== "string") {
          throw new Error("expected string key in fake bulkWrite filter");
        }

        const key = rawKey;
        const current = this.writeVersions.get(key) ?? 0;
        this.writeVersions.set(key, current + 1);
      }
    }

    return {
      matchedCount: operations.length,
    };
  }

  async findOneAndUpdate(
    _filter: Record<string, unknown>,
    _update: Record<string, unknown>,
    _options?: Record<string, unknown>,
  ) {
    return null;
  }
}

class FakeMongoMetadataCollection {
  readonly createIndexCalls: Array<Record<string, 1 | -1>> = [];
  readonly findOneAndUpdateCalls: Array<{
    filter: Record<string, unknown>;
    update: Record<string, unknown>;
    options: Record<string, unknown> | undefined;
  }> = [];
  private sequenceValue = 0;

  async createIndex(keys: Record<string, 1 | -1>, _options?: Record<string, unknown>) {
    this.createIndexCalls.push(keys);
    return "ok";
  }

  find(_filter: Record<string, unknown>) {
    return new EmptyMongoCursor();
  }

  async findOne(_filter: Record<string, unknown>) {
    return null;
  }

  async insertOne(_document: Record<string, unknown>) {
    return { acknowledged: true };
  }

  async updateOne(
    _filter: Record<string, unknown>,
    _update: Record<string, unknown>,
    _options?: Record<string, unknown>,
  ) {
    return {
      matchedCount: 1,
    };
  }

  async deleteOne(_filter: Record<string, unknown>) {
    return {
      acknowledged: true,
    };
  }

  async bulkWrite(_operations: AnyRecord[], _options?: Record<string, unknown>) {
    return {
      matchedCount: 0,
    };
  }

  async findOneAndUpdate(
    filter: Record<string, unknown>,
    update: Record<string, unknown>,
    options?: Record<string, unknown>,
  ) {
    this.findOneAndUpdateCalls.push({
      filter,
      update,
      options,
    });

    const updateRecord = update as {
      $inc?: {
        value?: number;
      };
    };

    const increment = updateRecord.$inc?.value ?? 0;
    this.sequenceValue += increment;

    const rawCollection = filter.collection;

    if (typeof rawCollection !== "string") {
      throw new Error("expected string collection in fake sequence filter");
    }

    return {
      collection: rawCollection,
      kind: "sequence",
      value: this.sequenceValue,
    };
  }
}

class FakeMongoDatabase {
  constructor(
    private readonly documents: FakeMongoDocumentsCollection,
    private readonly metadata: FakeMongoMetadataCollection,
  ) {}

  collection(name: string) {
    if (name === "nosql_odm_documents") {
      return this.documents;
    }

    if (name === "nosql_odm_metadata") {
      return this.metadata;
    }

    throw new Error(`Unexpected collection ${name}`);
  }
}

class FakeMongoDocumentsCollectionWithShortBulkAck extends FakeMongoDocumentsCollection {
  override async bulkWrite(operations: AnyRecord[], options?: Record<string, unknown>) {
    await super.bulkWrite(operations, options);

    return {
      matchedCount: Math.max(0, operations.length - 1),
      upsertedCount: 0,
    };
  }
}

describe("mongodb engine batch operations", () => {
  test("batchSet uses a single bulkWrite and sequence reservation for large batches", async () => {
    const documents = new FakeMongoDocumentsCollection();
    const metadata = new FakeMongoMetadataCollection();
    const engine = mongoDbEngine({
      database: new FakeMongoDatabase(documents, metadata),
    });
    const items = Array.from({ length: 200 }, (_, index) => {
      const key = `u${index + 1}`;

      return {
        key,
        doc: { key },
        indexes: { id: key },
      };
    });

    await engine.batchSet("users", items);

    expect(documents.bulkWriteCalls).toHaveLength(1);
    expect(documents.bulkWriteCalls[0]?.operations).toHaveLength(items.length);
    expect(documents.updateOneCalls).toHaveLength(0);
    expect(metadata.findOneAndUpdateCalls).toHaveLength(1);
  });

  test("batchSetWithResult bulk-writes unconditional items and keeps conditional conflict semantics", async () => {
    const documents = new FakeMongoDocumentsCollection();
    const metadata = new FakeMongoMetadataCollection();
    const engine = mongoDbEngine({
      database: new FakeMongoDatabase(documents, metadata),
    });

    const result = await engine.batchSetWithResult!("users", [
      { key: "u1", doc: { key: "u1" }, indexes: { id: "u1" } },
      { key: "u2", doc: { key: "u2" }, indexes: { id: "u2" } },
      { key: "u3", doc: { key: "u3" }, indexes: { id: "u3" }, expectedWriteToken: "1" },
      { key: "u4", doc: { key: "u4" }, indexes: { id: "u4" }, expectedWriteToken: "9" },
    ]);

    expect(result.persistedKeys).toEqual(["u1", "u2", "u3"]);
    expect(result.conflictedKeys).toEqual(["u4"]);
    expect(documents.bulkWriteCalls).toHaveLength(1);
    expect(documents.bulkWriteCalls[0]?.operations).toHaveLength(2);
    expect(documents.updateOneCalls).toHaveLength(2);
    expect(metadata.findOneAndUpdateCalls).toHaveLength(1);
  });

  test("batchSetWithResult throws when unconditional bulkWrite acknowledges fewer writes than expected", async () => {
    const documents = new FakeMongoDocumentsCollectionWithShortBulkAck();
    const metadata = new FakeMongoMetadataCollection();
    const engine = mongoDbEngine({
      database: new FakeMongoDatabase(documents, metadata),
    });
    let error: unknown = null;

    try {
      await engine.batchSetWithResult!("users", [
        { key: "u1", doc: { key: "u1" }, indexes: { id: "u1" } },
        { key: "u2", doc: { key: "u2" }, indexes: { id: "u2" } },
      ]);
    } catch (candidate) {
      error = candidate;
    }

    expect(error).toBeInstanceOf(Error);
    expect((error as Error).message).toBe(
      "MongoDB failed to persist one or more unconditional batch set writes",
    );
  });

  test("batchDelete uses bulkWrite instead of per-key deleteOne calls for large key sets", async () => {
    const documents = new FakeMongoDocumentsCollection();
    const metadata = new FakeMongoMetadataCollection();
    const engine = mongoDbEngine({
      database: new FakeMongoDatabase(documents, metadata),
    });
    const keys = Array.from({ length: 200 }, (_, index) => `u${(index % 150) + 1}`);

    await engine.batchDelete("users", keys);

    expect(documents.bulkWriteCalls).toHaveLength(1);
    expect(documents.bulkWriteCalls[0]?.operations).toHaveLength(150);
    expect(documents.deleteOneCalls).toHaveLength(0);
  });
});
