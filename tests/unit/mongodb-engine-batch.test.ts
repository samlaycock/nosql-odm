import { describe, expect, test } from "bun:test";

import { mongoDbEngine } from "../../src/engines/mongodb";

type AnyRecord = Record<string, unknown>;

interface BulkWriteCall {
  operations: AnyRecord[];
  options: Record<string, unknown> | undefined;
}

class ArrayMongoCursor {
  private sortedRecords: unknown[];
  private limitValue: number | null = null;

  constructor(records: unknown[]) {
    this.sortedRecords = records;
  }

  sort(sort: Record<string, 1 | -1>) {
    const entries = Object.entries(sort);

    this.sortedRecords = [...this.sortedRecords].sort((left, right) => {
      for (const [path, direction] of entries) {
        const leftValue = readDottedValue(left, path);
        const rightValue = readDottedValue(right, path);

        if (leftValue === rightValue) {
          continue;
        }

        if (leftValue === undefined || leftValue === null) {
          return direction === 1 ? -1 : 1;
        }

        if (rightValue === undefined || rightValue === null) {
          return direction === 1 ? 1 : -1;
        }

        return leftValue < rightValue ? -direction : direction;
      }

      return 0;
    });

    return this;
  }

  limit(value: number) {
    this.limitValue = value;
    return this;
  }

  async toArray(): Promise<unknown[]> {
    if (this.limitValue === null) {
      return [...this.sortedRecords];
    }

    return this.sortedRecords.slice(0, this.limitValue);
  }
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

function readDottedValue(value: unknown, path: string): unknown {
  const parts = path.split(".");
  let current = value;

  for (const part of parts) {
    if (!current || typeof current !== "object") {
      return undefined;
    }

    current = (current as Record<string, unknown>)[part];
  }

  return current;
}

function matchesFilter(record: Record<string, unknown>, filter: Record<string, unknown>): boolean {
  for (const [field, expected] of Object.entries(filter)) {
    if (field === "$and") {
      if (!Array.isArray(expected) || expected.some((item) => !matchesNestedFilter(record, item))) {
        return false;
      }

      continue;
    }

    if (field === "$or") {
      if (
        !Array.isArray(expected) ||
        expected.every((item) => !matchesNestedFilter(record, item))
      ) {
        return false;
      }

      continue;
    }

    const actual = readDottedValue(record, field);

    if (!matchesCondition(actual, expected)) {
      return false;
    }
  }

  return true;
}

function matchesNestedFilter(record: Record<string, unknown>, filter: unknown): boolean {
  return !!filter && typeof filter === "object" && matchesFilter(record, filter as AnyRecord);
}

function matchesCondition(actual: unknown, expected: unknown): boolean {
  if (!expected || typeof expected !== "object" || Array.isArray(expected)) {
    return actual === expected;
  }

  if ("$in" in expected) {
    const values = (expected as { $in?: unknown }).$in;
    return Array.isArray(values) && values.includes(actual);
  }

  return false;
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

class FakeMongoDocumentsCollectionWithRecords extends FakeMongoDocumentsCollection {
  readonly findCalls: Array<Record<string, unknown>> = [];

  constructor(private readonly records: Array<Record<string, unknown>>) {
    super();
  }

  override find(filter: Record<string, unknown>) {
    this.findCalls.push(filter);
    return new ArrayMongoCursor(this.records.filter((record) => matchesFilter(record, filter)));
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
  test('reports uniqueConstraints capability as "none"', () => {
    const documents = new FakeMongoDocumentsCollection();
    const metadata = new FakeMongoMetadataCollection();
    const engine = mongoDbEngine({
      database: new FakeMongoDatabase(documents, metadata),
    });

    expect(engine.capabilities?.uniqueConstraints).toBe("none");
  });

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

  test("probeUnique batches values into one MongoDB lookup and groups matching keys", async () => {
    const documents = new FakeMongoDocumentsCollectionWithRecords([
      {
        collection: "users",
        key: "u1",
        createdAt: 1,
        writeVersion: 1,
        doc: { id: "u1" },
        indexes: { primary: "u1", byEmail: "sam@example.com" },
        migrationTargetVersion: 0,
        migrationVersionState: "current",
        migrationIndexSignature: null,
      },
      {
        collection: "users",
        key: "u2",
        createdAt: 2,
        writeVersion: 1,
        doc: { id: "u2" },
        indexes: { primary: "u2", byEmail: "sam@example.com" },
        migrationTargetVersion: 0,
        migrationVersionState: "current",
        migrationIndexSignature: null,
      },
      {
        collection: "users",
        key: "u3",
        createdAt: 3,
        writeVersion: 1,
        doc: { id: "u3" },
        indexes: { primary: "u3", byEmail: "jamie@example.com" },
        migrationTargetVersion: 0,
        migrationVersionState: "current",
        migrationIndexSignature: null,
      },
      {
        collection: "teams",
        key: "t1",
        createdAt: 4,
        writeVersion: 1,
        doc: { id: "t1" },
        indexes: { primary: "t1", byEmail: "sam@example.com" },
        migrationTargetVersion: 0,
        migrationVersionState: "current",
        migrationIndexSignature: null,
      },
    ]);
    const metadata = new FakeMongoMetadataCollection();
    const engine = mongoDbEngine({
      database: new FakeMongoDatabase(documents, metadata),
    });

    const matches = await engine.probeUnique!("users", "byEmail", [
      "sam@example.com",
      "missing@example.com",
      "jamie@example.com",
      "sam@example.com",
    ]);

    expect(matches).toEqual([
      {
        value: "sam@example.com",
        keys: ["u1", "u2"],
      },
      {
        value: "jamie@example.com",
        keys: ["u3"],
      },
    ]);
    expect(documents.findCalls).toEqual([
      {
        collection: "users",
        "indexes.byEmail": {
          $in: ["sam@example.com", "missing@example.com", "jamie@example.com"],
        },
      },
    ]);
  });
});
