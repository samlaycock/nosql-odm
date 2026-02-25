import { beforeEach, describe, expect, test } from "bun:test";

import { dynamoDbEngine } from "../../src/engines/dynamodb";
import { firestoreEngine } from "../../src/engines/firestore";
import { mongoDbEngine } from "../../src/engines/mongodb";

type AnyRecord = Record<string, unknown>;

function getPath(record: AnyRecord, path: string): unknown {
  let current: unknown = record;

  for (const part of path.split(".")) {
    if (typeof current !== "object" || current === null) {
      return undefined;
    }

    current = (current as AnyRecord)[part];
  }

  return current;
}

function compareUnknown(a: unknown, b: unknown): number {
  if (typeof a === "number" && typeof b === "number") {
    return a - b;
  }

  return String(a).localeCompare(String(b));
}

function matchesMongoLikeFilter(record: AnyRecord, filter: AnyRecord): boolean {
  for (const [key, value] of Object.entries(filter)) {
    if (key === "$or") {
      if (!Array.isArray(value)) {
        return false;
      }

      if (
        !value.some((candidate) => isRecord(candidate) && matchesMongoLikeFilter(record, candidate))
      ) {
        return false;
      }

      continue;
    }

    const actual = getPath(record, key);

    if (isRecord(value)) {
      for (const [op, expected] of Object.entries(value)) {
        if (op === "$gt" && !(compareUnknown(actual, expected) > 0)) {
          return false;
        }

        if (op === "$gte" && !(compareUnknown(actual, expected) >= 0)) {
          return false;
        }

        if (op === "$lt" && !(compareUnknown(actual, expected) < 0)) {
          return false;
        }

        if (op === "$lte" && !(compareUnknown(actual, expected) <= 0)) {
          return false;
        }

        if (op === "$regex") {
          if (typeof actual !== "string" || typeof expected !== "string") {
            return false;
          }

          if (!new RegExp(expected).test(actual)) {
            return false;
          }
        }
      }

      continue;
    }

    if (actual !== value) {
      return false;
    }
  }

  return true;
}

function sortMongoLike(records: AnyRecord[], sort: Record<string, 1 | -1>): AnyRecord[] {
  return [...records].sort((a, b) => {
    for (const [field, dir] of Object.entries(sort)) {
      const cmp = compareUnknown(getPath(a, field), getPath(b, field));

      if (cmp !== 0) {
        return dir === -1 ? -cmp : cmp;
      }
    }

    return 0;
  });
}

function isRecord(value: unknown): value is AnyRecord {
  return typeof value === "object" && value !== null;
}

function makeEngineDoc(
  key: string,
  createdAt: number,
  indexes: Record<string, string>,
  doc: Record<string, unknown>,
) {
  return {
    collection: "users",
    key,
    createdAt,
    writeVersion: 1,
    doc,
    indexes,
    migrationTargetVersion: 1,
    migrationVersionState: "current",
    migrationIndexSignature: "[]",
  };
}

class FakeMongoCursor {
  private sortSpec: Record<string, 1 | -1> = {};
  private maxItems: number | null = null;

  constructor(
    private readonly source: AnyRecord[],
    private readonly onSort: (sort: Record<string, 1 | -1>) => void,
    private readonly onLimit: (value: number) => void,
  ) {}

  sort(sort: Record<string, 1 | -1>) {
    this.sortSpec = sort;
    this.onSort(sort);
    return this;
  }

  limit(value: number) {
    this.maxItems = value;
    this.onLimit(value);
    return this;
  }

  async toArray(): Promise<unknown[]> {
    const sorted =
      Object.keys(this.sortSpec).length > 0
        ? sortMongoLike(this.source, this.sortSpec)
        : this.source;

    if (this.maxItems === null) {
      return sorted;
    }

    return sorted.slice(0, this.maxItems);
  }
}

class FakeMongoCollection {
  readonly createIndexCalls: Array<Record<string, 1 | -1>> = [];
  lastFindFilter: AnyRecord | null = null;
  lastSort: Record<string, 1 | -1> | null = null;
  lastLimit: number | null = null;
  findOneCalls: AnyRecord[] = [];

  constructor(private readonly docs: AnyRecord[]) {}

  async createIndex(keys: Record<string, 1 | -1>, _options?: Record<string, unknown>) {
    this.createIndexCalls.push(keys);
    return "ok";
  }

  async findOne(filter: Record<string, unknown>) {
    this.findOneCalls.push(filter);
    return this.docs.find((doc) => matchesMongoLikeFilter(doc, filter));
  }

  find(filter: Record<string, unknown>) {
    this.lastFindFilter = filter;
    const matched = this.docs.filter((doc) => matchesMongoLikeFilter(doc, filter));

    return new FakeMongoCursor(
      matched,
      (sort) => {
        this.lastSort = sort;
      },
      (value) => {
        this.lastLimit = value;
      },
    );
  }

  async insertOne(_document: Record<string, unknown>) {
    throw new Error("not implemented");
  }

  async updateOne(
    _filter: Record<string, unknown>,
    _update: Record<string, unknown>,
    _options?: Record<string, unknown>,
  ) {
    return {
      matchedCount: 0,
    };
  }

  async deleteOne(_filter: Record<string, unknown>) {
    throw new Error("not implemented");
  }

  async findOneAndUpdate(
    _filter: Record<string, unknown>,
    _update: Record<string, unknown>,
    _options?: Record<string, unknown>,
  ) {
    throw new Error("not implemented");
  }
}

class FakeMongoDatabase {
  constructor(
    private readonly documents: FakeMongoCollection,
    private readonly metadata: FakeMongoCollection,
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

interface FakeFirestoreDocSnapshot {
  exists: boolean;
  id: string;
  data(): unknown;
}

class FakeFirestoreQuery {
  constructor(
    protected readonly docs: AnyRecord[],
    protected readonly whereCalls: Array<{ field: string; op: string; value: unknown }>,
  ) {}

  where(fieldPath: string, opStr: string, value: unknown) {
    this.whereCalls.push({ field: fieldPath, op: opStr, value });

    const filtered = this.docs.filter((doc) => {
      const actual = getPath(doc, fieldPath);

      if (opStr === "==") {
        return actual === value;
      }

      if (opStr === ">") {
        return compareUnknown(actual, value) > 0;
      }

      if (opStr === ">=") {
        return compareUnknown(actual, value) >= 0;
      }

      if (opStr === "<") {
        return compareUnknown(actual, value) < 0;
      }

      if (opStr === "<=") {
        return compareUnknown(actual, value) <= 0;
      }

      throw new Error(`Unsupported op ${opStr}`);
    });

    return new FakeFirestoreQuery(filtered, this.whereCalls);
  }

  limit(_limit: number) {
    return this;
  }

  async get() {
    const snapshots: FakeFirestoreDocSnapshot[] = this.docs.map((doc) => ({
      exists: true,
      id: String(doc.key),
      data: () => doc,
    }));

    return { docs: snapshots };
  }
}

class FakeFirestoreCollection extends FakeFirestoreQuery {
  constructor(docs: AnyRecord[], whereCalls: Array<{ field: string; op: string; value: unknown }>) {
    super(docs, whereCalls);
  }

  doc(id: string = "x") {
    return {
      id,
      get: async () => ({ exists: false, id, data: () => null }),
      set: async () => undefined,
      delete: async () => undefined,
    };
  }
}

class FakeFirestoreDatabase {
  readonly documentWhereCalls: Array<{ field: string; op: string; value: unknown }> = [];
  readonly docsCollection: FakeFirestoreCollection;

  constructor(docs: AnyRecord[]) {
    this.docsCollection = new FakeFirestoreCollection(docs, this.documentWhereCalls);
  }

  collection(path: string) {
    if (path === "nosql_odm_documents") {
      return this.docsCollection;
    }

    return new FakeFirestoreCollection([], []);
  }

  async runTransaction<T>(_fn: (transaction: unknown) => Promise<T>) {
    throw new Error("not implemented");
  }
}

class FakeDynamoClient {
  readonly inputs: AnyRecord[] = [];

  constructor(private readonly responses: AnyRecord[]) {}

  async send(command: { input: unknown }) {
    this.inputs.push(command.input as AnyRecord);
    const response = this.responses.shift();

    if (!response) {
      return {};
    }

    return response;
  }
}

function makeDynamoDocItem(
  key: string,
  createdAt: number,
  indexes: Record<string, string>,
  doc: Record<string, unknown>,
) {
  return {
    pk: "COL#users",
    sk: `DOC#${encodeURIComponent(key)}`,
    itemType: "doc",
    collection: "users",
    key,
    createdAt,
    writeVersion: 1,
    doc,
    indexes,
  };
}

describe("non-SQL query pushdown", () => {
  let mongoDocs: FakeMongoCollection;
  let mongoMeta: FakeMongoCollection;

  beforeEach(() => {
    mongoDocs = new FakeMongoCollection([
      makeEngineDoc("u1", 1, { byEmail: "a@example.com" }, { id: "u1" }),
      makeEngineDoc("u2", 2, { byEmail: "b@example.com" }, { id: "u2" }),
      makeEngineDoc("u3", 3, { byEmail: "c@example.com" }, { id: "u3" }),
    ]);
    mongoMeta = new FakeMongoCollection([]);
  });

  test("mongodb query pushes filter, sort, and limit to backend", async () => {
    const engine = mongoDbEngine({
      database: new FakeMongoDatabase(mongoDocs, mongoMeta),
    });

    const result = await engine.query("users", {
      index: "byEmail",
      filter: { value: { $gte: "a@example.com" } },
      sort: "asc",
      limit: 2,
    });

    expect(mongoDocs.lastFindFilter).toMatchObject({
      collection: "users",
      "indexes.byEmail": {
        $gte: "a@example.com",
      },
    });
    expect(mongoDocs.lastSort).toEqual({
      "indexes.byEmail": 1,
      createdAt: 1,
      key: 1,
    });
    expect(mongoDocs.lastLimit).toBe(3);
    expect(result.documents.map((doc) => doc.key)).toEqual(["u1", "u2"]);
    expect(result.cursor).toBe("u2");
  });

  test("mongodb query pushes cursor pagination when cursor matches filter", async () => {
    const engine = mongoDbEngine({
      database: new FakeMongoDatabase(mongoDocs, mongoMeta),
    });

    const result = await engine.query("users", {
      index: "byEmail",
      filter: { value: { $gte: "a@example.com" } },
      sort: "asc",
      cursor: "u1",
      limit: 1,
    });

    expect(mongoDocs.findOneCalls).toContainEqual({
      collection: "users",
      key: "u1",
    });
    expect(mongoDocs.lastFindFilter && "$or" in mongoDocs.lastFindFilter).toBe(true);
    expect(result.documents.map((doc) => doc.key)).toEqual(["u2"]);
    expect(result.cursor).toBe("u2");
  });

  test("firestore query pushes supported index filters into where clauses", async () => {
    const db = new FakeFirestoreDatabase([
      makeEngineDoc("u1", 2, { byEmail: "sam@example.com" }, { id: "u1" }),
      makeEngineDoc("u2", 1, { byEmail: "alex@example.com" }, { id: "u2" }),
      makeEngineDoc("u3", 3, { byEmail: "zoe@example.com" }, { id: "u3" }),
    ]);
    const engine = firestoreEngine({
      database: db as unknown as Parameters<typeof firestoreEngine>[0]["database"],
    });

    const result = await engine.query("users", {
      index: "byEmail",
      filter: { value: { $begins: "a" } },
    });

    expect(db.documentWhereCalls).toEqual([
      { field: "collection", op: "==", value: "users" },
      { field: "indexes.byEmail", op: ">=", value: "a" },
      { field: "indexes.byEmail", op: "<=", value: "a\uf8ff" },
    ]);
    expect(result.documents.map((doc) => doc.key)).toEqual(["u2"]);
  });

  test("dynamodb query adds a filter expression for index filters", async () => {
    const client = new FakeDynamoClient([
      {
        Items: [
          makeDynamoDocItem("u1", 2, { byEmail: "sam@example.com" }, { id: "u1" }),
          makeDynamoDocItem("u2", 1, { byEmail: "alex@example.com" }, { id: "u2" }),
        ],
      },
    ]);
    const engine = dynamoDbEngine({
      client,
      tableName: "tbl",
    });

    const result = await engine.query("users", {
      index: "byEmail",
      filter: { value: { $begins: "a" } },
    });

    expect(client.inputs).toHaveLength(1);
    expect(client.inputs[0]).toMatchObject({
      FilterExpression: "begins_with(#indexes.#indexName, :f_begins)",
      ExpressionAttributeNames: {
        "#indexes": "indexes",
        "#indexName": "byEmail",
      },
      ExpressionAttributeValues: {
        ":f_begins": "a",
      },
    });
    expect(result.documents.map((doc) => doc.key)).toEqual(["u2"]);
  });
});
