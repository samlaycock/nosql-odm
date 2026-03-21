import { describe, expect, test } from "bun:test";

import { firestoreEngine } from "../../src/engines/firestore";
import { EngineUniqueConstraintError } from "../../src/engines/types";

interface FakeFirestoreRecord {
  readonly [key: string]: unknown;
}

interface FakeWhereClause {
  readonly fieldPath: string;
  readonly opStr: string;
  readonly value: unknown;
}

class FakeFirestoreDocumentReference {
  readonly id: string;

  constructor(
    private readonly store: Map<string, FakeFirestoreRecord>,
    id: string,
  ) {
    this.id = id;
  }

  readSnapshot() {
    const data = this.store.get(this.id);

    return {
      exists: data !== undefined,
      id: this.id,
      data: () => (data === undefined ? null : structuredClone(data)),
    };
  }

  applyCreate(data: Record<string, unknown>) {
    if (this.store.has(this.id)) {
      throw new Error(`Document "${this.id}" already exists`);
    }

    this.store.set(this.id, structuredClone(data));
  }

  applySet(data: Record<string, unknown>, options?: { merge?: boolean }) {
    const next = options?.merge
      ? {
          ...this.store.get(this.id),
          ...structuredClone(data),
        }
      : structuredClone(data);

    this.store.set(this.id, next);
  }

  applyDelete() {
    this.store.delete(this.id);
  }

  async get() {
    return this.readSnapshot();
  }

  async set(data: Record<string, unknown>, options?: { merge?: boolean }) {
    this.applySet(data, options);
  }

  async delete() {
    this.applyDelete();
  }
}

class FakeFirestoreQuery {
  constructor(
    protected readonly store: Map<string, FakeFirestoreRecord>,
    private readonly filters: readonly FakeWhereClause[] = [],
  ) {}

  where(fieldPath: string, opStr: string, value: unknown) {
    return new FakeFirestoreQuery(this.store, [
      ...this.filters,
      {
        fieldPath,
        opStr,
        value,
      },
    ]);
  }

  limit(_limit: number) {
    return this;
  }

  async get() {
    const docs = [...this.store.entries()]
      .filter(([, record]) => this.filters.every((filter) => matchesWhereClause(record, filter)))
      .map(([id, record]) => ({
        exists: true,
        id,
        data: () => structuredClone(record),
      }));

    return { docs };
  }
}

class FakeFirestoreCollection extends FakeFirestoreQuery {
  constructor(store: Map<string, FakeFirestoreRecord>) {
    super(store);
  }

  doc(id: string = crypto.randomUUID()) {
    return new FakeFirestoreDocumentReference(this.store, id);
  }
}

class FakeFirestoreTransaction {
  private readonly pendingOperations: Array<() => void> = [];

  async get(ref: unknown) {
    return getFakeRef(ref).readSnapshot();
  }

  async getAll(...refs: unknown[]) {
    return Promise.all(refs.map(async (ref) => this.get(ref)));
  }

  create(ref: unknown, data: Record<string, unknown>) {
    this.pendingOperations.push(() => {
      getFakeRef(ref).applyCreate(data);
    });

    return this;
  }

  set(ref: unknown, data: Record<string, unknown>, options?: { merge?: boolean }) {
    this.pendingOperations.push(() => {
      getFakeRef(ref).applySet(data, options);
    });

    return this;
  }

  delete(ref: unknown) {
    this.pendingOperations.push(() => {
      getFakeRef(ref).applyDelete();
    });

    return this;
  }

  commit() {
    for (const operation of this.pendingOperations) {
      operation();
    }
  }
}

class FakeFirestoreDatabase {
  private readonly stores = new Map<string, Map<string, FakeFirestoreRecord>>();

  collection(path: string) {
    return new FakeFirestoreCollection(this.getStore(path));
  }

  async runTransaction<T>(updateFunction: (transaction: FakeFirestoreTransaction) => Promise<T>) {
    const transaction = new FakeFirestoreTransaction();
    const result = await updateFunction(transaction);
    transaction.commit();
    return result;
  }

  private getStore(path: string): Map<string, FakeFirestoreRecord> {
    const existing = this.stores.get(path);

    if (existing) {
      return existing;
    }

    const created = new Map<string, FakeFirestoreRecord>();
    this.stores.set(path, created);
    return created;
  }
}

class RejectingGetAllFirestoreTransaction extends FakeFirestoreTransaction {
  override getAll(..._refs: unknown[]) {
    return Promise.reject(new Error("getAll should not be called")) as ReturnType<
      FakeFirestoreTransaction["getAll"]
    >;
  }
}

class RejectingGetAllFirestoreDatabase extends FakeFirestoreDatabase {
  readonly transactions: RejectingGetAllFirestoreTransaction[] = [];

  override async runTransaction<T>(
    updateFunction: (transaction: FakeFirestoreTransaction) => Promise<T>,
  ) {
    const transaction = new RejectingGetAllFirestoreTransaction();
    this.transactions.push(transaction);
    const result = await updateFunction(transaction);
    transaction.commit();
    return result;
  }
}

function getFakeRef(ref: unknown): FakeFirestoreDocumentReference {
  if (!(ref instanceof FakeFirestoreDocumentReference)) {
    throw new Error("Expected a FakeFirestoreDocumentReference");
  }

  return ref;
}

function matchesWhereClause(record: FakeFirestoreRecord, clause: FakeWhereClause): boolean {
  const actual = readFieldValue(record, clause.fieldPath);

  switch (clause.opStr) {
    case "==":
      return actual === clause.value;
    case ">":
      return compareUnknown(actual, clause.value) > 0;
    case ">=":
      return compareUnknown(actual, clause.value) >= 0;
    case "<":
      return compareUnknown(actual, clause.value) < 0;
    case "<=":
      return compareUnknown(actual, clause.value) <= 0;
    default:
      throw new Error(`Unsupported Firestore op "${clause.opStr}"`);
  }
}

function readFieldValue(record: FakeFirestoreRecord, fieldPath: string): unknown {
  return fieldPath.split(".").reduce<unknown>((current, segment) => {
    if (!isRecord(current)) {
      return undefined;
    }

    return current[segment];
  }, record);
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

function compareUnknown(left: unknown, right: unknown): number {
  return String(left).localeCompare(String(right));
}

describe("firestoreEngine unique constraints", () => {
  function createEngine(database: FakeFirestoreDatabase = new FakeFirestoreDatabase()) {
    return firestoreEngine({
      database: database as unknown as Parameters<typeof firestoreEngine>[0]["database"],
    });
  }

  test("create rejects duplicate unique index ownership", async () => {
    const engine = createEngine();

    await engine.create(
      "users",
      "u1",
      { id: "u1", email: "sam@example.com" },
      { primary: "u1" },
      undefined,
      undefined,
      { byEmail: "sam@example.com" },
    );

    return expect(
      engine.create(
        "users",
        "u2",
        { id: "u2", email: "sam@example.com" },
        { primary: "u2" },
        undefined,
        undefined,
        { byEmail: "sam@example.com" },
      ),
    ).rejects.toBeInstanceOf(EngineUniqueConstraintError);
  });

  test("create does not rely on getAll for unique ownership reads", async () => {
    const database = new RejectingGetAllFirestoreDatabase();
    const engine = createEngine(database);

    await engine.create(
      "users",
      "u1",
      { id: "u1", email: "sam@example.com", username: "sam" },
      { primary: "u1" },
      undefined,
      undefined,
      { byEmail: "sam@example.com", byUsername: "sam" },
    );

    expect(database.transactions).toHaveLength(1);
  });

  test("create without unique indexes still succeeds when getAll is unavailable", async () => {
    const database = new RejectingGetAllFirestoreDatabase();
    const engine = createEngine(database);

    await engine.create("users", "u1", { id: "u1", email: "sam@example.com" }, { primary: "u1" });

    expect(database.transactions).toHaveLength(1);
  });

  test("update rejects duplicate unique index ownership", async () => {
    const engine = createEngine();

    await engine.create(
      "users",
      "u1",
      { id: "u1", email: "sam@example.com" },
      { primary: "u1" },
      undefined,
      undefined,
      { byEmail: "sam@example.com" },
    );
    await engine.create(
      "users",
      "u2",
      { id: "u2", email: "alex@example.com" },
      { primary: "u2" },
      undefined,
      undefined,
      { byEmail: "alex@example.com" },
    );

    return expect(
      engine.update(
        "users",
        "u2",
        { id: "u2", email: "sam@example.com" },
        { primary: "u2" },
        undefined,
        undefined,
        { byEmail: "sam@example.com" },
      ),
    ).rejects.toBeInstanceOf(EngineUniqueConstraintError);
  });

  test("update releases previous ownership when unique pairs contain null bytes", async () => {
    const engine = createEngine();

    await engine.create(
      "users",
      "u1",
      { id: "u1", marker: "initial" },
      { primary: "u1" },
      undefined,
      undefined,
      { "foo\u0000": "bar" },
    );

    await engine.update(
      "users",
      "u1",
      { id: "u1", marker: "updated" },
      { primary: "u1" },
      undefined,
      undefined,
      { foo: "\u0000bar" },
    );

    return expect(
      engine.create(
        "users",
        "u2",
        { id: "u2", marker: "fresh" },
        { primary: "u2" },
        undefined,
        undefined,
        { "foo\u0000": "bar" },
      ),
    ).resolves.toBeUndefined();
  });

  test("put preserves existing unique ownership when uniqueIndexes are omitted", async () => {
    const engine = createEngine();

    await engine.put(
      "users",
      "u1",
      { id: "u1", email: "sam@example.com" },
      { primary: "u1" },
      undefined,
      undefined,
      { byEmail: "sam@example.com" },
    );

    await engine.put("users", "u1", { id: "u1", email: "updated@example.com" }, { primary: "u1" });

    return expect(
      engine.create(
        "users",
        "u2",
        { id: "u2", email: "sam@example.com" },
        { primary: "u2" },
        undefined,
        undefined,
        { byEmail: "sam@example.com" },
      ),
    ).rejects.toBeInstanceOf(EngineUniqueConstraintError);
  });

  test("batchSet rejects duplicate unique index ownership", async () => {
    const engine = createEngine();

    await engine.create(
      "users",
      "u1",
      { id: "u1", email: "sam@example.com" },
      { primary: "u1" },
      undefined,
      undefined,
      { byEmail: "sam@example.com" },
    );

    return expect(
      engine.batchSet("users", [
        {
          key: "u2",
          doc: { id: "u2", email: "sam@example.com" },
          indexes: { primary: "u2" },
          uniqueIndexes: { byEmail: "sam@example.com" },
        },
      ]),
    ).rejects.toBeInstanceOf(EngineUniqueConstraintError);
  });

  test("batchSetWithResult rejects duplicate unique index ownership", async () => {
    const engine = createEngine();

    await engine.create(
      "users",
      "u1",
      { id: "u1", email: "sam@example.com" },
      { primary: "u1" },
      undefined,
      undefined,
      { byEmail: "sam@example.com" },
    );

    return expect(
      engine.batchSetWithResult("users", [
        {
          key: "u2",
          doc: { id: "u2", email: "sam@example.com" },
          indexes: { primary: "u2" },
          uniqueIndexes: { byEmail: "sam@example.com" },
        },
      ]),
    ).rejects.toBeInstanceOf(EngineUniqueConstraintError);
  });

  test("batchSetWithResult reports stale-token conflicts before unique checks", async () => {
    const engine = createEngine();

    await engine.put(
      "users",
      "u1",
      { id: "u1", email: "before@example.com" },
      { primary: "u1" },
      undefined,
      undefined,
      { byEmail: "before@example.com" },
    );
    await engine.put(
      "users",
      "u2",
      { id: "u2", email: "taken@example.com" },
      { primary: "u2" },
      undefined,
      undefined,
      { byEmail: "taken@example.com" },
    );

    const current = await engine.getWithMetadata("users", "u1");

    expect(current).not.toBeNull();

    await engine.update(
      "users",
      "u1",
      { id: "u1", email: "concurrent@example.com" },
      { primary: "u1" },
      undefined,
      undefined,
      { byEmail: "concurrent@example.com" },
    );

    const result = await engine.batchSetWithResult("users", [
      {
        key: "u1",
        doc: { id: "u1", email: "taken@example.com" },
        indexes: { primary: "u1" },
        expectedWriteToken: current?.writeToken,
        uniqueIndexes: { byEmail: "taken@example.com" },
      },
    ]);

    expect(result).toEqual({
      persistedKeys: [],
      conflictedKeys: ["u1"],
    });
    expect(await engine.get("users", "u1")).toEqual({
      id: "u1",
      email: "concurrent@example.com",
    });
  });

  test("delete releases unique ownership", async () => {
    const engine = createEngine();

    await engine.create(
      "users",
      "u1",
      { id: "u1", email: "sam@example.com" },
      { primary: "u1" },
      undefined,
      undefined,
      { byEmail: "sam@example.com" },
    );

    await engine.delete("users", "u1");

    return expect(
      engine.create(
        "users",
        "u2",
        { id: "u2", email: "sam@example.com" },
        { primary: "u2" },
        undefined,
        undefined,
        { byEmail: "sam@example.com" },
      ),
    ).resolves.toBeUndefined();
  });

  test("batchDelete releases unique ownership", async () => {
    const engine = createEngine();

    await engine.create(
      "users",
      "u1",
      { id: "u1", email: "sam@example.com" },
      { primary: "u1" },
      undefined,
      undefined,
      { byEmail: "sam@example.com" },
    );

    await engine.batchDelete("users", ["u1"]);

    return expect(
      engine.create(
        "users",
        "u2",
        { id: "u2", email: "sam@example.com" },
        { primary: "u2" },
        undefined,
        undefined,
        { byEmail: "sam@example.com" },
      ),
    ).resolves.toBeUndefined();
  });
});
