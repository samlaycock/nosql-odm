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
  test("create rejects duplicate unique index ownership", async () => {
    const engine = firestoreEngine({
      database: new FakeFirestoreDatabase() as unknown as Parameters<
        typeof firestoreEngine
      >[0]["database"],
    });

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

  test("put preserves existing unique ownership when uniqueIndexes are omitted", async () => {
    const engine = firestoreEngine({
      database: new FakeFirestoreDatabase() as unknown as Parameters<
        typeof firestoreEngine
      >[0]["database"],
    });

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

  test("delete releases unique ownership", async () => {
    const engine = firestoreEngine({
      database: new FakeFirestoreDatabase() as unknown as Parameters<
        typeof firestoreEngine
      >[0]["database"],
    });

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
});
