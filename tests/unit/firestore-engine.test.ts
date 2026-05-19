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

interface FakeOrderByClause {
  readonly fieldPath: string;
  readonly direction: "asc" | "desc";
}

interface FakeQueryState {
  readonly filters: readonly FakeWhereClause[];
  readonly orderBy: readonly FakeOrderByClause[];
  readonly startAfter: readonly unknown[];
  readonly limit: number | null;
}

interface FakeFirestoreInstrumentation {
  readonly documentGetCalls: string[];
  readonly getAllCalls: string[][];
  readonly queryReads: FakeQueryState[];
}

class FakeFirestoreDocumentReference {
  readonly id: string;

  constructor(
    private readonly store: Map<string, FakeFirestoreRecord>,
    private readonly instrumentation: FakeFirestoreInstrumentation,
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
    this.instrumentation.documentGetCalls.push(this.id);
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
    protected readonly instrumentation: FakeFirestoreInstrumentation,
    private readonly state: FakeQueryState = {
      filters: [],
      orderBy: [],
      startAfter: [],
      limit: null,
    },
  ) {}

  where(fieldPath: string, opStr: string, value: unknown) {
    return new FakeFirestoreQuery(this.store, this.instrumentation, {
      ...this.state,
      filters: [
        ...this.state.filters,
        {
          fieldPath,
          opStr,
          value,
        },
      ],
    });
  }

  orderBy(fieldPath: string, direction: "asc" | "desc" = "asc") {
    return new FakeFirestoreQuery(this.store, this.instrumentation, {
      ...this.state,
      orderBy: [
        ...this.state.orderBy,
        {
          fieldPath,
          direction,
        },
      ],
    });
  }

  startAfter(...values: unknown[]) {
    return new FakeFirestoreQuery(this.store, this.instrumentation, {
      ...this.state,
      startAfter: [...values],
    });
  }

  limit(limit: number) {
    return new FakeFirestoreQuery(this.store, this.instrumentation, {
      ...this.state,
      limit,
    });
  }

  async get() {
    this.instrumentation.queryReads.push({
      filters: [...this.state.filters],
      orderBy: [...this.state.orderBy],
      startAfter: [...this.state.startAfter],
      limit: this.state.limit,
    });

    let entries = [...this.store.entries()].filter(([, record]) =>
      this.state.filters.every((filter) => matchesWhereClause(record, filter)),
    );

    if (this.state.orderBy.length > 0) {
      entries = [...entries].sort((left, right) =>
        compareQueryEntries(left, right, this.state.orderBy),
      );
    }

    if (this.state.startAfter.length > 0) {
      const startAfterIndex = entries.findIndex((entry) =>
        queryEntryMatchesCursorValues(entry, this.state.orderBy, this.state.startAfter),
      );

      entries = startAfterIndex === -1 ? [] : entries.slice(startAfterIndex + 1);
    }

    if (this.state.limit !== null) {
      entries = entries.slice(0, this.state.limit);
    }

    const docs = entries.map(([id, record]) => ({
      exists: true,
      id,
      data: () => structuredClone(record),
    }));

    return { docs };
  }
}

class FakeFirestoreCollection extends FakeFirestoreQuery {
  constructor(
    store: Map<string, FakeFirestoreRecord>,
    instrumentation: FakeFirestoreInstrumentation,
  ) {
    super(store, instrumentation);
  }

  doc(id: string = crypto.randomUUID()) {
    return new FakeFirestoreDocumentReference(this.store, this.instrumentation, id);
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
  readonly instrumentation: FakeFirestoreInstrumentation = {
    documentGetCalls: [],
    getAllCalls: [],
    queryReads: [],
  };

  collection(path: string) {
    return new FakeFirestoreCollection(this.getStore(path), this.instrumentation);
  }

  async getAll(...refs: unknown[]) {
    const resolvedRefs = refs.map((ref) => getFakeRef(ref));
    this.instrumentation.getAllCalls.push(resolvedRefs.map((ref) => ref.id));
    return resolvedRefs.map((ref) => ref.readSnapshot());
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

class ReorderingGetAllFirestoreDatabase extends FakeFirestoreDatabase {
  override async getAll(...refs: unknown[]) {
    const snapshots = await super.getAll(...refs);
    return [...snapshots].reverse();
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

function createEngine(database: FakeFirestoreDatabase = new FakeFirestoreDatabase()) {
  return firestoreEngine({
    database: database as unknown as Parameters<typeof firestoreEngine>[0]["database"],
  });
}

function compareQueryEntries(
  left: readonly [string, FakeFirestoreRecord],
  right: readonly [string, FakeFirestoreRecord],
  orderBy: readonly FakeOrderByClause[],
): number {
  for (const clause of orderBy) {
    const leftValue = readFieldValue(left[1], clause.fieldPath);
    const rightValue = readFieldValue(right[1], clause.fieldPath);
    const base = compareUnknown(leftValue, rightValue);

    if (base !== 0) {
      return clause.direction === "desc" ? -base : base;
    }
  }

  return left[0].localeCompare(right[0]);
}

function queryEntryMatchesCursorValues(
  entry: readonly [string, FakeFirestoreRecord],
  orderBy: readonly FakeOrderByClause[],
  cursorValues: readonly unknown[],
): boolean {
  if (cursorValues.length !== orderBy.length) {
    return false;
  }

  return orderBy.every(
    (clause, index) => readFieldValue(entry[1], clause.fieldPath) === cursorValues[index],
  );
}

describe("firestoreEngine query execution", () => {
  test("batchGet uses getAll to read unique keys and preserves request order", async () => {
    const database = new FakeFirestoreDatabase();
    const engine = createEngine(database);

    await engine.batchSet("users", [
      { key: "u1", doc: { id: "u1", name: "A" }, indexes: { primary: "u1" } },
      { key: "u2", doc: { id: "u2", name: "B" }, indexes: { primary: "u2" } },
    ]);

    const results = await engine.batchGet("users", ["u2", "u1", "u2", "missing"]);

    expect(results.map((entry) => entry.key)).toEqual(["u2", "u1", "u2"]);
    expect(database.instrumentation.getAllCalls).toEqual([
      ["doc:users:u2", "doc:users:u1", "doc:users:missing"],
    ]);
    expect(database.instrumentation.documentGetCalls).toEqual([]);
  });

  test("batchGet chunks large getAll reads and does not rely on snapshot order", async () => {
    const database = new ReorderingGetAllFirestoreDatabase();
    const engine = createEngine(database);
    const items = Array.from({ length: 305 }, (_, index) => ({
      key: `u${String(index + 1)}`,
      doc: { id: `u${String(index + 1)}` },
      indexes: { primary: `u${String(index + 1)}` },
    }));
    const requestKeys = items.map((item) => item.key);

    await engine.batchSet("users", items);

    const results = await engine.batchGet("users", requestKeys);

    expect(database.instrumentation.getAllCalls).toHaveLength(2);
    expect(database.instrumentation.getAllCalls[0]).toHaveLength(300);
    expect(database.instrumentation.getAllCalls[1]).toEqual([
      "doc:users:u301",
      "doc:users:u302",
      "doc:users:u303",
      "doc:users:u304",
      "doc:users:u305",
    ]);
    expect(results.map((entry) => entry.key)).toEqual(requestKeys);
  });

  test("query falls back for $begins because Firestore has no Unicode-safe prefix range", async () => {
    const database = new FakeFirestoreDatabase();
    const engine = createEngine(database);

    await engine.batchSet("users", [
      {
        key: "u1",
        doc: { id: "u1", createdAt: "2025-01-01" },
        indexes: { byCreatedAt: "2025-01-01" },
      },
      {
        key: "u2",
        doc: { id: "u2", createdAt: "2025-02-01" },
        indexes: { byCreatedAt: "2025-02-01" },
      },
      {
        key: "u3",
        doc: { id: "u3", createdAt: "2025-03-01" },
        indexes: { byCreatedAt: "2025-03-01" },
      },
    ]);

    const firstPage = await engine.query("users", {
      index: "byCreatedAt",
      filter: { value: { $begins: "2025-" } },
      sort: "desc",
      limit: 2,
    });

    expect(firstPage.documents.map((entry) => entry.key)).toEqual(["u3", "u2"]);
    expect(database.instrumentation.queryReads.at(-1)).toEqual({
      filters: [{ fieldPath: "collection", opStr: "==", value: "users" }],
      orderBy: [],
      startAfter: [],
      limit: null,
    });

    const secondPage = await engine.query("users", {
      index: "byCreatedAt",
      filter: { value: { $begins: "2025-" } },
      sort: "desc",
      limit: 2,
      cursor: firstPage.cursor ?? undefined,
    });

    expect(secondPage.documents.map((entry) => entry.key)).toEqual(["u1"]);
    expect(database.instrumentation.queryReads.at(-1)).toEqual({
      filters: [{ fieldPath: "collection", opStr: "==", value: "users" }],
      orderBy: [],
      startAfter: [],
      limit: null,
    });
  });

  test("$begins fallback includes Unicode suffixes above old Firestore sentinels", async () => {
    const database = new FakeFirestoreDatabase();
    const engine = createEngine(database);

    await engine.batchSet("users", [
      {
        key: "u1",
        doc: { id: "u1" },
        indexes: { byRole: "member#\uf900" },
      },
      {
        key: "u2",
        doc: { id: "u2" },
        indexes: { byRole: "member#😀" },
      },
      {
        key: "u3",
        doc: { id: "u3" },
        indexes: { byRole: "admin#\uf900" },
      },
    ]);

    const results = await engine.query("users", {
      index: "byRole",
      filter: { value: { $begins: "member#" } },
      sort: "asc",
    });

    expect(results.documents.map((entry) => entry.key).sort()).toEqual(["u1", "u2"]);
    expect(database.instrumentation.queryReads.at(-1)).toEqual({
      filters: [{ fieldPath: "collection", opStr: "==", value: "users" }],
      orderBy: [],
      startAfter: [],
      limit: null,
    });
  });

  test("query pushes equality-only pagination into Firestore with scan cursors", async () => {
    const database = new FakeFirestoreDatabase();
    const engine = createEngine(database);

    await engine.batchSet("users", [
      {
        key: "u1",
        doc: { id: "u1", status: "active" },
        indexes: { status: "active" },
      },
      {
        key: "u2",
        doc: { id: "u2", status: "inactive" },
        indexes: { status: "inactive" },
      },
      {
        key: "u3",
        doc: { id: "u3", status: "active" },
        indexes: { status: "active" },
      },
      {
        key: "u4",
        doc: { id: "u4", status: "active" },
        indexes: { status: "active" },
      },
    ]);

    const firstPage = await engine.query("users", {
      index: "status",
      filter: { value: "active" },
      limit: 2,
    });

    expect(firstPage.documents.map((entry) => entry.key)).toEqual(["u1", "u3"]);
    expect(database.instrumentation.queryReads.at(-1)).toEqual({
      filters: [
        { fieldPath: "collection", opStr: "==", value: "users" },
        { fieldPath: "indexes.status", opStr: "==", value: "active" },
      ],
      orderBy: [
        { fieldPath: "createdAt", direction: "asc" },
        { fieldPath: "key", direction: "asc" },
      ],
      startAfter: [],
      limit: 3,
    });

    const secondPage = await engine.query("users", {
      index: "status",
      filter: { value: "active" },
      limit: 2,
      cursor: firstPage.cursor ?? undefined,
    });

    expect(secondPage.documents.map((entry) => entry.key)).toEqual(["u4"]);
    expect(database.instrumentation.queryReads.at(-1)).toEqual({
      filters: [
        { fieldPath: "collection", opStr: "==", value: "users" },
        { fieldPath: "indexes.status", opStr: "==", value: "active" },
      ],
      orderBy: [
        { fieldPath: "createdAt", direction: "asc" },
        { fieldPath: "key", direction: "asc" },
      ],
      startAfter: [expect.any(Number), "u3"],
      limit: 3,
    });
  });
});

describe("firestoreEngine unique constraints", () => {
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

  test("batchSet releases previous ownership when unique value changes", async () => {
    const engine = createEngine();

    await engine.batchSet("users", [
      {
        key: "u1",
        doc: { id: "u1", email: "old@example.com" },
        indexes: { primary: "u1" },
        uniqueIndexes: { byEmail: "old@example.com" },
      },
    ]);

    await engine.batchSet("users", [
      {
        key: "u1",
        doc: { id: "u1", email: "new@example.com" },
        indexes: { primary: "u1" },
        uniqueIndexes: { byEmail: "new@example.com" },
      },
    ]);

    return expect(
      engine.create(
        "users",
        "u2",
        { id: "u2", email: "old@example.com" },
        { primary: "u2" },
        undefined,
        undefined,
        { byEmail: "old@example.com" },
      ),
    ).resolves.toBeUndefined();
  });

  test("batchSetWithResult rejects duplicate unique ownership after persisting earlier items", async () => {
    const engine = createEngine();

    if (!engine.batchSetWithResult) {
      throw new Error("Expected Fake Firestore engine to implement batchSetWithResult");
    }

    await engine.create(
      "users",
      "u1",
      { id: "u1", email: "sam@example.com" },
      { primary: "u1" },
      undefined,
      undefined,
      { byEmail: "sam@example.com" },
    );

    let duplicateError: unknown;

    try {
      await engine.batchSetWithResult("users", [
        {
          key: "u2",
          doc: { id: "u2", email: "fresh@example.com" },
          indexes: { primary: "u2" },
          uniqueIndexes: { byEmail: "fresh@example.com" },
        },
        {
          key: "u3",
          doc: { id: "u3", email: "sam@example.com" },
          indexes: { primary: "u3" },
          uniqueIndexes: { byEmail: "sam@example.com" },
        },
      ]);
    } catch (error) {
      duplicateError = error;
    }

    expect(duplicateError).toBeInstanceOf(EngineUniqueConstraintError);
    expect(await engine.get("users", "u2")).toEqual({
      id: "u2",
      email: "fresh@example.com",
    });

    return expect(await engine.get("users", "u3")).toBeNull();
  });

  test("batchSetWithResult rethrows unique conflicts when the write token is current", async () => {
    const engine = createEngine();

    if (!engine.getWithMetadata) {
      throw new Error("Expected Fake Firestore engine to implement getWithMetadata");
    }

    if (!engine.batchSetWithResult) {
      throw new Error("Expected Fake Firestore engine to implement batchSetWithResult");
    }

    await engine.put(
      "users",
      "u1",
      { id: "u1", email: "taken@example.com" },
      { primary: "u1" },
      undefined,
      undefined,
      { byEmail: "taken@example.com" },
    );
    await engine.put(
      "users",
      "u2",
      { id: "u2", email: "before@example.com" },
      { primary: "u2" },
      undefined,
      undefined,
      { byEmail: "before@example.com" },
    );

    const current = await engine.getWithMetadata("users", "u2");

    expect(current).not.toBeNull();

    let uniqueConflictError: unknown;

    try {
      await engine.batchSetWithResult("users", [
        {
          key: "u2",
          doc: { id: "u2", email: "taken@example.com" },
          indexes: { primary: "u2" },
          expectedWriteToken: current?.writeToken,
          uniqueIndexes: { byEmail: "taken@example.com" },
        },
      ]);
    } catch (error) {
      uniqueConflictError = error;
    }

    expect(uniqueConflictError).toBeInstanceOf(EngineUniqueConstraintError);

    expect(await engine.get("users", "u2")).toEqual({
      id: "u2",
      email: "before@example.com",
    });
  });

  test("batchSetWithResult reports stale-token conflicts before unique checks", async () => {
    const engine = createEngine();

    if (!engine.getWithMetadata) {
      throw new Error("Expected Fake Firestore engine to implement getWithMetadata");
    }

    if (!engine.batchSetWithResult) {
      throw new Error("Expected Fake Firestore engine to implement batchSetWithResult");
    }

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
