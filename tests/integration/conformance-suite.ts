import { describe, expect, test } from "bun:test";

import { EngineUniqueConstraintError, type QueryEngine } from "../../src/engines/types";

interface QueryEngineConformanceSuiteOptions<TOptions = Record<string, unknown>> {
  readonly engineName: string;
  readonly getEngine: () => QueryEngine<TOptions>;
  readonly nextCollection: (prefix: string) => string;
  readonly assertEngineUniqueConstraintConformance?: boolean;
}

export function runQueryEngineConformanceSuite<TOptions = Record<string, unknown>>(
  options: QueryEngineConformanceSuiteOptions<TOptions>,
): void {
  const {
    engineName,
    getEngine,
    nextCollection,
    assertEngineUniqueConstraintConformance = false,
  } = options;
  const uniqueConstraintConformanceTest = assertEngineUniqueConstraintConformance
    ? test
    : test.skip;

  describe(`${engineName} conformance`, () => {
    test("preserves batchGet request order and duplicates", async () => {
      const engine = getEngine();
      const collection = nextCollection("users");

      await engine.batchSet(collection, [
        { key: "u1", doc: { id: "u1", name: "A" }, indexes: { primary: "u1" } },
        { key: "u2", doc: { id: "u2", name: "B" }, indexes: { primary: "u2" } },
      ]);

      const docs = await engine.batchGet(collection, ["u2", "u1", "u2", "missing"]);

      expect(docs.map((entry) => entry.key)).toEqual(["u2", "u1", "u2"]);
      expect(docs[0]?.doc).toEqual({ id: "u2", name: "B" });
      expect(docs[1]?.doc).toEqual({ id: "u1", name: "A" });
      expect(docs[2]?.doc).toEqual({ id: "u2", name: "B" });
      expect(docs[0]?.doc).not.toBe(docs[2]?.doc);
    });

    test("applies shared equality, comparison, and scan query semantics", async () => {
      const engine = getEngine();
      const collection = nextCollection("users");
      const otherCollection = nextCollection("posts");

      await engine.batchSet(collection, [
        {
          key: "u1",
          doc: { id: "u1", status: "active", createdAt: "2025-01-01" },
          indexes: { status: "active", byCreatedAt: "2025-01-01" },
        },
        {
          key: "u2",
          doc: { id: "u2", status: "active", createdAt: "2025-06-15" },
          indexes: { status: "active", byCreatedAt: "2025-06-15" },
        },
        {
          key: "u3",
          doc: { id: "u3", status: "inactive", createdAt: "2025-12-31" },
          indexes: { status: "inactive", byCreatedAt: "2025-12-31" },
        },
      ]);
      await engine.put(otherCollection, "p1", { id: "p1" }, { primary: "p1" });

      const equalityResults = await engine.query(collection, {
        index: "status",
        filter: { value: "active" },
      });
      const comparisonResults = await engine.query(collection, {
        index: "byCreatedAt",
        filter: { value: { $between: ["2025-01-01", "2025-06-15"] } },
        sort: "asc",
      });
      const scanResults = await engine.query(collection, {});

      expect(equalityResults.documents.map((entry) => entry.key).sort()).toEqual(["u1", "u2"]);
      expect(comparisonResults.documents.map((entry) => entry.key)).toEqual(["u1", "u2"]);
      expect(scanResults.documents.map((entry) => entry.key).sort()).toEqual(["u1", "u2", "u3"]);
    });

    test("uses shared sorting and cursor pagination semantics", async () => {
      const engine = getEngine();
      const sortedCollection = nextCollection("items");
      const pagedCollection = nextCollection("paged_users");

      await engine.batchSet(sortedCollection, [
        {
          key: "a",
          doc: { id: "a", createdAt: "2025-03-01" },
          indexes: { byCreatedAt: "2025-03-01" },
        },
        {
          key: "b",
          doc: { id: "b", createdAt: "2025-01-01" },
          indexes: { byCreatedAt: "2025-01-01" },
        },
        {
          key: "c",
          doc: { id: "c", createdAt: "2025-02-01" },
          indexes: { byCreatedAt: "2025-02-01" },
        },
      ]);
      await engine.batchSet(pagedCollection, [
        { key: "u1", doc: { id: "u1" }, indexes: { status: "active" } },
        { key: "u2", doc: { id: "u2" }, indexes: { status: "active" } },
        { key: "u3", doc: { id: "u3" }, indexes: { status: "active" } },
      ]);

      const asc = await engine.query(sortedCollection, {
        index: "byCreatedAt",
        filter: { value: { $begins: "2025-" } },
        sort: "asc",
      });
      const desc = await engine.query(sortedCollection, {
        index: "byCreatedAt",
        filter: { value: { $begins: "2025-" } },
        sort: "desc",
      });
      const page1 = await engine.query(pagedCollection, {
        index: "status",
        filter: { value: "active" },
        limit: 2,
      });
      const page2 = await engine.query(pagedCollection, {
        index: "status",
        filter: { value: "active" },
        limit: 2,
        cursor: page1.cursor ?? undefined,
      });

      expect(asc.documents.map((entry) => entry.key)).toEqual(["b", "c", "a"]);
      expect(desc.documents.map((entry) => entry.key)).toEqual(["a", "c", "b"]);
      expect(page1.documents).toHaveLength(2);
      expect(page1.cursor).not.toBeNull();
      expect(page2.documents).toHaveLength(1);
      expect(page2.cursor).toBeNull();
    });

    uniqueConstraintConformanceTest(
      "enforces engine-level unique index ownership consistently when opted in",
      async () => {
        const engine = getEngine();
        const collection = nextCollection("unique_users");

        await engine.create(
          collection,
          "u1",
          { id: "u1", email: "sam@example.com" },
          { primary: "u1" },
          undefined,
          undefined,
          { byEmail: "sam@example.com" },
        );
        await engine.create(
          collection,
          "u2",
          { id: "u2", email: "other@example.com" },
          { primary: "u2" },
          undefined,
          undefined,
          { byEmail: "other@example.com" },
        );

        await expect(
          engine.create(
            collection,
            "u3",
            { id: "u3", email: "sam@example.com" },
            { primary: "u3" },
            undefined,
            undefined,
            { byEmail: "sam@example.com" },
          ),
        ).rejects.toBeInstanceOf(EngineUniqueConstraintError);

        await expect(
          engine.update(
            collection,
            "u2",
            { id: "u2", email: "sam@example.com" },
            { primary: "u2" },
            undefined,
            undefined,
            { byEmail: "sam@example.com" },
          ),
        ).rejects.toBeInstanceOf(EngineUniqueConstraintError);

        await expect(
          engine.put(
            collection,
            "u2",
            { id: "u2", email: "sam@example.com" },
            { primary: "u2" },
            undefined,
            undefined,
            { byEmail: "sam@example.com" },
          ),
        ).rejects.toBeInstanceOf(EngineUniqueConstraintError);

        await expect(
          engine.batchSet(collection, [
            {
              key: "u4",
              doc: { id: "u4", email: "sam@example.com" },
              indexes: { primary: "u4" },
              uniqueIndexes: { byEmail: "sam@example.com" },
            },
          ]),
        ).rejects.toBeInstanceOf(EngineUniqueConstraintError);

        await expect(
          engine.batchSet(collection, [
            {
              key: "u4",
              doc: { id: "u4", email: "fresh@example.com" },
              indexes: { primary: "u4" },
              uniqueIndexes: { byEmail: "fresh@example.com" },
            },
            {
              key: "u5",
              doc: { id: "u5", email: "fresh@example.com" },
              indexes: { primary: "u5" },
              uniqueIndexes: { byEmail: "fresh@example.com" },
            },
          ]),
        ).rejects.toBeInstanceOf(EngineUniqueConstraintError);

        await engine.update(
          collection,
          "u1",
          { id: "u1", email: "renamed@example.com" },
          { primary: "u1" },
          undefined,
          undefined,
          { byEmail: "renamed@example.com" },
        );

        await engine.create(
          collection,
          "u3",
          { id: "u3", email: "sam@example.com" },
          { primary: "u3" },
          undefined,
          undefined,
          { byEmail: "sam@example.com" },
        );

        expect(await engine.get(collection, "u3")).toEqual({
          id: "u3",
          email: "sam@example.com",
        });
      },
    );

    uniqueConstraintConformanceTest(
      "batchSetWithResult rejects unique index violations instead of reporting conflicted keys",
      async () => {
        const engine = getEngine();

        if (!engine.batchSetWithResult) {
          expect(engine.batchSetWithResult).toBeUndefined();
          return;
        }

        const collection = nextCollection("unique_batch_result_users");

        await engine.create(
          collection,
          "u1",
          { id: "u1", email: "sam@example.com" },
          { primary: "u1" },
          undefined,
          undefined,
          { byEmail: "sam@example.com" },
        );

        await expect(
          engine.batchSetWithResult(collection, [
            {
              key: "u2",
              doc: { id: "u2", email: "sam@example.com" },
              indexes: { primary: "u2" },
              uniqueIndexes: { byEmail: "sam@example.com" },
            },
          ]),
        ).rejects.toBeInstanceOf(EngineUniqueConstraintError);

        expect(await engine.get(collection, "u2")).toBeNull();
      },
    );

    uniqueConstraintConformanceTest(
      "batchSetWithResult reports stale-token conflicts before unique checks",
      async () => {
        const engine = getEngine();

        if (!engine.batchSetWithResult) {
          expect(engine.batchSetWithResult).toBeUndefined();
          return;
        }

        const collection = nextCollection("unique_batch_stale_users");

        await engine.put(
          collection,
          "u1",
          {
            __v: 1,
            __indexes: ["primary"],
            id: "u1",
            name: "Before",
            email: "before@example.com",
          },
          { primary: "u1" },
          undefined,
          undefined,
          { byEmail: "before@example.com" },
        );
        await engine.put(
          collection,
          "u2",
          {
            __v: 1,
            __indexes: ["primary"],
            id: "u2",
            name: "Taken",
            email: "taken@example.com",
          },
          { primary: "u2" },
          undefined,
          undefined,
          { byEmail: "taken@example.com" },
        );

        const outdated = await engine.migration.getOutdated(collection, {
          version: 2,
          versionField: "__v",
          indexes: ["primary"],
          indexesField: "__indexes",
        });
        const token = outdated.documents.find((entry) => entry.key === "u1")?.writeToken;

        expect(typeof token).toBe("string");

        await engine.update(
          collection,
          "u1",
          {
            __v: 1,
            __indexes: ["primary"],
            id: "u1",
            name: "Concurrent",
            email: "concurrent@example.com",
          },
          { primary: "u1" },
          undefined,
          undefined,
          { byEmail: "concurrent@example.com" },
        );

        const result = await engine.batchSetWithResult(collection, [
          {
            key: "u1",
            doc: {
              __v: 2,
              __indexes: ["primary"],
              id: "u1",
              name: "Migrated",
              email: "taken@example.com",
            },
            indexes: { primary: "u1" },
            expectedWriteToken: token,
            uniqueIndexes: { byEmail: "taken@example.com" },
          },
        ]);

        expect(result.persistedKeys).toEqual([]);
        expect(result.conflictedKeys).toEqual(["u1"]);
        expect(await engine.get(collection, "u1")).toEqual({
          __v: 1,
          __indexes: ["primary"],
          id: "u1",
          name: "Concurrent",
          email: "concurrent@example.com",
        });
      },
    );

    test("skips stale migration writes consistently when supported", async () => {
      const engine = getEngine();

      if (!engine.batchSetWithResult) {
        expect(engine.batchSetWithResult).toBeUndefined();
        return;
      }

      const collection = nextCollection("users");

      await engine.put(
        collection,
        "u1",
        {
          __v: 1,
          __indexes: ["primary"],
          id: "u1",
          name: "Before",
          email: "before@example.com",
        },
        { primary: "u1" },
      );

      const outdated = await engine.migration.getOutdated(collection, {
        version: 2,
        versionField: "__v",
        indexes: ["primary"],
        indexesField: "__indexes",
      });
      const token = outdated.documents[0]?.writeToken;

      expect(typeof token).toBe("string");

      await engine.update(
        collection,
        "u1",
        {
          __v: 1,
          __indexes: ["primary"],
          id: "u1",
          name: "Concurrent",
          email: "concurrent@example.com",
        },
        { primary: "u1" },
      );

      const result = await engine.batchSetWithResult(collection, [
        {
          key: "u1",
          doc: {
            __v: 2,
            __indexes: ["primary"],
            id: "u1",
            firstName: "Before",
            lastName: "",
            email: "before@example.com",
          },
          indexes: { primary: "u1" },
          expectedWriteToken: token,
        },
      ]);

      expect(result.persistedKeys).toEqual([]);
      expect(result.conflictedKeys).toEqual(["u1"]);
      expect(await engine.get(collection, "u1")).toEqual({
        __v: 1,
        __indexes: ["primary"],
        id: "u1",
        name: "Concurrent",
        email: "concurrent@example.com",
      });
    });
  });
}
