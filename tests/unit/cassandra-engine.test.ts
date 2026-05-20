import { describe, expect, test } from "bun:test";

import { cassandraEngine } from "../../src/engines/cassandra";

interface CapturedQuery {
  readonly query: string;
  readonly params?: readonly unknown[];
}

describe("cassandraEngine", () => {
  test('reports uniqueConstraints capability as "none"', () => {
    const engine = cassandraEngine({
      client: {
        async execute() {
          return { rows: [] };
        },
      },
      keyspace: "test_keyspace",
    });

    expect(engine.capabilities?.uniqueConstraints).toBe("none");
  });

  test("uses query index table for indexed equality queries", async () => {
    const captured: CapturedQuery[] = [];
    const engine = cassandraEngine({
      client: {
        async execute(query, params) {
          captured.push({ query, params });

          if (query.includes("IF NOT EXISTS")) {
            return { rows: [{ "[applied]": true }] };
          }

          if (query.includes("SELECT doc_key") && query.includes("nosql_odm_indexes")) {
            return {
              rows: [{ doc_key: "u1", created_at: 1, index_value: "sam@example.com" }],
            };
          }

          if (query.includes("doc_key IN")) {
            return {
              rows: [
                {
                  doc_key: "u1",
                  created_at: 1,
                  write_version: 1,
                  doc: JSON.stringify({ id: "u1", email: "sam@example.com" }),
                  indexes: { byEmail: "sam@example.com" },
                  migration_target_version: 0,
                  migration_version_state: "unknown",
                  migration_index_signature: null,
                  migration_index_signature_sort: "",
                },
              ],
            };
          }

          return { rows: [] };
        },
      },
      keyspace: "test_keyspace",
    });

    const result = await engine.query("users", {
      index: "byEmail",
      filter: { value: "sam@example.com" },
    });

    expect(result.documents).toEqual([{ key: "u1", doc: { id: "u1", email: "sam@example.com" } }]);
    expect(result.diagnostics).toEqual({
      mode: "native_pushdown",
      reason: "native_pushdown",
      index: "byEmail",
    });
    expect(
      captured.some(
        (item) =>
          item.query.includes("FROM test_keyspace.nosql_odm_documents WHERE collection = ?") &&
          !item.query.includes("doc_key"),
      ),
    ).toBe(false);
  });

  test("maintains query index rows on create, update, and delete", async () => {
    const captured: CapturedQuery[] = [];
    const stored = new Map<string, Record<string, unknown>>();

    const engine = cassandraEngine({
      client: {
        async execute(query, params) {
          captured.push({ query, params });

          if (query.includes("SELECT doc_key") && query.includes("doc_key = ?")) {
            const row = stored.get(String(params?.[1]));
            return { rows: row ? [row] : [] };
          }

          if (query.includes("IF NOT EXISTS")) {
            stored.set(String(params?.[1]), {
              doc_key: params?.[1],
              created_at: params?.[2],
              write_version: params?.[3],
              doc: params?.[4],
              indexes: params?.[5],
              migration_target_version: params?.[6],
              migration_version_state: params?.[7],
              migration_index_signature: params?.[8],
              migration_index_signature_sort: params?.[9],
            });
            return { rows: [{ "[applied]": true }] };
          }

          if (query.startsWith("UPDATE test_keyspace.nosql_odm_documents")) {
            stored.set(String(params?.[9]), {
              doc_key: params?.[9],
              created_at: params?.[0],
              write_version: params?.[1],
              doc: params?.[2],
              indexes: params?.[3],
              migration_target_version: params?.[4],
              migration_version_state: params?.[5],
              migration_index_signature: params?.[6],
              migration_index_signature_sort: params?.[7],
            });
            return { rows: [{ "[applied]": true }] };
          }

          return { rows: [] };
        },
      },
      keyspace: "test_keyspace",
    });

    await engine.create("users", "u1", { id: "u1" }, { byEmail: "old@example.com" });
    await engine.update("users", "u1", { id: "u1" }, { byEmail: "new@example.com" });
    await engine.delete("users", "u1");

    const indexInserts = captured.filter(
      (item) =>
        item.query.includes("INSERT INTO test_keyspace.nosql_odm_indexes") &&
        item.params?.[1] === "byEmail",
    );
    const indexDeletes = captured.filter(
      (item) =>
        item.query.includes("DELETE FROM test_keyspace.nosql_odm_indexes") &&
        item.params?.[1] === "byEmail",
    );

    expect(indexInserts.map((item) => item.params?.[2])).toEqual([
      "old@example.com",
      "new@example.com",
    ]);
    expect(indexDeletes.map((item) => item.params?.[2])).toEqual([
      "old@example.com",
      "new@example.com",
    ]);
  });
});
