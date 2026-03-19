import { describe, expect, test } from "bun:test";

import { cassandraEngine } from "../../src/engines/cassandra";

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
});
