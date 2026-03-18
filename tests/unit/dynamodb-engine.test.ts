import { describe, expect, test } from "bun:test";

import { dynamoDbEngine } from "../../src/engines/dynamodb";

describe("dynamoDbEngine", () => {
  test('reports uniqueConstraints capability as "none"', () => {
    const engine = dynamoDbEngine({
      client: {
        async send() {
          throw new Error("Unexpected send");
        },
      },
      tableName: "test-table",
    });

    expect(engine.capabilities?.uniqueConstraints).toBe("none");
  });
});
