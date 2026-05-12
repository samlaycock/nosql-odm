import { describe, expect, test } from "bun:test";

import { reserveCreatedAtRange } from "../../src/engines/distributed-created-at";

describe("distributed createdAt allocation", () => {
  test("allocates many ordered values without shared metadata state", () => {
    const createdAts = reserveCreatedAtRange(5_000);
    const uniqueCreatedAts = new Set(createdAts);

    expect(uniqueCreatedAts.size).toBe(createdAts.length);

    for (let index = 1; index < createdAts.length; index += 1) {
      expect(createdAts[index]).toBeGreaterThan(createdAts[index - 1]!);
    }
  });
});
