import { describe, expect, test } from "bun:test";

import { chunkArray, normalizeBatchChunkSize } from "../../src/engines/batch-chunking";

describe("batch chunking helpers", () => {
  test("normalizes invalid chunk sizes to the fallback", () => {
    expect(normalizeBatchChunkSize(undefined, 32)).toBe(32);
    expect(normalizeBatchChunkSize(0, 32)).toBe(32);
    expect(normalizeBatchChunkSize(-4, 32)).toBe(32);
    expect(normalizeBatchChunkSize(Number.NaN, 32)).toBe(32);
  });

  test("floors valid chunk sizes and respects an optional maximum", () => {
    expect(normalizeBatchChunkSize(12.9, 32)).toBe(12);
    expect(normalizeBatchChunkSize(128, 32, 64)).toBe(64);
  });

  test("splits arrays into ordered chunks", () => {
    expect(Array.from(chunkArray(["a", "b", "c", "d", "e"], 2))).toEqual([
      ["a", "b"],
      ["c", "d"],
      ["e"],
    ]);
  });

  test("rejects non-positive chunk sizes", () => {
    expect(() => Array.from(chunkArray(["a"], 0))).toThrow(
      "chunkArray size must be a positive number",
    );
    expect(() => Array.from(chunkArray(["a"], -1))).toThrow(
      "chunkArray size must be a positive number",
    );
  });
});
