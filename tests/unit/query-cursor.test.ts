import { describe, expect, test } from "bun:test";

import type { QueryParams } from "../../src/engines/types";

import {
  encodeQueryPageCursor,
  resolveQueryPageStartIndex,
  type QueryCursorRecordPosition,
} from "../../src/engines/query-cursor";

interface FakeRecord {
  key: string;
  createdAt: number;
  indexes: Record<string, string>;
}

function position(record: FakeRecord, params: QueryParams): QueryCursorRecordPosition {
  return {
    key: record.key,
    createdAt: record.createdAt,
    indexValue: params.index ? (record.indexes[params.index] ?? "") : undefined,
  };
}

describe("query cursor helpers", () => {
  test("rejects malformed cursors explicitly", () => {
    const records: FakeRecord[] = [];

    expect(() =>
      resolveQueryPageStartIndex(records, "users", { cursor: "not-base64" }, position),
    ).toThrow("Invalid query cursor");
  });

  test("rejects cursors reused with different query parameters", () => {
    const params: QueryParams = {
      index: "byRole",
      filter: { value: "member" },
      sort: "asc",
      limit: 2,
    };
    const otherParams: QueryParams = {
      ...params,
      sort: "desc",
      cursor: encodeQueryPageCursor("users", params, {
        key: "u2",
        createdAt: 2,
        indexValue: "member",
      }),
    };

    expect(() => resolveQueryPageStartIndex([], "users", otherParams, position)).toThrow(
      "Query cursor does not match the requested query",
    );
  });

  test("resumes sorted pages after cursor row deletion with duplicate index values", () => {
    const params: QueryParams = {
      index: "byScore",
      filter: { value: { $gte: "10" } },
      sort: "asc",
      limit: 2,
    };
    const initial: FakeRecord[] = [
      { key: "a", createdAt: 1, indexes: { byScore: "10" } },
      { key: "b", createdAt: 2, indexes: { byScore: "10" } },
      { key: "c", createdAt: 3, indexes: { byScore: "10" } },
      { key: "d", createdAt: 4, indexes: { byScore: "11" } },
    ];
    const cursor = encodeQueryPageCursor("items", params, position(initial[1]!, params));
    const mutated: FakeRecord[] = [
      initial[0]!,
      initial[2]!,
      { key: "ab", createdAt: 15, indexes: { byScore: "10" } },
      { key: "bc", createdAt: 25, indexes: { byScore: "10" } },
      initial[3]!,
    ];

    const startIndex = resolveQueryPageStartIndex(
      mutated,
      "items",
      { ...params, cursor },
      position,
    );
    const page = mutated.slice(startIndex, startIndex + 2).map((record) => record.key);

    expect(page).toEqual(["c", "ab"]);
  });
});
