import { beforeEach, describe, expect, test } from "bun:test";

import type { QueryEngine } from "../../src/engines/types";

import { redisEngine } from "../../src/engines/redis";

const INDEX_MEMBER_CREATED_AT_WIDTH = 20;

type StoredHash = Record<string, string>;

class MockRedisClient {
  readonly hashes = new Map<string, StoredHash>();
  readonly strings = new Map<string, string>();
  readonly zsets = new Map<string, Map<string, number>>();
  readonly counters = new Map<string, number>();
  readonly zRangeCalls: string[] = [];
  batchFetchCalls = 0;
  indexPageCalls = 0;
  hashReads = 0;

  async get(key: string): Promise<unknown> {
    return this.strings.get(key) ?? null;
  }

  async del(keys: string | string[]): Promise<unknown> {
    const values = Array.isArray(keys) ? keys : [keys];

    for (const key of values) {
      this.hashes.delete(key);
      this.strings.delete(key);
      this.zsets.delete(key);
    }

    return values.length;
  }

  async hGetAll(key: string): Promise<unknown> {
    this.hashReads += 1;
    return { ...this.hashes.get(key) };
  }

  async zRange(key: string, start: number, stop: number): Promise<unknown> {
    this.zRangeCalls.push(key);
    const values = [...(this.zsets.get(key)?.entries() ?? [])]
      .sort((a, b) => {
        if (a[1] !== b[1]) {
          return a[1] - b[1];
        }

        return a[0].localeCompare(b[0]);
      })
      .map(([member]) => member);

    const normalizedStop = stop < 0 ? values.length + stop : stop;
    return values.slice(start, normalizedStop + 1);
  }

  async zRem(key: string, members: string | string[]): Promise<unknown> {
    const set = this.zsets.get(key);
    if (!set) {
      return 0;
    }

    let removed = 0;

    for (const member of Array.isArray(members) ? members : [members]) {
      if (set.delete(member)) {
        removed += 1;
      }
    }

    return removed;
  }

  async incr(key: string): Promise<unknown> {
    const current = Number(this.counters.get(key) ?? 0) + 1;
    this.counters.set(key, current);
    return current;
  }

  async eval(script: string, options: { keys: string[]; arguments: string[] }): Promise<unknown> {
    if (script.includes("local mode = ARGV[1]")) {
      return this.applyUpsert(options.keys, options.arguments);
    }

    if (
      script.includes("local prefix = ARGV[1]") &&
      script.includes('local createdAt = redis.call("HGET", KEYS[1], "createdAt")')
    ) {
      return this.applyDelete(options.keys, options.arguments);
    }

    if (script.includes("HMGET") && script.includes("documentHashKey(KEYS[1], KEYS[2], key)")) {
      this.batchFetchCalls += 1;
      return this.fetchBatchDocuments(options.keys, options.arguments);
    }

    if (script.includes("ZREVRANGEBYLEX") && script.includes("ZRANGEBYLEX")) {
      this.indexPageCalls += 1;
      return this.fetchIndexPage(options.keys[0]!, options.arguments);
    }

    throw new Error(`Unsupported mock Redis script: ${script.slice(0, 40)}`);
  }

  private applyUpsert(keys: string[], args: string[]): [number, number] {
    const [documentKey, orderKey] = keys;
    const [
      mode,
      prefix,
      collection,
      key,
      createdAtArg,
      docJson,
      indexesJson,
      targetVersion,
      versionState,
      indexSignature,
      indexSignatureToken,
      expectedWriteVersionRaw,
      uniqueIndexesProvided,
      nextUniqueIndexesJson,
    ] = args;
    const existing = this.hashes.get(documentKey!);

    if (mode === "create" && existing) {
      return [0, 0];
    }

    if (mode === "update" && !existing) {
      return [0, 0];
    }

    const previousWriteVersion = Number(existing?.writeVersion ?? "1");
    if (
      mode === "conditional" &&
      (!existing || previousWriteVersion !== Number(expectedWriteVersionRaw))
    ) {
      return [2, 0];
    }

    const createdAt = existing?.createdAt ?? createdAtArg!;
    const previousIndexes = parseIndexes(existing?.indexes ?? "{}");
    const nextIndexes = parseIndexes(indexesJson!);

    for (const [indexName, indexValue] of Object.entries(previousIndexes)) {
      this.zsets
        .get(indexSetKey(prefix!, collection!, indexName))
        ?.delete(encodeIndexMember(indexValue, Number(createdAt), key!));
    }

    for (const [indexName, indexValue] of Object.entries(nextIndexes)) {
      this.zAdd(
        indexSetKey(prefix!, collection!, indexName),
        0,
        encodeIndexMember(indexValue, Number(createdAt), key!),
      );
    }

    this.zAdd(orderKey!, Number(createdAt), key!);

    this.hashes.set(documentKey!, {
      createdAt,
      writeVersion: String(existing ? previousWriteVersion + 1 : 1),
      doc: docJson!,
      indexes: indexesJson!,
      uniqueIndexes:
        uniqueIndexesProvided === "1" ? nextUniqueIndexesJson! : (existing?.uniqueIndexes ?? "{}"),
      migrationTargetVersion: targetVersion!,
      migrationVersionState: versionState!,
      migrationIndexSignature: indexSignature!,
      migrationIndexSignatureToken: indexSignatureToken!,
    });

    return [1, existing ? previousWriteVersion + 1 : 1];
  }

  private applyDelete(keys: string[], args: string[]): number {
    const [documentKey, orderKey] = keys;
    const [prefix, collection, key] = args;
    const existing = this.hashes.get(documentKey!);

    if (existing) {
      const storedIndexes = existing["indexes"] ?? "{}";

      for (const [indexName, indexValue] of Object.entries(parseIndexes(storedIndexes))) {
        this.zsets
          .get(indexSetKey(prefix!, collection!, indexName))
          ?.delete(encodeIndexMember(indexValue, Number(existing.createdAt), key!));
      }
    }

    this.hashes.delete(documentKey!);
    this.zsets.get(orderKey!)?.delete(key!);
    return 1;
  }

  private fetchBatchDocuments(keys: string[], documentKeys: string[]): string[] {
    const [prefix, collection] = keys;
    const results: string[] = [];

    for (const key of documentKeys) {
      const record = this.hashes.get(documentHashKey(prefix!, collection!, key));

      if (!record) {
        continue;
      }

      results.push(
        JSON.stringify({
          key,
          createdAt: record.createdAt,
          writeVersion: record.writeVersion,
          doc: record.doc,
          indexes: record.indexes,
          migrationTargetVersion: record.migrationTargetVersion,
          migrationVersionState: record.migrationVersionState,
          migrationIndexSignature: record.migrationIndexSignature,
        }),
      );
    }

    return results;
  }

  private fetchIndexPage(key: string, args: string[]): string[] {
    const [sort, boundA, boundB, countRaw] = args;
    const count = Number(countRaw);
    const values = [...(this.zsets.get(key)?.keys() ?? [])].sort((a, b) => a.localeCompare(b));
    const filtered = values.filter((value) =>
      sort === "desc"
        ? matchesLexBound(value, boundA!, true) && matchesLexBound(value, boundB!, false)
        : matchesLexBound(value, boundA!, false) && matchesLexBound(value, boundB!, true),
    );
    const ordered = sort === "desc" ? filtered.reverse() : filtered;
    return ordered.slice(0, count);
  }

  private zAdd(key: string, score: number, member: string): void {
    let set = this.zsets.get(key);

    if (!set) {
      set = new Map();
      this.zsets.set(key, set);
    }

    set.set(member, score);
  }
}

let client: MockRedisClient;
let engine: QueryEngine<never>;

beforeEach(() => {
  client = new MockRedisClient();
  engine = redisEngine({
    client,
    keyPrefix: "test",
  });
});

describe("redisEngine", () => {
  test("batchGet uses a single batched document fetch", async () => {
    await engine.put("users", "u1", { id: "u1" }, { primary: "u1" });
    await engine.put("users", "u2", { id: "u2" }, { primary: "u2" });

    client.batchFetchCalls = 0;
    client.hashReads = 0;

    const results = await engine.batchGet("users", ["u2", "u1", "u2"]);

    expect(results.map((item) => item.key)).toEqual(["u2", "u1", "u2"]);
    expect(client.batchFetchCalls).toBe(1);
    expect(client.hashReads).toBe(0);
  });

  test("sorted indexed query does not fall back to collection order scans", async () => {
    await engine.put("users", "u1", { id: "u1" }, { byRole: "member#a" });
    await engine.put("users", "u2", { id: "u2" }, { byRole: "member#b" });

    client.zRangeCalls.length = 0;

    const results = await engine.query("users", {
      index: "byRole",
      filter: { value: { $begins: "member#" } },
      sort: "asc",
    });

    expect(results.documents.map((item) => item.key)).toEqual(["u1", "u2"]);
    expect(client.indexPageCalls).toBeGreaterThan(0);
    expect(client.zRangeCalls).not.toContain(collectionOrderKey("test", "users"));
  });

  test("update and delete keep index zsets in sync", async () => {
    await engine.put("users", "u1", { id: "u1" }, { byRole: "member#a" });
    await engine.update("users", "u1", { id: "u1" }, { byRole: "member#b" });

    expect([...client.zsets.get(indexSetKey("test", "users", "byRole"))!.keys()]).toEqual([
      encodeIndexMember("member#b", 1, "u1"),
    ]);

    await engine.delete("users", "u1");

    expect([
      ...(client.zsets.get(indexSetKey("test", "users", "byRole")) ?? new Map()).keys(),
    ]).toEqual([]);
  });
});

function documentHashKey(prefix: string, collection: string, key: string): string {
  return `${prefix}:doc:${collection}:${key}`;
}

function collectionOrderKey(prefix: string, collection: string): string {
  return `${prefix}:order:${collection}`;
}

function indexSetKey(prefix: string, collection: string, indexName: string): string {
  return `${prefix}:index:${collection}:${indexName}`;
}

function encodeIndexMember(indexValue: string, createdAt: number, key: string): string {
  return `${indexValue}\0${String(createdAt).padStart(INDEX_MEMBER_CREATED_AT_WIDTH, "0")}\0${key}`;
}

function parseIndexes(raw: string): Record<string, string> {
  return JSON.parse(raw) as Record<string, string>;
}

function matchesLexBound(value: string, bound: string, upper: boolean): boolean {
  if (bound === "-" || bound === "+") {
    return true;
  }

  const inclusive = bound.startsWith("[");
  const target = bound.slice(1);
  const comparison = value.localeCompare(target);

  if (upper) {
    return inclusive ? comparison <= 0 : comparison < 0;
  }

  return inclusive ? comparison >= 0 : comparison > 0;
}
