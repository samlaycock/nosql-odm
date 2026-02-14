import {
  type BatchSetResult,
  EngineDocumentAlreadyExistsError,
  EngineDocumentNotFoundError,
  type ComparableVersion,
  type EngineQueryResult,
  type FieldCondition,
  type KeyedDocument,
  type MigrationCriteria,
  type MigrationDocumentMetadata,
  type MigrationLock,
  type MigrationVersionState,
  type QueryEngine,
  type QueryParams,
  type ResolvedIndexKeys,
} from "./types";
import { DefaultMigrator } from "../migrator";

const DEFAULT_KEY_PREFIX = "nosql_odm";
const OUTDATED_PAGE_LIMIT = 100;
const OUTDATED_SYNC_CHUNK_SIZE = 100;
const NULL_INDEX_SIGNATURE = "";
const NULL_INDEX_SIGNATURE_TOKEN = "__null__";

const CREATE_DOCUMENT_SCRIPT = `
local function staleSet(prefix, collection, targetVersion)
  return prefix .. ":migration:stale:" .. collection .. ":" .. targetVersion
end

local function currentAnySet(prefix, collection, targetVersion)
  return prefix .. ":migration:current:any:" .. collection .. ":" .. targetVersion
end

local function currentSignatureSet(prefix, collection, targetVersion, signatureToken)
  return prefix .. ":migration:current:sig:" .. collection .. ":" .. targetVersion .. ":" .. signatureToken
end

local function removePreviousIndexes(prefix, collection, key, targetSetKey)
  local oldTarget = redis.call("HGET", KEYS[1], "migrationTargetVersion")
  local oldState = redis.call("HGET", KEYS[1], "migrationVersionState")
  local oldToken = redis.call("HGET", KEYS[1], "migrationIndexSignatureToken")

  if oldToken == false or oldToken == nil or oldToken == "" then
    oldToken = "__null__"
  end

  redis.call("ZREM", targetSetKey, key)

  if oldTarget and oldState then
    if oldState == "stale" then
      redis.call("ZREM", staleSet(prefix, collection, oldTarget), key)
    elseif oldState == "current" then
      redis.call("ZREM", currentAnySet(prefix, collection, oldTarget), key)
      redis.call("ZREM", currentSignatureSet(prefix, collection, oldTarget, oldToken), key)
    end
  end
end

local function addCurrentIndexes(prefix, collection, key, createdAt, targetVersion, versionState, signatureToken, targetSetKey)
  redis.call("ZADD", targetSetKey, tonumber(targetVersion), key)

  if versionState == "stale" then
    redis.call("ZADD", staleSet(prefix, collection, targetVersion), tonumber(createdAt), key)
  elseif versionState == "current" then
    redis.call("ZADD", currentAnySet(prefix, collection, targetVersion), tonumber(createdAt), key)
    redis.call("ZADD", currentSignatureSet(prefix, collection, targetVersion, signatureToken), tonumber(createdAt), key)
  end
end

local mode = ARGV[1]
local prefix = ARGV[2]
local collection = ARGV[3]
local key = ARGV[4]
local createdAtArg = ARGV[5]
local docJson = ARGV[6]
local indexesJson = ARGV[7]
local targetVersion = ARGV[8]
local versionState = ARGV[9]
local indexSignature = ARGV[10]
local indexSignatureToken = ARGV[11]
local expectedWriteVersionRaw = ARGV[12]

local exists = redis.call("EXISTS", KEYS[1]) == 1

if mode == "create" and exists then
  return {0, 0}
end

if mode == "update" and not exists then
  return {0, 0}
end

local previousWriteVersion = tonumber(redis.call("HGET", KEYS[1], "writeVersion") or "1")

if mode == "conditional" then
  local expectedWriteVersion = tonumber(expectedWriteVersionRaw)
  if (not exists) or (not expectedWriteVersion) or previousWriteVersion ~= expectedWriteVersion then
    return {2, 0}
  end
end

local createdAt = redis.call("HGET", KEYS[1], "createdAt")
if not createdAt then
  createdAt = createdAtArg
end

removePreviousIndexes(prefix, collection, key, KEYS[3])

local nextWriteVersion = 1
if exists then
  nextWriteVersion = previousWriteVersion + 1
end

redis.call(
  "HSET",
  KEYS[1],
  "createdAt",
  createdAt,
  "writeVersion",
  tostring(nextWriteVersion),
  "doc",
  docJson,
  "indexes",
  indexesJson,
  "migrationTargetVersion",
  targetVersion,
  "migrationVersionState",
  versionState,
  "migrationIndexSignature",
  indexSignature,
  "migrationIndexSignatureToken",
  indexSignatureToken
)
redis.call("ZADD", KEYS[2], tonumber(createdAt), key)
addCurrentIndexes(prefix, collection, key, createdAt, targetVersion, versionState, indexSignatureToken, KEYS[3])
return {1, nextWriteVersion}
`;

const DELETE_DOCUMENT_SCRIPT = `
local function staleSet(prefix, collection, targetVersion)
  return prefix .. ":migration:stale:" .. collection .. ":" .. targetVersion
end

local function currentAnySet(prefix, collection, targetVersion)
  return prefix .. ":migration:current:any:" .. collection .. ":" .. targetVersion
end

local function currentSignatureSet(prefix, collection, targetVersion, signatureToken)
  return prefix .. ":migration:current:sig:" .. collection .. ":" .. targetVersion .. ":" .. signatureToken
end

local prefix = ARGV[1]
local collection = ARGV[2]
local key = ARGV[3]

local oldTarget = redis.call("HGET", KEYS[1], "migrationTargetVersion")
local oldState = redis.call("HGET", KEYS[1], "migrationVersionState")
local oldToken = redis.call("HGET", KEYS[1], "migrationIndexSignatureToken")

if oldToken == false or oldToken == nil or oldToken == "" then
  oldToken = "__null__"
end

redis.call("DEL", KEYS[1])
redis.call("ZREM", KEYS[2], key)
redis.call("ZREM", KEYS[3], key)

if oldTarget and oldState then
  if oldState == "stale" then
    redis.call("ZREM", staleSet(prefix, collection, oldTarget), key)
  elseif oldState == "current" then
    redis.call("ZREM", currentAnySet(prefix, collection, oldTarget), key)
    redis.call("ZREM", currentSignatureSet(prefix, collection, oldTarget, oldToken), key)
  end
end

return 1
`;

const UPDATE_MIGRATION_METADATA_SCRIPT = `
local function staleSet(prefix, collection, targetVersion)
  return prefix .. ":migration:stale:" .. collection .. ":" .. targetVersion
end

local function currentAnySet(prefix, collection, targetVersion)
  return prefix .. ":migration:current:any:" .. collection .. ":" .. targetVersion
end

local function currentSignatureSet(prefix, collection, targetVersion, signatureToken)
  return prefix .. ":migration:current:sig:" .. collection .. ":" .. targetVersion .. ":" .. signatureToken
end

local prefix = ARGV[1]
local collection = ARGV[2]
local key = ARGV[3]
local targetVersion = ARGV[4]
local versionState = ARGV[5]
local indexSignature = ARGV[6]
local indexSignatureToken = ARGV[7]
local expectedWriteVersionRaw = ARGV[8]

if redis.call("EXISTS", KEYS[1]) == 0 then
  return 0
end

local expectedWriteVersion = tonumber(expectedWriteVersionRaw)
local currentWriteVersion = tonumber(redis.call("HGET", KEYS[1], "writeVersion") or "1")
if not expectedWriteVersion or currentWriteVersion ~= expectedWriteVersion then
  return 2
end

local createdAt = redis.call("HGET", KEYS[1], "createdAt")
if not createdAt then
  return 0
end

local oldTarget = redis.call("HGET", KEYS[1], "migrationTargetVersion")
local oldState = redis.call("HGET", KEYS[1], "migrationVersionState")
local oldToken = redis.call("HGET", KEYS[1], "migrationIndexSignatureToken")
if oldToken == false or oldToken == nil or oldToken == "" then
  oldToken = "__null__"
end

redis.call("ZREM", KEYS[2], key)
if oldTarget and oldState then
  if oldState == "stale" then
    redis.call("ZREM", staleSet(prefix, collection, oldTarget), key)
  elseif oldState == "current" then
    redis.call("ZREM", currentAnySet(prefix, collection, oldTarget), key)
    redis.call("ZREM", currentSignatureSet(prefix, collection, oldTarget, oldToken), key)
  end
end

redis.call(
  "HSET",
  KEYS[1],
  "migrationTargetVersion",
  targetVersion,
  "migrationVersionState",
  versionState,
  "migrationIndexSignature",
  indexSignature,
  "migrationIndexSignatureToken",
  indexSignatureToken
)
redis.call("ZADD", KEYS[2], tonumber(targetVersion), key)

if versionState == "stale" then
  redis.call("ZADD", staleSet(prefix, collection, targetVersion), tonumber(createdAt), key)
elseif versionState == "current" then
  redis.call("ZADD", currentAnySet(prefix, collection, targetVersion), tonumber(createdAt), key)
  redis.call("ZADD", currentSignatureSet(prefix, collection, targetVersion, indexSignatureToken), tonumber(createdAt), key)
end

return 1
`;

const FETCH_TARGET_CANDIDATES_SCRIPT = `
return redis.call("ZRANGEBYSCORE", KEYS[1], ARGV[1], ARGV[2], "LIMIT", 0, tonumber(ARGV[3]))
`;

const FETCH_ZSET_PAGE_BY_SCORE_SCRIPT = `
return redis.call("ZRANGEBYSCORE", KEYS[1], ARGV[1], "+inf", "WITHSCORES", "LIMIT", 0, tonumber(ARGV[2]))
`;

const BUILD_MISMATCH_SET_SCRIPT = `
redis.call("DEL", KEYS[1])

if redis.call("EXISTS", KEYS[2]) == 0 then
  return 0
end

if redis.call("EXISTS", KEYS[3]) == 1 then
  redis.call("ZDIFFSTORE", KEYS[1], 2, KEYS[2], KEYS[3])
else
  redis.call("ZUNIONSTORE", KEYS[1], 1, KEYS[2])
end

redis.call("EXPIRE", KEYS[1], tonumber(ARGV[1]))
return redis.call("ZCARD", KEYS[1])
`;

const ACQUIRE_LOCK_SCRIPT = `
if redis.call("EXISTS", KEYS[1]) == 0 then
  redis.call("HSET", KEYS[1], "lockId", ARGV[1], "acquiredAt", ARGV[2])
  return 1
end

if ARGV[3] ~= "1" then
  return 0
end

local existing = tonumber(redis.call("HGET", KEYS[1], "acquiredAt"))
if not existing then
  return -1
end

local staleBefore = tonumber(ARGV[4])
if existing <= staleBefore then
  redis.call("HSET", KEYS[1], "lockId", ARGV[1], "acquiredAt", ARGV[2])
  return 1
end

return 0
`;

const RELEASE_LOCK_SCRIPT = `
local lockId = redis.call("HGET", KEYS[1], "lockId")
if lockId and lockId == ARGV[1] then
  redis.call("DEL", KEYS[1])
  return 1
end

return 0
`;

const SAVE_CHECKPOINT_SCRIPT = `
local lockId = redis.call("HGET", KEYS[1], "lockId")
if lockId and lockId == ARGV[1] then
  redis.call("SET", KEYS[2], ARGV[2])
  return 1
end

return 0
`;

interface RedisEvalOptionsLike {
  keys: string[];
  arguments: string[];
}

interface RedisClientLike {
  get(key: string): Promise<unknown>;
  del(keys: string | string[]): Promise<unknown>;
  hGetAll(key: string): Promise<unknown>;
  zRange(key: string, start: number, stop: number): Promise<unknown>;
  zRem(key: string, members: string | string[]): Promise<unknown>;
  incr(key: string): Promise<unknown>;
  eval(script: string, options: RedisEvalOptionsLike): Promise<unknown>;
}

export interface RedisEngineOptions {
  client: RedisClientLike;
  keyPrefix?: string;
}

export interface RedisQueryEngine extends QueryEngine<never> {}

interface StoredDocumentRecord {
  key: string;
  createdAt: number;
  writeVersion: number;
  doc: Record<string, unknown>;
  indexes: ResolvedIndexKeys;
  migrationTargetVersion: number;
  migrationVersionState: MigrationVersionState;
  migrationIndexSignature: string | null;
  migrationIndexSignatureToken: string;
}

interface LockRecord {
  lockId: string;
  acquiredAt: number;
}

interface MigrationMetadata {
  targetVersion: number;
  versionState: MigrationVersionState;
  indexSignature: string | null;
}

type UpsertMode = "create" | "put" | "update" | "conditional";

interface OutdatedCursorState {
  phase: "stale" | "mismatch";
  staleScore: number | null;
  mismatchScore: number | null;
  criteriaVersion: number;
  expectedSignature: string;
}

export function redisEngine(options: RedisEngineOptions): RedisQueryEngine {
  const client = options.client;
  const keyPrefix = normalizePrefix(options.keyPrefix ?? DEFAULT_KEY_PREFIX);

  const engine: RedisQueryEngine = {
    async get(collection, key) {
      const record = await loadDocumentRecord(client, keyPrefix, collection, key);

      if (!record) {
        return null;
      }

      return structuredClone(record.doc);
    },

    async create(collection, key, doc, indexes, _options, migrationMetadata) {
      const result = await upsertDocument(
        client,
        keyPrefix,
        collection,
        key,
        doc,
        indexes,
        "create",
        undefined,
        migrationMetadata,
      );

      if (result.status !== "applied") {
        throw new EngineDocumentAlreadyExistsError(collection, key);
      }
    },

    async put(collection, key, doc, indexes, _options, migrationMetadata) {
      await upsertDocument(
        client,
        keyPrefix,
        collection,
        key,
        doc,
        indexes,
        "put",
        undefined,
        migrationMetadata,
      );
    },

    async update(collection, key, doc, indexes, _options, migrationMetadata) {
      const result = await upsertDocument(
        client,
        keyPrefix,
        collection,
        key,
        doc,
        indexes,
        "update",
        undefined,
        migrationMetadata,
      );

      if (result.status !== "applied") {
        throw new EngineDocumentNotFoundError(collection, key);
      }
    },

    async delete(collection, key) {
      const result = await evalScript(
        client,
        DELETE_DOCUMENT_SCRIPT,
        [
          documentHashKey(keyPrefix, collection, key),
          collectionOrderKey(keyPrefix, collection),
          migrationTargetVersionSetKey(keyPrefix, collection),
        ],
        [keyPrefix, collection, key],
      );

      parseInteger(result, "Redis returned an invalid delete result");
    },

    async query(collection, params) {
      const records = await listCollectionDocuments(client, keyPrefix, collection);
      const matched = matchDocuments(records, params);

      return paginate(matched, params);
    },

    async batchGet(collection, keys) {
      const fetched = new Map<string, StoredDocumentRecord>();
      const results: KeyedDocument[] = [];

      for (const key of uniqueStrings(keys)) {
        const record = await loadDocumentRecord(client, keyPrefix, collection, key);

        if (!record) {
          continue;
        }

        fetched.set(key, record);
      }

      for (const key of keys) {
        const record = fetched.get(key);

        if (!record) {
          continue;
        }

        results.push({
          key,
          doc: structuredClone(record.doc),
        });
      }

      return results;
    },

    async batchSet(collection, items) {
      for (const item of items) {
        await upsertDocument(
          client,
          keyPrefix,
          collection,
          item.key,
          item.doc,
          item.indexes,
          "put",
          undefined,
          item.migrationMetadata,
        );
      }
    },

    async batchSetWithResult(collection, items) {
      const persistedKeys: string[] = [];
      const conflictedKeys: string[] = [];

      for (const item of items) {
        const expectedWriteVersion =
          item.expectedWriteToken === undefined
            ? undefined
            : parseWriteToken(item.expectedWriteToken);
        const mode: UpsertMode = expectedWriteVersion === undefined ? "put" : "conditional";
        const result = await upsertDocument(
          client,
          keyPrefix,
          collection,
          item.key,
          item.doc,
          item.indexes,
          mode,
          expectedWriteVersion,
          item.migrationMetadata,
        );

        if (result.status === "conflict") {
          conflictedKeys.push(item.key);
          continue;
        }

        if (result.status === "missing") {
          conflictedKeys.push(item.key);
          continue;
        }

        persistedKeys.push(item.key);
      }

      return {
        persistedKeys,
        conflictedKeys,
      } satisfies BatchSetResult;
    },

    async batchDelete(collection, keys) {
      for (const key of keys) {
        const result = await evalScript(
          client,
          DELETE_DOCUMENT_SCRIPT,
          [
            documentHashKey(keyPrefix, collection, key),
            collectionOrderKey(keyPrefix, collection),
            migrationTargetVersionSetKey(keyPrefix, collection),
          ],
          [keyPrefix, collection, key],
        );

        parseInteger(result, "Redis returned an invalid batchDelete result");
      }
    },

    migration: {
      async acquireLock(collection, options) {
        const lock: MigrationLock = {
          id: randomId(),
          collection,
          acquiredAt: Date.now(),
        };

        const ttl = options?.ttl;
        const allowSteal = ttl !== undefined && Number.isFinite(ttl) && ttl >= 0;
        const staleBefore = allowSteal ? lock.acquiredAt - ttl : 0;
        const result = await evalScript(
          client,
          ACQUIRE_LOCK_SCRIPT,
          [migrationLockKey(keyPrefix, collection)],
          [lock.id, String(lock.acquiredAt), allowSteal ? "1" : "0", String(staleBefore)],
        );
        const code = parseInteger(result, "Redis returned an invalid lock acquire result");

        if (code === 1) {
          return lock;
        }

        if (code === 0) {
          return null;
        }

        if (code === -1) {
          throw new Error("Redis returned an invalid migration lock record");
        }

        throw new Error("Redis returned an invalid lock acquire result");
      },

      async releaseLock(lock) {
        const result = await evalScript(
          client,
          RELEASE_LOCK_SCRIPT,
          [migrationLockKey(keyPrefix, lock.collection)],
          [lock.id],
        );

        parseInteger(result, "Redis returned an invalid lock release result");
      },

      async getOutdated(collection, criteria, cursor) {
        await syncMigrationMetadataForCriteria(client, keyPrefix, collection, criteria);
        return getOutdatedDocuments(client, keyPrefix, collection, criteria, cursor);
      },

      async saveCheckpoint(lock, cursor) {
        const result = await evalScript(
          client,
          SAVE_CHECKPOINT_SCRIPT,
          [
            migrationLockKey(keyPrefix, lock.collection),
            migrationCheckpointKey(keyPrefix, lock.collection),
          ],
          [lock.id, cursor],
        );

        parseInteger(result, "Redis returned an invalid migration checkpoint result");
      },

      async loadCheckpoint(collection) {
        const value = await client.get(migrationCheckpointKey(keyPrefix, collection));

        if (value === null) {
          return null;
        }

        if (typeof value !== "string") {
          throw new Error("Redis returned an invalid migration checkpoint record");
        }

        return value;
      },

      async clearCheckpoint(collection) {
        await client.del(migrationCheckpointKey(keyPrefix, collection));
      },

      async getStatus(collection) {
        const rawLock = await client.hGetAll(migrationLockKey(keyPrefix, collection));
        const fields = parseStringRecord(rawLock, "migration lock record");

        if (Object.keys(fields).length === 0) {
          return null;
        }

        const lock = parseLockRecord(fields);
        const cursorRaw = await client.get(migrationCheckpointKey(keyPrefix, collection));

        if (cursorRaw !== null && typeof cursorRaw !== "string") {
          throw new Error("Redis returned an invalid migration checkpoint record");
        }

        return {
          lock: {
            id: lock.lockId,
            collection,
            acquiredAt: lock.acquiredAt,
          },
          cursor: cursorRaw ?? null,
        };
      },
    },
  };

  engine.migrator = new DefaultMigrator(engine);

  return engine;
}

async function evalScript(
  client: RedisClientLike,
  script: string,
  keys: string[],
  args: string[],
): Promise<unknown> {
  return client.eval(script, {
    keys,
    arguments: args,
  });
}

async function nextCreatedAt(
  client: RedisClientLike,
  keyPrefix: string,
  collection: string,
): Promise<number> {
  const raw = await client.incr(collectionSequenceKey(keyPrefix, collection));

  return parseInteger(raw, "Redis returned an invalid sequence value");
}

async function upsertDocument(
  client: RedisClientLike,
  keyPrefix: string,
  collection: string,
  key: string,
  doc: unknown,
  indexes: ResolvedIndexKeys,
  mode: UpsertMode,
  expectedWriteVersion: number | undefined,
  migrationMetadata: MigrationDocumentMetadata | undefined,
): Promise<{ status: "applied" | "missing" | "conflict"; writeVersion: number | null }> {
  const createdAt = await nextCreatedAt(client, keyPrefix, collection);
  const metadata =
    normalizeMigrationMetadata(migrationMetadata) ?? deriveLegacyMetadataFromDocument(doc);
  const result = await evalScript(
    client,
    CREATE_DOCUMENT_SCRIPT,
    [
      documentHashKey(keyPrefix, collection, key),
      collectionOrderKey(keyPrefix, collection),
      migrationTargetVersionSetKey(keyPrefix, collection),
    ],
    [
      mode,
      keyPrefix,
      collection,
      key,
      String(createdAt),
      serializeDocument(doc),
      serializeIndexes(indexes),
      String(metadata.targetVersion),
      metadata.versionState,
      metadata.indexSignature ?? NULL_INDEX_SIGNATURE,
      migrationSignatureToken(metadata.indexSignature),
      expectedWriteVersion === undefined ? "" : String(expectedWriteVersion),
    ],
  );
  const [statusCode, writeVersionRaw] = parseIntegerTuple(
    result,
    2,
    "Redis returned an invalid upsert result",
  );

  if (statusCode === 1) {
    return {
      status: "applied",
      writeVersion: writeVersionRaw ?? null,
    };
  }

  if (statusCode === 0) {
    return {
      status: mode === "update" ? "missing" : "conflict",
      writeVersion: null,
    };
  }

  if (statusCode === 2) {
    return {
      status: "conflict",
      writeVersion: null,
    };
  }

  throw new Error("Redis returned an invalid upsert result");
}

async function listCollectionDocuments(
  client: RedisClientLike,
  keyPrefix: string,
  collection: string,
): Promise<StoredDocumentRecord[]> {
  const rawKeys = await client.zRange(collectionOrderKey(keyPrefix, collection), 0, -1);
  const keys = parseStringArray(rawKeys, "query key list");
  const records: StoredDocumentRecord[] = [];

  for (const key of keys) {
    const record = await loadDocumentRecord(client, keyPrefix, collection, key);

    if (!record) {
      continue;
    }

    records.push(record);
  }

  return records;
}

async function loadDocumentRecord(
  client: RedisClientLike,
  keyPrefix: string,
  collection: string,
  key: string,
): Promise<StoredDocumentRecord | null> {
  const raw = await client.hGetAll(documentHashKey(keyPrefix, collection, key));
  const fields = parseStringRecord(raw, "document record");

  if (Object.keys(fields).length === 0) {
    return null;
  }

  return parseStoredDocumentRecord(key, fields);
}

async function syncMigrationMetadataForCriteria(
  client: RedisClientLike,
  keyPrefix: string,
  collection: string,
  criteria: MigrationCriteria,
): Promise<void> {
  await syncMigrationMetadataPhase(client, keyPrefix, collection, criteria, "low");
  await syncMigrationMetadataPhase(client, keyPrefix, collection, criteria, "high");
}

type SyncPhase = "low" | "high";

async function syncMigrationMetadataPhase(
  client: RedisClientLike,
  keyPrefix: string,
  collection: string,
  criteria: MigrationCriteria,
  phase: SyncPhase,
): Promise<void> {
  let loops = 0;

  while (true) {
    loops += 1;
    if (loops > 2048) {
      return;
    }

    const keysRaw = await evalScript(
      client,
      FETCH_TARGET_CANDIDATES_SCRIPT,
      [migrationTargetVersionSetKey(keyPrefix, collection)],
      [
        phase === "low" ? "-inf" : `(${String(criteria.version)}`,
        phase === "low" ? `(${String(criteria.version)}` : "+inf",
        String(OUTDATED_SYNC_CHUNK_SIZE),
      ],
    );
    const keys = parseStringArray(keysRaw, "migration sync key list");

    if (keys.length === 0) {
      return;
    }

    let changed = false;

    for (const key of keys) {
      const record = await loadDocumentRecord(client, keyPrefix, collection, key);

      if (!record) {
        await client.zRem(migrationTargetVersionSetKey(keyPrefix, collection), key);
        changed = true;
        continue;
      }

      const metadata = deriveMetadataForCriteria(record.doc, criteria);
      const updateRaw = await evalScript(
        client,
        UPDATE_MIGRATION_METADATA_SCRIPT,
        [
          documentHashKey(keyPrefix, collection, key),
          migrationTargetVersionSetKey(keyPrefix, collection),
        ],
        [
          keyPrefix,
          collection,
          key,
          String(metadata.targetVersion),
          metadata.versionState,
          metadata.indexSignature ?? NULL_INDEX_SIGNATURE,
          migrationSignatureToken(metadata.indexSignature),
          String(record.writeVersion),
        ],
      );
      const status = parseInteger(updateRaw, "Redis returned an invalid metadata update result");

      if (status === 1) {
        changed = true;
      }
    }

    if (!changed) {
      return;
    }
  }
}

async function getOutdatedDocuments(
  client: RedisClientLike,
  keyPrefix: string,
  collection: string,
  criteria: MigrationCriteria,
  cursor: string | undefined,
): Promise<EngineQueryResult> {
  const expectedSignature = computeIndexSignature(criteria.indexes);
  const expectedSignatureToken = migrationSignatureToken(expectedSignature);
  const mismatchKey = migrationMismatchTempKey(
    keyPrefix,
    collection,
    criteria.version,
    expectedSignatureToken,
  );

  await evalScript(
    client,
    BUILD_MISMATCH_SET_SCRIPT,
    [
      mismatchKey,
      migrationCurrentAnySetKey(keyPrefix, collection, criteria.version),
      migrationCurrentSignatureSetKey(
        keyPrefix,
        collection,
        criteria.version,
        expectedSignatureToken,
      ),
    ],
    ["30"],
  );

  let state = decodeOutdatedCursor(cursor, criteria.version, expectedSignature);
  const requested = OUTDATED_PAGE_LIMIT;
  let remaining = requested;
  const candidates: Array<{ key: string; source: "stale" | "mismatch" }> = [];
  let nextCursor: string | null = null;
  const staleSetKey = migrationStaleSetKey(keyPrefix, collection, criteria.version);

  if (state.phase === "stale") {
    const stale = await fetchZsetPageByScore(
      client,
      staleSetKey,
      state.staleScore,
      remaining + 1,
      "outdated stale key list",
    );

    if (stale.length > remaining) {
      const page = stale.slice(0, remaining);

      for (const item of page) {
        candidates.push({ key: item.key, source: "stale" });
      }

      const tail = page[page.length - 1];

      nextCursor = encodeOutdatedCursor({
        phase: "stale",
        staleScore: tail ? tail.score : state.staleScore,
        mismatchScore: state.mismatchScore,
        criteriaVersion: criteria.version,
        expectedSignature,
      });
      remaining = 0;
    } else {
      for (const item of stale) {
        candidates.push({ key: item.key, source: "stale" });
      }

      remaining -= stale.length;
      state = {
        phase: "mismatch",
        staleScore:
          stale.length > 0
            ? (stale[stale.length - 1]?.score ?? state.staleScore)
            : state.staleScore,
        mismatchScore: state.mismatchScore,
        criteriaVersion: criteria.version,
        expectedSignature,
      };
    }
  }

  if (remaining > 0 && state.phase === "mismatch") {
    const mismatch = await fetchZsetPageByScore(
      client,
      mismatchKey,
      state.mismatchScore,
      remaining + 1,
      "outdated mismatch key list",
    );

    if (mismatch.length > remaining) {
      const page = mismatch.slice(0, remaining);

      for (const item of page) {
        candidates.push({ key: item.key, source: "mismatch" });
      }

      const tail = page[page.length - 1];

      nextCursor = encodeOutdatedCursor({
        phase: "mismatch",
        staleScore: state.staleScore,
        mismatchScore: tail ? tail.score : state.mismatchScore,
        criteriaVersion: criteria.version,
        expectedSignature,
      });
    } else {
      for (const item of mismatch) {
        candidates.push({ key: item.key, source: "mismatch" });
      }

      nextCursor = null;
    }
  }

  const documents: KeyedDocument[] = [];

  for (const candidate of candidates) {
    const record = await loadDocumentRecord(client, keyPrefix, collection, candidate.key);

    if (!record) {
      if (candidate.source === "stale") {
        await client.zRem(staleSetKey, candidate.key);
      } else {
        await client.zRem(mismatchKey, candidate.key);
      }
      continue;
    }

    if (!isRecordOutdatedForCriteria(record, criteria.version, expectedSignature)) {
      continue;
    }

    documents.push({
      key: candidate.key,
      doc: structuredClone(record.doc),
      writeToken: String(record.writeVersion),
    });
  }

  return {
    documents,
    cursor: nextCursor,
  };
}

async function fetchZsetPageByScore(
  client: RedisClientLike,
  key: string,
  afterScore: number | null,
  count: number,
  context: string,
): Promise<Array<{ key: string; score: number }>> {
  const min = afterScore === null ? "-inf" : `(${String(afterScore)}`;
  const raw = await evalScript(
    client,
    FETCH_ZSET_PAGE_BY_SCORE_SCRIPT,
    [key],
    [min, String(count)],
  );

  if (!Array.isArray(raw) || raw.length % 2 !== 0) {
    throw new Error(`Redis returned an invalid ${context}`);
  }

  const results: Array<{ key: string; score: number }> = [];

  for (let i = 0; i < raw.length; i += 2) {
    const rawKey = raw[i];
    const rawScore = raw[i + 1];

    if (typeof rawKey !== "string") {
      throw new Error(`Redis returned an invalid ${context}`);
    }

    const score = parseInteger(rawScore, `Redis returned an invalid ${context}`);
    results.push({
      key: rawKey,
      score,
    });
  }

  return results;
}

function parseStoredDocumentRecord(
  key: string,
  fields: Record<string, string>,
): StoredDocumentRecord {
  const createdAt = parseInteger(fields.createdAt, "Redis returned an invalid document record");
  const writeVersion = parseWriteVersion(fields.writeVersion);
  const doc = parseDocument(fields.doc);
  const indexes = parseIndexes(fields.indexes);
  const metadata = parseMigrationMetadataFields(fields, doc);

  return {
    key,
    createdAt,
    writeVersion,
    doc,
    indexes,
    migrationTargetVersion: metadata.targetVersion,
    migrationVersionState: metadata.versionState,
    migrationIndexSignature: metadata.indexSignature,
    migrationIndexSignatureToken: migrationSignatureToken(metadata.indexSignature),
  };
}

function parseLockRecord(fields: Record<string, string>): LockRecord {
  const lockId = fields.lockId;
  const acquiredAt = parseInteger(
    fields.acquiredAt,
    "Redis returned an invalid migration lock record",
  );

  if (typeof lockId !== "string" || lockId.length === 0) {
    throw new Error("Redis returned an invalid migration lock record");
  }

  return {
    lockId,
    acquiredAt,
  };
}

function parseWriteVersion(raw: string | undefined): number {
  if (raw === undefined) {
    return 1;
  }

  const value = parseInteger(raw, "Redis returned an invalid document record");

  if (!Number.isInteger(value) || value <= 0) {
    throw new Error("Redis returned an invalid document record");
  }

  return value;
}

function parseMigrationMetadataFields(
  fields: Record<string, string>,
  doc: Record<string, unknown>,
): MigrationMetadata {
  const targetVersionRaw = fields.migrationTargetVersion;
  const versionStateRaw = fields.migrationVersionState;
  const indexSignatureRaw = fields.migrationIndexSignature;

  if (
    targetVersionRaw === undefined ||
    versionStateRaw === undefined ||
    indexSignatureRaw === undefined
  ) {
    return deriveLegacyMetadataFromDocument(doc);
  }

  const targetVersion = parseInteger(targetVersionRaw, "Redis returned an invalid document record");

  if (
    !Number.isInteger(targetVersion) ||
    targetVersion < 0 ||
    !isMigrationVersionState(versionStateRaw)
  ) {
    throw new Error("Redis returned an invalid document record");
  }

  if (typeof indexSignatureRaw !== "string") {
    throw new Error("Redis returned an invalid document record");
  }

  return {
    targetVersion,
    versionState: versionStateRaw,
    indexSignature: indexSignatureRaw === NULL_INDEX_SIGNATURE ? null : indexSignatureRaw,
  };
}

function normalizeMigrationMetadata(
  raw: MigrationDocumentMetadata | undefined,
): MigrationMetadata | null {
  if (!raw) {
    return null;
  }

  if (
    !Number.isFinite(raw.targetVersion) ||
    Math.floor(raw.targetVersion) !== raw.targetVersion ||
    raw.targetVersion < 0
  ) {
    throw new Error("Redis received invalid migration metadata target version");
  }

  if (!isMigrationVersionState(raw.versionState)) {
    throw new Error("Redis received invalid migration metadata state");
  }

  if (raw.indexSignature !== null && typeof raw.indexSignature !== "string") {
    throw new Error("Redis received invalid migration metadata index signature");
  }

  return {
    targetVersion: raw.targetVersion,
    versionState: raw.versionState,
    indexSignature: raw.indexSignature,
  };
}

function isMigrationVersionState(value: unknown): value is MigrationVersionState {
  return value === "current" || value === "stale" || value === "ahead" || value === "unknown";
}

function deriveLegacyMetadataFromDocument(doc: unknown): MigrationMetadata {
  if (!isRecord(doc)) {
    return {
      targetVersion: 0,
      versionState: "unknown",
      indexSignature: null,
    };
  }

  const rawVersion = normalizeNumericVersionFromUnknown(doc.__v);

  return {
    targetVersion: rawVersion ?? 0,
    versionState: rawVersion === null ? "unknown" : "current",
    indexSignature: computeIndexSignatureFromUnknown(doc.__indexes),
  };
}

function migrationSignatureToken(indexSignature: string | null): string {
  if (indexSignature === null) {
    return NULL_INDEX_SIGNATURE_TOKEN;
  }

  return Buffer.from(indexSignature, "utf8").toString("base64url");
}

function computeIndexSignature(indexes: readonly string[]): string {
  return JSON.stringify(indexes);
}

function computeIndexSignatureFromUnknown(raw: unknown): string | null {
  if (!Array.isArray(raw) || raw.some((value) => typeof value !== "string")) {
    return null;
  }

  return computeIndexSignature(raw as string[]);
}

function normalizeNumericVersionFromUnknown(raw: unknown): number | null {
  if (raw === undefined || raw === null) {
    return null;
  }

  if (typeof raw === "number") {
    return Number.isFinite(raw) ? raw : null;
  }

  if (typeof raw !== "string") {
    return null;
  }

  return normalizeNumericVersion(raw);
}

function parseDocument(raw: unknown): Record<string, unknown> {
  if (typeof raw !== "string") {
    throw new Error("Redis returned an invalid document record");
  }

  let parsed: unknown;

  try {
    parsed = JSON.parse(raw);
  } catch {
    throw new Error("Redis returned an invalid document record");
  }

  if (!isRecord(parsed)) {
    throw new Error("Redis returned an invalid document record");
  }

  return parsed;
}

function parseIndexes(raw: unknown): ResolvedIndexKeys {
  if (typeof raw !== "string") {
    throw new Error("Redis returned an invalid document record");
  }

  let parsed: unknown;

  try {
    parsed = JSON.parse(raw);
  } catch {
    throw new Error("Redis returned an invalid document record");
  }

  if (!isRecord(parsed)) {
    throw new Error("Redis returned an invalid document record");
  }

  const indexes: ResolvedIndexKeys = {};

  for (const [name, value] of Object.entries(parsed)) {
    if (typeof value !== "string") {
      throw new Error("Redis returned an invalid document record");
    }

    indexes[name] = value;
  }

  return indexes;
}

function serializeDocument(doc: unknown): string {
  const cloned = structuredClone(doc);

  if (!isRecord(cloned)) {
    throw new Error("Redis received a non-object document");
  }

  const serialized = JSON.stringify(cloned);

  if (typeof serialized !== "string") {
    throw new Error("Redis failed to serialize document");
  }

  return serialized;
}

function serializeIndexes(indexes: ResolvedIndexKeys): string {
  const normalized: ResolvedIndexKeys = {};

  for (const [name, value] of Object.entries(indexes)) {
    if (typeof value !== "string") {
      throw new Error("Redis received invalid index values");
    }

    normalized[name] = value;
  }

  const serialized = JSON.stringify(normalized);

  if (typeof serialized !== "string") {
    throw new Error("Redis failed to serialize index values");
  }

  return serialized;
}

function parseStringRecord(value: unknown, context: string): Record<string, string> {
  if (!isRecord(value)) {
    throw new Error(`Redis returned an invalid ${context}`);
  }

  const record: Record<string, string> = {};

  for (const [key, raw] of Object.entries(value)) {
    if (typeof raw !== "string") {
      throw new Error(`Redis returned an invalid ${context}`);
    }

    record[key] = raw;
  }

  return record;
}

function parseStringArray(value: unknown, context: string): string[] {
  if (!Array.isArray(value)) {
    throw new Error(`Redis returned an invalid ${context}`);
  }

  const values: string[] = [];

  for (const item of value) {
    if (typeof item !== "string") {
      throw new Error(`Redis returned an invalid ${context}`);
    }

    values.push(item);
  }

  return values;
}

function parseInteger(value: unknown, message: string): number {
  if (typeof value === "number") {
    if (!Number.isFinite(value)) {
      throw new Error(message);
    }

    return value;
  }

  if (typeof value === "bigint") {
    const asNumber = Number(value);

    if (!Number.isFinite(asNumber)) {
      throw new Error(message);
    }

    return asNumber;
  }

  if (typeof value === "string") {
    const parsed = Number(value);

    if (!Number.isFinite(parsed)) {
      throw new Error(message);
    }

    return parsed;
  }

  throw new Error(message);
}

function parseIntegerTuple(value: unknown, expectedLength: number, message: string): number[] {
  if (!Array.isArray(value) || value.length < expectedLength) {
    throw new Error(message);
  }

  const parsed: number[] = [];

  for (let i = 0; i < expectedLength; i++) {
    parsed.push(parseInteger(value[i], message));
  }

  return parsed;
}

function matchDocuments(
  records: StoredDocumentRecord[],
  params: QueryParams,
): StoredDocumentRecord[] {
  const indexName = params.index;
  const results: StoredDocumentRecord[] = [];

  for (const record of records) {
    if (!indexName || !params.filter) {
      results.push(record);
      continue;
    }

    const indexValue = record.indexes[indexName];

    if (indexValue !== undefined && matchesFilter(indexValue, params.filter.value)) {
      results.push(record);
    }
  }

  if (params.sort && indexName) {
    results.sort((a, b) => {
      const aValue = a.indexes[indexName] ?? "";
      const bValue = b.indexes[indexName] ?? "";
      const base =
        params.sort === "desc" ? bValue.localeCompare(aValue) : aValue.localeCompare(bValue);

      if (base !== 0) {
        return base;
      }

      if (a.createdAt !== b.createdAt) {
        return a.createdAt - b.createdAt;
      }

      return a.key.localeCompare(b.key);
    });
  }

  return results;
}

function paginate(records: StoredDocumentRecord[], params: QueryParams): EngineQueryResult {
  let startIndex = 0;

  if (params.cursor) {
    const cursorIndex = records.findIndex((record) => record.key === params.cursor);

    if (cursorIndex !== -1) {
      startIndex = cursorIndex + 1;
    }
  }

  const normalizedLimit = normalizeLimit(params.limit);
  const limit = normalizedLimit ?? records.length;
  const hasLimit = normalizedLimit !== null;

  if (limit <= 0) {
    return {
      documents: [],
      cursor: null,
    };
  }

  const page = records.slice(startIndex, startIndex + limit);
  const cursor =
    page.length > 0 && hasLimit && startIndex + limit < records.length
      ? page[page.length - 1]!.key
      : null;

  return {
    documents: page.map((record) => ({
      key: record.key,
      doc: structuredClone(record.doc),
    })),
    cursor,
  };
}

function matchesFilter(indexValue: string, filter: string | number | FieldCondition): boolean {
  if (typeof filter === "string" || typeof filter === "number") {
    return indexValue === String(filter);
  }

  return matchesCondition(indexValue, filter);
}

function matchesCondition(value: string, condition: FieldCondition): boolean {
  if (condition.$eq !== undefined && value !== String(condition.$eq as string | number)) {
    return false;
  }

  if (condition.$gt !== undefined && !(value > String(condition.$gt as string | number))) {
    return false;
  }

  if (condition.$gte !== undefined && !(value >= String(condition.$gte as string | number))) {
    return false;
  }

  if (condition.$lt !== undefined && !(value < String(condition.$lt as string | number))) {
    return false;
  }

  if (condition.$lte !== undefined && !(value <= String(condition.$lte as string | number))) {
    return false;
  }

  if (condition.$begins !== undefined && !value.startsWith(condition.$begins)) {
    return false;
  }

  if (condition.$between !== undefined) {
    const [low, high] = condition.$between as [string | number, string | number];

    if (!(value >= String(low) && value <= String(high))) {
      return false;
    }
  }

  return true;
}

function isRecordOutdatedForCriteria(
  record: StoredDocumentRecord,
  criteriaVersion: number,
  expectedSignature: string,
): boolean {
  if (record.migrationTargetVersion < criteriaVersion) {
    return true;
  }

  if (record.migrationTargetVersion > criteriaVersion) {
    return false;
  }

  if (record.migrationVersionState === "stale") {
    return true;
  }

  if (record.migrationVersionState !== "current") {
    return false;
  }

  return record.migrationIndexSignature !== expectedSignature;
}

function deriveMetadataForCriteria(doc: unknown, criteria: MigrationCriteria): MigrationMetadata {
  if (!isRecord(doc)) {
    return {
      targetVersion: criteria.version,
      versionState: "unknown",
      indexSignature: null,
    };
  }

  const parseVersion = criteria.parseVersion ?? defaultParseVersion;
  const compareVersions = criteria.compareVersions ?? defaultCompareVersions;
  const parsedVersion = parseVersion(doc[criteria.versionField]);
  const versionState = classifyVersionState(parsedVersion, criteria.version, compareVersions);

  return {
    targetVersion: criteria.version,
    versionState,
    indexSignature: computeIndexSignatureFromUnknown(doc[criteria.indexesField]),
  };
}

function classifyVersionState(
  parsedVersion: ComparableVersion | null,
  latest: number,
  compareVersions: (a: ComparableVersion, b: ComparableVersion) => number,
): MigrationVersionState {
  if (parsedVersion === null) {
    return "unknown";
  }

  const cmp = safeCompare(parsedVersion, latest, compareVersions);

  if (cmp === null) {
    return "unknown";
  }

  if (cmp < 0) {
    return "stale";
  }

  if (cmp > 0) {
    return "ahead";
  }

  return "current";
}

function safeCompare(
  a: ComparableVersion,
  b: ComparableVersion,
  compareVersions: (a: ComparableVersion, b: ComparableVersion) => number,
): -1 | 0 | 1 | null {
  try {
    const raw = compareVersions(a, b);

    if (!Number.isFinite(raw)) {
      return null;
    }

    if (raw < 0) {
      return -1;
    }

    if (raw > 0) {
      return 1;
    }

    return 0;
  } catch {
    return null;
  }
}

function defaultParseVersion(raw: unknown): ComparableVersion | null {
  if (raw === undefined || raw === null) {
    return 1;
  }

  if (typeof raw === "number") {
    return Number.isFinite(raw) ? raw : null;
  }

  if (typeof raw === "string") {
    const trimmed = raw.trim();

    if (trimmed.length === 0) {
      return null;
    }

    const numeric = normalizeNumericVersion(trimmed);

    if (numeric !== null) {
      return numeric;
    }

    return trimmed;
  }

  return null;
}

function defaultCompareVersions(a: ComparableVersion, b: ComparableVersion): number {
  if (typeof a === "number" && typeof b === "number") {
    return a - b;
  }

  const numericA = normalizeNumericVersion(String(a));
  const numericB = normalizeNumericVersion(String(b));

  if (numericA !== null && numericB !== null) {
    return numericA - numericB;
  }

  return String(a).localeCompare(String(b), undefined, {
    numeric: true,
    sensitivity: "base",
  });
}

function normalizeNumericVersion(value: string): number | null {
  const trimmed = value.trim();
  const match = /^v?(-?\d+)$/i.exec(trimmed);

  if (!match) {
    return null;
  }

  const parsed = Number(match[1]);

  return Number.isFinite(parsed) ? parsed : null;
}

function normalizeLimit(limit: number | undefined): number | null {
  if (limit === undefined || !Number.isFinite(limit)) {
    return null;
  }

  if (limit <= 0) {
    return 0;
  }

  return Math.floor(limit);
}

function parseWriteToken(raw: string): number {
  if (raw.trim().length === 0 || !/^\d+$/.test(raw)) {
    throw new Error("Redis received an invalid expected write token");
  }

  const parsed = Number(raw);

  if (!Number.isFinite(parsed) || !Number.isInteger(parsed) || parsed <= 0) {
    throw new Error("Redis received an invalid expected write token");
  }

  return parsed;
}

function encodeOutdatedCursor(state: OutdatedCursorState): string {
  return Buffer.from(JSON.stringify(state), "utf8").toString("base64url");
}

function decodeOutdatedCursor(
  cursor: string | undefined,
  criteriaVersion: number,
  expectedSignature: string,
): OutdatedCursorState {
  const fallback: OutdatedCursorState = {
    phase: "stale",
    staleScore: null,
    mismatchScore: null,
    criteriaVersion,
    expectedSignature,
  };

  if (!cursor) {
    return fallback;
  }

  try {
    const parsed = JSON.parse(Buffer.from(cursor, "base64url").toString("utf8"));

    if (!isRecord(parsed)) {
      return fallback;
    }

    if (
      (parsed.phase !== "stale" && parsed.phase !== "mismatch") ||
      parsed.criteriaVersion !== criteriaVersion ||
      parsed.expectedSignature !== expectedSignature ||
      !isNullableFiniteNumber(parsed.staleScore) ||
      !isNullableFiniteNumber(parsed.mismatchScore)
    ) {
      return fallback;
    }

    return {
      phase: parsed.phase,
      staleScore: parsed.staleScore ?? null,
      mismatchScore: parsed.mismatchScore ?? null,
      criteriaVersion,
      expectedSignature,
    };
  } catch {
    return fallback;
  }
}

function isNullableFiniteNumber(value: unknown): value is number | null {
  if (value === null) {
    return true;
  }

  return typeof value === "number" && Number.isFinite(value);
}

function normalizePrefix(prefix: string): string {
  const normalized = prefix.trim();

  if (normalized.length === 0) {
    throw new Error("Redis keyPrefix must not be empty");
  }

  return normalized;
}

function documentHashKey(prefix: string, collection: string, key: string): string {
  return `${prefix}:doc:${collection}:${key}`;
}

function collectionOrderKey(prefix: string, collection: string): string {
  return `${prefix}:order:${collection}`;
}

function collectionSequenceKey(prefix: string, collection: string): string {
  return `${prefix}:sequence:${collection}`;
}

function migrationTargetVersionSetKey(prefix: string, collection: string): string {
  return `${prefix}:migration:target:${collection}`;
}

function migrationStaleSetKey(prefix: string, collection: string, targetVersion: number): string {
  return `${prefix}:migration:stale:${collection}:${String(targetVersion)}`;
}

function migrationCurrentAnySetKey(
  prefix: string,
  collection: string,
  targetVersion: number,
): string {
  return `${prefix}:migration:current:any:${collection}:${String(targetVersion)}`;
}

function migrationCurrentSignatureSetKey(
  prefix: string,
  collection: string,
  targetVersion: number,
  signatureToken: string,
): string {
  return `${prefix}:migration:current:sig:${collection}:${String(targetVersion)}:${signatureToken}`;
}

function migrationMismatchTempKey(
  prefix: string,
  collection: string,
  targetVersion: number,
  signatureToken: string,
): string {
  return `${prefix}:migration:tmp:mismatch:${collection}:${String(targetVersion)}:${signatureToken}`;
}

function migrationLockKey(prefix: string, collection: string): string {
  return `${prefix}:migration:lock:${collection}`;
}

function migrationCheckpointKey(prefix: string, collection: string): string {
  return `${prefix}:migration:checkpoint:${collection}`;
}

function randomId(): string {
  if (typeof crypto !== "undefined" && typeof crypto.randomUUID === "function") {
    return crypto.randomUUID();
  }

  const random = Math.random().toString(16).slice(2);
  const now = Date.now().toString(16);

  return `${now}-${random}`;
}

function uniqueStrings(values: string[]): string[] {
  const unique: string[] = [];
  const seen = new Set<string>();

  for (const value of values) {
    if (seen.has(value)) {
      continue;
    }

    seen.add(value);
    unique.push(value);
  }

  return unique;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}
