import { DefaultMigrator } from "../migrator";
import { encodeQueryPageCursor, resolveQueryPageStartIndex } from "./query-cursor";
import {
  EngineDocumentAlreadyExistsError,
  EngineDocumentNotFoundError,
  EngineUniqueConstraintError,
  type QueryEngine,
  type QueryParams,
  type EngineQueryResult,
  type KeyedDocument,
  type ResolvedIndexKeys,
  type FieldCondition,
  type MigrationLock,
} from "./types";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

interface StoredDocument {
  createdAt: number;
  doc: Record<string, unknown>;
  indexes: ResolvedIndexKeys;
  uniqueIndexes: ResolvedIndexKeys;
}

// ---------------------------------------------------------------------------
// Options
// ---------------------------------------------------------------------------

export interface MemoryEngineOptions {
  /**
   * Called before every write (create/put/batchSet). Throw to simulate a write failure.
   * Receives the collection name, document key, and the document.
   */
  onBeforePut?: (collection: string, key: string, doc: unknown) => void;
}

// ---------------------------------------------------------------------------
// Memory Engine
// ---------------------------------------------------------------------------

export function memoryEngine(options?: MemoryEngineOptions): MemoryQueryEngine {
  const collections = new Map<string, Map<string, StoredDocument>>();
  const uniqueOwnership = new Map<string, Map<string, Map<string, string>>>();
  const locks = new Map<string, MigrationLock>();
  const checkpoints = new Map<string, string>();
  let createdAtSequence = 0;
  let engineOptions = options ?? {};

  function getCollection(collection: string): Map<string, StoredDocument> {
    let col = collections.get(collection);

    if (!col) {
      col = new Map();
      collections.set(collection, col);
    }

    return col;
  }

  function cloneUniqueOwnershipState(
    state: Map<string, Map<string, string>>,
  ): Map<string, Map<string, string>> {
    const cloned = new Map<string, Map<string, string>>();

    for (const [indexName, values] of state) {
      cloned.set(indexName, new Map(values));
    }

    return cloned;
  }

  function getCollectionUniqueOwnership(collection: string): Map<string, Map<string, string>> {
    let state = uniqueOwnership.get(collection);

    if (!state) {
      state = new Map();
      uniqueOwnership.set(collection, state);
    }

    return state;
  }

  function reserveUniqueIndexes(
    collection: string,
    key: string,
    nextUniqueIndexes: ResolvedIndexKeys,
    collectionUniqueState: Map<string, Map<string, string>>,
  ): void {
    for (const [indexName, indexValue] of Object.entries(nextUniqueIndexes)) {
      let ownerByValue = collectionUniqueState.get(indexName);

      if (!ownerByValue) {
        ownerByValue = new Map();
        collectionUniqueState.set(indexName, ownerByValue);
      }

      const existingKey = ownerByValue.get(indexValue);

      if (existingKey && existingKey !== key) {
        throw new EngineUniqueConstraintError(collection, key, indexName, indexValue, existingKey);
      }
    }

    for (const [indexName, ownerByValue] of collectionUniqueState) {
      for (const [indexValue, ownerKey] of ownerByValue) {
        if (ownerKey === key) {
          ownerByValue.delete(indexValue);
        }
      }

      if (ownerByValue.size === 0) {
        collectionUniqueState.delete(indexName);
      }
    }

    for (const [indexName, indexValue] of Object.entries(nextUniqueIndexes)) {
      let ownerByValue = collectionUniqueState.get(indexName);

      if (!ownerByValue) {
        ownerByValue = new Map();
        collectionUniqueState.set(indexName, ownerByValue);
      }

      ownerByValue.set(indexValue, key);
    }
  }

  const engine: MemoryQueryEngine = {
    capabilities: {
      uniqueConstraints: "atomic",
    },

    setOptions(newOptions: MemoryEngineOptions) {
      engineOptions = newOptions;
    },

    async get(collection, key) {
      const col = getCollection(collection);
      const stored = col.get(key);

      return stored ? structuredClone(stored.doc) : null;
    },

    async create(collection, key, doc, indexes, _options, _migrationMetadata, uniqueIndexes) {
      engineOptions.onBeforePut?.(collection, key, doc);

      const col = getCollection(collection);
      const collectionUniqueState = getCollectionUniqueOwnership(collection);

      if (col.has(key)) {
        throw new EngineDocumentAlreadyExistsError(collection, key);
      }

      reserveUniqueIndexes(collection, key, uniqueIndexes ?? {}, collectionUniqueState);

      col.set(key, {
        createdAt: ++createdAtSequence,
        doc: structuredClone(doc) as Record<string, unknown>,
        indexes: { ...indexes },
        uniqueIndexes: { ...uniqueIndexes },
      });
    },

    async put(collection, key, doc, indexes, _options, _migrationMetadata, uniqueIndexes) {
      engineOptions.onBeforePut?.(collection, key, doc);

      const col = getCollection(collection);
      const collectionUniqueState = getCollectionUniqueOwnership(collection);
      const existing = col.get(key);

      reserveUniqueIndexes(collection, key, uniqueIndexes ?? {}, collectionUniqueState);

      col.set(key, {
        createdAt: existing?.createdAt ?? ++createdAtSequence,
        doc: structuredClone(doc) as Record<string, unknown>,
        indexes: { ...indexes },
        uniqueIndexes: { ...uniqueIndexes },
      });
    },

    async update(collection, key, doc, indexes, _options, _migrationMetadata, uniqueIndexes) {
      engineOptions.onBeforePut?.(collection, key, doc);

      const col = getCollection(collection);
      const collectionUniqueState = getCollectionUniqueOwnership(collection);

      const existing = col.get(key);

      if (!existing) {
        throw new EngineDocumentNotFoundError(collection, key);
      }

      reserveUniqueIndexes(collection, key, uniqueIndexes ?? {}, collectionUniqueState);

      col.set(key, {
        createdAt: existing.createdAt,
        doc: structuredClone(doc) as Record<string, unknown>,
        indexes: { ...indexes },
        uniqueIndexes: { ...uniqueIndexes },
      });
    },

    async delete(collection, key) {
      const col = getCollection(collection);
      const collectionUniqueState = getCollectionUniqueOwnership(collection);
      const existing = col.get(key);

      if (existing) {
        reserveUniqueIndexes(collection, key, {}, collectionUniqueState);
      }

      col.delete(key);
    },

    async query(collection, params) {
      const col = getCollection(collection);
      const results = matchDocuments(col, params);

      return paginateQuery(collection, results, params);
    },

    async batchGet(collection, keys) {
      const col = getCollection(collection);
      const results: KeyedDocument[] = [];

      for (const key of keys) {
        const stored = col.get(key);
        if (stored) {
          results.push({ key, doc: structuredClone(stored.doc) });
        }
      }

      return results;
    },

    async batchSet(collection, items) {
      const col = getCollection(collection);
      const collectionUniqueState = getCollectionUniqueOwnership(collection);
      const stagedUniqueState = cloneUniqueOwnershipState(collectionUniqueState);

      // Validate unique constraints for the whole batch up front so duplicate
      // unique values fail before any write is applied.
      for (const item of items) {
        reserveUniqueIndexes(collection, item.key, item.uniqueIndexes ?? {}, stagedUniqueState);
      }

      for (const item of items) {
        engineOptions.onBeforePut?.(collection, item.key, item.doc);

        reserveUniqueIndexes(collection, item.key, item.uniqueIndexes ?? {}, collectionUniqueState);
        const existing = col.get(item.key);

        col.set(item.key, {
          createdAt: existing?.createdAt ?? ++createdAtSequence,
          doc: structuredClone(item.doc) as Record<string, unknown>,
          indexes: { ...item.indexes },
          uniqueIndexes: { ...item.uniqueIndexes },
        });
      }
    },

    async batchDelete(collection, keys) {
      const col = getCollection(collection);
      const collectionUniqueState = getCollectionUniqueOwnership(collection);

      for (const key of keys) {
        if (col.has(key)) {
          reserveUniqueIndexes(collection, key, {}, collectionUniqueState);
        }
        col.delete(key);
      }
    },

    migration: {
      async acquireLock(collection, options?) {
        const existing = locks.get(collection);

        if (existing) {
          // If TTL is provided and valid and the existing lock is stale, replace it.
          const ttl = options?.ttl;
          const canSteal =
            ttl !== undefined &&
            Number.isFinite(ttl) &&
            ttl >= 0 &&
            Date.now() - existing.acquiredAt >= ttl;

          if (!canSteal) {
            return null;
          }
        }

        const lock: MigrationLock = {
          id: crypto.randomUUID(),
          collection,
          acquiredAt: Date.now(),
        };

        locks.set(collection, lock);

        return lock;
      },

      // Only releases the lock if the caller's lock ID matches the stored one,
      // preventing a stale lock holder from releasing a lock acquired by someone else.
      async releaseLock(lock) {
        const existing = locks.get(lock.collection);

        if (existing?.id === lock.id) {
          locks.delete(lock.collection);
        }
      },

      async getOutdated(collection, criteria, cursor?) {
        const col = getCollection(collection);
        const outdated: KeyedStoredDocument[] = [];
        const parseVersion = criteria.parseVersion ?? defaultParseVersion;
        const compareVersions = criteria.compareVersions ?? defaultCompareVersions;

        for (const [key, stored] of col) {
          const doc = stored.doc;
          const parsedVersion = parseVersion(doc[criteria.versionField]);
          const storedIndexes = doc[criteria.indexesField] as string[] | undefined;
          const versionState = classifyVersionState(
            parsedVersion,
            criteria.version,
            compareVersions,
          );

          // Stale versions are outdated and should be migrated.
          if (versionState === "stale") {
            outdated.push({ key, stored });
            continue;
          }

          // Versions that cannot be safely compared/migrated are ignored.
          if (versionState !== "current") {
            continue;
          }

          // Current version but stored indexes don't match expected indexes.
          if (
            !storedIndexes ||
            storedIndexes.length !== criteria.indexes.length ||
            !storedIndexes.every((name, i) => name === criteria.indexes[i])
          ) {
            outdated.push({ key, stored });
          }
        }

        // Reuse paginate for cursor-based pagination
        return paginate(outdated, {
          cursor,
          limit: normalizeOutdatedPageLimit(criteria.pageSizeHint),
        });
      },

      async saveCheckpoint(lock, cursor) {
        const existing = locks.get(lock.collection);

        // Ignore stale lock holders so they cannot overwrite checkpoint
        // state after a lock has been replaced (e.g. via TTL-based steal).
        if (existing?.id !== lock.id) {
          return;
        }

        checkpoints.set(lock.collection, cursor);
      },

      async loadCheckpoint(collection) {
        return checkpoints.get(collection) ?? null;
      },

      async clearCheckpoint(collection) {
        checkpoints.delete(collection);
      },

      async getStatus(collection) {
        const lock = locks.get(collection);

        if (!lock) {
          return null;
        }

        return {
          lock: { ...lock },
          cursor: checkpoints.get(collection) ?? null,
        };
      },
    },
  };

  engine.migrator = new DefaultMigrator(engine);

  return engine;
}

// ---------------------------------------------------------------------------
// Extended engine type with test helpers
// ---------------------------------------------------------------------------

export interface MemoryQueryEngine extends QueryEngine<never> {
  /** Replace engine options at runtime (useful for injecting failures mid-test). */
  setOptions(options: MemoryEngineOptions): void;
}

// ---------------------------------------------------------------------------
// Query matching
// ---------------------------------------------------------------------------

interface KeyedStoredDocument {
  key: string;
  stored: StoredDocument;
}

// Filters documents by matching their stored index value for the given index name
// against the query filter. All comparisons are lexicographic (string-based).
// When no index/filter is provided, returns all documents (scan behavior).
function matchDocuments(
  col: Map<string, StoredDocument>,
  params: QueryParams,
): KeyedStoredDocument[] {
  const indexName = params.index;
  const results: KeyedStoredDocument[] = [];

  for (const [key, stored] of col) {
    // No index/filter = return all documents
    if (!indexName || !params.filter) {
      results.push({ key, stored });
      continue;
    }

    const indexValue = stored.indexes[indexName];

    if (indexValue !== undefined && matchesFilter(indexValue, params.filter.value)) {
      results.push({ key, stored });
    }
  }

  if (params.sort && indexName) {
    results.sort((a, b) => {
      const aVal = a.stored.indexes[indexName] ?? "";
      const bVal = b.stored.indexes[indexName] ?? "";
      const base = params.sort === "desc" ? bVal.localeCompare(aVal) : aVal.localeCompare(bVal);

      if (base !== 0) {
        return base;
      }

      if (a.stored.createdAt !== b.stored.createdAt) {
        return a.stored.createdAt - b.stored.createdAt;
      }

      return a.key.localeCompare(b.key);
    });
  }

  return results;
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

type ComparableVersion = string | number;
type VersionState = "current" | "stale" | "ahead" | "unknown";

function classifyVersionState(
  parsedVersion: ComparableVersion | null,
  latest: number,
  compareVersions: (a: ComparableVersion, b: ComparableVersion) => number,
): VersionState {
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

function normalizeOutdatedPageLimit(value: number | undefined): number {
  if (value === undefined || !Number.isFinite(value)) {
    return 100;
  }

  return Math.max(1, Math.floor(value));
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

// ---------------------------------------------------------------------------
// Pagination
// ---------------------------------------------------------------------------

// Query pagination uses opaque, query-bound cursors so continuation remains
// stable when the previous page's last item is removed.
function paginateQuery(
  collection: string,
  results: KeyedStoredDocument[],
  params: QueryParams,
): EngineQueryResult {
  const startIndex = resolveQueryPageStartIndex(
    results,
    collection,
    params,
    (record, queryParams) => ({
      key: record.key,
      createdAt: record.stored.createdAt,
      indexValue: queryParams.index ? (record.stored.indexes[queryParams.index] ?? "") : undefined,
    }),
  );
  const normalizedLimit = normalizeLimit(params.limit);
  const limit = normalizedLimit ?? results.length;
  const hasLimit = normalizedLimit !== null;

  if (limit <= 0) {
    return {
      documents: [],
      cursor: null,
    };
  }

  const page = results.slice(startIndex, startIndex + limit);
  const cursor =
    page.length > 0 && hasLimit && startIndex + limit < results.length
      ? encodeQueryPageCursor(collection, params, {
          key: page[page.length - 1]!.key,
          createdAt: page[page.length - 1]!.stored.createdAt,
          indexValue: params.index
            ? (page[page.length - 1]!.stored.indexes[params.index] ?? "")
            : undefined,
        })
      : null;

  return {
    documents: page.map(({ key, stored }) => ({
      key,
      doc: structuredClone(stored.doc),
    })),
    cursor,
  };
}

function paginate(results: KeyedStoredDocument[], params: QueryParams): EngineQueryResult {
  let startIndex = 0;

  if (params.cursor) {
    const cursorIndex = results.findIndex(({ key }) => key === params.cursor);

    if (cursorIndex !== -1) {
      startIndex = cursorIndex + 1;
    }
  }

  const normalizedLimit = normalizeLimit(params.limit);
  const limit = normalizedLimit ?? results.length;
  const hasLimit = normalizedLimit !== null;

  if (limit <= 0) {
    return {
      documents: [],
      cursor: null,
    };
  }

  const page = results.slice(startIndex, startIndex + limit);
  const cursor =
    page.length > 0 && hasLimit && startIndex + limit < results.length
      ? page[page.length - 1]!.key
      : null;

  return {
    documents: page.map(({ key, stored }) => ({
      key,
      doc: structuredClone(stored.doc),
    })),
    cursor,
  };
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
