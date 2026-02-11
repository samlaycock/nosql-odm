import type {
  QueryEngine,
  QueryParams,
  EngineQueryResult,
  KeyedDocument,
  ResolvedIndexKeys,
  FieldCondition,
  MigrationLock,
} from "./types";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

interface StoredDocument {
  doc: Record<string, unknown>;
  indexes: ResolvedIndexKeys;
}

// ---------------------------------------------------------------------------
// Options
// ---------------------------------------------------------------------------

export interface MemoryEngineOptions {
  /**
   * Called before every put(). Throw to simulate a write failure.
   * Receives the collection name, document key, and the document.
   */
  onBeforePut?: (collection: string, key: string, doc: unknown) => void;
}

// ---------------------------------------------------------------------------
// Memory Engine
// ---------------------------------------------------------------------------

export function memoryEngine(options?: MemoryEngineOptions): MemoryQueryEngine {
  const collections = new Map<string, Map<string, StoredDocument>>();
  const locks = new Map<string, MigrationLock>();
  const checkpoints = new Map<string, string>();
  let engineOptions = options ?? {};

  function getCollection(collection: string): Map<string, StoredDocument> {
    let col = collections.get(collection);

    if (!col) {
      col = new Map();
      collections.set(collection, col);
    }

    return col;
  }

  const engine: MemoryQueryEngine = {
    setOptions(newOptions: MemoryEngineOptions) {
      engineOptions = newOptions;
    },

    async get(collection, key) {
      const col = getCollection(collection);
      const stored = col.get(key);

      return stored ? structuredClone(stored.doc) : null;
    },

    async put(collection, key, doc, indexes) {
      engineOptions.onBeforePut?.(collection, key, doc);

      const col = getCollection(collection);

      col.set(key, {
        doc: structuredClone(doc) as Record<string, unknown>,
        indexes: { ...indexes },
      });
    },

    async delete(collection, key) {
      const col = getCollection(collection);

      col.delete(key);
    },

    async query(collection, params) {
      const col = getCollection(collection);
      const results = matchDocuments(col, params);

      return paginate(results, params);
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

      for (const item of items) {
        engineOptions.onBeforePut?.(collection, item.key, item.doc);

        col.set(item.key, {
          doc: structuredClone(item.doc) as Record<string, unknown>,
          indexes: { ...item.indexes },
        });
      }
    },

    async batchDelete(collection, keys) {
      const col = getCollection(collection);

      for (const key of keys) {
        col.delete(key);
      }
    },

    migration: {
      async acquireLock(collection, options?) {
        const existing = locks.get(collection);

        if (existing) {
          // If TTL is provided and the existing lock is stale, replace it.
          if (options?.ttl !== undefined && Date.now() - existing.acquiredAt >= options.ttl) {
            // Stale lock — fall through to create a new one
          } else {
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

        for (const [key, stored] of col) {
          const doc = stored.doc;
          const docVersion = (doc[criteria.versionField] as number | undefined) ?? 1;
          const storedIndexes = doc[criteria.indexesField] as string[] | undefined;

          // Document is outdated if version is stale
          if (docVersion < criteria.version) {
            outdated.push({ key, stored });
            continue;
          }

          // Or if stored indexes don't match expected indexes
          if (
            !storedIndexes ||
            storedIndexes.length !== criteria.indexes.length ||
            !storedIndexes.every((name, i) => name === criteria.indexes[i])
          ) {
            outdated.push({ key, stored });
          }
        }

        // Reuse paginate for cursor-based pagination
        return paginate(outdated, { cursor, limit: 100 });
      },

      async saveCheckpoint(lock, cursor) {
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

      return params.sort === "desc" ? bVal.localeCompare(aVal) : aVal.localeCompare(bVal);
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

// ---------------------------------------------------------------------------
// Pagination
// ---------------------------------------------------------------------------

// Pagination uses key-based cursors: the cursor is always the key of the last
// item on the previous page. The next page starts after that key.
function paginate(results: KeyedStoredDocument[], params: QueryParams): EngineQueryResult {
  let startIndex = 0;

  if (params.cursor) {
    const cursorIndex = results.findIndex(({ key }) => key === params.cursor);

    if (cursorIndex !== -1) {
      startIndex = cursorIndex + 1;
    }
  }

  const limit = params.limit ?? results.length;

  if (limit <= 0) {
    return {
      documents: [],
      cursor: null,
    };
  }

  const page = results.slice(startIndex, startIndex + limit);
  const cursor =
    page.length > 0 && startIndex + limit < results.length ? page[page.length - 1]!.key : null;

  return {
    documents: page.map(({ key, stored }) => ({
      key,
      doc: structuredClone(stored.doc),
    })),
    cursor,
  };
}
