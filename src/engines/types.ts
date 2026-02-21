// ---------------------------------------------------------------------------
// Shared engine types — the contract between store ↔ engine
// ---------------------------------------------------------------------------

/**
 * Index values resolved to concrete strings, keyed by index key.
 * Passed to the engine on writes.
 */
export type ResolvedIndexKeys = Record<string, string>;
export type ComparableVersion = string | number;
export type MigrationVersionState = "current" | "stale" | "ahead" | "unknown";

export interface MigrationDocumentMetadata {
  /**
   * The schema version this state was evaluated against.
   */
  targetVersion: number;
  /**
   * Version state relative to targetVersion.
   */
  versionState: MigrationVersionState;
  /**
   * Canonical signature of stored index names for the document.
   */
  indexSignature: string | null;
}

export interface BatchSetItem {
  key: string;
  doc: unknown;
  indexes: ResolvedIndexKeys;
  uniqueIndexes?: ResolvedIndexKeys;
  migrationMetadata?: MigrationDocumentMetadata;
  /**
   * Optional optimistic-write token captured when the document was read.
   * Engines that support conditional writes can use this to avoid clobbering
   * concurrent updates.
   */
  expectedWriteToken?: string;
}

export interface BatchSetResult {
  persistedKeys: string[];
  conflictedKeys: string[];
}

/**
 * Engine-level duplicate-key error used by create() to signal that the
 * storage key already exists in the collection.
 */
export class EngineDocumentAlreadyExistsError extends Error {
  readonly collection: string;
  readonly key: string;

  constructor(collection: string, key: string) {
    super(`Document "${key}" already exists in model "${collection}"`);
    this.name = "EngineDocumentAlreadyExistsError";
    this.collection = collection;
    this.key = key;
  }
}

/**
 * Engine-level missing-key error used by update() to signal that the
 * storage key does not exist in the collection.
 */
export class EngineDocumentNotFoundError extends Error {
  readonly collection: string;
  readonly key: string;

  constructor(collection: string, key: string) {
    super(`Document "${key}" not found in model "${collection}"`);
    this.name = "EngineDocumentNotFoundError";
    this.collection = collection;
    this.key = key;
  }
}

/**
 * Engine-level unique index violation used by write methods when a unique
 * index value is already owned by another document key.
 */
export class EngineUniqueConstraintError extends Error {
  readonly collection: string;
  readonly indexName: string;
  readonly indexValue: string;
  readonly key: string;
  readonly existingKey: string | null;

  constructor(
    collection: string,
    key: string,
    indexName: string,
    indexValue: string,
    existingKey?: string | null,
  ) {
    super(
      `Unique index "${indexName}" violation in model "${collection}" for value "${indexValue}"`,
    );
    this.name = "EngineUniqueConstraintError";
    this.collection = collection;
    this.indexName = indexName;
    this.indexValue = indexValue;
    this.key = key;
    this.existingKey = existingKey ?? null;
  }
}

// ---------------------------------------------------------------------------
// Query
// ---------------------------------------------------------------------------

export type FieldCondition = {
  $eq?: unknown;
  $gt?: unknown;
  $lt?: unknown;
  $gte?: unknown;
  $lte?: unknown;
  $begins?: string;
  $between?: [unknown, unknown];
};

export interface QueryFilter {
  /** Filter on the index value. A plain value is shorthand for $eq. */
  value: string | number | FieldCondition;
}

export interface QueryParams {
  /** Raw index name — mutually exclusive with `where`. */
  index?: string;
  /** Raw index filter — used with `index`. */
  filter?: QueryFilter;
  /** Field-level filter — resolves the index automatically. Mutually exclusive with `index`/`filter`. */
  where?: WhereFilter;
  limit?: number;
  cursor?: string;
  sort?: "asc" | "desc";
}

/**
 * Field-level filter for `where` queries. Exactly one key (the field name)
 * mapping to a plain value (shorthand for $eq) or a FieldCondition.
 */
export type WhereFilter = Record<string, string | number | FieldCondition>;

// ---------------------------------------------------------------------------
// Engine result types
// ---------------------------------------------------------------------------

/**
 * A document paired with its storage key, returned by engine methods
 * so the store can identify documents without guessing.
 */
export interface KeyedDocument {
  key: string;
  doc: unknown;
  /**
   * Optional engine-specific optimistic-write token for conditional updates.
   */
  writeToken?: string;
}

export interface EngineGetResult {
  doc: unknown;
  /**
   * Optional engine-specific optimistic-write token for conditional updates.
   */
  writeToken?: string;
}

/**
 * Engine-level query result. Unlike the user-facing QueryResult, documents
 * include their storage keys.
 */
export interface EngineQueryResult {
  documents: KeyedDocument[];
  cursor: string | null;
}

// ---------------------------------------------------------------------------
// Migration
// ---------------------------------------------------------------------------

export interface MigrationLock {
  id: string;
  collection: string;
  acquiredAt: number;
  [key: string]: unknown;
}

export interface AcquireLockOptions {
  /** Time-to-live in milliseconds. If an existing lock is older than this,
   *  it is considered stale and will be forcibly replaced. */
  ttl?: number;
}

/**
 * Criteria passed to the engine's getOutdated() method so it can efficiently
 * find documents that need migration or reindexing.
 */
export interface MigrationCriteria {
  /** The current latest schema version number. */
  version: number;
  /** The document field that stores the schema version (e.g. "__v"). */
  versionField: string;
  /** Sorted array of expected index names for this model. */
  indexes: string[];
  /** The document field that stores the applied index names (e.g. "__indexes"). */
  indexesField: string;
  /** Optional parser for raw document version values. */
  parseVersion?: (raw: unknown) => ComparableVersion | null;
  /** Optional comparator for parsed version values. */
  compareVersions?: (a: ComparableVersion, b: ComparableVersion) => number;
  /** Optional engine hint for migration page size. */
  pageSizeHint?: number;
  /** Optional engine hint to skip expensive metadata sync for continuation pages. */
  skipMetadataSyncHint?: boolean;
}

export interface MigrationStatus {
  lock: MigrationLock;
  cursor: string | null;
}

// ---------------------------------------------------------------------------
// Query Engine (implemented by each adapter package)
// ---------------------------------------------------------------------------

export interface QueryEngine<TOptions = Record<string, unknown>> {
  capabilities?: {
    uniqueConstraints: "atomic" | "none";
  };

  get(collection: string, key: string, options?: TOptions): Promise<unknown>;
  getWithMetadata?(
    collection: string,
    key: string,
    options?: TOptions,
  ): Promise<EngineGetResult | null>;

  /**
   * Creates a new document. Must reject with EngineDocumentAlreadyExistsError
   * when the key already exists.
   */
  create(
    collection: string,
    key: string,
    doc: unknown,
    indexes: ResolvedIndexKeys,
    options?: TOptions,
    migrationMetadata?: MigrationDocumentMetadata,
    uniqueIndexes?: ResolvedIndexKeys,
  ): Promise<void>;

  put(
    collection: string,
    key: string,
    doc: unknown,
    indexes: ResolvedIndexKeys,
    options?: TOptions,
    migrationMetadata?: MigrationDocumentMetadata,
    uniqueIndexes?: ResolvedIndexKeys,
  ): Promise<void>;

  /**
   * Updates an existing document. Must reject with EngineDocumentNotFoundError
   * when the key does not exist.
   */
  update(
    collection: string,
    key: string,
    doc: unknown,
    indexes: ResolvedIndexKeys,
    options?: TOptions,
    migrationMetadata?: MigrationDocumentMetadata,
    uniqueIndexes?: ResolvedIndexKeys,
  ): Promise<void>;

  delete(collection: string, key: string, options?: TOptions): Promise<void>;

  query(collection: string, params: QueryParams, options?: TOptions): Promise<EngineQueryResult>;
  queryWithMetadata?(
    collection: string,
    params: QueryParams,
    options?: TOptions,
  ): Promise<EngineQueryResult>;

  batchGet(collection: string, keys: string[], options?: TOptions): Promise<KeyedDocument[]>;
  batchGetWithMetadata?(
    collection: string,
    keys: string[],
    options?: TOptions,
  ): Promise<KeyedDocument[]>;
  batchSet(collection: string, items: BatchSetItem[], options?: TOptions): Promise<void>;
  batchSetWithResult?(
    collection: string,
    items: BatchSetItem[],
    options?: TOptions,
  ): Promise<BatchSetResult>;
  batchDelete(collection: string, keys: string[], options?: TOptions): Promise<void>;

  migration: {
    /**
     * Acquires a migration lock for the given collection. Returns `null` if
     * a lock is already held and cannot be acquired.
     *
     * When `options.ttl` is provided and an existing lock's `acquiredAt` is
     * older than `ttl` milliseconds, the engine should consider it stale and
     * replace it with a new lock (returning the new lock).
     */
    acquireLock(collection: string, options?: AcquireLockOptions): Promise<MigrationLock | null>;
    releaseLock(lock: MigrationLock): Promise<void>;
    /**
     * Returns paginated documents that need migration or reindexing.
     * A document is outdated if its version is below `criteria.version`
     * OR its stored index names don't match `criteria.indexes`.
     * The engine decides the most efficient way to find these documents.
     */
    getOutdated(
      collection: string,
      criteria: MigrationCriteria,
      cursor?: string,
    ): Promise<EngineQueryResult>;
    saveCheckpoint?(lock: MigrationLock, cursor: string): Promise<void>;
    loadCheckpoint?(collection: string): Promise<string | null>;
    clearCheckpoint?(collection: string): Promise<void>;
    /**
     * Returns the current migration status for the given collection.
     * If no lock is held, returns `null`. If a lock is held, returns
     * the lock and the last saved checkpoint cursor (if any).
     */
    getStatus?(collection: string): Promise<MigrationStatus | null>;
  };

  migrator?: import("../migrator").Migrator<TOptions>;
}
