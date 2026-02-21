import type { ProjectionSkipReason } from "./model";

import {
  EngineDocumentAlreadyExistsError,
  EngineDocumentNotFoundError,
  EngineUniqueConstraintError,
  type EngineGetResult,
  type MigrationDocumentMetadata,
  type MigrationLock,
  type QueryEngine,
  type QueryParams,
  type QueryFilter,
  type WhereFilter,
  type MigrationCriteria,
  type MigrationStatus,
} from "./engines/types";
import {
  DefaultMigrator,
  type MigrationHooks,
  type MigrationModelContext,
  type MigrationNextPageResult,
  type MigrationRunOptions,
  type MigrationRunProgress,
  type Migrator,
  type MigrationScope,
  MigrationScopeConflictError,
} from "./migrator";
import { ModelDefinition } from "./model";

export type {
  MigrationHooks,
  MigrationNextPageResult,
  MigrationRunOptions,
  MigrationRunProgress,
  Migrator,
} from "./migrator";

// ---------------------------------------------------------------------------
// Public store types
// ---------------------------------------------------------------------------

export interface QueryResult<T> {
  documents: T[];
  cursor: string | null;
}

export interface BatchSetInputItem<T> {
  key: string;
  data: T;
}

export interface MigrationResult {
  model: string;
  status: "completed" | "skipped" | "failed";
  reason?: string;
  error?: unknown;
  migrated?: number;
  skipped?: number;
  skipReasons?: MigrationSkipReasons;
}

export type MigrationSkipReason = ProjectionSkipReason | "concurrent_write";
export type MigrationSkipReasons = Partial<Record<MigrationSkipReason, number>>;
export type ProjectionSkipOperation = "findByKey" | "query" | "batchGet" | "update";

export interface ProjectionSkippedEvent {
  readonly model: string;
  readonly key: string;
  readonly reason: MigrationSkipReason;
  readonly operation: ProjectionSkipOperation;
  readonly error?: unknown;
}

export interface ProjectionHooks {
  onProjectionSkipped?(event: ProjectionSkippedEvent): void | Promise<void>;
}

type AnyString = string & {};

type ModelDataInputForUpdate<T> = T extends object ? Partial<T> : T;

type IndexNameInput<
  TStaticIndexNames extends string,
  THasDynamicIndexes extends boolean,
> = THasDynamicIndexes extends true ? TStaticIndexNames | AnyString : TStaticIndexNames;

type ModelQueryParams<TStaticIndexNames extends string, THasDynamicIndexes extends boolean> = Omit<
  QueryParams,
  "index"
> & {
  index?: IndexNameInput<TStaticIndexNames, THasDynamicIndexes>;
};

/**
 * A model bound to a store. Provides the query/mutation API for a single
 * model, with engine options threaded through for passthrough to the engine.
 */
export interface BoundModel<
  T,
  TOptions = Record<string, unknown>,
  TStaticIndexNames extends string = string,
  THasDynamicIndexes extends boolean = false,
> {
  findByKey(key: string, options?: TOptions): Promise<T | null>;

  query(
    params: ModelQueryParams<TStaticIndexNames, THasDynamicIndexes>,
    options?: TOptions,
  ): Promise<QueryResult<T>>;

  create(key: string, data: T, options?: TOptions): Promise<T>;

  update(key: string, data: ModelDataInputForUpdate<T>, options?: TOptions): Promise<T>;

  delete(key: string, options?: TOptions): Promise<void>;

  batchGet(keys: string[], options?: TOptions): Promise<T[]>;
  batchSet(items: BatchSetInputItem<T>[], options?: TOptions): Promise<T[]>;
  batchDelete(keys: string[], options?: TOptions): Promise<void>;

  getOrCreateMigration(options?: MigrationRunOptions): Promise<MigrationRunProgress>;
  migrateNextPage(options?: MigrationRunOptions): Promise<MigrationNextPageResult>;
  migrateAll(options?: MigrationRunOptions): Promise<MigrationResult>;
  getMigrationStatus(): Promise<MigrationStatus | null>;
  getMigrationProgress(): Promise<MigrationRunProgress | null>;
}

/**
 * The store. Models are accessible as properties keyed by model name.
 */
type ModelData<M> = M extends ModelDefinition<infer T, any, any, any, any> ? T : never;
type ModelStaticIndexNames<M> =
  M extends ModelDefinition<any, any, any, infer TStaticIndexes, any> ? TStaticIndexes : never;
type ModelHasDynamicIndexes<M> =
  M extends ModelDefinition<any, any, any, any, infer THasDynamicIndexes>
    ? THasDynamicIndexes
    : false;

export type Store<
  TModels extends readonly ModelDefinition<any, any, string, any, any>[],
  TOptions = Record<string, unknown>,
> = {
  [M in TModels[number] as M["name"]]: BoundModel<
    ModelData<M>,
    TOptions,
    ModelStaticIndexNames<M>,
    ModelHasDynamicIndexes<M>
  >;
} & {
  getOrCreateMigration(options?: MigrationRunOptions): Promise<MigrationRunProgress>;
  migrateNextPage(options?: MigrationRunOptions): Promise<MigrationNextPageResult>;
  getMigrationProgress(): Promise<MigrationRunProgress | null>;
  migrateAll(options?: MigrationRunOptions): Promise<MigrationResult[]>;
};

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

export class DocumentAlreadyExistsError extends Error {
  constructor(collection: string, key: string) {
    super(`Document "${key}" already exists in model "${collection}"`);
    this.name = "DocumentAlreadyExistsError";
  }
}

export class DocumentNotFoundError extends Error {
  constructor(collection: string, key: string) {
    super(`Document "${key}" not found in model "${collection}"`);
    this.name = "DocumentNotFoundError";
  }
}

export class UniqueConstraintError extends Error {
  readonly collection: string;
  readonly key: string;
  readonly indexName: string;
  readonly indexValue: string;
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
    this.name = "UniqueConstraintError";
    this.collection = collection;
    this.key = key;
    this.indexName = indexName;
    this.indexValue = indexValue;
    this.existingKey = existingKey ?? null;
  }
}

export class MigrationProjectionError extends Error {
  readonly collection: string;
  readonly key: string | null;
  readonly reason: ProjectionSkipReason;
  override readonly cause?: unknown;

  constructor(
    collection: string,
    reason: ProjectionSkipReason,
    key?: string,
    options?: {
      cause?: unknown;
    },
  ) {
    super(
      `Migration projection failed for model "${collection}"${
        key ? ` and key "${key}"` : ""
      }: ${reason}`,
    );
    this.name = "MigrationProjectionError";
    this.collection = collection;
    this.key = key ?? null;
    this.reason = reason;
    this.cause = options?.cause;
  }
}

export class MigrationAlreadyRunningError extends Error {
  constructor(collection: string) {
    super(`Migration is already running for collection "${collection}"`);
    this.name = "MigrationAlreadyRunningError";
  }
}

export class MissingMigratorError extends Error {
  constructor() {
    super(
      "No migrator is configured for this store. Pass one via createStore(..., { migrator }) or use an engine that provides engine.migrator.",
    );
    this.name = "MissingMigratorError";
  }
}

export { MigrationScopeConflictError };

export interface UniqueConstraintLockOptions {
  /** Lock TTL in milliseconds for unique-constraint guards. */
  ttlMs?: number;
  /** Maximum lock-acquisition attempts before failing. */
  maxAttempts?: number;
  /** Delay between lock-acquisition retries in milliseconds. */
  retryDelayMs?: number;
  /**
   * Renewal interval in milliseconds for long-running guarded operations.
   * Set to `null` to disable lock renewal.
   */
  heartbeatIntervalMs?: number | null;
}

export interface CreateStoreOptions<TOptions = Record<string, unknown>> {
  migrator?: Migrator<TOptions>;
  migrationHooks?: MigrationHooks;
  projectionHooks?: ProjectionHooks;
  uniqueConstraintLock?: UniqueConstraintLockOptions;
}

interface ResolvedUniqueConstraintLockOptions {
  readonly ttlMs: number;
  readonly maxAttempts: number;
  readonly retryDelayMs: number;
  readonly heartbeatIntervalMs: number | null;
}

interface UniqueConstraintLockLease {
  readonly currentLock: () => MigrationLock;
  readonly stop: () => Promise<void>;
  readonly assertHealthy: () => void;
  readonly waitForFailure: () => Promise<void>;
}

const DEFAULT_UNIQUE_CONSTRAINT_LOCK_TTL_MS = 30_000;
const DEFAULT_UNIQUE_CONSTRAINT_LOCK_MAX_ATTEMPTS = 200;
const DEFAULT_UNIQUE_CONSTRAINT_LOCK_RETRY_DELAY_MS = 10;

function normalizeIntegerOption(
  value: number | null | undefined,
  name: string,
  minimum: number,
  defaultValue: number,
): number {
  if (value === undefined || value === null) {
    return defaultValue;
  }

  if (!Number.isFinite(value) || !Number.isInteger(value) || value < minimum) {
    throw new Error(
      `createStore option "uniqueConstraintLock.${name}" must be an integer >= ${String(minimum)}`,
    );
  }

  return value;
}

function resolveUniqueConstraintLockOptions(
  options?: UniqueConstraintLockOptions,
): ResolvedUniqueConstraintLockOptions {
  const ttlMs = normalizeIntegerOption(
    options?.ttlMs,
    "ttlMs",
    1,
    DEFAULT_UNIQUE_CONSTRAINT_LOCK_TTL_MS,
  );
  const maxAttempts = normalizeIntegerOption(
    options?.maxAttempts,
    "maxAttempts",
    1,
    DEFAULT_UNIQUE_CONSTRAINT_LOCK_MAX_ATTEMPTS,
  );
  const retryDelayMs = normalizeIntegerOption(
    options?.retryDelayMs,
    "retryDelayMs",
    0,
    DEFAULT_UNIQUE_CONSTRAINT_LOCK_RETRY_DELAY_MS,
  );
  const defaultHeartbeatIntervalMs = Math.max(1, Math.floor(ttlMs / 2));
  const heartbeatInput = options?.heartbeatIntervalMs;
  const heartbeatIntervalMs =
    heartbeatInput === null || heartbeatInput === 0
      ? null
      : normalizeIntegerOption(
          heartbeatInput,
          "heartbeatIntervalMs",
          1,
          defaultHeartbeatIntervalMs,
        );

  if (heartbeatIntervalMs !== null && heartbeatIntervalMs > ttlMs) {
    throw new Error(
      'createStore option "uniqueConstraintLock.heartbeatIntervalMs" must be <= "uniqueConstraintLock.ttlMs"',
    );
  }

  return {
    ttlMs,
    maxAttempts,
    retryDelayMs,
    heartbeatIntervalMs,
  };
}

type JsonPrimitive = string | number | boolean | null;
type JsonValue = JsonPrimitive | JsonValue[] | { [key: string]: JsonValue };

function ensureJsonCompatibleDocument(
  value: object,
  collection: string,
  key: string,
): Record<string, unknown> {
  const visited = new WeakSet<object>();

  const visit = (candidate: unknown, path: string): void => {
    if (candidate === null) {
      return;
    }

    const candidateType = typeof candidate;

    if (candidateType === "string" || candidateType === "boolean") {
      return;
    }

    if (candidateType === "number") {
      if (!Number.isFinite(candidate as number)) {
        throw new Error(
          `Document "${key}" in model "${collection}" is not JSON-compatible at ${path}: non-finite numbers are not allowed`,
        );
      }

      return;
    }

    if (candidateType === "undefined") {
      throw new Error(
        `Document "${key}" in model "${collection}" is not JSON-compatible at ${path}: undefined is not allowed`,
      );
    }

    if (candidateType === "bigint") {
      throw new Error(
        `Document "${key}" in model "${collection}" is not JSON-compatible at ${path}: bigint is not allowed`,
      );
    }

    if (candidateType === "symbol") {
      throw new Error(
        `Document "${key}" in model "${collection}" is not JSON-compatible at ${path}: symbol is not allowed`,
      );
    }

    if (candidateType === "function") {
      throw new Error(
        `Document "${key}" in model "${collection}" is not JSON-compatible at ${path}: function is not allowed`,
      );
    }

    if (candidateType !== "object") {
      throw new Error(
        `Document "${key}" in model "${collection}" is not JSON-compatible at ${path}: unsupported value type`,
      );
    }

    const objectValue = candidate as object;

    if (visited.has(objectValue)) {
      throw new Error(
        `Document "${key}" in model "${collection}" is not JSON-compatible at ${path}: circular references are not allowed`,
      );
    }

    visited.add(objectValue);

    if (Array.isArray(candidate)) {
      for (let i = 0; i < candidate.length; i++) {
        visit(candidate[i], `${path}[${String(i)}]`);
      }
      return;
    }

    const proto = Object.getPrototypeOf(candidate);

    if (proto !== Object.prototype && proto !== null) {
      const constructorName =
        (candidate as { constructor?: { name?: string } }).constructor?.name ?? "Object";
      throw new Error(
        `Document "${key}" in model "${collection}" is not JSON-compatible at ${path}: unsupported object type "${constructorName}"`,
      );
    }

    const entries = Object.entries(candidate as Record<string, unknown>);

    for (const [entryKey, entryValue] of entries) {
      visit(entryValue, `${path}.${entryKey}`);
    }
  };

  visit(value as JsonValue, "$");

  return value as Record<string, unknown>;
}

// ---------------------------------------------------------------------------
// BoundModel — a model bound to an engine
// ---------------------------------------------------------------------------

class BoundModelImpl<
  T,
  TOptions = Record<string, unknown>,
  TStaticIndexNames extends string = string,
  THasDynamicIndexes extends boolean = false,
> {
  private model: ModelDefinition<T, any, string, TStaticIndexNames, THasDynamicIndexes>;
  private engine: QueryEngine<TOptions>;
  private migrator: Migrator<TOptions> | null;
  private projectionHooks: ProjectionHooks | undefined;
  private uniqueConstraintLockOptions: ResolvedUniqueConstraintLockOptions;

  constructor(
    model: ModelDefinition<T, any, string, TStaticIndexNames, THasDynamicIndexes>,
    engine: QueryEngine<TOptions>,
    migrator: Migrator<TOptions> | null,
    projectionHooks?: ProjectionHooks,
    uniqueConstraintLockOptions?: ResolvedUniqueConstraintLockOptions,
  ) {
    this.model = model;
    this.engine = engine;
    this.migrator = migrator;
    this.projectionHooks = projectionHooks;
    this.uniqueConstraintLockOptions =
      uniqueConstraintLockOptions ?? resolveUniqueConstraintLockOptions();
  }

  async findByKey(key: string, options?: TOptions): Promise<T | null> {
    const stored = await this.getWithOptionalWriteToken(key, options);

    if (!stored) {
      return null;
    }

    const projected = await this.model.projectToLatest(stored.doc);

    if (!projected.ok) {
      await this.handleProjectionFailure(projected.reason, key, "findByKey", projected.error);
      return null;
    }

    if (projected.migrated) {
      await this.writebackMany(
        [{ key, value: projected.value, expectedWriteToken: stored.writeToken }],
        "findByKey",
        options,
      );
    }

    return projected.value;
  }

  async query(
    params: ModelQueryParams<TStaticIndexNames, THasDynamicIndexes>,
    options?: TOptions,
  ): Promise<QueryResult<T>> {
    const resolved = this.resolveQuery(params);
    const raw = this.engine.queryWithMetadata
      ? await this.engine.queryWithMetadata(this.model.name, resolved, options)
      : await this.engine.query(this.model.name, resolved, options);
    const documents: T[] = [];
    const writebacks: { key: string; value: T; expectedWriteToken?: string }[] = [];

    for (const { key, doc: rawDoc, writeToken } of raw.documents) {
      const projected = await this.model.projectToLatest(rawDoc as Record<string, unknown>);

      if (!projected.ok) {
        await this.handleProjectionFailure(projected.reason, key, "query", projected.error);
        continue;
      }

      if (projected.migrated) {
        writebacks.push({
          key,
          value: projected.value,
          expectedWriteToken: this.normalizeWriteToken(writeToken),
        });
      }

      documents.push(projected.value);
    }

    await this.writebackMany(writebacks, "query", options);

    return { documents, cursor: raw.cursor };
  }

  async create(key: string, data: T, options?: TOptions): Promise<T> {
    const validated = await this.model.validate(data);
    const doc = this.stamp(validated as object, key);
    const indexes = this.model.resolveIndexKeys(validated);
    const uniqueIndexes = this.model.resolveUniqueIndexKeys(validated);
    const migrationMetadata = this.currentMigrationMetadata();

    try {
      await this.withUniqueConstraintGuard([{ key, uniqueIndexes }], options, async () => {
        await this.engine.create(
          this.model.name,
          key,
          doc,
          indexes,
          options,
          migrationMetadata,
          uniqueIndexes,
        );
      });
    } catch (error) {
      if (error instanceof EngineDocumentAlreadyExistsError) {
        throw new DocumentAlreadyExistsError(this.model.name, key);
      }
      if (error instanceof EngineUniqueConstraintError) {
        throw new UniqueConstraintError(
          this.model.name,
          key,
          error.indexName,
          error.indexValue,
          error.existingKey,
        );
      }

      throw error;
    }

    return validated;
  }

  async update(key: string, data: ModelDataInputForUpdate<T>, options?: TOptions): Promise<T> {
    const existing = await this.engine.get(this.model.name, key, options);

    if (existing === null || existing === undefined) {
      throw new DocumentNotFoundError(this.model.name, key);
    }

    const existingDoc = existing as Record<string, unknown>;
    let current: Record<string, unknown>;
    const projected = await this.model.projectToLatest(existingDoc);

    if (projected.ok) {
      current = projected.value as Record<string, unknown>;
    } else {
      await this.handleProjectionFailure(projected.reason, key, "update", projected.error);
      // Ignore migration failures on read/merge path and attempt to apply the
      // update against the stored document as-is.
      current = existingDoc;
    }

    const merged = { ...current, ...(data as object) };
    const validated = await this.model.validate(merged);
    const doc = this.stamp(validated as object, key);
    const indexes = this.model.resolveIndexKeys(validated);
    const uniqueIndexes = this.model.resolveUniqueIndexKeys(validated);
    const migrationMetadata = this.currentMigrationMetadata();

    try {
      await this.withUniqueConstraintGuard([{ key, uniqueIndexes }], options, async () => {
        await this.engine.update(
          this.model.name,
          key,
          doc,
          indexes,
          options,
          migrationMetadata,
          uniqueIndexes,
        );
      });
    } catch (error) {
      if (error instanceof EngineDocumentNotFoundError) {
        throw new DocumentNotFoundError(this.model.name, key);
      }
      if (error instanceof EngineUniqueConstraintError) {
        throw new UniqueConstraintError(
          this.model.name,
          key,
          error.indexName,
          error.indexValue,
          error.existingKey,
        );
      }

      throw error;
    }

    return validated;
  }

  async delete(key: string, options?: TOptions): Promise<void> {
    await this.engine.delete(this.model.name, key, options);
  }

  async batchGet(keys: string[], options?: TOptions): Promise<T[]> {
    const rawDocs = this.engine.batchGetWithMetadata
      ? await this.engine.batchGetWithMetadata(this.model.name, keys, options)
      : await this.engine.batchGet(this.model.name, keys, options);
    const results: T[] = [];
    const writebacks: { key: string; value: T; expectedWriteToken?: string }[] = [];

    for (const { key, doc: rawDoc, writeToken } of rawDocs) {
      const projected = await this.model.projectToLatest(rawDoc as Record<string, unknown>);

      if (!projected.ok) {
        await this.handleProjectionFailure(projected.reason, key, "batchGet", projected.error);
        continue;
      }

      if (projected.migrated) {
        writebacks.push({
          key,
          value: projected.value,
          expectedWriteToken: this.normalizeWriteToken(writeToken),
        });
      }

      results.push(projected.value);
    }

    await this.writebackMany(writebacks, "batchGet", options);

    return results;
  }

  async batchSet(items: BatchSetInputItem<T>[], options?: TOptions): Promise<T[]> {
    const prepared: {
      key: string;
      validated: T;
      doc: object;
      indexes: Record<string, string>;
      uniqueIndexes: Record<string, string>;
      migrationMetadata: MigrationDocumentMetadata;
    }[] = [];

    for (const item of items) {
      if (typeof item.key !== "string") {
        throw new Error(`Invalid document key for model "${this.model.name}"`);
      }

      const validated = await this.model.validate(item.data);

      prepared.push({
        key: item.key,
        validated,
        doc: this.stamp(validated as object, item.key),
        indexes: this.model.resolveIndexKeys(validated),
        uniqueIndexes: this.model.resolveUniqueIndexKeys(validated),
        migrationMetadata: this.currentMigrationMetadata(),
      });
    }

    try {
      await this.withUniqueConstraintGuard(
        prepared.map((item) => ({
          key: item.key,
          uniqueIndexes: item.uniqueIndexes,
        })),
        options,
        async () => {
          await this.engine.batchSet(
            this.model.name,
            prepared.map((item) => ({
              key: item.key,
              doc: item.doc,
              indexes: item.indexes,
              uniqueIndexes: item.uniqueIndexes,
              migrationMetadata: item.migrationMetadata,
            })),
            options,
          );
        },
      );
    } catch (error) {
      if (error instanceof EngineUniqueConstraintError) {
        throw new UniqueConstraintError(
          this.model.name,
          error.key,
          error.indexName,
          error.indexValue,
          error.existingKey,
        );
      }

      throw error;
    }

    return prepared.map((item) => item.validated);
  }

  async batchDelete(keys: string[], options?: TOptions): Promise<void> {
    await this.engine.batchDelete(this.model.name, keys, options);
  }

  async getOrCreateMigration(options?: MigrationRunOptions): Promise<MigrationRunProgress> {
    const migrator = this.requireMigrator();
    const contexts = this.modelContextMap();
    const result = await migrator.getOrCreateRun(this.modelScope(), contexts, options);
    return result.progress;
  }

  async migrateNextPage(options?: MigrationRunOptions): Promise<MigrationNextPageResult> {
    const migrator = this.requireMigrator();
    return migrator.migrateNextPage(this.modelScope(), this.modelContextMap(), options);
  }

  async migrateAll(options?: MigrationRunOptions): Promise<MigrationResult> {
    const collection = this.model.name;
    let migrated = 0;
    let skipped = 0;
    const skipReasons: MigrationSkipReasons = {};

    await this.getOrCreateMigration(options);

    while (true) {
      const page = await this.migrateNextPage(options);

      if (page.status === "busy") {
        throw new MigrationAlreadyRunningError(collection);
      }

      migrated += page.migrated;
      skipped += page.skipped;

      if (page.skipReasons) {
        for (const [reason, count] of Object.entries(page.skipReasons)) {
          skipReasons[reason as MigrationSkipReason] =
            (skipReasons[reason as MigrationSkipReason] ?? 0) + count;
        }
      }

      if (page.completed) {
        break;
      }
    }

    return {
      model: collection,
      status: "completed",
      migrated,
      skipped,
      skipReasons: skipped > 0 ? skipReasons : undefined,
    };
  }

  async getMigrationStatus(): Promise<MigrationStatus | null> {
    if (!this.engine.migration.getStatus) {
      return null;
    }

    return this.engine.migration.getStatus(this.model.name);
  }

  async getMigrationProgress(): Promise<MigrationRunProgress | null> {
    const migrator = this.requireMigrator();
    return migrator.getProgress(this.modelScope());
  }

  // ---------------------------------------------------------------------------
  // Private helpers
  // ---------------------------------------------------------------------------

  private async getWithOptionalWriteToken(
    key: string,
    options?: TOptions,
  ): Promise<{ doc: Record<string, unknown>; writeToken?: string } | null> {
    if (this.engine.getWithMetadata) {
      const raw: EngineGetResult | null = await this.engine.getWithMetadata(
        this.model.name,
        key,
        options,
      );

      if (raw === null || raw === undefined) {
        return null;
      }

      return {
        doc: raw.doc as Record<string, unknown>,
        writeToken: this.normalizeWriteToken(raw.writeToken),
      };
    }

    const raw = await this.engine.get(this.model.name, key, options);

    if (raw === null || raw === undefined) {
      return null;
    }

    return {
      doc: raw as Record<string, unknown>,
    };
  }

  private normalizeWriteToken(raw: unknown): string | undefined {
    if (typeof raw !== "string" || raw.length === 0) {
      return undefined;
    }

    return raw;
  }

  // Stamps the version and index names onto a document for storage.
  private stamp(data: object, key: string): Record<string, unknown> {
    const stamped = {
      ...data,
      [this.model.options.versionField]: this.model.latestVersion,
      [this.model.options.indexesField]: this.model.indexNames,
    };

    return ensureJsonCompatibleDocument(stamped, this.model.name, key);
  }

  private currentMigrationMetadata(): MigrationDocumentMetadata {
    return {
      targetVersion: this.model.latestVersion,
      versionState: "current",
      indexSignature: this.computeIndexSignature(this.model.indexNames),
    };
  }

  private computeIndexSignature(indexes: readonly string[]): string {
    return JSON.stringify(indexes);
  }

  // Writes migrated documents back to the engine so future reads don't need
  // to re-migrate. Skipped in "readonly" and "eager" modes — readonly because
  // writes are not permitted, eager because migration is expected to happen
  // via migrateAll() rather than on individual reads.
  private async writebackMany(
    items: { key: string; value: T; expectedWriteToken?: string }[],
    operation: ProjectionSkipOperation,
    options?: TOptions,
  ): Promise<void> {
    if (this.model.options.migration !== "lazy") {
      return;
    }

    if (items.length === 0) {
      return;
    }

    const prepared = items.map((item) => ({
      key: item.key,
      doc: this.stamp(item.value as object, item.key),
      indexes: this.model.resolveIndexKeys(item.value),
      uniqueIndexes: this.model.resolveUniqueIndexKeys(item.value),
      migrationMetadata: this.currentMigrationMetadata(),
      expectedWriteToken: this.normalizeWriteToken(item.expectedWriteToken),
    }));

    let conflictedKeys: string[] = [];

    try {
      const result = await this.withUniqueConstraintGuard(
        prepared.map((item) => ({
          key: item.key,
          uniqueIndexes: item.uniqueIndexes,
        })),
        options,
        async () => {
          if (this.engine.batchSetWithResult) {
            return this.engine.batchSetWithResult(this.model.name, prepared, options);
          }

          await this.engine.batchSet(this.model.name, prepared, options);
          return null;
        },
      );

      if (result) {
        conflictedKeys = result.conflictedKeys;
      }
    } catch (error) {
      if (error instanceof EngineUniqueConstraintError) {
        throw new UniqueConstraintError(
          this.model.name,
          error.key,
          error.indexName,
          error.indexValue,
          error.existingKey,
        );
      }

      throw error;
    }

    if (conflictedKeys.length > 0) {
      await Promise.all(
        conflictedKeys.map((key) =>
          this.runProjectionHook({
            model: this.model.name,
            key,
            reason: "concurrent_write",
            operation,
          }),
        ),
      );
    }
  }

  private migrationCriteria(): MigrationCriteria {
    return {
      version: this.model.latestVersion,
      versionField: this.model.options.versionField,
      indexes: this.model.indexNames,
      indexesField: this.model.options.indexesField,
      parseVersion: this.model.options.parseVersion,
      compareVersions: this.model.options.compareVersions,
    };
  }

  createMigrationContext(): MigrationModelContext {
    return {
      collection: this.model.name,
      criteria: this.migrationCriteria(),
      project: async (doc) => {
        const projected = await this.model.projectToLatest(doc);

        if (!projected.ok && this.shouldThrowProjectionErrors()) {
          throw this.createProjectionError(projected.reason, undefined, projected.error);
        }

        return projected;
      },
      toBatchSetItem: (key, value) => ({
        key,
        doc: this.stamp(value as object, key),
        indexes: this.model.resolveIndexKeys(value as T),
        uniqueIndexes: this.model.resolveUniqueIndexKeys(value as T),
        migrationMetadata: this.currentMigrationMetadata(),
      }),
      persist: async (items) => {
        try {
          return this.withUniqueConstraintGuard(
            items.map((item) => ({
              key: item.key,
              uniqueIndexes: item.uniqueIndexes ?? {},
            })),
            undefined,
            async () =>
              this.engine.batchSetWithResult
                ? this.engine.batchSetWithResult(this.model.name, items)
                : this.engine.batchSet(this.model.name, items),
          );
        } catch (error) {
          if (error instanceof EngineUniqueConstraintError) {
            throw new UniqueConstraintError(
              this.model.name,
              error.key,
              error.indexName,
              error.indexValue,
              error.existingKey,
            );
          }

          throw error;
        }
      },
    };
  }

  modelName(): string {
    return this.model.name;
  }

  private modelContextMap(): ReadonlyMap<string, MigrationModelContext> {
    return new Map([[this.model.name, this.createMigrationContext()]]);
  }

  private modelScope(): MigrationScope {
    return {
      scope: "model",
      model: this.model.name,
    };
  }

  private requireMigrator(): Migrator<TOptions> {
    if (!this.migrator) {
      throw new MissingMigratorError();
    }

    return this.migrator;
  }

  private shouldThrowProjectionErrors(): boolean {
    return this.model.options.migrationErrors === "throw";
  }

  private async handleProjectionFailure(
    reason: ProjectionSkipReason,
    key: string,
    operation: ProjectionSkipOperation,
    cause?: unknown,
  ): Promise<void> {
    if (!this.shouldThrowProjectionErrors()) {
      await this.runProjectionHook({
        model: this.model.name,
        key,
        reason,
        operation,
        error: cause,
      });
      return;
    }

    throw this.createProjectionError(reason, key, cause);
  }

  private createProjectionError(
    reason: ProjectionSkipReason,
    key?: string,
    cause?: unknown,
  ): MigrationProjectionError {
    if (cause instanceof MigrationProjectionError) {
      return cause;
    }

    return new MigrationProjectionError(this.model.name, reason, key, { cause });
  }

  private async runProjectionHook(event: ProjectionSkippedEvent): Promise<void> {
    const hooks = this.projectionHooks;

    if (!hooks?.onProjectionSkipped) {
      return;
    }

    try {
      await hooks.onProjectionSkipped(event);
    } catch {
      // Hook errors are intentionally ignored so read/query behavior is stable.
    }
  }

  private async withUniqueConstraintGuard<R>(
    candidates: readonly {
      key: string;
      uniqueIndexes: Record<string, string>;
    }[],
    options: TOptions | undefined,
    operation: () => Promise<R>,
  ): Promise<R> {
    if (!candidates.some((candidate) => Object.keys(candidate.uniqueIndexes).length > 0)) {
      return operation();
    }

    const lockLease = this.createUniqueConstraintLockLease(
      await this.acquireUniqueConstraintLock(),
    );
    let result: R | undefined = undefined;
    let operationError: unknown = null;

    try {
      await this.assertNoUniqueConstraintConflicts(candidates, options);
      lockLease.assertHealthy();

      const operationResult = operation()
        .then((value) => ({
          status: "success" as const,
          value,
        }))
        .catch((error) => ({
          status: "error" as const,
          error,
        }));
      const raceResult = await Promise.race([
        operationResult,
        lockLease.waitForFailure().then(() => ({
          status: "lease_failed" as const,
        })),
      ]);

      if (raceResult.status === "lease_failed") {
        lockLease.assertHealthy();
        throw new Error(
          `Unique-constraint lock lease entered an invalid state for model "${this.model.name}"`,
        );
      }

      if (raceResult.status === "error") {
        operationError = raceResult.error;
      } else {
        result = raceResult.value;
      }
    } finally {
      await lockLease.stop();
      await this.engine.migration.releaseLock(lockLease.currentLock());
    }

    lockLease.assertHealthy();

    if (operationError !== null) {
      throw operationError;
    }

    return result as R;
  }

  private async acquireUniqueConstraintLock(): Promise<MigrationLock> {
    const collection = this.uniqueConstraintLockCollection();
    const { ttlMs, maxAttempts, retryDelayMs } = this.uniqueConstraintLockOptions;

    for (const attempt of Array.from({ length: maxAttempts }, (_, index) => index)) {
      const lock = await this.engine.migration.acquireLock(collection, { ttl: ttlMs });

      if (lock) {
        return lock;
      }

      if (attempt === maxAttempts - 1) {
        break;
      }

      await this.sleep(retryDelayMs);
    }

    throw new Error(
      `Unable to acquire unique-constraint lock for model "${this.model.name}" after ${String(maxAttempts)} attempts`,
    );
  }

  private uniqueConstraintLockCollection(): string {
    return `${this.model.name}::__unique_constraints`;
  }

  private createUniqueConstraintLockLease(initialLock: MigrationLock): UniqueConstraintLockLease {
    const intervalMs = this.uniqueConstraintLockOptions.heartbeatIntervalMs;

    if (intervalMs === null) {
      return {
        currentLock() {
          return initialLock;
        },
        async stop() {},
        assertHealthy() {},
        async waitForFailure() {
          await new Promise<void>(() => {
            // This lease has no heartbeat, so renewal cannot fail.
          });
        },
      };
    }

    let notifyFailure: (() => void) | null = null;
    const failurePromise = new Promise<void>((resolve) => {
      notifyFailure = resolve;
    });
    const state = {
      active: true,
      currentLock: initialLock,
      timer: null as ReturnType<typeof setTimeout> | null,
      pendingRenewal: null as Promise<void> | null,
      error: null as unknown,
    };
    const modelName = this.model.name;
    const collection = this.uniqueConstraintLockCollection();
    const scheduleNext = (): void => {
      if (!state.active || state.error !== null) {
        return;
      }

      state.timer = setTimeout(() => {
        state.timer = null;
        state.pendingRenewal = renewLock().finally(() => {
          state.pendingRenewal = null;
        });
      }, intervalMs);
    };
    const renewLock = async (): Promise<void> => {
      if (!state.active || state.error !== null) {
        return;
      }

      try {
        const renewed = await this.engine.migration.acquireLock(collection, { ttl: 0 });

        if (!renewed) {
          throw new Error(`Unable to renew unique-constraint lock for model "${modelName}"`);
        }

        state.currentLock = renewed;
        scheduleNext();
      } catch (error) {
        state.error = error;
        state.active = false;
        notifyFailure?.();
      }
    };

    scheduleNext();

    return {
      currentLock() {
        return state.currentLock;
      },
      async stop() {
        state.active = false;

        if (state.timer !== null) {
          clearTimeout(state.timer);
          state.timer = null;
        }

        if (state.pendingRenewal) {
          await state.pendingRenewal;
        }
      },
      assertHealthy() {
        if (state.error !== null) {
          throw new Error(`Unique-constraint lock renewal failed for model "${modelName}"`, {
            cause: state.error,
          });
        }
      },
      async waitForFailure() {
        await failurePromise;
      },
    };
  }

  private async sleep(delayMs: number): Promise<void> {
    await new Promise<void>((resolve) => {
      setTimeout(resolve, delayMs);
    });
  }

  private async assertNoUniqueConstraintConflicts(
    candidates: readonly {
      key: string;
      uniqueIndexes: Record<string, string>;
    }[],
    options?: TOptions,
  ): Promise<void> {
    const candidateByKey = new Map<string, Record<string, string>>();
    const ownerByIndexValue = new Map<string, string>();

    for (const candidate of candidates) {
      candidateByKey.set(candidate.key, candidate.uniqueIndexes);

      for (const [indexName, indexValue] of Object.entries(candidate.uniqueIndexes)) {
        const token = `${indexName}\u0000${indexValue}`;
        const existingOwner = ownerByIndexValue.get(token);

        if (existingOwner && existingOwner !== candidate.key) {
          throw new UniqueConstraintError(
            this.model.name,
            candidate.key,
            indexName,
            indexValue,
            existingOwner,
          );
        }

        ownerByIndexValue.set(token, candidate.key);
      }
    }

    for (const [token, ownerKey] of ownerByIndexValue) {
      const separator = token.indexOf("\u0000");
      const indexName = token.slice(0, separator);
      const indexValue = token.slice(separator + 1);
      const result = await this.engine.query(
        this.model.name,
        {
          index: indexName,
          filter: { value: indexValue },
          limit: 10,
        },
        options,
      );

      for (const { key: existingKey } of result.documents) {
        if (existingKey === ownerKey) {
          continue;
        }

        const plannedForExisting = candidateByKey.get(existingKey);

        if (plannedForExisting && plannedForExisting[indexName] !== indexValue) {
          continue;
        }

        throw new UniqueConstraintError(
          this.model.name,
          ownerKey,
          indexName,
          indexValue,
          existingKey,
        );
      }
    }
  }

  // Resolves query params: `where` shorthand → `index`/`filter`, or pass through
  // `index`/`filter`, or no filter at all (scan all documents).
  // The user-facing `index` param uses the query name; this resolves it to the
  // storage key that the engine understands.
  private resolveQuery(
    params: ModelQueryParams<TStaticIndexNames, THasDynamicIndexes>,
  ): QueryParams {
    const hasIndex = params.index !== undefined;
    const hasWhere = params.where !== undefined;

    if (hasIndex && hasWhere) {
      throw new Error(`Cannot use both "index"/"filter" and "where" in the same query`);
    }

    if (hasWhere) {
      return {
        ...this.resolveWhere(params.where!),
        limit: params.limit,
        cursor: params.cursor,
        sort: params.sort,
      };
    }

    if (hasIndex) {
      return {
        ...params,
        index: this.resolveIndexName(params.index!),
      };
    }

    return params;
  }

  // Resolves a user-facing index name to the engine-level index identifier.
  // For static-name indexes, maps the query name to the storage key (same value).
  // For dynamic-name indexes, the user provides the resolved name directly
  // (e.g. "acme#user") — we pass it through to the engine as-is.
  private resolveIndexName(name: string): string {
    const idx = this.model.indexes.find((i) => typeof i.name === "string" && i.name === name);

    if (idx) {
      return idx.key;
    }

    // If no static match, the name might be a resolved dynamic index name.
    // Dynamic indexes have function names — users query them with the
    // computed name directly (e.g. { index: "acme#user", filter: ... }).
    const hasDynamicIndexes = this.model.indexes.some((i) => typeof i.name === "function");

    if (hasDynamicIndexes) {
      return name;
    }

    const available = this.model.indexes
      .filter((i) => typeof i.name === "string")
      .map((i) => i.name as string)
      .join(", ");
    throw new Error(`No index named "${name}". Available indexes: ${available || "(none)"}`);
  }

  private resolveWhere(where: WhereFilter): {
    index: string;
    filter: QueryFilter;
  } {
    const fields = Object.keys(where);

    if (fields.length !== 1) {
      throw new Error(
        `"where" must contain exactly one field, got ${fields.length}: ${fields.join(", ")}`,
      );
    }

    const field = fields[0]!;
    const value = where[field];

    if (value === undefined) {
      throw new Error(`"where.${field}" must have a value`);
    }

    // Find an index whose `value` is the string field name
    const matchingIndex = this.model.indexes.find(
      (idx) => typeof idx.value === "string" && idx.value === field,
    );

    if (!matchingIndex) {
      const indexedFields = this.model.indexes
        .filter((idx) => typeof idx.value === "string")
        .map((idx) => idx.value as string);

      throw new Error(
        `No index found for field "${field}". Indexed fields: ${
          indexedFields.length > 0
            ? indexedFields.join(", ")
            : "(none — all indexes use custom functions)"
        }. ` + `For function-based indexes, use { index: "...", filter: { value: ... } } instead.`,
      );
    }

    // Use the storage key for the engine, not the query name
    return { index: matchingIndex.key, filter: { value } };
  }
}

// ---------------------------------------------------------------------------
// createStore()
// ---------------------------------------------------------------------------

export function createStore<
  const TModels extends readonly ModelDefinition<any, any, string, any, any>[],
  TOptions = Record<string, unknown>,
>(
  engine: QueryEngine<TOptions>,
  models: TModels,
  options?: CreateStoreOptions<TOptions>,
): Store<TModels, TOptions> {
  if (options?.migrator && options?.migrationHooks) {
    throw new Error(
      'createStore options "migrator" and "migrationHooks" cannot be provided together. ' +
        "When using a custom migrator, wire hooks inside that migrator.",
    );
  }

  const engineMigrator = options?.migrationHooks
    ? new DefaultMigrator(engine, {
        hooks: options.migrationHooks,
      })
    : (engine.migrator ?? null);
  const migrator = options?.migrator ?? engineMigrator;
  const uniqueConstraintLockOptions = resolveUniqueConstraintLockOptions(
    options?.uniqueConstraintLock,
  );
  const boundModels = new Map<string, BoundModelImpl<any, TOptions, any, any>>();

  for (const modelDef of models) {
    if (boundModels.has(modelDef.name)) {
      throw new Error(`Duplicate model name: "${modelDef.name}"`);
    }

    const hasUniqueIndexes = modelDef.indexes.some((index) => index.unique === true);

    if (hasUniqueIndexes && engine.capabilities?.uniqueConstraints !== "atomic") {
      throw new Error(
        `Model "${modelDef.name}" declares unique indexes, but the configured engine does not support atomic unique constraints`,
      );
    }

    boundModels.set(
      modelDef.name,
      new BoundModelImpl(
        modelDef,
        engine,
        migrator,
        options?.projectionHooks,
        uniqueConstraintLockOptions,
      ),
    );
  }

  const allModelNames = Array.from(boundModels.keys()).sort((a, b) => a.localeCompare(b));
  const storeScope: MigrationScope = {
    scope: "store",
    models: allModelNames,
  };

  const migrationContexts = (): ReadonlyMap<string, MigrationModelContext> => {
    const contexts = new Map<string, MigrationModelContext>();

    for (const boundModel of boundModels.values()) {
      contexts.set(boundModel.modelName(), boundModel.createMigrationContext());
    }

    return contexts;
  };

  const requireMigrator = (): Migrator<TOptions> => {
    if (!migrator) {
      throw new MissingMigratorError();
    }

    return migrator;
  };

  const store = {
    async getOrCreateMigration(options?: MigrationRunOptions): Promise<MigrationRunProgress> {
      const activeMigrator = requireMigrator();
      const result = await activeMigrator.getOrCreateRun(storeScope, migrationContexts(), options);
      return result.progress;
    },

    async migrateNextPage(options?: MigrationRunOptions): Promise<MigrationNextPageResult> {
      const activeMigrator = requireMigrator();
      return activeMigrator.migrateNextPage(storeScope, migrationContexts(), options);
    },

    async getMigrationProgress(): Promise<MigrationRunProgress | null> {
      const activeMigrator = requireMigrator();
      return activeMigrator.getProgress(storeScope);
    },

    async migrateAll(options?: MigrationRunOptions): Promise<MigrationResult[]> {
      const activeMigrator = requireMigrator();
      await activeMigrator.getOrCreateRun(storeScope, migrationContexts(), options);

      const aggregates = new Map<
        string,
        {
          migrated: number;
          skipped: number;
          skipReasons: MigrationSkipReasons;
        }
      >();

      for (const name of allModelNames) {
        aggregates.set(name, {
          migrated: 0,
          skipped: 0,
          skipReasons: {},
        });
      }

      while (true) {
        const page = await activeMigrator.migrateNextPage(storeScope, migrationContexts(), options);

        if (page.status === "busy") {
          throw new MigrationAlreadyRunningError("store");
        }

        if (page.model) {
          const aggregate = aggregates.get(page.model);

          if (aggregate) {
            aggregate.migrated += page.migrated;
            aggregate.skipped += page.skipped;

            if (page.skipReasons) {
              for (const [reason, count] of Object.entries(page.skipReasons)) {
                aggregate.skipReasons[reason as MigrationSkipReason] =
                  (aggregate.skipReasons[reason as MigrationSkipReason] ?? 0) + count;
              }
            }
          }
        }

        if (page.completed) {
          break;
        }
      }

      return allModelNames.map((modelName) => {
        const aggregate = aggregates.get(modelName)!;
        return {
          model: modelName,
          status: "completed",
          migrated: aggregate.migrated,
          skipped: aggregate.skipped,
          skipReasons: aggregate.skipped > 0 ? aggregate.skipReasons : undefined,
        } satisfies MigrationResult;
      });
    },
  } as Store<TModels, TOptions>;

  const storeRecord = store as unknown as Record<string, unknown>;

  for (const [name, boundModel] of boundModels) {
    storeRecord[name] = {
      findByKey: boundModel.findByKey.bind(boundModel),
      query: boundModel.query.bind(boundModel),
      create: boundModel.create.bind(boundModel),
      update: boundModel.update.bind(boundModel),
      delete: boundModel.delete.bind(boundModel),
      batchGet: boundModel.batchGet.bind(boundModel),
      batchSet: boundModel.batchSet.bind(boundModel),
      batchDelete: boundModel.batchDelete.bind(boundModel),
      getOrCreateMigration: boundModel.getOrCreateMigration.bind(boundModel),
      migrateNextPage: boundModel.migrateNextPage.bind(boundModel),
      migrateAll: boundModel.migrateAll.bind(boundModel),
      getMigrationStatus: boundModel.getMigrationStatus.bind(boundModel),
      getMigrationProgress: boundModel.getMigrationProgress.bind(boundModel),
    } satisfies BoundModel<any, TOptions, any, any>;
  }

  return store;
}
