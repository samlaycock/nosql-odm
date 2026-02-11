import type {
  QueryEngine,
  QueryParams,
  QueryFilter,
  WhereFilter,
  MigrationCriteria,
  MigrationStatus,
} from "./engines/types";
import type { Model } from "./model";
import { ModelDefinition } from "./model";

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
}

/**
 * A model bound to a store. Provides the query/mutation API for a single
 * model, with engine options threaded through for passthrough to the engine.
 */
export interface BoundModel<T, TOptions = Record<string, unknown>> {
  findByKey(key: string, options?: TOptions): Promise<T | null>;

  query(params: QueryParams, options?: TOptions): Promise<QueryResult<T>>;

  create(key: string, data: T, options?: TOptions): Promise<T>;

  update(key: string, data: Partial<T>, options?: TOptions): Promise<T>;

  delete(key: string, options?: TOptions): Promise<void>;

  batchGet(keys: string[], options?: TOptions): Promise<T[]>;
  batchSet(items: BatchSetInputItem<T>[], options?: TOptions): Promise<T[]>;
  batchDelete(keys: string[], options?: TOptions): Promise<void>;

  migrateAll(): Promise<MigrationResult>;
  getMigrationStatus(): Promise<MigrationStatus | null>;
}

/**
 * The store. Models are accessible as properties keyed by model name.
 */
export type Store<TModels extends Model<any, any>[], TOptions = Record<string, unknown>> = {
  [M in TModels[number] as M["name"]]: BoundModel<
    M extends Model<infer T, any> ? T : never,
    TOptions
  >;
} & {
  migrateAll(): Promise<MigrationResult[]>;
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

export class MigrationAlreadyRunningError extends Error {
  constructor(collection: string) {
    super(`Migration is already running for collection "${collection}"`);
    this.name = "MigrationAlreadyRunningError";
  }
}

// ---------------------------------------------------------------------------
// BoundModel — a model bound to an engine
// ---------------------------------------------------------------------------

class BoundModelImpl<T> {
  private model: ModelDefinition<T>;
  private engine: QueryEngine<any>;

  constructor(model: ModelDefinition<T>, engine: QueryEngine<any>) {
    this.model = model;
    this.engine = engine;
  }

  async findByKey(key: string, options?: unknown): Promise<T | null> {
    const raw = await this.engine.get(this.model.name, key, options);

    if (raw === null || raw === undefined) {
      return null;
    }

    const doc = raw as Record<string, unknown>;
    const versionField = this.model.options.versionField;
    const docVersion = (doc[versionField] as number | undefined) ?? 1;

    // Lazy migration: if the document is on an older schema version, migrate it
    // forward and write the updated document back to the engine before returning.
    if (docVersion < this.model.latestVersion) {
      const migrated = await this.model.migrate(doc);
      await this.writeback(key, migrated, options);
      return migrated;
    }

    return await this.model.validate(raw);
  }

  async query(params: QueryParams, options?: unknown): Promise<QueryResult<T>> {
    const resolved = this.resolveQuery(params);
    const raw = await this.engine.query(this.model.name, resolved, options);
    const documents: T[] = [];

    for (const { key, doc: rawDoc } of raw.documents) {
      const doc = rawDoc as Record<string, unknown>;
      const versionField = this.model.options.versionField;
      const docVersion = (doc[versionField] as number | undefined) ?? 1;

      if (docVersion < this.model.latestVersion) {
        const migrated = await this.model.migrate(doc);
        await this.writeback(key, migrated, options);
        documents.push(migrated);
      } else {
        documents.push(await this.model.validate(rawDoc));
      }
    }

    return { documents, cursor: raw.cursor };
  }

  async create(key: string, data: T, options?: unknown): Promise<T> {
    const existing = await this.engine.get(this.model.name, key, options);

    if (existing !== null && existing !== undefined) {
      throw new DocumentAlreadyExistsError(this.model.name, key);
    }

    const validated = await this.model.validate(data);
    const doc = this.stamp(validated as object);
    const indexes = this.model.resolveIndexKeys(validated);

    await this.engine.put(this.model.name, key, doc, indexes, options);

    return validated;
  }

  async update(key: string, data: Partial<T>, options?: unknown): Promise<T> {
    const existing = await this.engine.get(this.model.name, key, options);

    if (existing === null || existing === undefined) {
      throw new Error(`Document "${key}" not found in model "${this.model.name}"`);
    }

    const existingDoc = existing as Record<string, unknown>;
    const versionField = this.model.options.versionField;
    const docVersion = (existingDoc[versionField] as number | undefined) ?? 1;

    // Migrate the existing document first so the partial update merges against
    // the latest schema shape, not a potentially outdated one.
    let current: Record<string, unknown>;
    if (docVersion < this.model.latestVersion) {
      current = (await this.model.migrate(existingDoc)) as Record<string, unknown>;
    } else {
      current = existingDoc;
    }

    const merged = { ...current, ...(data as object) };
    const validated = await this.model.validate(merged);
    const doc = this.stamp(validated as object);
    const indexes = this.model.resolveIndexKeys(validated);

    await this.engine.put(this.model.name, key, doc, indexes, options);

    return validated;
  }

  async delete(key: string, options?: unknown): Promise<void> {
    await this.engine.delete(this.model.name, key, options);
  }

  async batchGet(keys: string[], options?: unknown): Promise<T[]> {
    if (!this.engine.batchGet) {
      throw new Error(`Engine does not support batchGet`);
    }

    const rawDocs = await this.engine.batchGet(this.model.name, keys, options);
    const results: T[] = [];

    for (const { key, doc: rawDoc } of rawDocs) {
      const doc = rawDoc as Record<string, unknown>;
      const versionField = this.model.options.versionField;
      const docVersion = (doc[versionField] as number | undefined) ?? 1;

      if (docVersion < this.model.latestVersion) {
        const migrated = await this.model.migrate(doc);
        await this.writeback(key, migrated, options);
        results.push(migrated);
      } else {
        results.push(await this.model.validate(rawDoc));
      }
    }

    return results;
  }

  async batchSet(items: BatchSetInputItem<T>[], options?: unknown): Promise<T[]> {
    const prepared: {
      key: string;
      validated: T;
      doc: object;
      indexes: Record<string, string>;
    }[] = [];

    for (const item of items) {
      if (typeof item.key !== "string") {
        throw new Error(`Invalid document key for model "${this.model.name}"`);
      }

      const validated = await this.model.validate(item.data);

      prepared.push({
        key: item.key,
        validated,
        doc: this.stamp(validated as object),
        indexes: this.model.resolveIndexKeys(validated),
      });
    }

    if (this.engine.batchSet) {
      await this.engine.batchSet(
        this.model.name,
        prepared.map((item) => ({
          key: item.key,
          doc: item.doc,
          indexes: item.indexes,
        })),
        options,
      );
    } else {
      for (const item of prepared) {
        await this.engine.put(this.model.name, item.key, item.doc, item.indexes, options);
      }
    }

    return prepared.map((item) => item.validated);
  }

  async batchDelete(keys: string[], options?: unknown): Promise<void> {
    if (this.engine.batchDelete) {
      await this.engine.batchDelete(this.model.name, keys, options);
      return;
    }

    for (const key of keys) {
      await this.engine.delete(this.model.name, key, options);
    }
  }

  async migrateAll(): Promise<MigrationResult> {
    const collection = this.model.name;
    const lock = await this.engine.migration.acquireLock(collection);

    if (!lock) {
      throw new MigrationAlreadyRunningError(collection);
    }

    let migrated = 0;

    try {
      // Build criteria so the engine can efficiently find outdated documents.
      const criteria: MigrationCriteria = {
        version: this.model.latestVersion,
        versionField: this.model.options.versionField,
        indexes: this.model.indexNames,
        indexesField: this.model.options.indexesField,
      };

      // Resume from the last saved checkpoint if a previous run was interrupted.
      let cursor = (await this.engine.migration.loadCheckpoint?.(collection)) ?? undefined;

      do {
        // The engine returns only documents that need migration or reindexing.
        const page = await this.engine.migration.getOutdated(collection, criteria, cursor);

        for (const { key, doc: rawDoc } of page.documents) {
          const doc = rawDoc as Record<string, unknown>;
          const versionField = this.model.options.versionField;
          const docVersion = (doc[versionField] as number | undefined) ?? 1;

          if (docVersion < this.model.latestVersion) {
            // Migrate stale documents forward, stamp metadata, recompute indexes.
            const migratedDoc = await this.model.migrate(doc);
            const stamped = this.stamp(migratedDoc as object);
            const indexes = this.model.resolveIndexKeys(migratedDoc);
            await this.engine.put(collection, key, stamped, indexes);
            migrated++;
          } else {
            // Current version but indexes are outdated — recompute and stamp.
            const validated = await this.model.validate(rawDoc);
            const stamped = this.stamp(validated as object);
            const indexes = this.model.resolveIndexKeys(validated);
            await this.engine.put(collection, key, stamped, indexes);
          }
        }

        cursor = page.cursor ?? undefined;

        if (cursor) {
          await this.engine.migration.saveCheckpoint?.(lock, cursor);
        }
      } while (cursor);

      // Clear checkpoint after a successful full run so future runs scan
      // from the beginning and cannot skip older stale documents.
      await this.engine.migration.clearCheckpoint?.(collection);

      return { model: collection, status: "completed", migrated };
    } finally {
      await this.engine.migration.releaseLock(lock);
    }
  }

  async getMigrationStatus(): Promise<MigrationStatus | null> {
    if (!this.engine.migration.getStatus) {
      return null;
    }

    return this.engine.migration.getStatus(this.model.name);
  }

  // ---------------------------------------------------------------------------
  // Private helpers
  // ---------------------------------------------------------------------------

  // Stamps the version and index names onto a document for storage.
  private stamp(data: object): Record<string, unknown> {
    return {
      ...data,
      [this.model.options.versionField]: this.model.latestVersion,
      [this.model.options.indexesField]: this.model.indexNames,
    };
  }

  // Writes the migrated document back to the engine so future reads don't need
  // to re-migrate. Skipped in "readonly" and "eager" modes — readonly because
  // writes are not permitted, eager because migration is expected to happen
  // via migrateAll() rather than on individual reads.
  private async writeback(key: string, data: T, options?: unknown): Promise<void> {
    if (this.model.options.migration !== "lazy") {
      return;
    }

    const doc = this.stamp(data as object);
    const indexes = this.model.resolveIndexKeys(data);

    await this.engine.put(this.model.name, key, doc, indexes, options);
  }

  // Resolves query params: `where` shorthand → `index`/`filter`, or pass through
  // `index`/`filter`, or no filter at all (scan all documents).
  // The user-facing `index` param uses the query name; this resolves it to the
  // storage key that the engine understands.
  private resolveQuery(params: QueryParams): QueryParams {
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

export function createStore(
  engine: QueryEngine<any>,
  models: ModelDefinition<any>[],
): Record<string, any> {
  const boundModels = new Map<string, BoundModelImpl<any>>();

  for (const modelDef of models) {
    if (boundModels.has(modelDef.name)) {
      throw new Error(`Duplicate model name: "${modelDef.name}"`);
    }

    boundModels.set(modelDef.name, new BoundModelImpl(modelDef, engine));
  }

  const store: Record<string, any> = {
    // Migrates all models sequentially. Errors are caught per-model so one
    // model's failure doesn't prevent the others from being migrated.
    async migrateAll(): Promise<MigrationResult[]> {
      const results: MigrationResult[] = [];

      for (const [name, boundModel] of boundModels) {
        try {
          const result = await boundModel.migrateAll();
          results.push(result);
        } catch (err) {
          if (err instanceof MigrationAlreadyRunningError) {
            results.push({
              model: name,
              status: "skipped",
              reason: "already running",
            });
          } else {
            results.push({ model: name, status: "failed", error: err });
          }
        }
      }

      return results;
    },
  };

  for (const [name, boundModel] of boundModels) {
    store[name] = {
      findByKey: boundModel.findByKey.bind(boundModel),
      query: boundModel.query.bind(boundModel),
      create: boundModel.create.bind(boundModel),
      update: boundModel.update.bind(boundModel),
      delete: boundModel.delete.bind(boundModel),
      batchGet: boundModel.batchGet.bind(boundModel),
      batchSet: boundModel.batchSet.bind(boundModel),
      batchDelete: boundModel.batchDelete.bind(boundModel),
      migrateAll: boundModel.migrateAll.bind(boundModel),
      getMigrationStatus: boundModel.getMigrationStatus.bind(boundModel),
    };
  }

  return store;
}
