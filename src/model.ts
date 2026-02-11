import type { StandardSchemaV1 } from "@standard-schema/spec";
import type { ResolvedIndexKeys } from "./engines/types";

// ---------------------------------------------------------------------------
// Public model types
// ---------------------------------------------------------------------------

export type MigrationStrategy = "lazy" | "readonly" | "eager";

export interface ModelOptions {
  migration?: MigrationStrategy;
  versionField?: string;
  indexesField?: string;
}

/**
 * Options for a versioned schema (version 2+). The migrate function receives
 * the previous version's shape and returns the new shape.
 */
export interface SchemaOptions<TShape, TPrev> {
  migrate: (old: TPrev) => TShape;
}

/**
 * An index value can be a direct field reference (keyof the latest shape)
 * or a function that computes a string from the model data.
 */
export type IndexValue<T> = (keyof T & string) | ((data: T) => string);

export interface IndexDefinition<T> {
  name: string;
  value: IndexValue<T>;
  unique?: boolean;
}

/**
 * An index definition where `name` is computed dynamically from document data.
 * Because the name varies per document, a static `key` must be provided as
 * the first argument to `.index(key, definition)` for storage tracking.
 */
export interface DynamicIndexDefinition<T> {
  name: (data: T) => string;
  value: (data: T) => string;
  unique?: boolean;
}

/**
 * A built model definition. Holds the schema version chain, index
 * definitions, and model-level options.
 */
export interface Model<_T, _TOptions = Record<string, unknown>> {
  readonly name: string;
  readonly options: ModelOptions;
  readonly latestVersion: number;
}

/**
 * The model builder. Schemas must be chained in version order.
 * Indexes reference the latest schema's shape.
 */
export interface ModelBuilder<T, TOptions = Record<string, unknown>> {
  schema<TNext>(
    version: number,
    shape: StandardSchemaV1<TNext>,
    options: SchemaOptions<TNext, T>,
  ): ModelBuilder<TNext, TOptions>;

  /** Add an index with a static name. The name serves as both query name and storage key. */
  index(definition: IndexDefinition<T>): ModelBuilder<T, TOptions>;
  /** Add an index with a dynamic name. The key is the storage identity (tracked in __indexes). */
  index(key: string, definition: DynamicIndexDefinition<T>): ModelBuilder<T, TOptions>;

  build(): Model<T, TOptions>;
}

/**
 * Initial model builder returned by model(). Accepts the first schema
 * (version 1, no migrate function).
 */
export interface InitialModelBuilder<TOptions = Record<string, unknown>> {
  schema<T>(version: 1, shape: StandardSchemaV1<T>): ModelBuilder<T, TOptions>;
}

// ---------------------------------------------------------------------------
// Internal schema version representation
// ---------------------------------------------------------------------------

interface StoredSchemaVersion {
  version: number;
  shape: StandardSchemaV1;
  migrate?: (old: unknown) => unknown;
}

// ---------------------------------------------------------------------------
// Internal index representation (always has key)
// ---------------------------------------------------------------------------

interface StoredIndex<T> {
  /** Storage identity key — used in __indexes stamp for tracking. */
  key: string;
  /** Index name — static string or function that computes the engine-level index name per document. */
  name: string | ((data: T) => string);
  value: IndexValue<T>;
  unique?: boolean;
}

// ---------------------------------------------------------------------------
// ModelDefinition — the built model object
// ---------------------------------------------------------------------------

export class ModelDefinition<T = unknown> {
  readonly name: string;
  readonly options: Required<ModelOptions>;
  readonly versions: StoredSchemaVersion[];
  readonly indexes: StoredIndex<T>[];

  constructor(
    name: string,
    options: Required<ModelOptions>,
    versions: StoredSchemaVersion[],
    indexes: StoredIndex<T>[],
  ) {
    this.name = name;
    this.options = options;
    this.versions = versions;
    this.indexes = indexes;
  }

  get latestVersion(): number {
    return this.versions.length;
  }

  get latestShape(): StandardSchemaV1 {
    const latest = this.versions[this.versions.length - 1];

    if (!latest) {
      throw new Error(`Model "${this.name}" has no schema versions`);
    }

    return latest.shape;
  }

  // Uses the Standard Schema `~standard` protocol to validate data against the latest schema version.
  async validate(data: unknown): Promise<T> {
    const result = await this.latestShape["~standard"].validate(data);

    if ("issues" in result && result.issues) {
      throw new ValidationError(this.name, result.issues);
    }

    return (result as { value: unknown }).value as T;
  }

  // Runs the forward-only migration chain from the document's version to the latest.
  // Documents without a version field are assumed to be version 1 (the first schema).
  async migrate(doc: Record<string, unknown>): Promise<T> {
    const versionField = this.options.versionField;
    const docVersion = (doc[versionField] as number | undefined) ?? 1;
    const latest = this.latestVersion;

    if (docVersion > latest) {
      throw new VersionError(this.name, docVersion, latest);
    }

    if (docVersion === latest) {
      return await this.validate(doc);
    }

    // Walk through each version step sequentially — v2's migrate receives v1 data,
    // v3's migrate receives the output of v2's migrate, and so on.
    let current: unknown = doc;

    for (let v = docVersion + 1; v <= latest; v++) {
      const version = this.versions[v - 1];

      if (!version?.migrate) {
        throw new MigrationError(this.name, v);
      }

      current = version.migrate(current);
    }

    const validated = await this.validate(current);

    return validated;
  }

  /** Sorted array of current index keys for this model. */
  get indexNames(): string[] {
    return this.indexes.map((idx) => idx.key).sort();
  }

  resolveIndexKeys(data: T): ResolvedIndexKeys {
    const resolved: ResolvedIndexKeys = {};

    for (const index of this.indexes) {
      // For static-name indexes, the resolved name equals the key.
      // For dynamic-name indexes, the resolved name is computed from document data,
      // and becomes the engine-level index identifier for this document.
      const resolvedName = typeof index.name === "function" ? index.name(data) : index.name;
      resolved[resolvedName] = resolveIndexValue(index.value as IndexValue<unknown>, data);
    }

    return resolved;
  }
}

// ---------------------------------------------------------------------------
// Builder
// ---------------------------------------------------------------------------

// Two-stage builder pattern: InitialModelBuilderImpl accepts only version 1 (no migrate),
// then returns a ModelBuilderImpl which accepts subsequent versioned schemas (with migrate).
// This enforces at the type level that the first schema cannot have a migration function.

class ModelBuilderImpl<T> {
  private name: string;
  private options: Required<ModelOptions>;
  private versions: StoredSchemaVersion[];
  private indexes: StoredIndex<T>[];

  constructor(
    name: string,
    options: Required<ModelOptions>,
    versions: StoredSchemaVersion[],
    indexes: StoredIndex<T>[],
  ) {
    this.name = name;
    this.options = options;
    this.versions = versions;
    this.indexes = indexes;
  }

  // Adding a new schema version resets indexes because the shape may have changed,
  // so any previously defined indexes (referencing the old shape) would be invalid.
  schema<TNext>(
    version: number,
    shape: StandardSchemaV1<TNext>,
    options: SchemaOptions<TNext, T>,
  ): ModelBuilderImpl<TNext> {
    const expectedVersion = this.versions.length + 1;

    if (version !== expectedVersion) {
      throw new Error(`Schema version must be ${expectedVersion}, got ${version}`);
    }

    if (!options.migrate) {
      throw new Error(`Schema version ${version} must have a migrate function`);
    }

    const versions: StoredSchemaVersion[] = [
      ...this.versions,
      {
        version,
        shape: shape as StandardSchemaV1,
        migrate: options.migrate as (old: unknown) => unknown,
      },
    ];

    return new ModelBuilderImpl<TNext>(
      this.name,
      this.options,
      versions,
      [] as unknown as StoredIndex<TNext>[],
    );
  }

  index(
    keyOrDefinition: string | IndexDefinition<T>,
    definition?: DynamicIndexDefinition<T>,
  ): ModelBuilderImpl<T> {
    let stored: StoredIndex<T>;

    if (typeof keyOrDefinition === "string") {
      // Dynamic-name form: index(key, { name: fn, value: fn })
      // The key is a static identifier for __indexes tracking.
      if (!definition) {
        throw new Error(
          `index("${keyOrDefinition}", ...) requires a definition object as the second argument`,
        );
      }

      stored = {
        key: keyOrDefinition,
        name: definition.name,
        value: definition.value as IndexValue<T>,
        unique: definition.unique,
      };
    } else {
      // Static-name form: index({ name: "byEmail", value: "email" | fn })
      // The name serves as both query name and storage key.
      const def = keyOrDefinition;

      stored = {
        key: def.name,
        name: def.name,
        value: def.value,
        unique: def.unique,
      };
    }

    return new ModelBuilderImpl<T>(this.name, this.options, this.versions, [
      ...this.indexes,
      stored,
    ]);
  }

  build(): ModelDefinition<T> {
    if (this.versions.length === 0) {
      throw new Error(`Model "${this.name}" must have at least one schema version`);
    }

    return new ModelDefinition<T>(this.name, this.options, this.versions, this.indexes);
  }
}

// ---------------------------------------------------------------------------
// model() factory
// ---------------------------------------------------------------------------

const DEFAULT_OPTIONS: Required<ModelOptions> = {
  migration: "lazy",
  versionField: "__v",
  indexesField: "__indexes",
};

export function model(name: string, options?: ModelOptions): InitialModelBuilderImpl {
  const resolved: Required<ModelOptions> = {
    ...DEFAULT_OPTIONS,
    ...options,
  };

  return new InitialModelBuilderImpl(name, resolved);
}

export class InitialModelBuilderImpl {
  private name: string;
  private options: Required<ModelOptions>;

  constructor(name: string, options: Required<ModelOptions>) {
    this.name = name;
    this.options = options;
  }

  schema<T>(version: 1, shape: StandardSchemaV1<T>): ModelBuilderImpl<T> {
    if (version !== 1) {
      throw new Error(`First schema version must be 1, got ${String(version)}`);
    }

    const versions: StoredSchemaVersion[] = [
      {
        version: 1,
        shape: shape as StandardSchemaV1,
      },
    ];

    return new ModelBuilderImpl<T>(this.name, this.options, versions, []);
  }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function resolveIndexValue<T>(value: IndexValue<T>, data: T): string {
  if (typeof value === "function") {
    return value(data);
  }

  return String((data as Record<string, unknown>)[value]);
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

export class ValidationError extends Error {
  readonly issues: readonly StandardSchemaV1.Issue[];

  constructor(modelName: string, issues: readonly StandardSchemaV1.Issue[]) {
    super(`Validation failed for model "${modelName}": ${issues.map((i) => i.message).join(", ")}`);
    this.name = "ValidationError";
    this.issues = issues;
  }
}

export class VersionError extends Error {
  constructor(modelName: string, docVersion: number, latestVersion: number) {
    super(
      `Document version ${docVersion} for model "${modelName}" exceeds latest schema version ${latestVersion}`,
    );
    this.name = "VersionError";
  }
}

export class MigrationError extends Error {
  constructor(modelName: string, version: number) {
    super(`Schema version ${version} for model "${modelName}" is missing a migrate function`);
    this.name = "MigrationError";
  }
}
