import type { StandardSchemaV1 } from "@standard-schema/spec";

import type { ResolvedIndexKeys } from "./engines/types";

// ---------------------------------------------------------------------------
// Public model types
// ---------------------------------------------------------------------------

export type MigrationStrategy = "lazy" | "readonly" | "eager";
export type MigrationErrorMode = "ignore" | "throw";

export type VersionValue = string | number;
export type ParseVersion = (raw: unknown) => VersionValue | null;
export type CompareVersions = (a: VersionValue, b: VersionValue) => number;

export interface ModelOptions {
  migration?: MigrationStrategy;
  migrationErrors?: MigrationErrorMode;
  versionField?: string;
  indexesField?: string;
  parseVersion?: ParseVersion;
  compareVersions?: CompareVersions;
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

export interface IndexDefinition<T, TName extends string = string> {
  name: TName;
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
export interface Model<
  _T,
  _TOptions = Record<string, unknown>,
  _TName extends string = string,
  _TStaticIndexNames extends string = never,
  _THasDynamicIndexes extends boolean = false,
> {
  readonly name: _TName;
  readonly options: ModelOptions;
  readonly latestVersion: number;
  /** Type-only phantom fields used for inference in store generics. */
  readonly __shape__?: _T;
  readonly __options__?: _TOptions;
  readonly __staticIndexNames__?: _TStaticIndexNames;
  readonly __hasDynamicIndexes__?: _THasDynamicIndexes;
}

/**
 * The model builder. Schemas must be chained in version order.
 * Indexes reference the latest schema's shape.
 */
export interface ModelBuilder<
  T,
  TOptions = Record<string, unknown>,
  TName extends string = string,
  TStaticIndexNames extends string = never,
  THasDynamicIndexes extends boolean = false,
> {
  schema<TNext>(
    version: number,
    shape: StandardSchemaV1<TNext>,
    options: SchemaOptions<TNext, T>,
  ): ModelBuilder<TNext, TOptions, TName, never, false>;

  /** Add an index with a static name. The name serves as both query name and storage key. */
  index<TIndexName extends string>(
    definition: IndexDefinition<T, TIndexName>,
  ): ModelBuilder<T, TOptions, TName, TStaticIndexNames | TIndexName, THasDynamicIndexes>;
  /** Runtime-validated dynamic form; throws if no definition is supplied. */
  index(key: string): ModelBuilder<T, TOptions, TName, TStaticIndexNames, THasDynamicIndexes>;
  /** Add an index with a dynamic name. The key is the storage identity (tracked in __indexes). */
  index(
    key: string,
    definition: DynamicIndexDefinition<T>,
  ): ModelBuilder<T, TOptions, TName, TStaticIndexNames, true>;

  build(): Model<T, TOptions, TName, TStaticIndexNames, THasDynamicIndexes>;
}

/**
 * Initial model builder returned by model(). Accepts the first schema
 * (version 1, no migrate function).
 */
export interface InitialModelBuilder<
  TName extends string = string,
  TOptions = Record<string, unknown>,
> {
  schema<T>(version: 1, shape: StandardSchemaV1<T>): ModelBuilder<T, TOptions, TName, never, false>;
}

// ---------------------------------------------------------------------------
// Internal schema version representation
// ---------------------------------------------------------------------------

interface StoredSchemaVersion {
  version: number;
  shape: StandardSchemaV1;
  migrate?: (old: unknown) => unknown;
}

export type ProjectionSkipReason =
  | "invalid_version"
  | "ahead_of_latest"
  | "unknown_source_version"
  | "version_compare_error"
  | "migration_error"
  | "validation_error";

export type ProjectionResult<T> =
  | {
      ok: true;
      value: T;
      migrated: boolean;
    }
  | {
      ok: false;
      reason: ProjectionSkipReason;
      error?: unknown;
    };

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

export class ModelDefinition<
  T = unknown,
  TOptions = Record<string, unknown>,
  TName extends string = string,
  TStaticIndexNames extends string = never,
  THasDynamicIndexes extends boolean = false,
> implements Model<T, TOptions, TName, TStaticIndexNames, THasDynamicIndexes> {
  readonly name: TName;
  readonly options: Required<ModelOptions>;
  readonly versions: StoredSchemaVersion[];
  readonly indexes: StoredIndex<T>[];

  constructor(
    name: TName,
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

  get latestVersionValue(): VersionValue {
    return this.latestVersion;
  }

  /**
   * Projects an arbitrary stored document to the latest schema shape.
   * Returns a skip result instead of throwing when migration/validation
   * cannot be safely completed.
   */
  async projectToLatest(doc: Record<string, unknown>): Promise<ProjectionResult<T>> {
    const versionField = this.options.versionField;
    const parsedVersion = this.options.parseVersion(doc[versionField]);

    if (parsedVersion === null) {
      return { ok: false, reason: "invalid_version" };
    }

    const latest = this.latestVersionValue;
    const latestCmp = this.safeCompareVersions(parsedVersion, latest);

    if (!latestCmp.ok) {
      return { ok: false, reason: "version_compare_error", error: latestCmp.error };
    }

    if (latestCmp.value > 0) {
      return { ok: false, reason: "ahead_of_latest" };
    }

    if (latestCmp.value === 0) {
      try {
        const validated = await this.validate(doc);
        return { ok: true, value: validated, migrated: false };
      } catch (error) {
        return { ok: false, reason: "validation_error", error };
      }
    }

    // Find the schema version that matches the document's declared version.
    let sourceIndex = -1;
    for (let i = 0; i < this.versions.length; i++) {
      const version = this.versions[i]!;
      const cmp = this.safeCompareVersions(parsedVersion, version.version);

      if (!cmp.ok) {
        return { ok: false, reason: "version_compare_error", error: cmp.error };
      }

      if (cmp.value === 0) {
        sourceIndex = i;
        break;
      }
    }

    if (sourceIndex === -1) {
      return { ok: false, reason: "unknown_source_version" };
    }

    // Walk through each version step sequentially — v2's migrate receives v1 data,
    // v3's migrate receives the output of v2's migrate, and so on.
    let current: unknown = doc;

    for (let i = sourceIndex + 1; i < this.versions.length; i++) {
      const version = this.versions[i];

      if (!version?.migrate) {
        return {
          ok: false,
          reason: "migration_error",
          error: new MigrationError(this.name, version?.version ?? i + 1),
        };
      }

      try {
        current = version.migrate(current);
      } catch (error) {
        return { ok: false, reason: "migration_error", error };
      }
    }

    try {
      const validated = await this.validate(current);
      return { ok: true, value: validated, migrated: true };
    } catch (error) {
      return { ok: false, reason: "validation_error", error };
    }
  }

  // Runs the forward-only migration chain from the document's version to the latest.
  // Documents without a version field are assumed to be version 1 (the first schema).
  async migrate(doc: Record<string, unknown>): Promise<T> {
    const projected = await this.projectToLatest(doc);

    if (projected.ok) {
      return projected.value;
    }

    if (projected.reason === "ahead_of_latest") {
      const rawVersion = doc[this.options.versionField];
      throw new VersionError(this.name, String(rawVersion), this.latestVersionValue);
    }

    if (projected.reason === "migration_error" && projected.error) {
      throw projected.error;
    }

    if (projected.reason === "validation_error" && projected.error) {
      throw projected.error;
    }

    if (projected.reason === "unknown_source_version") {
      const rawVersion = doc[this.options.versionField];
      throw new VersionError(this.name, String(rawVersion), this.latestVersionValue);
    }

    if (projected.reason === "invalid_version") {
      throw new Error(`Document version for model "${this.name}" is invalid and cannot be parsed`);
    }

    if (projected.reason === "version_compare_error") {
      throw projected.error ?? new Error(`Version comparison failed for model "${this.name}"`);
    }

    throw new Error(`Migration failed for model "${this.name}"`);
  }

  private safeCompareVersions(
    a: VersionValue,
    b: VersionValue,
  ): { ok: true; value: -1 | 0 | 1 } | { ok: false; error: unknown } {
    try {
      const raw = this.options.compareVersions(a, b);

      if (!Number.isFinite(raw)) {
        return {
          ok: false,
          error: new Error(`compareVersions must return a finite number`),
        };
      }

      if (raw < 0) {
        return { ok: true, value: -1 };
      }

      if (raw > 0) {
        return { ok: true, value: 1 };
      }

      return { ok: true, value: 0 };
    } catch (error) {
      return { ok: false, error };
    }
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

  resolveUniqueIndexKeys(data: T): ResolvedIndexKeys {
    const resolved: ResolvedIndexKeys = {};

    for (const index of this.indexes) {
      if (index.unique !== true) {
        continue;
      }

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

class ModelBuilderImpl<
  T,
  TOptions = Record<string, unknown>,
  TName extends string = string,
  TStaticIndexNames extends string = never,
  THasDynamicIndexes extends boolean = false,
> implements ModelBuilder<T, TOptions, TName, TStaticIndexNames, THasDynamicIndexes> {
  private name: TName;
  private options: Required<ModelOptions>;
  private versions: StoredSchemaVersion[];
  private indexes: StoredIndex<T>[];

  constructor(
    name: TName,
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
  ): ModelBuilderImpl<TNext, TOptions, TName, never, false> {
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

    return new ModelBuilderImpl<TNext, TOptions, TName, never, false>(
      this.name,
      this.options,
      versions,
      [],
    );
  }

  index<TIndexName extends string>(
    definition: IndexDefinition<T, TIndexName>,
  ): ModelBuilderImpl<T, TOptions, TName, TStaticIndexNames | TIndexName, THasDynamicIndexes>;
  index(key: string): ModelBuilderImpl<T, TOptions, TName, TStaticIndexNames, THasDynamicIndexes>;
  index(
    key: string,
    definition: DynamicIndexDefinition<T>,
  ): ModelBuilderImpl<T, TOptions, TName, TStaticIndexNames, true>;
  index(
    keyOrDefinition: string | IndexDefinition<T, string>,
    definition?: DynamicIndexDefinition<T>,
  ): ModelBuilderImpl<T, TOptions, TName, TStaticIndexNames | string, boolean> {
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

    return new ModelBuilderImpl<T, TOptions, TName, TStaticIndexNames | string, boolean>(
      this.name,
      this.options,
      this.versions,
      [...this.indexes, stored],
    );
  }

  build(): ModelDefinition<T, TOptions, TName, TStaticIndexNames, THasDynamicIndexes> {
    if (this.versions.length === 0) {
      throw new Error(`Model "${this.name}" must have at least one schema version`);
    }

    this.validateIndexes();

    return new ModelDefinition<T, TOptions, TName, TStaticIndexNames, THasDynamicIndexes>(
      this.name,
      this.options,
      this.versions,
      this.indexes,
    );
  }

  private validateIndexes(): void {
    const keyCategories = new Map<string, "static index name" | "dynamic index key">();

    for (const index of this.indexes) {
      const category = typeof index.name === "string" ? "static index name" : "dynamic index key";
      const existingCategory = keyCategories.get(index.key);

      if (existingCategory) {
        if (existingCategory === category) {
          throw new Error(`Model "${this.name}" has duplicate ${category} "${index.key}"`);
        }

        throw new Error(
          `Model "${this.name}" has duplicate index key "${index.key}" shared by a ${existingCategory} and a ${category}`,
        );
      }

      keyCategories.set(index.key, category);
    }
  }
}

// ---------------------------------------------------------------------------
// model() factory
// ---------------------------------------------------------------------------

const DEFAULT_OPTIONS: Required<ModelOptions> = {
  migration: "lazy",
  migrationErrors: "ignore",
  versionField: "__v",
  indexesField: "__indexes",
  parseVersion: defaultParseVersion,
  compareVersions: defaultCompareVersions,
};

export function model<TName extends string>(
  name: TName,
  options?: ModelOptions,
): InitialModelBuilderImpl<TName> {
  const resolved: Required<ModelOptions> = {
    ...DEFAULT_OPTIONS,
    ...options,
  };

  return new InitialModelBuilderImpl<TName>(name, resolved);
}

export class InitialModelBuilderImpl<
  TName extends string = string,
> implements InitialModelBuilder<TName> {
  private name: TName;
  private options: Required<ModelOptions>;

  constructor(name: TName, options: Required<ModelOptions>) {
    this.name = name;
    this.options = options;
  }

  schema<T>(
    version: 1,
    shape: StandardSchemaV1<T>,
  ): ModelBuilderImpl<T, Record<string, unknown>, TName, never, false> {
    if (version !== 1) {
      throw new Error(`First schema version must be 1, got ${String(version)}`);
    }

    const versions: StoredSchemaVersion[] = [
      {
        version: 1,
        shape: shape as StandardSchemaV1,
      },
    ];

    return new ModelBuilderImpl<T, Record<string, unknown>, TName, never, false>(
      this.name,
      this.options,
      versions,
      [],
    );
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

function defaultParseVersion(raw: unknown): VersionValue | null {
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

    const normalizedNumeric = normalizeNumericVersion(trimmed);

    if (normalizedNumeric !== null) {
      return normalizedNumeric;
    }

    return trimmed;
  }

  return null;
}

function defaultCompareVersions(a: VersionValue, b: VersionValue): number {
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
  constructor(modelName: string, docVersion: VersionValue, latestVersion: VersionValue) {
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
