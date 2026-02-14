import {
  type BatchSetItem,
  type EngineQueryResult,
  type MigrationCriteria,
  type MigrationLock,
  type QueryEngine,
} from "./engines/types";

export interface MigrationRunOptions {
  /** Lock TTL in milliseconds. If provided, stale locks can be replaced by the engine. */
  readonly lockTtlMs?: number;
}

export interface MigrationProjectionResult {
  readonly ok: boolean;
  readonly value?: unknown;
  readonly migrated?: boolean;
  readonly reason?: string;
  readonly error?: unknown;
}

export interface MigrationModelContext {
  readonly collection: string;
  readonly criteria: MigrationCriteria;
  project(doc: Record<string, unknown>): Promise<MigrationProjectionResult>;
  toBatchSetItem(key: string, value: unknown): BatchSetItem;
  persist(items: BatchSetItem[]): Promise<void>;
}

export type MigrationScope =
  | {
      readonly scope: "model";
      readonly model: string;
    }
  | {
      readonly scope: "store";
      readonly models: readonly string[];
    };

export interface MigrationModelProgress {
  readonly migrated: number;
  readonly skipped: number;
  readonly pages: number;
  readonly skipReasons: Record<string, number>;
}

export interface MigrationRunProgress {
  readonly id: string;
  readonly scope: "model" | "store";
  readonly models: readonly string[];
  readonly modelIndex: number;
  readonly cursor: string | null;
  readonly startedAt: number;
  readonly updatedAt: number;
  readonly running: boolean;
  readonly totals: {
    readonly migrated: number;
    readonly skipped: number;
  };
  readonly progressByModel: Record<string, MigrationModelProgress>;
}

export interface MigrationGetOrCreateResult {
  readonly progress: MigrationRunProgress;
  readonly created: boolean;
}

export interface MigrationNextPageResult {
  readonly status: "busy" | "processed" | "completed";
  readonly model: string | null;
  readonly migrated: number;
  readonly skipped: number;
  readonly skipReasons?: Record<string, number>;
  readonly completed: boolean;
  readonly hasMore: boolean;
  readonly progress: MigrationRunProgress | null;
}

export interface MigrationCreatedEvent {
  readonly progress: MigrationRunProgress;
}

export interface MigrationResumedEvent {
  readonly progress: MigrationRunProgress;
}

export interface MigrationPageClaimedEvent {
  readonly runId: string;
  readonly scope: "model" | "store";
  readonly model: string;
  readonly cursor: string | null;
  readonly documentCount: number;
}

export interface MigrationDocumentMigratedEvent {
  readonly runId: string;
  readonly model: string;
  readonly key: string;
}

export interface MigrationDocumentSkippedEvent {
  readonly runId: string;
  readonly model: string;
  readonly key: string;
  readonly reason: string;
  readonly error?: unknown;
}

export interface MigrationPageCommittedEvent {
  readonly runId: string;
  readonly model: string;
  readonly migrated: number;
  readonly skipped: number;
  readonly cursor: string | null;
  readonly hasMore: boolean;
}

export interface MigrationCompletedEvent {
  readonly progress: MigrationRunProgress;
}

export interface MigrationFailedEvent {
  readonly runId: string;
  readonly scope: "model" | "store";
  readonly error: unknown;
  readonly progress: MigrationRunProgress | null;
}

export interface MigrationHooks {
  onMigrationCreated?(event: MigrationCreatedEvent): void | Promise<void>;
  onMigrationResumed?(event: MigrationResumedEvent): void | Promise<void>;
  onPageClaimed?(event: MigrationPageClaimedEvent): void | Promise<void>;
  onDocumentMigrated?(event: MigrationDocumentMigratedEvent): void | Promise<void>;
  onDocumentSkipped?(event: MigrationDocumentSkippedEvent): void | Promise<void>;
  onPageCommitted?(event: MigrationPageCommittedEvent): void | Promise<void>;
  onMigrationCompleted?(event: MigrationCompletedEvent): void | Promise<void>;
  onMigrationFailed?(event: MigrationFailedEvent): void | Promise<void>;
}

export interface Migrator<_TOptions = Record<string, unknown>> {
  getOrCreateRun(
    scope: MigrationScope,
    contexts: ReadonlyMap<string, MigrationModelContext>,
    options?: MigrationRunOptions,
  ): Promise<MigrationGetOrCreateResult>;

  migrateNextPage(
    scope: MigrationScope,
    contexts: ReadonlyMap<string, MigrationModelContext>,
    options?: MigrationRunOptions,
  ): Promise<MigrationNextPageResult>;

  getProgress(scope: MigrationScope): Promise<MigrationRunProgress | null>;
}

interface PersistedMigrationModelProgress {
  migrated: number;
  skipped: number;
  pages: number;
  skipReasons: Record<string, number>;
}

interface PersistedMigrationRun {
  id: string;
  scope: "model" | "store";
  models: string[];
  modelIndex: number;
  cursor: string | null;
  startedAt: number;
  updatedAt: number;
  progressByModel: Record<string, PersistedMigrationModelProgress>;
}

interface DefaultMigratorOptions {
  readonly hooks?: MigrationHooks;
}

const STORE_SCOPE_KEY = "__nosql_odm_migration_scope__store";

export class MigrationScopeConflictError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "MigrationScopeConflictError";
  }
}

export class DefaultMigrator<TOptions = Record<string, unknown>> implements Migrator<TOptions> {
  private readonly engine: QueryEngine<TOptions>;
  private readonly hooks: MigrationHooks | undefined;

  constructor(engine: QueryEngine<TOptions>, options?: DefaultMigratorOptions) {
    this.engine = engine;
    this.hooks = options?.hooks;
  }

  async getOrCreateRun(
    scope: MigrationScope,
    contexts: ReadonlyMap<string, MigrationModelContext>,
    options?: MigrationRunOptions,
  ): Promise<MigrationGetOrCreateResult> {
    this.assertPersistenceSupport();
    const normalizedScope = normalizeScope(scope);
    this.assertContexts(normalizedScope, contexts);
    await this.assertScopeIsNotCovered(normalizedScope, options?.lockTtlMs);

    const runKey = this.scopeKey(normalizedScope);
    const lock = await this.engine.migration.acquireLock(
      runKey,
      options?.lockTtlMs !== undefined ? { ttl: options.lockTtlMs } : undefined,
    );

    if (!lock) {
      const existing = await this.loadRun(runKey);

      if (existing) {
        return {
          progress: toProgress(existing, true),
          created: false,
        };
      }

      const locked = await this.engine.migration.getStatus?.(runKey);
      const progress = locked ? placeholderProgress(normalizedScope, locked.lock.acquiredAt) : null;

      if (progress) {
        return {
          progress,
          created: false,
        };
      }

      throw new Error("Migration run is currently busy");
    }

    try {
      const existing = await this.loadRun(runKey);

      if (existing) {
        const resumed = {
          ...existing,
          updatedAt: Date.now(),
        };
        await this.saveRun(lock, resumed);
        await this.runHook("onMigrationResumed", { progress: toProgress(resumed, true) });

        return {
          progress: toProgress(resumed, true),
          created: false,
        };
      }

      const created = createRun(normalizedScope);
      await this.saveRun(lock, created);
      await this.runHook("onMigrationCreated", { progress: toProgress(created, true) });

      return {
        progress: toProgress(created, true),
        created: true,
      };
    } finally {
      await this.engine.migration.releaseLock(lock);
    }
  }

  async migrateNextPage(
    scope: MigrationScope,
    contexts: ReadonlyMap<string, MigrationModelContext>,
    options?: MigrationRunOptions,
  ): Promise<MigrationNextPageResult> {
    this.assertPersistenceSupport();
    const normalizedScope = normalizeScope(scope);
    this.assertContexts(normalizedScope, contexts);

    const runKey = this.scopeKey(normalizedScope);
    const lock = await this.engine.migration.acquireLock(
      runKey,
      options?.lockTtlMs !== undefined ? { ttl: options.lockTtlMs } : undefined,
    );

    if (!lock) {
      return {
        status: "busy",
        model: null,
        migrated: 0,
        skipped: 0,
        completed: false,
        hasMore: true,
        progress: await this.getProgress(normalizedScope),
      };
    }

    let run: PersistedMigrationRun | null = null;
    let shouldNotifyFailure = true;

    try {
      const existing = await this.loadRun(runKey);

      if (!existing) {
        try {
          await this.assertScopeIsNotCovered(normalizedScope, options?.lockTtlMs);
        } catch (error) {
          if (error instanceof MigrationScopeConflictError) {
            shouldNotifyFailure = false;
          }

          throw error;
        }
      }

      run = existing ?? createRun(normalizedScope);

      if (!existing) {
        await this.saveRun(lock, run);
        await this.runHook("onMigrationCreated", { progress: toProgress(run, true) });
      }

      while (run.modelIndex < run.models.length) {
        const modelName = run.models[run.modelIndex]!;
        const context = contexts.get(modelName);

        if (!context) {
          throw new Error(`Missing migration context for model "${modelName}"`);
        }

        const page = await this.engine.migration.getOutdated(
          modelName,
          context.criteria,
          run.cursor ?? undefined,
        );

        if (page.documents.length === 0) {
          if (page.cursor) {
            run.cursor = page.cursor;
            run.updatedAt = Date.now();
            continue;
          }

          run.modelIndex += 1;
          run.cursor = null;
          run.updatedAt = Date.now();
          continue;
        }

        await this.runHook("onPageClaimed", {
          runId: run.id,
          scope: run.scope,
          model: modelName,
          cursor: run.cursor,
          documentCount: page.documents.length,
        });

        const result = await this.migrateDocuments(run, modelName, context, page);

        run.cursor = page.cursor;
        run.updatedAt = Date.now();

        if (run.cursor === null) {
          run.modelIndex += 1;
        }

        const completed = run.modelIndex >= run.models.length;

        if (completed) {
          await this.engine.migration.clearCheckpoint?.(runKey);
          const completedProgress = toProgress(run, false);
          await this.runHook("onMigrationCompleted", { progress: completedProgress });

          return {
            status: "completed",
            model: modelName,
            migrated: result.migrated,
            skipped: result.skipped,
            skipReasons: hasKeys(result.skipReasons) ? result.skipReasons : undefined,
            completed: true,
            hasMore: false,
            progress: null,
          };
        }

        await this.saveRun(lock, run);
        const progress = toProgress(run, true);

        await this.runHook("onPageCommitted", {
          runId: run.id,
          model: modelName,
          migrated: result.migrated,
          skipped: result.skipped,
          cursor: run.cursor,
          hasMore: true,
        });

        return {
          status: "processed",
          model: modelName,
          migrated: result.migrated,
          skipped: result.skipped,
          skipReasons: hasKeys(result.skipReasons) ? result.skipReasons : undefined,
          completed: false,
          hasMore: true,
          progress,
        };
      }

      await this.engine.migration.clearCheckpoint?.(runKey);
      const completedProgress = toProgress(run, false);
      await this.runHook("onMigrationCompleted", { progress: completedProgress });

      return {
        status: "completed",
        model: null,
        migrated: 0,
        skipped: 0,
        completed: true,
        hasMore: false,
        progress: null,
      };
    } catch (error) {
      if (shouldNotifyFailure) {
        await this.runHook("onMigrationFailed", {
          runId: run?.id ?? randomId(),
          scope: normalizedScope.scope,
          error,
          progress: run ? toProgress(run, true) : null,
        });
      }
      throw error;
    } finally {
      await this.engine.migration.releaseLock(lock);
    }
  }

  async getProgress(scope: MigrationScope): Promise<MigrationRunProgress | null> {
    this.assertPersistenceSupport();
    const normalizedScope = normalizeScope(scope);
    const runKey = this.scopeKey(normalizedScope);
    const persisted = await this.loadRun(runKey);

    if (persisted) {
      return toProgress(persisted, true);
    }

    const status = await this.engine.migration.getStatus?.(runKey);

    if (!status) {
      return null;
    }

    return placeholderProgress(normalizedScope, status.lock.acquiredAt);
  }

  private async migrateDocuments(
    run: PersistedMigrationRun,
    modelName: string,
    context: MigrationModelContext,
    page: EngineQueryResult,
  ): Promise<{
    migrated: number;
    skipped: number;
    skipReasons: Record<string, number>;
  }> {
    let migrated = 0;
    let skipped = 0;
    const skipReasons: Record<string, number> = {};
    const writes: BatchSetItem[] = [];

    for (const { key, doc: rawDoc } of page.documents) {
      const projected = await context.project(rawDoc as Record<string, unknown>);

      if (!projected.ok) {
        skipped += 1;
        const reason = projected.reason ?? "unknown";
        skipReasons[reason] = (skipReasons[reason] ?? 0) + 1;
        await this.runHook("onDocumentSkipped", {
          runId: run.id,
          model: modelName,
          key,
          reason,
          error: projected.error,
        });
        continue;
      }

      writes.push(context.toBatchSetItem(key, projected.value));

      if (projected.migrated) {
        migrated += 1;
      }

      await this.runHook("onDocumentMigrated", {
        runId: run.id,
        model: modelName,
        key,
      });
    }

    if (writes.length > 0) {
      await context.persist(writes);
    }

    const current = ensureModelProgress(run, modelName);
    current.migrated += migrated;
    current.skipped += skipped;
    current.pages += 1;

    for (const [reason, count] of Object.entries(skipReasons)) {
      current.skipReasons[reason] = (current.skipReasons[reason] ?? 0) + count;
    }

    return {
      migrated,
      skipped,
      skipReasons,
    };
  }

  private scopeKey(scope: MigrationScope): string {
    if (scope.scope === "store") {
      return STORE_SCOPE_KEY;
    }

    return scope.model;
  }

  private async assertScopeIsNotCovered(
    scope: MigrationScope,
    lockTtlMs: number | undefined,
  ): Promise<void> {
    if (scope.scope === "model") {
      const storeActive = await this.isScopeActive(
        { scope: "store", models: [scope.model] },
        lockTtlMs,
      );

      if (storeActive) {
        throw new MigrationScopeConflictError(
          `Cannot start model migration for "${scope.model}" while a store migration is in progress`,
        );
      }

      return;
    }

    for (const modelName of scope.models) {
      const modelActive = await this.isScopeActive({ scope: "model", model: modelName }, lockTtlMs);

      if (modelActive) {
        throw new MigrationScopeConflictError(
          `Cannot start store migration because model "${modelName}" is already being migrated`,
        );
      }
    }
  }

  private async isScopeActive(
    scope: MigrationScope,
    lockTtlMs: number | undefined,
  ): Promise<boolean> {
    const runKey = this.scopeKey(scope);
    const existing = await this.loadRun(runKey);

    if (existing) {
      return true;
    }

    const status = await this.engine.migration.getStatus?.(runKey);

    if (!status) {
      return false;
    }

    if (
      lockTtlMs !== undefined &&
      Number.isFinite(lockTtlMs) &&
      lockTtlMs >= 0 &&
      Date.now() - status.lock.acquiredAt >= lockTtlMs
    ) {
      return false;
    }

    return true;
  }

  private async loadRun(runKey: string): Promise<PersistedMigrationRun | null> {
    const raw = await this.engine.migration.loadCheckpoint?.(runKey);

    if (!raw) {
      return null;
    }

    return parseRun(raw);
  }

  private async saveRun(lock: MigrationLock, run: PersistedMigrationRun): Promise<void> {
    await this.engine.migration.saveCheckpoint?.(lock, serializeRun(run));
  }

  private assertPersistenceSupport(): void {
    if (
      !this.engine.migration.loadCheckpoint ||
      !this.engine.migration.saveCheckpoint ||
      !this.engine.migration.clearCheckpoint
    ) {
      throw new Error("The configured engine does not support migration run persistence");
    }
  }

  private assertContexts(
    scope: MigrationScope,
    contexts: ReadonlyMap<string, MigrationModelContext>,
  ): void {
    const models = scope.scope === "model" ? [scope.model] : scope.models;

    for (const modelName of models) {
      if (!contexts.has(modelName)) {
        throw new Error(`Missing migration context for model "${modelName}"`);
      }
    }
  }

  private async runHook<T extends keyof MigrationHooks>(
    name: T,
    payload: Parameters<NonNullable<MigrationHooks[T]>>[0],
  ): Promise<void> {
    const hook = this.hooks?.[name];

    if (!hook) {
      return;
    }

    try {
      await hook(payload as never);
    } catch {
      // Hook errors are intentionally ignored so migration flow remains stable.
    }
  }
}

function normalizeScope(scope: MigrationScope): MigrationScope {
  if (scope.scope === "model") {
    return {
      scope: "model",
      model: scope.model,
    };
  }

  return {
    scope: "store",
    models: uniqueStrings(scope.models),
  };
}

function createRun(scope: MigrationScope): PersistedMigrationRun {
  const startedAt = Date.now();
  const models = scope.scope === "model" ? [scope.model] : uniqueStrings(scope.models);

  const progressByModel: Record<string, PersistedMigrationModelProgress> = {};

  for (const modelName of models) {
    progressByModel[modelName] = {
      migrated: 0,
      skipped: 0,
      pages: 0,
      skipReasons: {},
    };
  }

  return {
    id: randomId(),
    scope: scope.scope,
    models,
    modelIndex: 0,
    cursor: null,
    startedAt,
    updatedAt: startedAt,
    progressByModel,
  };
}

function ensureModelProgress(
  run: PersistedMigrationRun,
  modelName: string,
): PersistedMigrationModelProgress {
  const existing = run.progressByModel[modelName];

  if (existing) {
    return existing;
  }

  const created: PersistedMigrationModelProgress = {
    migrated: 0,
    skipped: 0,
    pages: 0,
    skipReasons: {},
  };
  run.progressByModel[modelName] = created;
  return created;
}

function toProgress(run: PersistedMigrationRun, running: boolean): MigrationRunProgress {
  let migrated = 0;
  let skipped = 0;

  for (const model of Object.values(run.progressByModel)) {
    migrated += model.migrated;
    skipped += model.skipped;
  }

  return {
    id: run.id,
    scope: run.scope,
    models: [...run.models],
    modelIndex: run.modelIndex,
    cursor: run.cursor,
    startedAt: run.startedAt,
    updatedAt: run.updatedAt,
    running,
    totals: {
      migrated,
      skipped,
    },
    progressByModel: structuredClone(run.progressByModel),
  };
}

function placeholderProgress(scope: MigrationScope, startedAt: number): MigrationRunProgress {
  const models = scope.scope === "model" ? [scope.model] : [...scope.models];
  const progressByModel: Record<string, MigrationModelProgress> = {};

  for (const modelName of models) {
    progressByModel[modelName] = {
      migrated: 0,
      skipped: 0,
      pages: 0,
      skipReasons: {},
    };
  }

  return {
    id: "unknown",
    scope: scope.scope,
    models,
    modelIndex: 0,
    cursor: null,
    startedAt,
    updatedAt: startedAt,
    running: true,
    totals: {
      migrated: 0,
      skipped: 0,
    },
    progressByModel,
  };
}

function parseRun(raw: string): PersistedMigrationRun {
  let parsed: unknown;

  try {
    parsed = JSON.parse(raw);
  } catch {
    throw new Error("Migration run state is invalid JSON");
  }

  if (!isRecord(parsed)) {
    throw new Error("Migration run state is invalid");
  }

  const id = readString(parsed, "id", "migration run state");
  const scopeRaw = readString(parsed, "scope", "migration run state");

  if (scopeRaw !== "model" && scopeRaw !== "store") {
    throw new Error("Migration run state has an invalid scope");
  }

  const modelsRaw = parsed.models;

  if (!Array.isArray(modelsRaw) || !modelsRaw.every((value) => typeof value === "string")) {
    throw new Error("Migration run state has invalid models");
  }

  const modelIndex = readFiniteInteger(parsed, "modelIndex", "migration run state");
  const cursorRaw = parsed.cursor;

  if (cursorRaw !== null && typeof cursorRaw !== "string") {
    throw new Error("Migration run state has an invalid cursor");
  }

  const startedAt = readFiniteInteger(parsed, "startedAt", "migration run state");
  const updatedAt = readFiniteInteger(parsed, "updatedAt", "migration run state");

  const progressByModelRaw = parsed.progressByModel;

  if (!isRecord(progressByModelRaw)) {
    throw new Error("Migration run state has invalid model progress");
  }

  const progressByModel: Record<string, PersistedMigrationModelProgress> = {};

  for (const [modelName, rawProgress] of Object.entries(progressByModelRaw)) {
    if (!isRecord(rawProgress)) {
      throw new Error("Migration run state has invalid model progress");
    }

    const skipReasonsRaw = rawProgress.skipReasons;

    if (!isRecord(skipReasonsRaw)) {
      throw new Error("Migration run state has invalid skip reasons");
    }

    const skipReasons: Record<string, number> = {};

    for (const [reason, rawCount] of Object.entries(skipReasonsRaw)) {
      if (typeof rawCount !== "number" || !Number.isFinite(rawCount) || rawCount < 0) {
        throw new Error("Migration run state has invalid skip reason counts");
      }

      skipReasons[reason] = rawCount;
    }

    progressByModel[modelName] = {
      migrated: readFiniteInteger(rawProgress, "migrated", "migration run state model progress"),
      skipped: readFiniteInteger(rawProgress, "skipped", "migration run state model progress"),
      pages: readFiniteInteger(rawProgress, "pages", "migration run state model progress"),
      skipReasons,
    };
  }

  return {
    id,
    scope: scopeRaw,
    models: modelsRaw,
    modelIndex,
    cursor: cursorRaw,
    startedAt,
    updatedAt,
    progressByModel,
  };
}

function serializeRun(run: PersistedMigrationRun): string {
  return JSON.stringify(run);
}

function uniqueStrings(values: readonly string[]): string[] {
  const seen = new Set<string>();
  const unique: string[] = [];

  for (const value of values) {
    if (!seen.has(value)) {
      seen.add(value);
      unique.push(value);
    }
  }

  unique.sort((a, b) => a.localeCompare(b));
  return unique;
}

function hasKeys(record: Record<string, number>): boolean {
  return Object.keys(record).length > 0;
}

function randomId(): string {
  if (typeof crypto !== "undefined" && typeof crypto.randomUUID === "function") {
    return crypto.randomUUID();
  }

  const now = Date.now().toString(36);
  const random = Math.random().toString(36).slice(2);
  return `${now}_${random}`;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function readString(record: Record<string, unknown>, key: string, context: string): string {
  const value = record[key];

  if (typeof value !== "string") {
    throw new Error(`Invalid ${context}: expected string at "${key}"`);
  }

  return value;
}

function readFiniteInteger(record: Record<string, unknown>, key: string, context: string): number {
  const value = record[key];

  if (typeof value !== "number" || !Number.isFinite(value) || Math.floor(value) !== value) {
    throw new Error(`Invalid ${context}: expected finite integer at "${key}"`);
  }

  return value;
}
