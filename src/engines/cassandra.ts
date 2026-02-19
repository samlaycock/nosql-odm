import { DefaultMigrator } from "../migrator";
import {
  type BatchSetItem,
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

const DEFAULT_DOCUMENTS_TABLE = "nosql_odm_documents";
const DEFAULT_METADATA_TABLE = "nosql_odm_metadata";
const DEFAULT_MIGRATION_INDEX_TABLE = "nosql_odm_migration_index";
const OUTDATED_PAGE_LIMIT = 100;
const BATCH_GET_CHUNK_SIZE = 100;
const OUTDATED_SYNC_CHUNK_SIZE = 100;
const NULL_INDEX_SIGNATURE = "";

interface CassandraExecutionOptions {
  prepare?: boolean;
  fetchSize?: number;
  pageState?: string;
}

interface CassandraRowLike {
  get?(column: string): unknown;
  [column: string]: unknown;
}

interface CassandraResultLike {
  rows?: CassandraRowLike[];
  pageState?: string;
}

interface CassandraClientLike {
  execute(
    query: string,
    params?: unknown[],
    options?: CassandraExecutionOptions,
  ): Promise<CassandraResultLike>;
}

export interface CassandraEngineOptions {
  client: CassandraClientLike;
  keyspace: string;
  documentsTable?: string;
  metadataTable?: string;
  migrationIndexTable?: string;
}

export interface CassandraQueryEngine extends QueryEngine<never> {}

interface StoredDocumentRow {
  key: string;
  createdAt: number;
  writeVersion: number;
  doc: Record<string, unknown>;
  indexes: ResolvedIndexKeys;
  migrationTargetVersion: number;
  migrationVersionState: MigrationVersionState;
  migrationIndexSignature: string | null;
  migrationIndexSignatureSort: string;
}

interface LockRow {
  lockId: string;
  acquiredAt: number;
}

interface MigrationMetadata {
  targetVersion: number;
  versionState: MigrationVersionState;
  indexSignature: string | null;
}

interface MigrationIndexRow {
  key: string;
  targetVersion: number;
  versionState: MigrationVersionState;
  indexSignatureSort: string;
}

type OutdatedCursorPhase = "stale-low" | "stale-current" | "current-low" | "current-high";

interface OutdatedCursorState {
  phase: OutdatedCursorPhase;
  criteriaVersion: number;
  expectedSignature: string;
  pageState?: string;
}

export function cassandraEngine(options: CassandraEngineOptions): CassandraQueryEngine {
  const client = options.client;
  const documentsTable = qualifyTableName(
    options.keyspace,
    options.documentsTable ?? DEFAULT_DOCUMENTS_TABLE,
    "documentsTable",
  );
  const metadataTable = qualifyTableName(
    options.keyspace,
    options.metadataTable ?? DEFAULT_METADATA_TABLE,
    "metadataTable",
  );
  const migrationIndexTable = qualifyTableName(
    options.keyspace,
    options.migrationIndexTable ?? DEFAULT_MIGRATION_INDEX_TABLE,
    "migrationIndexTable",
  );

  const ready = ensureSchema(client, documentsTable, metadataTable, migrationIndexTable);
  let lastCreatedAt = 0;

  function nextCreatedAt(): number {
    const now = Date.now();

    if (now <= lastCreatedAt) {
      lastCreatedAt += 1;
      return lastCreatedAt;
    }

    lastCreatedAt = now;
    return now;
  }

  const engine: CassandraQueryEngine = {
    capabilities: {
      uniqueConstraints: "atomic",
    },

    async get(collection, key) {
      await ready;

      const row = await getDocumentRow(client, documentsTable, collection, key);

      if (!row) {
        return null;
      }

      return structuredClone(row.doc);
    },

    async create(collection, key, doc, indexes, _options, migrationMetadata) {
      await ready;

      const payload = toDocumentWritePayload(
        collection,
        key,
        nextCreatedAt(),
        1,
        doc,
        indexes,
        migrationMetadata,
      );
      const created = await createDocumentWithMetadata(
        client,
        documentsTable,
        migrationIndexTable,
        payload,
      );

      if (!created) {
        throw new EngineDocumentAlreadyExistsError(collection, key);
      }
    },

    async put(collection, key, doc, indexes, _options, migrationMetadata) {
      await ready;

      await putDocumentWithMetadata(
        client,
        documentsTable,
        migrationIndexTable,
        collection,
        key,
        doc,
        indexes,
        migrationMetadata,
        nextCreatedAt,
      );
    },

    async update(collection, key, doc, indexes, _options, migrationMetadata) {
      await ready;

      const updated = await updateDocumentWithMetadata(
        client,
        documentsTable,
        migrationIndexTable,
        collection,
        key,
        doc,
        indexes,
        migrationMetadata,
      );

      if (!updated) {
        throw new EngineDocumentNotFoundError(collection, key);
      }
    },

    async delete(collection, key) {
      await ready;

      await deleteDocumentAndMigrationIndex(
        client,
        documentsTable,
        migrationIndexTable,
        collection,
        key,
      );
    },

    async query(collection, params) {
      await ready;

      const rows = await listCollectionDocuments(client, documentsTable, collection);
      const matched = matchDocuments(rows, params);

      return paginate(matched, params);
    },

    async batchGet(collection, keys) {
      await ready;

      const fetched = await batchGetDocuments(client, documentsTable, collection, keys);
      const results: KeyedDocument[] = [];

      for (const key of keys) {
        const row = fetched.get(key);

        if (!row) {
          continue;
        }

        results.push({ key, doc: structuredClone(row.doc) });
      }

      return results;
    },

    async batchSet(collection, items) {
      await ready;

      for (const item of items) {
        await putDocumentWithMetadata(
          client,
          documentsTable,
          migrationIndexTable,
          collection,
          item.key,
          item.doc,
          item.indexes,
          item.migrationMetadata,
          nextCreatedAt,
        );
      }
    },

    async batchSetWithResult(collection, items) {
      await ready;

      return batchSetDocumentsWithResult(
        client,
        documentsTable,
        migrationIndexTable,
        collection,
        items,
        nextCreatedAt,
      );
    },

    async batchDelete(collection, keys) {
      await ready;

      for (const key of keys) {
        await deleteDocumentAndMigrationIndex(
          client,
          documentsTable,
          migrationIndexTable,
          collection,
          key,
        );
      }
    },

    migration: {
      async acquireLock(collection, options?) {
        await ready;

        const lock: MigrationLock = {
          id: randomId(),
          collection,
          acquiredAt: Date.now(),
        };

        const insertRows = await execute(
          client,
          `INSERT INTO ${metadataTable} (collection, kind, lock_id, acquired_at) VALUES (?, 'lock', ?, ?) IF NOT EXISTS`,
          [collection, lock.id, lock.acquiredAt],
          true,
        );

        if (wasApplied(insertRows)) {
          return lock;
        }

        const ttl = options?.ttl;
        const allowSteal = ttl !== undefined && Number.isFinite(ttl) && ttl >= 0;

        if (!allowSteal) {
          return null;
        }

        const staleBefore = lock.acquiredAt - ttl;
        const updateRows = await execute(
          client,
          `UPDATE ${metadataTable} SET lock_id = ?, acquired_at = ? WHERE collection = ? AND kind = 'lock' IF acquired_at <= ?`,
          [lock.id, lock.acquiredAt, collection, staleBefore],
          true,
        );

        return wasApplied(updateRows) ? lock : null;
      },

      async releaseLock(lock) {
        await ready;

        await execute(
          client,
          `DELETE FROM ${metadataTable} WHERE collection = ? AND kind = 'lock' IF lock_id = ?`,
          [lock.collection, lock.id],
          true,
        );
      },

      async getOutdated(collection, criteria, cursor) {
        await ready;

        if (criteria.skipMetadataSyncHint !== true) {
          await syncMigrationMetadataForCriteria(
            client,
            documentsTable,
            migrationIndexTable,
            collection,
            criteria,
          );
        }
        return getOutdatedDocuments(
          client,
          documentsTable,
          migrationIndexTable,
          collection,
          criteria,
          cursor,
        );
      },

      async saveCheckpoint(lock, cursor) {
        await ready;

        const currentLock = await loadLock(client, metadataTable, lock.collection);

        if (!currentLock || currentLock.lockId !== lock.id) {
          return;
        }

        await execute(
          client,
          `INSERT INTO ${metadataTable} (collection, kind, cursor) VALUES (?, 'checkpoint', ?)`,
          [lock.collection, cursor],
          true,
        );
      },

      async loadCheckpoint(collection) {
        await ready;

        const rows = await execute(
          client,
          `SELECT cursor FROM ${metadataTable} WHERE collection = ? AND kind = 'checkpoint'`,
          [collection],
          true,
        );

        if (rows.length === 0) {
          return null;
        }

        return parseCheckpointCursor(rows[0]!);
      },

      async clearCheckpoint(collection) {
        await ready;

        await execute(
          client,
          `DELETE FROM ${metadataTable} WHERE collection = ? AND kind = 'checkpoint'`,
          [collection],
          true,
        );
      },

      async getStatus(collection) {
        await ready;

        const lock = await loadLock(client, metadataTable, collection);

        if (!lock) {
          return null;
        }

        const checkpointRows = await execute(
          client,
          `SELECT cursor FROM ${metadataTable} WHERE collection = ? AND kind = 'checkpoint'`,
          [collection],
          true,
        );

        return {
          lock: {
            id: lock.lockId,
            collection,
            acquiredAt: lock.acquiredAt,
          },
          cursor: checkpointRows.length > 0 ? parseCheckpointCursor(checkpointRows[0]!) : null,
        };
      },
    },
  };

  engine.migrator = new DefaultMigrator(engine);

  return engine;
}

async function ensureSchema(
  client: CassandraClientLike,
  documentsTable: string,
  metadataTable: string,
  migrationIndexTable: string,
): Promise<void> {
  await execute(
    client,
    `CREATE TABLE IF NOT EXISTS ${documentsTable} (
      collection text,
      doc_key text,
      created_at bigint,
      write_version bigint,
      doc text,
      indexes map<text, text>,
      migration_target_version int,
      migration_version_state text,
      migration_index_signature text,
      migration_index_signature_sort text,
      PRIMARY KEY ((collection), doc_key)
    )`,
  );

  await ensureColumn(client, documentsTable, "write_version", "bigint");
  await ensureColumn(client, documentsTable, "migration_target_version", "int");
  await ensureColumn(client, documentsTable, "migration_version_state", "text");
  await ensureColumn(client, documentsTable, "migration_index_signature", "text");
  await ensureColumn(client, documentsTable, "migration_index_signature_sort", "text");

  await execute(
    client,
    `CREATE TABLE IF NOT EXISTS ${metadataTable} (
      collection text,
      kind text,
      lock_id text,
      acquired_at bigint,
      cursor text,
      PRIMARY KEY ((collection), kind)
    )`,
  );

  await execute(
    client,
    `CREATE TABLE IF NOT EXISTS ${migrationIndexTable} (
      collection text,
      target_version int,
      version_state text,
      index_signature_sort text,
      doc_key text,
      PRIMARY KEY ((collection), target_version, version_state, index_signature_sort, doc_key)
    )`,
  );
}

async function listCollectionDocuments(
  client: CassandraClientLike,
  documentsTable: string,
  collection: string,
): Promise<StoredDocumentRow[]> {
  const rows = await execute(
    client,
    `SELECT doc_key, created_at, write_version, doc, indexes, migration_target_version, migration_version_state, migration_index_signature, migration_index_signature_sort FROM ${documentsTable} WHERE collection = ?`,
    [collection],
    true,
  );

  const parsed = rows.map(parseStoredDocumentRow);

  parsed.sort((a, b) => {
    if (a.createdAt !== b.createdAt) {
      return a.createdAt - b.createdAt;
    }

    return a.key.localeCompare(b.key);
  });

  return parsed;
}

async function getDocumentRow(
  client: CassandraClientLike,
  documentsTable: string,
  collection: string,
  key: string,
): Promise<StoredDocumentRow | null> {
  const rows = await execute(
    client,
    `SELECT doc_key, created_at, write_version, doc, indexes, migration_target_version, migration_version_state, migration_index_signature, migration_index_signature_sort FROM ${documentsTable} WHERE collection = ? AND doc_key = ?`,
    [collection, key],
    true,
  );

  if (rows.length === 0) {
    return null;
  }

  return parseStoredDocumentRow(rows[0]!);
}

async function batchGetDocuments(
  client: CassandraClientLike,
  documentsTable: string,
  collection: string,
  keys: string[],
): Promise<Map<string, StoredDocumentRow>> {
  const result = new Map<string, StoredDocumentRow>();

  for (const chunk of chunkArray(uniqueStrings(keys), BATCH_GET_CHUNK_SIZE)) {
    if (chunk.length === 0) {
      continue;
    }

    const placeholders = chunk.map(() => "?").join(", ");
    const rows = await execute(
      client,
      `SELECT doc_key, created_at, write_version, doc, indexes, migration_target_version, migration_version_state, migration_index_signature, migration_index_signature_sort FROM ${documentsTable} WHERE collection = ? AND doc_key IN (${placeholders})`,
      [collection, ...chunk],
      true,
    );

    for (const row of rows) {
      const parsed = parseStoredDocumentRow(row);
      result.set(parsed.key, parsed);
    }
  }

  return result;
}

async function loadLock(
  client: CassandraClientLike,
  metadataTable: string,
  collection: string,
): Promise<LockRow | null> {
  const rows = await execute(
    client,
    `SELECT lock_id, acquired_at FROM ${metadataTable} WHERE collection = ? AND kind = 'lock'`,
    [collection],
    true,
  );

  if (rows.length === 0) {
    return null;
  }

  return parseLockRow(rows[0]!);
}

function parseStoredDocumentRow(row: CassandraRowLike): StoredDocumentRow {
  const key = readStringColumn(row, "doc_key", "document row");
  const createdAt = readFiniteNumberColumn(row, "created_at", "document row");
  const writeVersion = readWriteVersionColumn(row);
  const rawDoc = readStringColumn(row, "doc", "document row");
  const parsedDoc = parseDocument(rawDoc);
  const rawIndexes = readColumn(row, "indexes");
  const indexes = parseIndexes(rawIndexes);
  const migrationMetadata = parseMigrationMetadataColumns(row, parsedDoc);
  const migrationIndexSignatureSort = parseMigrationIndexSignatureSortColumn(
    row,
    migrationMetadata.indexSignature,
  );

  return {
    key,
    createdAt,
    writeVersion,
    doc: parsedDoc,
    indexes,
    migrationTargetVersion: migrationMetadata.targetVersion,
    migrationVersionState: migrationMetadata.versionState,
    migrationIndexSignature: migrationMetadata.indexSignature,
    migrationIndexSignatureSort,
  };
}

function parseLockRow(row: CassandraRowLike): LockRow {
  const lockId = readStringColumn(row, "lock_id", "migration lock row");
  const acquiredAt = readFiniteNumberColumn(row, "acquired_at", "migration lock row");

  return {
    lockId,
    acquiredAt,
  };
}

function parseCheckpointCursor(row: CassandraRowLike): string {
  return readStringColumn(row, "cursor", "migration checkpoint row");
}

function readWriteVersionColumn(row: CassandraRowLike): number {
  const raw = readColumn(row, "write_version");

  if (raw === undefined || raw === null) {
    return 1;
  }

  const value = readFiniteNumberColumn(row, "write_version", "document row");

  if (!Number.isInteger(value) || value <= 0) {
    throw new Error("Cassandra returned an invalid document row");
  }

  return value;
}

function parseMigrationMetadataColumns(
  row: CassandraRowLike,
  doc: Record<string, unknown>,
): MigrationMetadata {
  const targetVersionRaw = readColumn(row, "migration_target_version");
  const versionStateRaw = readColumn(row, "migration_version_state");
  const indexSignatureRaw = readColumn(row, "migration_index_signature");

  if (
    targetVersionRaw === undefined ||
    targetVersionRaw === null ||
    versionStateRaw === undefined ||
    versionStateRaw === null
  ) {
    return deriveLegacyMetadataFromDocument(doc);
  }

  const targetVersion = normalizeIntegerNumber(targetVersionRaw, "document row");

  if (targetVersion < 0 || !isMigrationVersionState(versionStateRaw)) {
    throw new Error("Cassandra returned an invalid document row");
  }

  if (
    indexSignatureRaw !== null &&
    indexSignatureRaw !== undefined &&
    typeof indexSignatureRaw !== "string"
  ) {
    throw new Error("Cassandra returned an invalid document row");
  }

  return {
    targetVersion,
    versionState: versionStateRaw,
    indexSignature: indexSignatureRaw ?? null,
  };
}

function parseMigrationIndexSignatureSortColumn(
  row: CassandraRowLike,
  indexSignature: string | null,
): string {
  const raw = readColumn(row, "migration_index_signature_sort");

  if (raw === undefined || raw === null) {
    return toIndexSignatureSort(indexSignature);
  }

  if (typeof raw !== "string") {
    throw new Error("Cassandra returned an invalid document row");
  }

  return raw;
}

function readColumn(row: CassandraRowLike, column: string): unknown {
  if (typeof row.get === "function") {
    return row.get(column);
  }

  return row[column];
}

function readStringColumn(row: CassandraRowLike, column: string, context: string): string {
  const value = readColumn(row, column);

  if (typeof value !== "string") {
    throw new Error(`Cassandra returned an invalid ${context}`);
  }

  return value;
}

function readFiniteNumberColumn(row: CassandraRowLike, column: string, context: string): number {
  const value = readColumn(row, column);

  if (typeof value === "number") {
    if (!Number.isFinite(value)) {
      throw new Error(`Cassandra returned an invalid ${context}`);
    }

    return value;
  }

  if (typeof value === "bigint") {
    const asNumber = Number(value);

    if (!Number.isFinite(asNumber)) {
      throw new Error(`Cassandra returned an invalid ${context}`);
    }

    return asNumber;
  }

  if (isRecord(value) && typeof value.toNumber === "function") {
    const asNumber = value.toNumber();

    if (typeof asNumber !== "number" || !Number.isFinite(asNumber)) {
      throw new Error(`Cassandra returned an invalid ${context}`);
    }

    return asNumber;
  }

  throw new Error(`Cassandra returned an invalid ${context}`);
}

function normalizeIntegerNumber(value: unknown, context: string): number {
  if (typeof value === "number") {
    if (!Number.isFinite(value) || !Number.isInteger(value)) {
      throw new Error(`Cassandra returned an invalid ${context}`);
    }

    return value;
  }

  if (typeof value === "bigint") {
    const asNumber = Number(value);

    if (!Number.isFinite(asNumber) || !Number.isInteger(asNumber)) {
      throw new Error(`Cassandra returned an invalid ${context}`);
    }

    return asNumber;
  }

  if (isRecord(value) && typeof value.toNumber === "function") {
    const asNumber = value.toNumber();

    if (!Number.isFinite(asNumber) || !Number.isInteger(asNumber)) {
      throw new Error(`Cassandra returned an invalid ${context}`);
    }

    return asNumber;
  }

  throw new Error(`Cassandra returned an invalid ${context}`);
}

function parseIndexes(value: unknown): ResolvedIndexKeys {
  if (value === undefined || value === null) {
    return {};
  }

  if (value instanceof Map) {
    const indexes: ResolvedIndexKeys = {};

    for (const [name, raw] of value.entries()) {
      if (typeof name !== "string" || typeof raw !== "string") {
        throw new Error("Cassandra returned an invalid document row");
      }

      indexes[name] = raw;
    }

    return indexes;
  }

  if (isRecord(value)) {
    const indexes: ResolvedIndexKeys = {};

    for (const [name, raw] of Object.entries(value)) {
      if (typeof raw !== "string") {
        throw new Error("Cassandra returned an invalid document row");
      }

      indexes[name] = raw;
    }

    return indexes;
  }

  throw new Error("Cassandra returned an invalid document row");
}

function parseDocument(serialized: string): Record<string, unknown> {
  let parsed: unknown;

  try {
    parsed = JSON.parse(serialized);
  } catch {
    throw new Error("Cassandra returned an invalid document row");
  }

  if (!isRecord(parsed)) {
    throw new Error("Cassandra returned an invalid document row");
  }

  return parsed;
}

function serializeDocument(doc: unknown): string {
  const cloned = structuredClone(doc);

  if (!isRecord(cloned)) {
    throw new Error("Cassandra received a non-object document");
  }

  const serialized = JSON.stringify(cloned);

  if (typeof serialized !== "string") {
    throw new Error("Cassandra failed to serialize document");
  }

  return serialized;
}

function normalizeIndexes(indexes: ResolvedIndexKeys): ResolvedIndexKeys {
  const normalized: ResolvedIndexKeys = {};

  for (const [name, value] of Object.entries(indexes)) {
    if (typeof value !== "string") {
      throw new Error("Cassandra received invalid index values");
    }

    normalized[name] = value;
  }

  return normalized;
}

async function ensureColumn(
  client: CassandraClientLike,
  tableName: string,
  columnName: string,
  columnType: string,
): Promise<void> {
  try {
    await execute(client, `ALTER TABLE ${tableName} ADD ${columnName} ${columnType}`);
  } catch (error) {
    if (isAlreadyExistsError(error)) {
      return;
    }

    throw error;
  }
}

function isAlreadyExistsError(error: unknown): boolean {
  if (!isRecord(error)) {
    return false;
  }

  const code = error.code;

  if (code === 240 || code === 8704) {
    return true;
  }

  const message = error.message;

  return (
    typeof message === "string" && /already exists|conflicts with an existing column/i.test(message)
  );
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
    throw new Error("Cassandra received invalid migration metadata target version");
  }

  if (!isMigrationVersionState(raw.versionState)) {
    throw new Error("Cassandra received invalid migration metadata state");
  }

  if (raw.indexSignature !== null && typeof raw.indexSignature !== "string") {
    throw new Error("Cassandra received invalid migration metadata index signature");
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

function toIndexSignatureSort(indexSignature: string | null): string {
  return indexSignature ?? NULL_INDEX_SIGNATURE;
}

interface DocumentWritePayload {
  collection: string;
  key: string;
  createdAt: number;
  writeVersion: number;
  serializedDoc: string;
  indexes: ResolvedIndexKeys;
  metadata: MigrationMetadata;
}

function toDocumentWritePayload(
  collection: string,
  key: string,
  createdAt: number,
  writeVersion: number,
  doc: unknown,
  indexes: ResolvedIndexKeys,
  migrationMetadata: MigrationDocumentMetadata | undefined,
): DocumentWritePayload {
  const metadata =
    normalizeMigrationMetadata(migrationMetadata) ?? deriveLegacyMetadataFromDocument(doc);

  return {
    collection,
    key,
    createdAt,
    writeVersion,
    serializedDoc: serializeDocument(doc),
    indexes: normalizeIndexes(indexes),
    metadata,
  };
}

async function putDocumentWithMetadata(
  client: CassandraClientLike,
  documentsTable: string,
  migrationIndexTable: string,
  collection: string,
  key: string,
  doc: unknown,
  indexes: ResolvedIndexKeys,
  migrationMetadata: MigrationDocumentMetadata | undefined,
  nextCreatedAt: () => number,
): Promise<void> {
  for (let attempt = 0; attempt < 12; attempt++) {
    const existing = await getDocumentRow(client, documentsTable, collection, key);

    if (!existing) {
      const payload = toDocumentWritePayload(
        collection,
        key,
        nextCreatedAt(),
        1,
        doc,
        indexes,
        migrationMetadata,
      );
      const created = await createDocumentWithMetadata(
        client,
        documentsTable,
        migrationIndexTable,
        payload,
      );

      if (created) {
        return;
      }

      continue;
    }

    const payload = toDocumentWritePayload(
      collection,
      key,
      existing.createdAt,
      existing.writeVersion + 1,
      doc,
      indexes,
      migrationMetadata,
    );
    const applied = await updateDocumentAndMigrationIndexConditionally(
      client,
      documentsTable,
      migrationIndexTable,
      payload,
      existing.writeVersion,
      {
        targetVersion: existing.migrationTargetVersion,
        versionState: existing.migrationVersionState,
        indexSignatureSort: existing.migrationIndexSignatureSort,
      },
    );

    if (applied) {
      return;
    }
  }

  throw new Error("Cassandra failed to persist document after repeated concurrent writes");
}

async function createDocumentWithMetadata(
  client: CassandraClientLike,
  documentsTable: string,
  migrationIndexTable: string,
  payload: DocumentWritePayload,
): Promise<boolean> {
  const rows = await execute(
    client,
    `INSERT INTO ${documentsTable} (
      collection,
      doc_key,
      created_at,
      write_version,
      doc,
      indexes,
      migration_target_version,
      migration_version_state,
      migration_index_signature,
      migration_index_signature_sort
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) IF NOT EXISTS`,
    [
      payload.collection,
      payload.key,
      payload.createdAt,
      payload.writeVersion,
      payload.serializedDoc,
      payload.indexes,
      payload.metadata.targetVersion,
      payload.metadata.versionState,
      payload.metadata.indexSignature,
      toIndexSignatureSort(payload.metadata.indexSignature),
    ],
    true,
  );

  if (!wasApplied(rows)) {
    return false;
  }

  await refreshMigrationIndexForExpectedWriteVersion(
    client,
    documentsTable,
    migrationIndexTable,
    payload.collection,
    payload.key,
    payload.writeVersion,
    undefined,
  );
  return true;
}

async function updateDocumentWithMetadata(
  client: CassandraClientLike,
  documentsTable: string,
  migrationIndexTable: string,
  collection: string,
  key: string,
  doc: unknown,
  indexes: ResolvedIndexKeys,
  migrationMetadata: MigrationDocumentMetadata | undefined,
): Promise<boolean> {
  for (let attempt = 0; attempt < 12; attempt++) {
    const existing = await getDocumentRow(client, documentsTable, collection, key);

    if (!existing) {
      return false;
    }

    const payload = toDocumentWritePayload(
      collection,
      key,
      existing.createdAt,
      existing.writeVersion + 1,
      doc,
      indexes,
      migrationMetadata,
    );
    const applied = await updateDocumentAndMigrationIndexConditionally(
      client,
      documentsTable,
      migrationIndexTable,
      payload,
      existing.writeVersion,
      {
        targetVersion: existing.migrationTargetVersion,
        versionState: existing.migrationVersionState,
        indexSignatureSort: existing.migrationIndexSignatureSort,
      },
    );

    if (applied) {
      return true;
    }
  }

  return false;
}

async function updateDocumentAndMigrationIndexConditionally(
  client: CassandraClientLike,
  documentsTable: string,
  migrationIndexTable: string,
  payload: DocumentWritePayload,
  expectedWriteVersion: number,
  previousIndex: {
    targetVersion: number;
    versionState: MigrationVersionState;
    indexSignatureSort: string;
  },
): Promise<boolean> {
  const rows = await execute(
    client,
    `UPDATE ${documentsTable}
      SET
        created_at = ?,
        write_version = ?,
        doc = ?,
        indexes = ?,
        migration_target_version = ?,
        migration_version_state = ?,
        migration_index_signature = ?,
        migration_index_signature_sort = ?
      WHERE collection = ? AND doc_key = ?
      IF write_version = ?`,
    [
      payload.createdAt,
      payload.writeVersion,
      payload.serializedDoc,
      payload.indexes,
      payload.metadata.targetVersion,
      payload.metadata.versionState,
      payload.metadata.indexSignature,
      toIndexSignatureSort(payload.metadata.indexSignature),
      payload.collection,
      payload.key,
      expectedWriteVersion,
    ],
    true,
  );

  if (!wasApplied(rows)) {
    return false;
  }

  await refreshMigrationIndexForExpectedWriteVersion(
    client,
    documentsTable,
    migrationIndexTable,
    payload.collection,
    payload.key,
    payload.writeVersion,
    previousIndex,
  );
  return true;
}

async function updateMigrationMetadataConditionally(
  client: CassandraClientLike,
  documentsTable: string,
  migrationIndexTable: string,
  collection: string,
  key: string,
  expectedWriteVersionAfterUpdate: number,
  metadata: MigrationMetadata,
  expectedWriteVersion: number,
  previousIndex: {
    targetVersion: number;
    versionState: MigrationVersionState;
    indexSignatureSort: string;
  },
): Promise<boolean> {
  const rows = await execute(
    client,
    `UPDATE ${documentsTable}
      SET
        migration_target_version = ?,
        migration_version_state = ?,
        migration_index_signature = ?,
        migration_index_signature_sort = ?
      WHERE collection = ? AND doc_key = ?
      IF write_version = ?`,
    [
      metadata.targetVersion,
      metadata.versionState,
      metadata.indexSignature,
      toIndexSignatureSort(metadata.indexSignature),
      collection,
      key,
      expectedWriteVersion,
    ],
    true,
  );

  if (!wasApplied(rows)) {
    return false;
  }

  await refreshMigrationIndexForExpectedWriteVersion(
    client,
    documentsTable,
    migrationIndexTable,
    collection,
    key,
    expectedWriteVersionAfterUpdate,
    previousIndex,
  );
  return true;
}

async function refreshMigrationIndexForExpectedWriteVersion(
  client: CassandraClientLike,
  documentsTable: string,
  migrationIndexTable: string,
  collection: string,
  key: string,
  expectedWriteVersion: number,
  previousIndex:
    | {
        targetVersion: number;
        versionState: MigrationVersionState;
        indexSignatureSort: string;
      }
    | undefined,
): Promise<void> {
  const current = await getDocumentRow(client, documentsTable, collection, key);

  if (!current || current.writeVersion !== expectedWriteVersion) {
    return;
  }

  await upsertMigrationIndexRow(
    client,
    migrationIndexTable,
    collection,
    key,
    current,
    previousIndex,
  );
}

async function upsertMigrationIndexRow(
  client: CassandraClientLike,
  migrationIndexTable: string,
  collection: string,
  key: string,
  row: StoredDocumentRow,
  previousIndex:
    | {
        targetVersion: number;
        versionState: MigrationVersionState;
        indexSignatureSort: string;
      }
    | undefined,
): Promise<void> {
  const currentIndex = {
    targetVersion: row.migrationTargetVersion,
    versionState: row.migrationVersionState,
    indexSignatureSort: row.migrationIndexSignatureSort,
  };

  if (
    previousIndex &&
    (previousIndex.targetVersion !== currentIndex.targetVersion ||
      previousIndex.versionState !== currentIndex.versionState ||
      previousIndex.indexSignatureSort !== currentIndex.indexSignatureSort)
  ) {
    await execute(
      client,
      `DELETE FROM ${migrationIndexTable} WHERE collection = ? AND target_version = ? AND version_state = ? AND index_signature_sort = ? AND doc_key = ?`,
      [
        collection,
        previousIndex.targetVersion,
        previousIndex.versionState,
        previousIndex.indexSignatureSort,
        key,
      ],
      true,
    );
  }

  await execute(
    client,
    `INSERT INTO ${migrationIndexTable} (collection, target_version, version_state, index_signature_sort, doc_key) VALUES (?, ?, ?, ?, ?)`,
    [
      collection,
      currentIndex.targetVersion,
      currentIndex.versionState,
      currentIndex.indexSignatureSort,
      key,
    ],
    true,
  );
}

async function deleteDocumentAndMigrationIndex(
  client: CassandraClientLike,
  documentsTable: string,
  migrationIndexTable: string,
  collection: string,
  key: string,
): Promise<void> {
  const existing = await getDocumentRow(client, documentsTable, collection, key);

  if (!existing) {
    await execute(
      client,
      `DELETE FROM ${documentsTable} WHERE collection = ? AND doc_key = ?`,
      [collection, key],
      true,
    );
    return;
  }

  await execute(
    client,
    `
      BEGIN BATCH
        DELETE FROM ${documentsTable} WHERE collection = ? AND doc_key = ?;
        DELETE FROM ${migrationIndexTable}
        WHERE collection = ? AND target_version = ? AND version_state = ? AND index_signature_sort = ? AND doc_key = ?;
      APPLY BATCH
    `,
    [
      collection,
      key,
      collection,
      existing.migrationTargetVersion,
      existing.migrationVersionState,
      existing.migrationIndexSignatureSort,
      key,
    ],
    true,
  );
}

async function batchSetDocumentsWithResult(
  client: CassandraClientLike,
  documentsTable: string,
  migrationIndexTable: string,
  collection: string,
  items: BatchSetItem[],
  nextCreatedAt: () => number,
): Promise<BatchSetResult> {
  const persistedKeys: string[] = [];
  const conflictedKeys: string[] = [];

  for (const item of items) {
    const expectedWriteVersion =
      item.expectedWriteToken === undefined ? undefined : parseWriteToken(item.expectedWriteToken);

    if (expectedWriteVersion === undefined) {
      await putDocumentWithMetadata(
        client,
        documentsTable,
        migrationIndexTable,
        collection,
        item.key,
        item.doc,
        item.indexes,
        item.migrationMetadata,
        nextCreatedAt,
      );
      persistedKeys.push(item.key);
      continue;
    }

    const existing = await getDocumentRow(client, documentsTable, collection, item.key);

    if (!existing || existing.writeVersion !== expectedWriteVersion) {
      conflictedKeys.push(item.key);
      continue;
    }

    const payload = toDocumentWritePayload(
      collection,
      item.key,
      existing.createdAt,
      existing.writeVersion + 1,
      item.doc,
      item.indexes,
      item.migrationMetadata,
    );
    const applied = await updateDocumentAndMigrationIndexConditionally(
      client,
      documentsTable,
      migrationIndexTable,
      payload,
      expectedWriteVersion,
      {
        targetVersion: existing.migrationTargetVersion,
        versionState: existing.migrationVersionState,
        indexSignatureSort: existing.migrationIndexSignatureSort,
      },
    );

    if (!applied) {
      conflictedKeys.push(item.key);
      continue;
    }

    persistedKeys.push(item.key);
  }

  return {
    persistedKeys,
    conflictedKeys,
  };
}

interface PagedResult {
  rows: CassandraRowLike[];
  pageState?: string;
}

async function executePaged(
  client: CassandraClientLike,
  query: string,
  params?: unknown[],
  options?: CassandraExecutionOptions,
): Promise<PagedResult> {
  const result = await client.execute(query, params, options);
  const rows = result.rows;

  if (rows === undefined || rows === null) {
    return {
      rows: [],
      pageState: undefined,
    };
  }

  if (!Array.isArray(rows)) {
    throw new Error("Cassandra returned an invalid query result");
  }

  const pageState = result.pageState;

  if (pageState !== undefined && pageState !== null && typeof pageState !== "string") {
    throw new Error("Cassandra returned an invalid query page state");
  }

  return {
    rows,
    pageState: pageState ?? undefined,
  };
}

async function execute(
  client: CassandraClientLike,
  query: string,
  params?: unknown[],
  prepare?: boolean,
): Promise<CassandraRowLike[]> {
  const result = await executePaged(client, query, params, prepare ? { prepare: true } : undefined);
  return result.rows;
}

function wasApplied(rows: CassandraRowLike[]): boolean {
  if (rows.length === 0) {
    throw new Error("Cassandra returned an invalid conditional write result");
  }

  const appliedRaw = readColumn(rows[0]!, "[applied]");

  if (typeof appliedRaw !== "boolean") {
    throw new Error("Cassandra returned an invalid conditional write result");
  }

  return appliedRaw;
}

async function syncMigrationMetadataForCriteria(
  client: CassandraClientLike,
  documentsTable: string,
  migrationIndexTable: string,
  collection: string,
  criteria: MigrationCriteria,
): Promise<void> {
  await syncMigrationMetadataPhase(
    client,
    documentsTable,
    migrationIndexTable,
    collection,
    criteria,
    "target-low",
  );
  await syncMigrationMetadataPhase(
    client,
    documentsTable,
    migrationIndexTable,
    collection,
    criteria,
    "target-high",
  );
}

type SyncPhase = "target-low" | "target-high";

async function syncMigrationMetadataPhase(
  client: CassandraClientLike,
  documentsTable: string,
  migrationIndexTable: string,
  collection: string,
  criteria: MigrationCriteria,
  phase: SyncPhase,
): Promise<void> {
  while (true) {
    const paged = await querySyncCandidates(
      client,
      migrationIndexTable,
      collection,
      criteria.version,
      phase,
      undefined,
      OUTDATED_SYNC_CHUNK_SIZE,
    );

    if (paged.rows.length === 0) {
      return;
    }

    const indexRows = paged.rows.map(parseMigrationIndexRow);
    const docs = await batchGetDocuments(
      client,
      documentsTable,
      collection,
      indexRows.map((row) => row.key),
    );

    for (const indexRow of indexRows) {
      const docRow = docs.get(indexRow.key);

      if (!docRow) {
        await deleteMigrationIndexRow(
          client,
          migrationIndexTable,
          collection,
          indexRow.targetVersion,
          indexRow.versionState,
          indexRow.indexSignatureSort,
          indexRow.key,
        );
        continue;
      }

      const nextMetadata = deriveMetadataForCriteria(docRow.doc, criteria);
      const applied = await updateMigrationMetadataConditionally(
        client,
        documentsTable,
        migrationIndexTable,
        collection,
        docRow.key,
        docRow.writeVersion,
        nextMetadata,
        docRow.writeVersion,
        {
          targetVersion: indexRow.targetVersion,
          versionState: indexRow.versionState,
          indexSignatureSort: indexRow.indexSignatureSort,
        },
      );

      if (!applied) {
        continue;
      }
    }

    if (paged.rows.length < OUTDATED_SYNC_CHUNK_SIZE) {
      return;
    }
  }
}

async function querySyncCandidates(
  client: CassandraClientLike,
  migrationIndexTable: string,
  collection: string,
  targetVersion: number,
  phase: SyncPhase,
  pageState: string | undefined,
  fetchSize: number,
): Promise<PagedResult> {
  if (phase === "target-low") {
    return executePaged(
      client,
      `SELECT doc_key, target_version, version_state, index_signature_sort FROM ${migrationIndexTable} WHERE collection = ? AND target_version < ?`,
      [collection, targetVersion],
      {
        prepare: true,
        pageState,
        fetchSize,
      },
    );
  }

  return executePaged(
    client,
    `SELECT doc_key, target_version, version_state, index_signature_sort FROM ${migrationIndexTable} WHERE collection = ? AND target_version > ?`,
    [collection, targetVersion],
    {
      prepare: true,
      pageState,
      fetchSize,
    },
  );
}

async function getOutdatedDocuments(
  client: CassandraClientLike,
  documentsTable: string,
  migrationIndexTable: string,
  collection: string,
  criteria: MigrationCriteria,
  cursor: string | undefined,
): Promise<EngineQueryResult> {
  const pageLimit = normalizeOutdatedPageLimit(criteria.pageSizeHint);
  const expectedSignature = computeIndexSignature(criteria.indexes);
  let state = decodeOutdatedCursor(cursor, criteria.version, expectedSignature);
  const candidates: MigrationIndexRow[] = [];
  let remaining = pageLimit;
  let nextCursor: string | null = null;
  const expectedSort = toIndexSignatureSort(expectedSignature);

  while (remaining > 0) {
    const paged = await queryOutdatedPhase(
      client,
      migrationIndexTable,
      collection,
      criteria.version,
      state.phase,
      expectedSort,
      state.pageState,
      remaining,
    );
    const rows = paged.rows.map(parseMigrationIndexRow);
    candidates.push(...rows);
    remaining -= rows.length;

    if (remaining === 0) {
      if (paged.pageState) {
        nextCursor = encodeOutdatedCursor({
          phase: state.phase,
          criteriaVersion: criteria.version,
          expectedSignature,
          pageState: paged.pageState,
        });
      } else {
        const nextPhase = advanceOutdatedPhase(state.phase);
        nextCursor = nextPhase
          ? encodeOutdatedCursor({
              phase: nextPhase,
              criteriaVersion: criteria.version,
              expectedSignature,
            })
          : null;
      }
      break;
    }

    if (paged.pageState) {
      state = {
        ...state,
        pageState: paged.pageState,
      };
      continue;
    }

    const nextPhase = advanceOutdatedPhase(state.phase);

    if (!nextPhase) {
      break;
    }

    state = {
      phase: nextPhase,
      criteriaVersion: criteria.version,
      expectedSignature,
    };
  }

  const docsByKey = await batchGetDocuments(
    client,
    documentsTable,
    collection,
    candidates.map((candidate) => candidate.key),
  );
  const documents: KeyedDocument[] = [];

  for (const candidate of candidates) {
    const doc = docsByKey.get(candidate.key);

    if (!doc || !isDocumentOutdatedForCriteria(doc, criteria.version, expectedSignature)) {
      continue;
    }

    documents.push({
      key: candidate.key,
      doc: structuredClone(doc.doc),
      writeToken: String(doc.writeVersion),
    });
  }

  return {
    documents,
    cursor: nextCursor,
  };
}

function normalizeOutdatedPageLimit(value: number | undefined): number {
  if (value === undefined || !Number.isFinite(value)) {
    return OUTDATED_PAGE_LIMIT;
  }

  return Math.max(1, Math.floor(value));
}

async function queryOutdatedPhase(
  client: CassandraClientLike,
  migrationIndexTable: string,
  collection: string,
  criteriaVersion: number,
  phase: OutdatedCursorPhase,
  expectedSignatureSort: string,
  pageState: string | undefined,
  fetchSize: number,
): Promise<PagedResult> {
  if (phase === "stale-low") {
    return executePaged(
      client,
      `SELECT doc_key, target_version, version_state, index_signature_sort FROM ${migrationIndexTable} WHERE collection = ? AND target_version < ?`,
      [collection, criteriaVersion],
      {
        prepare: true,
        pageState,
        fetchSize,
      },
    );
  }

  if (phase === "stale-current") {
    return executePaged(
      client,
      `SELECT doc_key, target_version, version_state, index_signature_sort FROM ${migrationIndexTable} WHERE collection = ? AND target_version = ? AND version_state = 'stale'`,
      [collection, criteriaVersion],
      {
        prepare: true,
        pageState,
        fetchSize,
      },
    );
  }

  if (phase === "current-low") {
    return executePaged(
      client,
      `SELECT doc_key, target_version, version_state, index_signature_sort FROM ${migrationIndexTable} WHERE collection = ? AND target_version = ? AND version_state = 'current' AND index_signature_sort < ?`,
      [collection, criteriaVersion, expectedSignatureSort],
      {
        prepare: true,
        pageState,
        fetchSize,
      },
    );
  }

  return executePaged(
    client,
    `SELECT doc_key, target_version, version_state, index_signature_sort FROM ${migrationIndexTable} WHERE collection = ? AND target_version = ? AND version_state = 'current' AND index_signature_sort > ?`,
    [collection, criteriaVersion, expectedSignatureSort],
    {
      prepare: true,
      pageState,
      fetchSize,
    },
  );
}

function parseMigrationIndexRow(row: CassandraRowLike): MigrationIndexRow {
  const key = readStringColumn(row, "doc_key", "migration index row");
  const targetVersion = readFiniteNumberColumn(row, "target_version", "migration index row");
  const versionState = readColumn(row, "version_state");
  const indexSignatureSort = readStringColumn(row, "index_signature_sort", "migration index row");

  if (
    !Number.isInteger(targetVersion) ||
    targetVersion < 0 ||
    !isMigrationVersionState(versionState)
  ) {
    throw new Error("Cassandra returned an invalid migration index row");
  }

  return {
    key,
    targetVersion,
    versionState,
    indexSignatureSort,
  };
}

function isDocumentOutdatedForCriteria(
  row: StoredDocumentRow,
  criteriaVersion: number,
  expectedSignature: string,
): boolean {
  if (row.migrationTargetVersion < criteriaVersion) {
    return true;
  }

  if (row.migrationTargetVersion > criteriaVersion) {
    return false;
  }

  if (row.migrationVersionState === "stale") {
    return true;
  }

  if (row.migrationVersionState !== "current") {
    return false;
  }

  return row.migrationIndexSignature !== expectedSignature;
}

function advanceOutdatedPhase(phase: OutdatedCursorPhase): OutdatedCursorPhase | null {
  if (phase === "stale-low") {
    return "stale-current";
  }

  if (phase === "stale-current") {
    return "current-low";
  }

  if (phase === "current-low") {
    return "current-high";
  }

  return null;
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
    phase: "stale-low",
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
      (parsed.phase !== "stale-low" &&
        parsed.phase !== "stale-current" &&
        parsed.phase !== "current-low" &&
        parsed.phase !== "current-high") ||
      parsed.criteriaVersion !== criteriaVersion ||
      parsed.expectedSignature !== expectedSignature
    ) {
      return fallback;
    }

    const pageState = parsed.pageState;

    if (pageState !== undefined && pageState !== null && typeof pageState !== "string") {
      return fallback;
    }

    return {
      phase: parsed.phase,
      criteriaVersion,
      expectedSignature,
      pageState: pageState ?? undefined,
    };
  } catch {
    return fallback;
  }
}

async function deleteMigrationIndexRow(
  client: CassandraClientLike,
  migrationIndexTable: string,
  collection: string,
  targetVersion: number,
  versionState: MigrationVersionState,
  indexSignatureSort: string,
  key: string,
): Promise<void> {
  await execute(
    client,
    `DELETE FROM ${migrationIndexTable} WHERE collection = ? AND target_version = ? AND version_state = ? AND index_signature_sort = ? AND doc_key = ?`,
    [collection, targetVersion, versionState, indexSignatureSort, key],
    true,
  );
}

function matchDocuments(records: StoredDocumentRow[], params: QueryParams): StoredDocumentRow[] {
  const indexName = params.index;
  const results: StoredDocumentRow[] = [];

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

function paginate(records: StoredDocumentRow[], params: QueryParams): EngineQueryResult {
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
    throw new Error("Cassandra received an invalid expected write token");
  }

  const parsed = Number(raw);

  if (!Number.isFinite(parsed) || !Number.isInteger(parsed) || parsed <= 0) {
    throw new Error("Cassandra received an invalid expected write token");
  }

  return parsed;
}

function randomId(): string {
  if (typeof crypto !== "undefined" && typeof crypto.randomUUID === "function") {
    return crypto.randomUUID();
  }

  const random = Math.random().toString(16).slice(2);
  const now = Date.now().toString(16);

  return `${now}-${random}`;
}

function qualifyTableName(keyspace: string, table: string, optionName: string): string {
  validateIdentifier(keyspace, "keyspace");
  validateIdentifier(table, optionName);

  return `${keyspace}.${table}`;
}

function validateIdentifier(value: string, label: string): void {
  if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(value)) {
    throw new Error(`Invalid Cassandra identifier for ${label}: "${value}"`);
  }
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

function chunkArray<T>(items: T[], size: number): T[][] {
  const chunks: T[][] = [];

  for (let i = 0; i < items.length; i += size) {
    chunks.push(items.slice(i, i + size));
  }

  return chunks;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}
