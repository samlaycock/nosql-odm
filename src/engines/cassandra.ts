import {
  EngineDocumentAlreadyExistsError,
  EngineDocumentNotFoundError,
  type ComparableVersion,
  type EngineQueryResult,
  type FieldCondition,
  type KeyedDocument,
  type MigrationCriteria,
  type MigrationLock,
  type QueryEngine,
  type QueryParams,
  type ResolvedIndexKeys,
} from "./types";
import { DefaultMigrator } from "../migrator";

const DEFAULT_DOCUMENTS_TABLE = "nosql_odm_documents";
const DEFAULT_METADATA_TABLE = "nosql_odm_metadata";
const OUTDATED_PAGE_LIMIT = 100;
const BATCH_GET_CHUNK_SIZE = 100;

interface CassandraExecutionOptions {
  prepare?: boolean;
}

interface CassandraRowLike {
  get?(column: string): unknown;
  [column: string]: unknown;
}

interface CassandraResultLike {
  rows?: CassandraRowLike[];
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
}

export interface CassandraQueryEngine extends QueryEngine<never> {}

interface StoredDocumentRow {
  key: string;
  createdAt: number;
  doc: Record<string, unknown>;
  indexes: ResolvedIndexKeys;
}

interface LockRow {
  lockId: string;
  acquiredAt: number;
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

  const ready = ensureSchema(client, documentsTable, metadataTable);
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
    async get(collection, key) {
      await ready;

      const row = await getDocumentRow(client, documentsTable, collection, key);

      if (!row) {
        return null;
      }

      return structuredClone(row.doc);
    },

    async create(collection, key, doc, indexes) {
      await ready;

      const createdAt = nextCreatedAt();
      const serialized = serializeDocument(doc);

      const rows = await execute(
        client,
        `INSERT INTO ${documentsTable} (collection, doc_key, created_at, doc, indexes) VALUES (?, ?, ?, ?, ?) IF NOT EXISTS`,
        [collection, key, createdAt, serialized, { ...indexes }],
        true,
      );

      if (!wasApplied(rows)) {
        throw new EngineDocumentAlreadyExistsError(collection, key);
      }
    },

    async put(collection, key, doc, indexes) {
      await ready;

      const existing = await getDocumentRow(client, documentsTable, collection, key);
      const createdAt = existing?.createdAt ?? nextCreatedAt();
      const serialized = serializeDocument(doc);

      await execute(
        client,
        `INSERT INTO ${documentsTable} (collection, doc_key, created_at, doc, indexes) VALUES (?, ?, ?, ?, ?)`,
        [collection, key, createdAt, serialized, { ...indexes }],
        true,
      );
    },

    async update(collection, key, doc, indexes) {
      await ready;

      const serialized = serializeDocument(doc);
      const rows = await execute(
        client,
        `UPDATE ${documentsTable} SET doc = ?, indexes = ? WHERE collection = ? AND doc_key = ? IF EXISTS`,
        [serialized, { ...indexes }, collection, key],
        true,
      );

      if (!wasApplied(rows)) {
        throw new EngineDocumentNotFoundError(collection, key);
      }
    },

    async delete(collection, key) {
      await ready;

      await execute(
        client,
        `DELETE FROM ${documentsTable} WHERE collection = ? AND doc_key = ?`,
        [collection, key],
        true,
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

      const existing = await batchGetDocuments(
        client,
        documentsTable,
        collection,
        items.map((item) => item.key),
      );

      for (const item of items) {
        const createdAt = existing.get(item.key)?.createdAt ?? nextCreatedAt();
        const serialized = serializeDocument(item.doc);

        await execute(
          client,
          `INSERT INTO ${documentsTable} (collection, doc_key, created_at, doc, indexes) VALUES (?, ?, ?, ?, ?)`,
          [collection, item.key, createdAt, serialized, { ...item.indexes }],
          true,
        );
      }
    },

    async batchDelete(collection, keys) {
      await ready;

      for (const key of keys) {
        await execute(
          client,
          `DELETE FROM ${documentsTable} WHERE collection = ? AND doc_key = ?`,
          [collection, key],
          true,
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

        const rows = await listCollectionDocuments(client, documentsTable, collection);
        const parseVersion = criteria.parseVersion ?? defaultParseVersion;
        const compareVersions = criteria.compareVersions ?? defaultCompareVersions;
        const outdated: StoredDocumentRow[] = [];

        for (const row of rows) {
          if (isOutdated(row.doc, criteria, parseVersion, compareVersions)) {
            outdated.push(row);
          }
        }

        return paginate(outdated, {
          cursor,
          limit: OUTDATED_PAGE_LIMIT,
        });
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
): Promise<void> {
  await execute(
    client,
    `CREATE TABLE IF NOT EXISTS ${documentsTable} (
      collection text,
      doc_key text,
      created_at bigint,
      doc text,
      indexes map<text, text>,
      PRIMARY KEY ((collection), doc_key)
    )`,
  );

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
}

async function listCollectionDocuments(
  client: CassandraClientLike,
  documentsTable: string,
  collection: string,
): Promise<StoredDocumentRow[]> {
  const rows = await execute(
    client,
    `SELECT doc_key, created_at, doc, indexes FROM ${documentsTable} WHERE collection = ?`,
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
    `SELECT doc_key, created_at, doc, indexes FROM ${documentsTable} WHERE collection = ? AND doc_key = ?`,
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
      `SELECT doc_key, created_at, doc, indexes FROM ${documentsTable} WHERE collection = ? AND doc_key IN (${placeholders})`,
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
  const rawDoc = readStringColumn(row, "doc", "document row");
  const parsedDoc = parseDocument(rawDoc);
  const rawIndexes = readColumn(row, "indexes");
  const indexes = parseIndexes(rawIndexes);

  return {
    key,
    createdAt,
    doc: parsedDoc,
    indexes,
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

async function execute(
  client: CassandraClientLike,
  query: string,
  params?: unknown[],
  prepare?: boolean,
): Promise<CassandraRowLike[]> {
  const result = await client.execute(query, params, prepare ? { prepare: true } : undefined);
  const rows = result.rows;

  if (rows === undefined || rows === null) {
    return [];
  }

  if (!Array.isArray(rows)) {
    throw new Error("Cassandra returned an invalid query result");
  }

  return rows;
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

function isOutdated(
  doc: Record<string, unknown>,
  criteria: MigrationCriteria,
  parseVersion: (raw: unknown) => ComparableVersion | null,
  compareVersions: (a: ComparableVersion, b: ComparableVersion) => number,
): boolean {
  const parsedVersion = parseVersion(doc[criteria.versionField]);
  const storedIndexes = doc[criteria.indexesField];
  const versionState = classifyVersionState(parsedVersion, criteria.version, compareVersions);

  if (versionState === "stale") {
    return true;
  }

  if (versionState !== "current") {
    return false;
  }

  if (!Array.isArray(storedIndexes)) {
    return true;
  }

  if (storedIndexes.length !== criteria.indexes.length) {
    return true;
  }

  return !storedIndexes.every((name, i) => name === criteria.indexes[i]);
}

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
