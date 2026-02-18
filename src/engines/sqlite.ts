import type Database from "better-sqlite3";
import {
  type BatchSetResult,
  EngineDocumentAlreadyExistsError,
  EngineDocumentNotFoundError,
  EngineUniqueConstraintError,
  type AcquireLockOptions,
  type ComparableVersion,
  type EngineQueryResult,
  type FieldCondition,
  type KeyedDocument,
  type MigrationDocumentMetadata,
  type MigrationCriteria,
  type MigrationLock,
  type MigrationVersionState,
  type QueryEngine,
  type QueryParams,
  type ResolvedIndexKeys,
} from "./types";
import { DefaultMigrator } from "../migrator";

const LATEST_SCHEMA_VERSION = 2;
const OUTDATED_PAGE_LIMIT = 100;
const OUTDATED_SCAN_CHUNK_SIZE = 256;

interface DocumentRow {
  id: number;
  doc_key: string;
  doc_json: string;
  write_version: number;
}

interface QueryRow {
  key: string;
  doc_json: string;
}

interface CursorRow {
  id: number;
  index_value: string;
}

interface UniqueConflictRow {
  doc_key: string;
}

interface LockRow {
  lock_id: string;
  acquired_at: number;
}

interface CheckpointRow {
  cursor: string;
}

interface StatusRow {
  lock_id: string;
  acquired_at: number;
  cursor: string | null;
}

interface MigrationMetadata {
  targetVersion: number;
  versionState: MigrationVersionState;
  indexSignature: string | null;
}

export interface SqliteEngineOptions {
  database: Database.Database;
}

export interface SqliteQueryEngine extends QueryEngine<never> {
  readonly db: Database.Database;
  close(): void;
}

export function sqliteEngine(options: SqliteEngineOptions): SqliteQueryEngine {
  const db = options.database;

  configureDatabase(db, options);
  runMigrations(db);

  const selectDocumentByKeyStmt = db.prepare<[string, string], { doc_json: string } | undefined>(
    `SELECT doc_json FROM documents WHERE collection = ? AND doc_key = ?`,
  );

  const upsertDocumentStmt = db.prepare<[string, string, string]>(`
    INSERT INTO documents (collection, doc_key, doc_json)
    VALUES (?, ?, ?)
    ON CONFLICT (collection, doc_key) DO UPDATE SET
      doc_json = excluded.doc_json,
      write_version = documents.write_version + 1
  `);

  const insertDocumentStmt = db.prepare<[string, string, string]>(
    `INSERT INTO documents (collection, doc_key, doc_json) VALUES (?, ?, ?)`,
  );

  const deleteDocumentStmt = db.prepare<[string, string]>(
    `DELETE FROM documents WHERE collection = ? AND doc_key = ?`,
  );

  const updateDocumentStmt = db.prepare<[string, string, string]>(`
    UPDATE documents
    SET
      doc_json = ?,
      write_version = write_version + 1
    WHERE collection = ? AND doc_key = ?
  `);

  const updateDocumentWithTokenStmt = db.prepare<[string, string, string, number]>(`
    UPDATE documents
    SET
      doc_json = ?,
      write_version = write_version + 1
    WHERE collection = ?
      AND doc_key = ?
      AND write_version = ?
  `);

  const deleteIndexesForDocumentStmt = db.prepare<[string, string]>(
    `DELETE FROM index_entries WHERE collection = ? AND doc_key = ?`,
  );

  const insertIndexStmt = db.prepare<[string, string, string, string]>(`
    INSERT INTO index_entries (collection, doc_key, index_name, index_value)
    VALUES (?, ?, ?, ?)
  `);

  const selectUniqueConflictStmt = db.prepare<
    [string, string, string, string],
    UniqueConflictRow | undefined
  >(
    `
      SELECT doc_key
      FROM index_entries
      WHERE collection = ?
        AND index_name = ?
        AND index_value = ?
        AND doc_key <> ?
      LIMIT 1
    `,
  );

  const selectDocumentIdByKeyStmt = db.prepare<[string, string], { id: number } | undefined>(
    `SELECT id FROM documents WHERE collection = ? AND doc_key = ?`,
  );

  const queryScanPageStmt = db.prepare<[string, number, number], QueryRow>(`
    SELECT doc_key AS key, doc_json
    FROM documents
    WHERE collection = ? AND id > ?
    ORDER BY id ASC
    LIMIT ?
  `);

  const selectLockStmt = db.prepare<[string], LockRow | undefined>(
    `SELECT lock_id, acquired_at FROM migration_locks WHERE collection = ?`,
  );

  const insertLockStmt = db.prepare<[string, string, number]>(
    `INSERT INTO migration_locks (collection, lock_id, acquired_at) VALUES (?, ?, ?)`,
  );

  const updateLockStmt = db.prepare<[string, number, string]>(`
    UPDATE migration_locks
    SET lock_id = ?, acquired_at = ?
    WHERE collection = ?
  `);

  const releaseLockStmt = db.prepare<[string, string]>(
    `DELETE FROM migration_locks WHERE collection = ? AND lock_id = ?`,
  );

  const saveCheckpointStmt = db.prepare<[string, string, string, string]>(`
    INSERT INTO migration_checkpoints (collection, cursor)
    SELECT ?, ?
    WHERE EXISTS (
      SELECT 1
      FROM migration_locks
      WHERE collection = ? AND lock_id = ?
    )
    ON CONFLICT (collection) DO UPDATE SET
      cursor = excluded.cursor
  `);

  const loadCheckpointStmt = db.prepare<[string], CheckpointRow | undefined>(
    `SELECT cursor FROM migration_checkpoints WHERE collection = ?`,
  );

  const clearCheckpointStmt = db.prepare<[string]>(
    `DELETE FROM migration_checkpoints WHERE collection = ?`,
  );

  const loadStatusStmt = db.prepare<[string], StatusRow | undefined>(`
    SELECT l.lock_id, l.acquired_at, c.cursor
    FROM migration_locks l
    LEFT JOIN migration_checkpoints c ON c.collection = l.collection
    WHERE l.collection = ?
  `);

  const upsertMigrationMetadataStmt = db.prepare<[string, string, number, string, string | null]>(`
    INSERT INTO migration_metadata (
      collection,
      doc_key,
      target_version,
      version_state,
      index_signature
    )
    VALUES (?, ?, ?, ?, ?)
    ON CONFLICT (collection, doc_key) DO UPDATE SET
      target_version = excluded.target_version,
      version_state = excluded.version_state,
      index_signature = excluded.index_signature
  `);

  const queryOutdatedByMetadataStmt = db.prepare<
    [string, number, string, number, number],
    DocumentRow
  >(`
    SELECT d.id, d.doc_key, d.doc_json, d.write_version
    FROM migration_metadata m
    INNER JOIN documents d
      ON d.collection = m.collection
     AND d.doc_key = m.doc_key
    WHERE m.collection = ?
      AND d.id > ?
      AND (
        m.version_state = 'stale'
        OR (
          m.version_state = 'current'
          AND (m.index_signature IS NULL OR m.index_signature <> ?)
        )
      )
      AND m.target_version = ?
    ORDER BY d.id ASC
    LIMIT ?
  `);

  const scanMissingMetadataStmt = db.prepare<
    [string, number, number],
    Pick<DocumentRow, "doc_key" | "doc_json">
  >(`
    SELECT d.doc_key, d.doc_json
    FROM documents d
    LEFT JOIN migration_metadata m
      ON m.collection = d.collection
     AND m.doc_key = d.doc_key
    WHERE d.collection = ?
      AND (
        m.doc_key IS NULL
        OR m.target_version IS NULL
        OR m.target_version <> ?
      )
    ORDER BY d.id ASC
    LIMIT ?
  `);

  const assertUniqueIndexes = (
    collection: string,
    key: string,
    uniqueIndexes: ResolvedIndexKeys,
  ): void => {
    for (const [indexName, indexValue] of Object.entries(uniqueIndexes)) {
      const conflict = selectUniqueConflictStmt.get(collection, indexName, String(indexValue), key);

      if (conflict) {
        throw new EngineUniqueConstraintError(
          collection,
          key,
          indexName,
          String(indexValue),
          conflict.doc_key,
        );
      }
    }
  };

  const putTxn = db.transaction(
    (
      collection: string,
      key: string,
      docJson: string,
      indexes: ResolvedIndexKeys,
      uniqueIndexes: ResolvedIndexKeys,
      metadata: MigrationMetadata,
    ) => {
      assertUniqueIndexes(collection, key, uniqueIndexes);
      upsertDocumentStmt.run(collection, key, docJson);
      deleteIndexesForDocumentStmt.run(collection, key);

      for (const [indexName, indexValue] of Object.entries(indexes)) {
        insertIndexStmt.run(collection, key, indexName, String(indexValue));
      }

      upsertMigrationMetadataStmt.run(
        collection,
        key,
        metadata.targetVersion,
        metadata.versionState,
        metadata.indexSignature,
      );
    },
  );

  const updateTxn = db.transaction(
    (
      collection: string,
      key: string,
      docJson: string,
      indexes: ResolvedIndexKeys,
      uniqueIndexes: ResolvedIndexKeys,
      metadata: MigrationMetadata,
    ) => {
      assertUniqueIndexes(collection, key, uniqueIndexes);
      const result = updateDocumentStmt.run(docJson, collection, key) as { changes?: unknown };
      const changes = Number(result?.changes ?? 0);

      if (!Number.isFinite(changes) || changes < 1) {
        throw new EngineDocumentNotFoundError(collection, key);
      }

      deleteIndexesForDocumentStmt.run(collection, key);

      for (const [indexName, indexValue] of Object.entries(indexes)) {
        insertIndexStmt.run(collection, key, indexName, String(indexValue));
      }

      upsertMigrationMetadataStmt.run(
        collection,
        key,
        metadata.targetVersion,
        metadata.versionState,
        metadata.indexSignature,
      );
    },
  );

  const createTxn = db.transaction(
    (
      collection: string,
      key: string,
      docJson: string,
      indexes: ResolvedIndexKeys,
      uniqueIndexes: ResolvedIndexKeys,
      metadata: MigrationMetadata,
    ) => {
      assertUniqueIndexes(collection, key, uniqueIndexes);
      insertDocumentStmt.run(collection, key, docJson);

      for (const [indexName, indexValue] of Object.entries(indexes)) {
        insertIndexStmt.run(collection, key, indexName, String(indexValue));
      }

      upsertMigrationMetadataStmt.run(
        collection,
        key,
        metadata.targetVersion,
        metadata.versionState,
        metadata.indexSignature,
      );
    },
  );

  const batchSetTxn = db.transaction(
    (
      collection: string,
      items: {
        key: string;
        docJson: string;
        indexes: ResolvedIndexKeys;
        uniqueIndexes: ResolvedIndexKeys;
        metadata: MigrationMetadata;
      }[],
    ): void => {
      for (const item of items) {
        assertUniqueIndexes(collection, item.key, item.uniqueIndexes);
        upsertDocumentStmt.run(collection, item.key, item.docJson);
        deleteIndexesForDocumentStmt.run(collection, item.key);

        for (const [indexName, indexValue] of Object.entries(item.indexes)) {
          insertIndexStmt.run(collection, item.key, indexName, String(indexValue));
        }

        upsertMigrationMetadataStmt.run(
          collection,
          item.key,
          item.metadata.targetVersion,
          item.metadata.versionState,
          item.metadata.indexSignature,
        );
      }
    },
  );

  const batchSetWithResultTxn = db.transaction(
    (
      collection: string,
      items: {
        key: string;
        docJson: string;
        indexes: ResolvedIndexKeys;
        uniqueIndexes: ResolvedIndexKeys;
        metadata: MigrationMetadata;
        expectedWriteToken?: string;
      }[],
    ): BatchSetResult => {
      const persistedKeys: string[] = [];
      const conflictedKeys: string[] = [];

      for (const item of items) {
        assertUniqueIndexes(collection, item.key, item.uniqueIndexes);

        if (item.expectedWriteToken !== undefined) {
          const expectedWriteVersion = parseWriteToken(item.expectedWriteToken);
          const result = updateDocumentWithTokenStmt.run(
            item.docJson,
            collection,
            item.key,
            expectedWriteVersion,
          ) as { changes?: unknown };
          const changes = Number(result?.changes ?? 0);

          if (!Number.isFinite(changes) || changes < 1) {
            conflictedKeys.push(item.key);
            continue;
          }
        } else {
          upsertDocumentStmt.run(collection, item.key, item.docJson);
        }

        deleteIndexesForDocumentStmt.run(collection, item.key);

        for (const [indexName, indexValue] of Object.entries(item.indexes)) {
          insertIndexStmt.run(collection, item.key, indexName, String(indexValue));
        }

        upsertMigrationMetadataStmt.run(
          collection,
          item.key,
          item.metadata.targetVersion,
          item.metadata.versionState,
          item.metadata.indexSignature,
        );
        persistedKeys.push(item.key);
      }

      return {
        persistedKeys,
        conflictedKeys,
      };
    },
  );

  const batchDeleteTxn = db.transaction((collection: string, keys: string[]): void => {
    for (const key of keys) {
      deleteDocumentStmt.run(collection, key);
    }
  });

  const acquireLockTxn = db.transaction(
    (
      collection: string,
      options: AcquireLockOptions | undefined,
      now: number,
      lockId: string,
    ): MigrationLock | null => {
      const existing = selectLockStmt.get(collection);

      if (existing) {
        const ttl = normalizeTtl(options?.ttl);

        if (ttl === null || now - existing.acquired_at < ttl) {
          return null;
        }

        updateLockStmt.run(lockId, now, collection);
      } else {
        insertLockStmt.run(collection, lockId, now);
      }

      return {
        id: lockId,
        collection,
        acquiredAt: now,
      };
    },
  );

  const engine: SqliteQueryEngine = {
    capabilities: {
      uniqueConstraints: "atomic",
    },

    db,

    close() {
      db.close();
    },

    async get(collection, key) {
      const row = selectDocumentByKeyStmt.get(collection, key);

      if (!row) {
        return null;
      }

      return parseStoredDocument(row.doc_json, collection, key);
    },

    async create(collection, key, doc, indexes, _options, migrationMetadata, uniqueIndexes) {
      const docJson = serializeDocument(doc, collection, key);
      const metadata = normalizeMigrationMetadata(migrationMetadata) ?? deriveLegacyMetadata(doc);

      try {
        createTxn(collection, key, docJson, indexes, uniqueIndexes ?? {}, metadata);
      } catch (error) {
        if (error instanceof EngineUniqueConstraintError) {
          throw error;
        }

        if (isSqliteUniqueConstraintError(error)) {
          throw new EngineDocumentAlreadyExistsError(collection, key);
        }

        throw error;
      }
    },

    async put(collection, key, doc, indexes, _options, migrationMetadata, uniqueIndexes) {
      const docJson = serializeDocument(doc, collection, key);
      const metadata = normalizeMigrationMetadata(migrationMetadata) ?? deriveLegacyMetadata(doc);

      putTxn(collection, key, docJson, indexes, uniqueIndexes ?? {}, metadata);
    },

    async update(collection, key, doc, indexes, _options, migrationMetadata, uniqueIndexes) {
      const docJson = serializeDocument(doc, collection, key);
      const metadata = normalizeMigrationMetadata(migrationMetadata) ?? deriveLegacyMetadata(doc);
      updateTxn(collection, key, docJson, indexes, uniqueIndexes ?? {}, metadata);
    },

    async delete(collection, key) {
      deleteDocumentStmt.run(collection, key);
    },

    async query(collection, params) {
      if (!params.index || !params.filter) {
        return queryByCollectionScan(collection, params);
      }

      return queryByIndex(collection, params);
    },

    async batchGet(collection, keys) {
      const results: KeyedDocument[] = [];

      for (const key of keys) {
        const row = selectDocumentByKeyStmt.get(collection, key);

        if (!row) {
          continue;
        }

        results.push({
          key,
          doc: parseStoredDocument(row.doc_json, collection, key),
        });
      }

      return results;
    },

    async batchSet(collection, items) {
      const prepared = items.map((item) => ({
        key: item.key,
        docJson: serializeDocument(item.doc, collection, item.key),
        indexes: item.indexes,
        uniqueIndexes: item.uniqueIndexes ?? {},
        metadata:
          normalizeMigrationMetadata(item.migrationMetadata) ?? deriveLegacyMetadata(item.doc),
      }));

      batchSetTxn(collection, prepared);
    },

    async batchSetWithResult(collection, items) {
      const prepared = items.map((item) => ({
        key: item.key,
        docJson: serializeDocument(item.doc, collection, item.key),
        indexes: item.indexes,
        uniqueIndexes: item.uniqueIndexes ?? {},
        metadata:
          normalizeMigrationMetadata(item.migrationMetadata) ?? deriveLegacyMetadata(item.doc),
        expectedWriteToken: item.expectedWriteToken,
      }));

      return batchSetWithResultTxn(collection, prepared);
    },

    async batchDelete(collection, keys) {
      batchDeleteTxn(collection, keys);
    },

    migration: {
      async acquireLock(collection, options) {
        return acquireLockTxn.immediate(collection, options, Date.now(), randomId());
      },

      async releaseLock(lock) {
        releaseLockStmt.run(lock.collection, lock.id);
      },

      async getOutdated(collection, criteria, cursor) {
        return getOutdatedDocuments(
          db,
          scanMissingMetadataStmt,
          upsertMigrationMetadataStmt,
          queryOutdatedByMetadataStmt,
          selectDocumentIdByKeyStmt,
          collection,
          criteria,
          cursor,
        );
      },

      async saveCheckpoint(lock, cursor) {
        saveCheckpointStmt.run(lock.collection, cursor, lock.collection, lock.id);
      },

      async loadCheckpoint(collection) {
        return loadCheckpointStmt.get(collection)?.cursor ?? null;
      },

      async clearCheckpoint(collection) {
        clearCheckpointStmt.run(collection);
      },

      async getStatus(collection) {
        const row = loadStatusStmt.get(collection);

        if (!row) {
          return null;
        }

        return {
          lock: {
            id: row.lock_id,
            collection,
            acquiredAt: row.acquired_at,
          },
          cursor: row.cursor ?? null,
        };
      },
    },
  };

  function queryByCollectionScan(collection: string, params: QueryParams): EngineQueryResult {
    const limit = normalizeLimit(params.limit);

    if (limit === 0) {
      return { documents: [], cursor: null };
    }

    const cursorId = resolveCursorId(collection, params.cursor, selectDocumentIdByKeyStmt);
    const fetchLimit = limit === null ? Number.MAX_SAFE_INTEGER : limit + 1;
    const rows = queryScanPageStmt.all(collection, cursorId, fetchLimit);

    return formatPage(rows, limit, collection);
  }

  function queryByIndex(collection: string, params: QueryParams): EngineQueryResult {
    const limit = normalizeLimit(params.limit);

    if (limit === 0) {
      return { documents: [], cursor: null };
    }

    const index = params.index!;
    const filter = buildFilterSql(params.filter!.value);
    const sort = params.sort;

    const cursor = params.cursor
      ? resolveIndexedCursor(collection, index, params.cursor, filter)
      : null;

    const whereParts = [`ix.collection = ?`, `ix.index_name = ?`];
    const args: (string | number)[] = [collection, index];

    if (filter.sql) {
      whereParts.push(filter.sql);
      args.push(...filter.args);
    }

    if (cursor) {
      if (sort === "asc") {
        whereParts.push(`(ix.index_value > ? OR (ix.index_value = ? AND d.id > ?))`);
        args.push(cursor.index_value, cursor.index_value, cursor.id);
      } else if (sort === "desc") {
        whereParts.push(`(ix.index_value < ? OR (ix.index_value = ? AND d.id > ?))`);
        args.push(cursor.index_value, cursor.index_value, cursor.id);
      } else {
        whereParts.push(`d.id > ?`);
        args.push(cursor.id);
      }
    }

    const orderBy =
      sort === "asc"
        ? `ORDER BY ix.index_value ASC, d.id ASC`
        : sort === "desc"
          ? `ORDER BY ix.index_value DESC, d.id ASC`
          : `ORDER BY d.id ASC`;

    const fetchLimit = limit === null ? Number.MAX_SAFE_INTEGER : limit + 1;
    const sql = `
      SELECT d.doc_key AS key, d.doc_json
      FROM index_entries ix
      INNER JOIN documents d
        ON d.collection = ix.collection
       AND d.doc_key = ix.doc_key
      WHERE ${whereParts.join(" AND ")}
      ${orderBy}
      LIMIT ?
    `;

    const rows = db.prepare(sql).all(...args, fetchLimit) as QueryRow[];

    return formatPage(rows, limit, collection);
  }

  function resolveIndexedCursor(
    collection: string,
    index: string,
    key: string,
    filter: SqlFilter,
  ): CursorRow | null {
    const whereParts = [`ix.collection = ?`, `ix.index_name = ?`, `d.doc_key = ?`];
    const args: (string | number)[] = [collection, index, key];

    if (filter.sql) {
      whereParts.push(filter.sql);
      args.push(...filter.args);
    }

    const row = db
      .prepare(`
        SELECT d.id, ix.index_value
        FROM index_entries ix
        INNER JOIN documents d
          ON d.collection = ix.collection
         AND d.doc_key = ix.doc_key
        WHERE ${whereParts.join(" AND ")}
        LIMIT 1
      `)
      .get(...args) as CursorRow | undefined;

    return row ?? null;
  }

  function formatPage(
    rows: QueryRow[],
    limit: number | null,
    collection: string,
  ): EngineQueryResult {
    const hasLimit = limit !== null;
    const hasMore = hasLimit && rows.length > limit;
    const pageRows = hasMore ? rows.slice(0, limit) : rows;

    return {
      documents: pageRows.map((row) => ({
        key: row.key,
        doc: parseStoredDocument(row.doc_json, collection, row.key),
      })),
      cursor: hasMore ? (pageRows[pageRows.length - 1]?.key ?? null) : null,
    };
  }

  engine.migrator = new DefaultMigrator(engine);

  return engine;
}

function configureDatabase(db: Database.Database, _options: SqliteEngineOptions): void {
  db.exec("PRAGMA foreign_keys = ON");
  db.exec("PRAGMA case_sensitive_like = ON");
}

function runMigrations(db: Database.Database): void {
  const currentVersion = readUserVersion(db);

  if (!Number.isFinite(currentVersion) || currentVersion < 0) {
    throw new Error(`Invalid SQLite user_version: ${String(currentVersion)}`);
  }

  if (currentVersion > LATEST_SCHEMA_VERSION) {
    throw new Error(
      `SQLite schema version ${String(currentVersion)} is newer than supported version ${String(LATEST_SCHEMA_VERSION)}`,
    );
  }

  if (currentVersion === LATEST_SCHEMA_VERSION) {
    return;
  }

  const migrate = db.transaction((fromVersion: number) => {
    if (fromVersion < 1) {
      db.exec(`
        CREATE TABLE IF NOT EXISTS documents (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          collection TEXT NOT NULL,
          doc_key TEXT NOT NULL,
          doc_json TEXT NOT NULL,
          write_version INTEGER NOT NULL DEFAULT 1,
          UNIQUE (collection, doc_key)
        );

        CREATE INDEX IF NOT EXISTS idx_documents_collection_id
          ON documents (collection, id);

        CREATE TABLE IF NOT EXISTS index_entries (
          collection TEXT NOT NULL,
          doc_key TEXT NOT NULL,
          index_name TEXT NOT NULL,
          index_value TEXT NOT NULL,
          PRIMARY KEY (collection, doc_key, index_name),
          FOREIGN KEY (collection, doc_key)
            REFERENCES documents (collection, doc_key)
            ON DELETE CASCADE
        );

        CREATE INDEX IF NOT EXISTS idx_index_entries_lookup
          ON index_entries (collection, index_name, index_value, doc_key);

        CREATE INDEX IF NOT EXISTS idx_index_entries_scan
          ON index_entries (collection, index_name, doc_key);

        CREATE TABLE IF NOT EXISTS migration_locks (
          collection TEXT PRIMARY KEY,
          lock_id TEXT NOT NULL,
          acquired_at INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS migration_checkpoints (
          collection TEXT PRIMARY KEY,
          cursor TEXT NOT NULL
        );
      `);

      ensureMigrationMetadataSchema(db);
    }

    if (fromVersion < 2) {
      ensureColumn(db, "documents", "write_version", "INTEGER NOT NULL DEFAULT 1");
      ensureMigrationMetadataSchema(db);
    }

    setUserVersion(db, LATEST_SCHEMA_VERSION);
  });

  migrate.immediate(currentVersion);
}

function readUserVersion(db: Database.Database): number {
  const row = db.prepare(`PRAGMA user_version`).get() as
    | {
        user_version?: unknown;
      }
    | undefined;

  return Number(row?.user_version ?? 0);
}

function setUserVersion(db: Database.Database, version: number): void {
  db.exec(`PRAGMA user_version = ${String(version)}`);
}

function serializeDocument(doc: unknown, collection: string, key: string): string {
  const encoded = JSON.stringify(doc);

  if (encoded === undefined) {
    throw new Error(
      `Document for collection "${collection}" and key "${key}" cannot be serialized to JSON`,
    );
  }

  return encoded;
}

function isSqliteUniqueConstraintError(error: unknown): boolean {
  if (typeof error === "object" && error !== null) {
    const code = (error as { code?: unknown }).code;

    if (typeof code === "string" && code.startsWith("SQLITE_CONSTRAINT")) {
      return true;
    }
  }

  const message = error instanceof Error ? error.message : String(error);

  return (
    message.includes("UNIQUE constraint failed") ||
    message.includes("PRIMARY KEY constraint failed")
  );
}

function parseStoredDocument(encoded: string, collection: string, key: string): unknown {
  try {
    return JSON.parse(encoded);
  } catch (error) {
    throw new Error(
      `Stored document for collection "${collection}" and key "${key}" is invalid JSON: ${
        error instanceof Error ? error.message : String(error)
      }`,
    );
  }
}

function resolveCursorId(
  collection: string,
  cursor: string | undefined,
  selectDocumentIdByKeyStmt: Database.Statement<
    [string, string],
    | {
        id: number;
      }
    | undefined
  >,
): number {
  if (!cursor) {
    return 0;
  }

  return selectDocumentIdByKeyStmt.get(collection, cursor)?.id ?? 0;
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

function normalizeTtl(ttl: number | undefined): number | null {
  if (ttl === undefined || !Number.isFinite(ttl) || ttl < 0) {
    return null;
  }

  return ttl;
}

interface SqlFilter {
  sql: string;
  args: string[];
}

function buildFilterSql(filter: string | number | FieldCondition): SqlFilter {
  if (typeof filter === "string" || typeof filter === "number") {
    return {
      sql: `ix.index_value = ?`,
      args: [String(filter)],
    };
  }

  const clauses: string[] = [];
  const args: string[] = [];

  if (filter.$eq !== undefined) {
    clauses.push(`ix.index_value = ?`);
    args.push(String(filter.$eq as string | number));
  }

  if (filter.$gt !== undefined) {
    clauses.push(`ix.index_value > ?`);
    args.push(String(filter.$gt as string | number));
  }

  if (filter.$gte !== undefined) {
    clauses.push(`ix.index_value >= ?`);
    args.push(String(filter.$gte as string | number));
  }

  if (filter.$lt !== undefined) {
    clauses.push(`ix.index_value < ?`);
    args.push(String(filter.$lt as string | number));
  }

  if (filter.$lte !== undefined) {
    clauses.push(`ix.index_value <= ?`);
    args.push(String(filter.$lte as string | number));
  }

  if (filter.$begins !== undefined) {
    clauses.push(`ix.index_value LIKE ? ESCAPE '\\'`);
    args.push(`${escapeLikePattern(filter.$begins)}%`);
  }

  if (filter.$between !== undefined) {
    const [low, high] = filter.$between as [string | number, string | number];
    clauses.push(`ix.index_value >= ? AND ix.index_value <= ?`);
    args.push(String(low), String(high));
  }

  if (clauses.length === 0) {
    return {
      sql: ``,
      args: [],
    };
  }

  return {
    sql: clauses.map((clause) => `(${clause})`).join(" AND "),
    args,
  };
}

function escapeLikePattern(input: string): string {
  return input.replace(/[\\%_]/g, "\\$&");
}

function randomId(): string {
  if (typeof crypto !== "undefined" && typeof crypto.randomUUID === "function") {
    return crypto.randomUUID();
  }

  const random = Math.random().toString(16).slice(2);
  const now = Date.now().toString(16);

  return `${now}-${random}`;
}

function getOutdatedDocuments(
  db: Database.Database,
  scanMissingMetadataStmt: Database.Statement<
    [string, number, number],
    Pick<DocumentRow, "doc_key" | "doc_json">
  >,
  upsertMigrationMetadataStmt: Database.Statement<[string, string, number, string, string | null]>,
  queryOutdatedByMetadataStmt: Database.Statement<
    [string, number, string, number, number],
    DocumentRow
  >,
  selectDocumentIdByKeyStmt: Database.Statement<[string, string], { id: number } | undefined>,
  collection: string,
  criteria: MigrationCriteria,
  cursor?: string,
): EngineQueryResult {
  if (criteria.skipMetadataSyncHint !== true) {
    syncMissingMigrationMetadata(
      db,
      scanMissingMetadataStmt,
      upsertMigrationMetadataStmt,
      collection,
      criteria,
    );
  }
  const pageLimit = normalizeOutdatedPageLimit(criteria.pageSizeHint);
  const startId = resolveCursorId(collection, cursor, selectDocumentIdByKeyStmt);
  const expectedSignature = computeIndexSignature(criteria.indexes);
  const rows = queryOutdatedByMetadataStmt.all(
    collection,
    startId,
    expectedSignature,
    criteria.version,
    pageLimit + 1,
  );
  const hasMore = rows.length > pageLimit;
  const pageRows = hasMore ? rows.slice(0, pageLimit) : rows;
  const documents: KeyedDocument[] = pageRows.map((row) => ({
    key: row.doc_key,
    doc: parseStoredDocument(row.doc_json, collection, row.doc_key),
    writeToken: String(row.write_version),
  }));

  return {
    documents,
    cursor: hasMore ? (documents[documents.length - 1]?.key ?? null) : null,
  };
}

function normalizeOutdatedPageLimit(value: number | undefined): number {
  if (value === undefined || !Number.isFinite(value)) {
    return OUTDATED_PAGE_LIMIT;
  }

  return Math.max(1, Math.floor(value));
}

function syncMissingMigrationMetadata(
  db: Database.Database,
  scanMissingMetadataStmt: Database.Statement<
    [string, number, number],
    Pick<DocumentRow, "doc_key" | "doc_json">
  >,
  upsertMigrationMetadataStmt: Database.Statement<[string, string, number, string, string | null]>,
  collection: string,
  criteria: MigrationCriteria,
): void {
  while (true) {
    const rows = scanMissingMetadataStmt.all(
      collection,
      criteria.version,
      OUTDATED_SCAN_CHUNK_SIZE,
    );

    if (rows.length === 0) {
      return;
    }

    const syncTxn = db.transaction(
      (
        pending: Pick<DocumentRow, "doc_key" | "doc_json">[],
        targetCollection: string,
        targetCriteria: MigrationCriteria,
      ) => {
        for (const row of pending) {
          const doc = parseStoredDocument(row.doc_json, targetCollection, row.doc_key);
          const metadata = deriveMetadataForCriteria(doc, targetCriteria);
          upsertMigrationMetadataStmt.run(
            targetCollection,
            row.doc_key,
            metadata.targetVersion,
            metadata.versionState,
            metadata.indexSignature,
          );
        }
      },
    );

    syncTxn.immediate(rows, collection, criteria);

    if (rows.length < OUTDATED_SCAN_CHUNK_SIZE) {
      return;
    }
  }
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

  return {
    targetVersion: criteria.version,
    versionState: classifyVersionState(parsedVersion, criteria.version, compareVersions),
    indexSignature: computeIndexSignatureFromUnknown(doc[criteria.indexesField]),
  };
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
    raw.targetVersion <= 0
  ) {
    throw new Error("SQLite received invalid migration metadata target version");
  }

  if (!isMigrationVersionState(raw.versionState)) {
    throw new Error("SQLite received invalid migration metadata state");
  }

  if (raw.indexSignature !== null && typeof raw.indexSignature !== "string") {
    throw new Error("SQLite received invalid migration metadata index signature");
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

function deriveLegacyMetadata(doc: unknown): MigrationMetadata {
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

function computeIndexSignatureFromUnknown(raw: unknown): string | null {
  if (!Array.isArray(raw) || raw.some((value) => typeof value !== "string")) {
    return null;
  }

  return computeIndexSignature(raw as string[]);
}

function computeIndexSignature(indexes: readonly string[]): string {
  return JSON.stringify(indexes);
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

function parseWriteToken(raw: string): number {
  const value = Number(raw);

  if (!Number.isFinite(value) || Math.floor(value) !== value || value <= 0) {
    throw new Error("SQLite received an invalid write token");
  }

  return value;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function hasColumn(db: Database.Database, table: string, column: string): boolean {
  const rows = db.prepare(`PRAGMA table_info(${table})`).all() as { name?: unknown }[];

  return rows.some((row) => row.name === column);
}

function ensureColumn(
  db: Database.Database,
  table: string,
  column: string,
  definitionSql: string,
): void {
  if (hasColumn(db, table, column)) {
    return;
  }

  db.exec(`ALTER TABLE ${table} ADD COLUMN ${column} ${definitionSql}`);
}

function ensureMigrationMetadataSchema(db: Database.Database): void {
  db.exec(`
    CREATE TABLE IF NOT EXISTS migration_metadata (
      collection TEXT NOT NULL,
      doc_key TEXT NOT NULL,
      target_version INTEGER NULL,
      version_state TEXT NULL,
      index_signature TEXT NULL,
      PRIMARY KEY (collection, doc_key),
      FOREIGN KEY (collection, doc_key)
        REFERENCES documents (collection, doc_key)
        ON DELETE CASCADE
    );

    CREATE INDEX IF NOT EXISTS idx_migration_metadata_target_state_doc
      ON migration_metadata (collection, target_version, version_state, doc_key);

    CREATE INDEX IF NOT EXISTS idx_migration_metadata_target_state_signature
      ON migration_metadata (collection, target_version, version_state, index_signature, doc_key);
  `);

  ensureColumn(db, "migration_metadata", "target_version", "INTEGER NULL");
  ensureColumn(db, "migration_metadata", "version_state", "TEXT NULL");
  ensureColumn(db, "migration_metadata", "index_signature", "TEXT NULL");
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
