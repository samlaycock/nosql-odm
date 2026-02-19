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
  type MigrationDocumentMetadata,
  type MigrationCriteria,
  type MigrationVersionState,
  type QueryEngine,
  type QueryParams,
  type ResolvedIndexKeys,
} from "./types";

const DEFAULT_DOCUMENTS_TABLE = "nosql_odm_documents";
const DEFAULT_INDEXES_TABLE = "nosql_odm_index_entries";
const DEFAULT_MIGRATION_METADATA_TABLE = "nosql_odm_migration_metadata";
const DEFAULT_MIGRATION_LOCKS_TABLE = "nosql_odm_migration_locks";
const DEFAULT_MIGRATION_CHECKPOINTS_TABLE = "nosql_odm_migration_checkpoints";

const OUTDATED_PAGE_LIMIT = 100;
const OUTDATED_SCAN_CHUNK_SIZE = 256;

interface MySqlQueryableLike {
  query(sql: string, params?: unknown[]): Promise<[unknown, unknown?]>;
  execute(sql: string, params?: unknown[]): Promise<[unknown, unknown?]>;
}

interface MySqlConnectionLike extends MySqlQueryableLike {
  beginTransaction(): Promise<void>;
  commit(): Promise<void>;
  rollback(): Promise<void>;
  release(): void;
}

interface MySqlPoolLike extends MySqlQueryableLike {
  getConnection(): Promise<MySqlConnectionLike>;
}

export interface MySqlEngineOptions {
  client: MySqlPoolLike;
  database?: string;
  documentsTable?: string;
  indexesTable?: string;
  migrationMetadataTable?: string;
  migrationLocksTable?: string;
  migrationCheckpointsTable?: string;
}

export interface MySqlQueryEngine extends QueryEngine<never> {}

interface QueryRow {
  key: string;
  doc: Record<string, unknown>;
}

interface CursorRow {
  id: number;
  indexValue: string;
}

interface LockRow {
  lockId: string;
  acquiredAt: number;
}

interface TableRefs {
  documentsTable: string;
  indexesTable: string;
  migrationMetadataTable: string;
  migrationLocksTable: string;
  migrationCheckpointsTable: string;
}

export function mySqlEngine(options: MySqlEngineOptions): MySqlQueryEngine {
  const client = options.client;
  const refs = buildTableRefs(options);
  const ready = ensureSchema(client, refs);

  const engine: MySqlQueryEngine = {
    capabilities: {
      uniqueConstraints: "atomic",
    },

    async get(collection, key) {
      await ready;

      const row = await fetchOptionalRow(client, {
        sql: `
          SELECT doc_key AS \`key\`, doc_json
          FROM ${refs.documentsTable}
          WHERE collection = ? AND doc_key = ?
          LIMIT 1
        `,
        params: [collection, key],
        errorMessage: "MySQL returned an invalid document row",
      });

      if (!row) {
        return null;
      }

      return parseQueryRow(row, collection).doc;
    },

    async create(collection, key, doc, indexes, _options, migrationMetadata) {
      await ready;

      const docJson = serializeDocument(doc);
      const normalizedIndexes = normalizeIndexes(indexes);
      const metadata = normalizeMigrationMetadata(migrationMetadata) ?? deriveLegacyMetadata(doc);

      try {
        await withTransaction(client, async (tx) => {
          await tx.execute(
            `
              INSERT INTO ${refs.documentsTable} (collection, doc_key, doc_json)
              VALUES (?, ?, CAST(? AS JSON))
            `,
            [collection, key, docJson],
          );

          await replaceIndexes(tx, refs, collection, key, normalizedIndexes, false);
          await upsertMigrationMetadata(tx, refs, collection, key, metadata);
        });
      } catch (error) {
        if (isMySqlDuplicateKeyError(error)) {
          throw new EngineDocumentAlreadyExistsError(collection, key);
        }

        throw error;
      }
    },

    async put(collection, key, doc, indexes, _options, migrationMetadata) {
      await ready;

      const docJson = serializeDocument(doc);
      const normalizedIndexes = normalizeIndexes(indexes);
      const metadata = normalizeMigrationMetadata(migrationMetadata) ?? deriveLegacyMetadata(doc);

      await withTransaction(client, async (tx) => {
        await tx.execute(
          `
            INSERT INTO ${refs.documentsTable} (collection, doc_key, doc_json)
            VALUES (?, ?, CAST(? AS JSON))
            ON DUPLICATE KEY UPDATE
              doc_json = VALUES(doc_json),
              write_version = write_version + 1
          `,
          [collection, key, docJson],
        );

        await replaceIndexes(tx, refs, collection, key, normalizedIndexes, true);
        await upsertMigrationMetadata(tx, refs, collection, key, metadata);
      });
    },

    async update(collection, key, doc, indexes, _options, migrationMetadata) {
      await ready;

      const docJson = serializeDocument(doc);
      const normalizedIndexes = normalizeIndexes(indexes);
      const metadata = normalizeMigrationMetadata(migrationMetadata) ?? deriveLegacyMetadata(doc);

      await withTransaction(client, async (tx) => {
        const [result] = await tx.execute(
          `
            UPDATE ${refs.documentsTable}
            SET
              doc_json = CAST(? AS JSON),
              write_version = write_version + 1
            WHERE collection = ? AND doc_key = ?
          `,
          [docJson, collection, key],
        );

        if (readAffectedRows(result, "MySQL returned an invalid update result") === 0) {
          throw new EngineDocumentNotFoundError(collection, key);
        }

        await replaceIndexes(tx, refs, collection, key, normalizedIndexes, true);
        await upsertMigrationMetadata(tx, refs, collection, key, metadata);
      });
    },

    async delete(collection, key) {
      await ready;

      await client.execute(
        `DELETE FROM ${refs.documentsTable} WHERE collection = ? AND doc_key = ?`,
        [collection, key],
      );
    },

    async query(collection, params) {
      await ready;

      if (!params.index || !params.filter) {
        return queryByCollectionScan(client, refs, collection, params);
      }

      return queryByIndex(client, refs, collection, params);
    },

    async batchGet(collection, keys) {
      await ready;

      const uniqueKeys = uniqueStrings(keys);

      if (uniqueKeys.length === 0) {
        return [];
      }

      const placeholders = createPlaceholders(uniqueKeys.length);
      const rows = await fetchRows(client, {
        sql: `
          SELECT doc_key AS \`key\`, doc_json
          FROM ${refs.documentsTable}
          WHERE collection = ? AND doc_key IN (${placeholders})
        `,
        params: [collection, ...uniqueKeys],
        errorMessage: "MySQL returned an invalid batch get result",
      });

      const fetched = new Map<string, Record<string, unknown>>();

      for (const row of rows) {
        const parsed = parseQueryRow(row, collection);
        fetched.set(parsed.key, parsed.doc);
      }

      const results: KeyedDocument[] = [];

      for (const key of keys) {
        const doc = fetched.get(key);

        if (!doc) {
          continue;
        }

        results.push({
          key,
          doc: structuredClone(doc),
        });
      }

      return results;
    },

    async batchSet(collection, items) {
      await ready;

      await withTransaction(client, async (tx) => {
        await applyBatchSet(tx, refs, collection, items);
      });
    },

    async batchSetWithResult(collection, items) {
      await ready;

      return withTransaction(client, async (tx) => applyBatchSet(tx, refs, collection, items));
    },

    async batchDelete(collection, keys) {
      await ready;

      const uniqueKeys = uniqueStrings(keys);

      if (uniqueKeys.length === 0) {
        return;
      }

      const placeholders = createPlaceholders(uniqueKeys.length);

      await client.execute(
        `DELETE FROM ${refs.documentsTable} WHERE collection = ? AND doc_key IN (${placeholders})`,
        [collection, ...uniqueKeys],
      );
    },

    migration: {
      async acquireLock(collection, options) {
        await ready;

        return withTransaction(client, async (tx) => {
          const now = Date.now();
          const lockId = randomId();
          const existing = await fetchOptionalRow(tx, {
            sql: `
              SELECT lock_id, acquired_at
              FROM ${refs.migrationLocksTable}
              WHERE collection = ?
              FOR UPDATE
            `,
            params: [collection],
            errorMessage: "MySQL returned an invalid migration lock row",
          });

          if (existing) {
            const parsedExisting = parseLockRow(
              existing,
              "MySQL returned an invalid migration lock row",
            );
            const ttl = normalizeTtl(options?.ttl);

            if (ttl === null || now - parsedExisting.acquiredAt < ttl) {
              return null;
            }

            await tx.execute(
              `
                UPDATE ${refs.migrationLocksTable}
                SET lock_id = ?, acquired_at = ?
                WHERE collection = ?
              `,
              [lockId, now, collection],
            );
          } else {
            await tx.execute(
              `
                INSERT INTO ${refs.migrationLocksTable} (collection, lock_id, acquired_at)
                VALUES (?, ?, ?)
              `,
              [collection, lockId, now],
            );
          }

          return {
            id: lockId,
            collection,
            acquiredAt: now,
          };
        });
      },

      async releaseLock(lock) {
        await ready;

        await client.execute(
          `DELETE FROM ${refs.migrationLocksTable} WHERE collection = ? AND lock_id = ?`,
          [lock.collection, lock.id],
        );
      },

      async getOutdated(collection, criteria, cursor) {
        await ready;

        return getOutdatedDocuments(client, refs, collection, criteria, cursor);
      },

      async saveCheckpoint(lock, cursor) {
        await ready;

        await client.execute(
          `
            INSERT INTO ${refs.migrationCheckpointsTable} (collection, checkpoint_cursor)
            SELECT ?, ?
            FROM DUAL
            WHERE EXISTS (
              SELECT 1
              FROM ${refs.migrationLocksTable}
              WHERE collection = ? AND lock_id = ?
            )
            ON DUPLICATE KEY UPDATE checkpoint_cursor = VALUES(checkpoint_cursor)
          `,
          [lock.collection, cursor, lock.collection, lock.id],
        );
      },

      async loadCheckpoint(collection) {
        await ready;

        const row = await fetchOptionalRow(client, {
          sql: `
            SELECT checkpoint_cursor
            FROM ${refs.migrationCheckpointsTable}
            WHERE collection = ?
            LIMIT 1
          `,
          params: [collection],
          errorMessage: "MySQL returned an invalid migration checkpoint row",
        });

        if (!row) {
          return null;
        }

        return readStringField(
          row,
          "checkpoint_cursor",
          "MySQL returned an invalid migration checkpoint row",
        );
      },

      async clearCheckpoint(collection) {
        await ready;

        await client.execute(`DELETE FROM ${refs.migrationCheckpointsTable} WHERE collection = ?`, [
          collection,
        ]);
      },

      async getStatus(collection) {
        await ready;

        const row = await fetchOptionalRow(client, {
          sql: `
            SELECT l.lock_id, l.acquired_at, c.checkpoint_cursor
            FROM ${refs.migrationLocksTable} l
            LEFT JOIN ${refs.migrationCheckpointsTable} c
              ON c.collection = l.collection
            WHERE l.collection = ?
            LIMIT 1
          `,
          params: [collection],
          errorMessage: "MySQL returned an invalid migration status row",
        });

        if (!row) {
          return null;
        }

        const lock = parseLockRow(row, "MySQL returned an invalid migration status row");
        const cursorValue = row.checkpoint_cursor;

        if (cursorValue !== null && typeof cursorValue !== "string") {
          throw new Error("MySQL returned an invalid migration status row");
        }

        return {
          lock: {
            id: lock.lockId,
            collection,
            acquiredAt: lock.acquiredAt,
          },
          cursor: cursorValue,
        };
      },
    },
  };

  engine.migrator = new DefaultMigrator(engine);

  return engine;
}

async function queryByCollectionScan(
  client: MySqlQueryableLike,
  refs: TableRefs,
  collection: string,
  params: QueryParams,
): Promise<EngineQueryResult> {
  const limit = normalizeLimit(params.limit);

  if (limit === 0) {
    return {
      documents: [],
      cursor: null,
    };
  }

  const cursorId = await resolveCursorId(client, refs, collection, params.cursor);
  const values: (string | number)[] = [collection, cursorId];

  let sql = `
    SELECT doc_key AS \`key\`, doc_json
    FROM ${refs.documentsTable}
    WHERE collection = ? AND id > ?
    ORDER BY id ASC
  `;

  if (limit !== null) {
    values.push(limit + 1);
    sql += ` LIMIT ?`;
  }

  const rows = await fetchRows(client, {
    sql,
    params: values,
    errorMessage: "MySQL returned an invalid query result",
  });

  return formatPage(rows, limit, collection);
}

async function queryByIndex(
  client: MySqlQueryableLike,
  refs: TableRefs,
  collection: string,
  params: QueryParams,
): Promise<EngineQueryResult> {
  const limit = normalizeLimit(params.limit);

  if (limit === 0) {
    return {
      documents: [],
      cursor: null,
    };
  }

  const index = params.index!;
  const filter = params.filter!.value;
  const sort = params.sort;
  const builder = createSqlBuilder();
  const whereClauses = [
    `ix.collection = ${builder.push(collection)}`,
    `ix.index_name = ${builder.push(index)}`,
  ];

  const filterSql = buildFilterSql(filter, builder);

  if (filterSql.length > 0) {
    whereClauses.push(filterSql);
  }

  const cursor = params.cursor
    ? await resolveIndexedCursor(client, refs, collection, index, params.cursor, filter)
    : null;

  if (cursor) {
    if (sort === "asc") {
      const valueArg = builder.push(cursor.indexValue);
      const valueTieArg = builder.push(cursor.indexValue);
      const idArg = builder.push(cursor.id);

      whereClauses.push(
        `(ix.index_value > ${valueArg} OR (ix.index_value = ${valueTieArg} AND d.id > ${idArg}))`,
      );
    } else if (sort === "desc") {
      const valueArg = builder.push(cursor.indexValue);
      const valueTieArg = builder.push(cursor.indexValue);
      const idArg = builder.push(cursor.id);

      whereClauses.push(
        `(ix.index_value < ${valueArg} OR (ix.index_value = ${valueTieArg} AND d.id > ${idArg}))`,
      );
    } else {
      const idArg = builder.push(cursor.id);

      whereClauses.push(`d.id > ${idArg}`);
    }
  }

  const orderBy =
    sort === "asc"
      ? `ORDER BY ix.index_value ASC, d.id ASC`
      : sort === "desc"
        ? `ORDER BY ix.index_value DESC, d.id ASC`
        : `ORDER BY d.id ASC`;

  let sql = `
    SELECT d.doc_key AS \`key\`, d.doc_json
    FROM ${refs.indexesTable} ix
    INNER JOIN ${refs.documentsTable} d
      ON d.collection = ix.collection
     AND d.doc_key = ix.doc_key
    WHERE ${whereClauses.join(" AND ")}
    ${orderBy}
  `;

  if (limit !== null) {
    sql += ` LIMIT ${builder.push(limit + 1)}`;
  }

  const rows = await fetchRows(client, {
    sql,
    params: builder.values,
    errorMessage: "MySQL returned an invalid query result",
  });

  return formatPage(rows, limit, collection);
}

async function resolveIndexedCursor(
  client: MySqlQueryableLike,
  refs: TableRefs,
  collection: string,
  index: string,
  key: string,
  filter: string | number | FieldCondition,
): Promise<CursorRow | null> {
  const builder = createSqlBuilder();
  const whereClauses = [
    `ix.collection = ${builder.push(collection)}`,
    `ix.index_name = ${builder.push(index)}`,
    `d.doc_key = ${builder.push(key)}`,
  ];

  const filterSql = buildFilterSql(filter, builder);

  if (filterSql.length > 0) {
    whereClauses.push(filterSql);
  }

  const row = await fetchOptionalRow(client, {
    sql: `
      SELECT d.id, ix.index_value
      FROM ${refs.indexesTable} ix
      INNER JOIN ${refs.documentsTable} d
        ON d.collection = ix.collection
       AND d.doc_key = ix.doc_key
      WHERE ${whereClauses.join(" AND ")}
      LIMIT 1
    `,
    params: builder.values,
    errorMessage: "MySQL returned an invalid indexed cursor row",
  });

  if (!row) {
    return null;
  }

  return {
    id: readFiniteInteger(row, "id", "MySQL returned an invalid indexed cursor row"),
    indexValue: readStringField(row, "index_value", "MySQL returned an invalid indexed cursor row"),
  };
}

function formatPage(
  rows: Record<string, unknown>[],
  limit: number | null,
  collection: string,
): EngineQueryResult {
  const hasLimit = limit !== null;
  const hasMore = hasLimit && rows.length > limit;
  const pageRows = hasMore ? rows.slice(0, limit) : rows;

  return {
    documents: pageRows.map((row) => {
      const parsed = parseQueryRow(row, collection);

      return {
        key: parsed.key,
        doc: structuredClone(parsed.doc),
      };
    }),
    cursor: hasMore
      ? readStringField(
          pageRows[pageRows.length - 1]!,
          "key",
          "MySQL returned an invalid query result",
        )
      : null,
  };
}

async function replaceIndexes(
  client: MySqlQueryableLike,
  refs: TableRefs,
  collection: string,
  key: string,
  indexes: ResolvedIndexKeys,
  clearExisting: boolean,
): Promise<void> {
  if (clearExisting) {
    await client.execute(`DELETE FROM ${refs.indexesTable} WHERE collection = ? AND doc_key = ?`, [
      collection,
      key,
    ]);
  }

  for (const [indexName, indexValue] of Object.entries(indexes)) {
    await client.execute(
      `
        INSERT INTO ${refs.indexesTable} (collection, doc_key, index_name, index_value)
        VALUES (?, ?, ?, ?)
      `,
      [collection, key, indexName, indexValue],
    );
  }
}

interface MigrationMetadata {
  targetVersion: number;
  versionState: MigrationVersionState;
  indexSignature: string | null;
}

async function upsertMigrationMetadata(
  client: MySqlQueryableLike,
  refs: TableRefs,
  collection: string,
  key: string,
  metadata: MigrationMetadata,
): Promise<void> {
  await client.execute(
    `
      INSERT INTO ${refs.migrationMetadataTable} (collection, doc_key, target_version, version_state, index_signature)
      VALUES (?, ?, ?, ?, ?)
      ON DUPLICATE KEY UPDATE
        target_version = VALUES(target_version),
        version_state = VALUES(version_state),
        index_signature = VALUES(index_signature)
    `,
    [collection, key, metadata.targetVersion, metadata.versionState, metadata.indexSignature],
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
    raw.targetVersion <= 0
  ) {
    throw new Error("MySQL received invalid migration metadata target version");
  }

  if (!isMigrationVersionState(raw.versionState)) {
    throw new Error("MySQL received invalid migration metadata state");
  }

  if (raw.indexSignature !== null && typeof raw.indexSignature !== "string") {
    throw new Error("MySQL received invalid migration metadata index signature");
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

async function applyBatchSet(
  tx: MySqlConnectionLike,
  refs: TableRefs,
  collection: string,
  items: BatchSetItem[],
): Promise<BatchSetResult> {
  const persistedKeys: string[] = [];
  const conflictedKeys: string[] = [];

  for (const item of items) {
    const docJson = serializeDocument(item.doc);
    const normalizedIndexes = normalizeIndexes(item.indexes);
    const metadata =
      normalizeMigrationMetadata(item.migrationMetadata) ?? deriveLegacyMetadata(item.doc);

    if (item.expectedWriteToken !== undefined) {
      const expectedWriteVersion = parseWriteToken(item.expectedWriteToken);
      const [result] = await tx.execute(
        `
          UPDATE ${refs.documentsTable}
          SET
            doc_json = CAST(? AS JSON),
            write_version = write_version + 1
          WHERE collection = ?
            AND doc_key = ?
            AND write_version = ?
        `,
        [docJson, collection, item.key, expectedWriteVersion],
      );

      if (
        readAffectedRows(result, "MySQL returned an invalid conditional batch-set result") === 0
      ) {
        conflictedKeys.push(item.key);
        continue;
      }

      await replaceIndexes(tx, refs, collection, item.key, normalizedIndexes, true);
      await upsertMigrationMetadata(tx, refs, collection, item.key, metadata);
      persistedKeys.push(item.key);
      continue;
    }

    await tx.execute(
      `
        INSERT INTO ${refs.documentsTable} (collection, doc_key, doc_json)
        VALUES (?, ?, CAST(? AS JSON))
        ON DUPLICATE KEY UPDATE
          doc_json = VALUES(doc_json),
          write_version = write_version + 1
      `,
      [collection, item.key, docJson],
    );

    await replaceIndexes(tx, refs, collection, item.key, normalizedIndexes, true);
    await upsertMigrationMetadata(tx, refs, collection, item.key, metadata);
    persistedKeys.push(item.key);
  }

  return {
    persistedKeys,
    conflictedKeys,
  };
}

async function ensureSchema(client: MySqlQueryableLike, refs: TableRefs): Promise<void> {
  await client.query(`
    CREATE TABLE IF NOT EXISTS ${refs.documentsTable} (
      id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
      collection VARCHAR(191) NOT NULL,
      doc_key VARCHAR(191) NOT NULL,
      doc_json JSON NOT NULL,
      write_version BIGINT UNSIGNED NOT NULL DEFAULT 1,
      PRIMARY KEY (id),
      UNIQUE KEY uq_collection_doc_key (collection, doc_key),
      KEY idx_collection_id (collection, id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
  `);

  await ensureColumn(
    client,
    refs.documentsTable,
    "write_version",
    "BIGINT UNSIGNED NOT NULL DEFAULT 1",
  );

  await client.query(`
    CREATE TABLE IF NOT EXISTS ${refs.indexesTable} (
      collection VARCHAR(191) NOT NULL,
      doc_key VARCHAR(191) NOT NULL,
      index_name VARCHAR(191) NOT NULL,
      index_value TEXT NOT NULL,
      PRIMARY KEY (collection, doc_key, index_name),
      FOREIGN KEY (collection, doc_key)
        REFERENCES ${refs.documentsTable} (collection, doc_key)
        ON DELETE CASCADE,
      KEY idx_lookup (collection, index_name, index_value(191), doc_key),
      KEY idx_scan (collection, index_name, doc_key)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
  `);

  await client.query(`
    CREATE TABLE IF NOT EXISTS ${refs.migrationMetadataTable} (
      collection VARCHAR(191) NOT NULL,
      doc_key VARCHAR(191) NOT NULL,
      target_version INT NULL,
      version_state VARCHAR(16) NULL,
      index_signature TEXT NULL,
      PRIMARY KEY (collection, doc_key),
      FOREIGN KEY (collection, doc_key)
        REFERENCES ${refs.documentsTable} (collection, doc_key)
        ON DELETE CASCADE,
      KEY idx_target_state_doc (collection, target_version, version_state, doc_key),
      KEY idx_target_state_signature (collection, target_version, version_state, index_signature(191), doc_key)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
  `);

  await ensureColumn(client, refs.migrationMetadataTable, "target_version", "INT NULL");
  await ensureColumn(client, refs.migrationMetadataTable, "version_state", "VARCHAR(16) NULL");
  await ensureColumn(client, refs.migrationMetadataTable, "index_signature", "TEXT NULL");

  await ensureIndex(
    client,
    refs.migrationMetadataTable,
    "idx_target_state_doc",
    "(collection, target_version, version_state, doc_key)",
  );

  await ensureIndex(
    client,
    refs.migrationMetadataTable,
    "idx_target_state_signature",
    "(collection, target_version, version_state, index_signature(191), doc_key)",
  );

  await client.query(`
    CREATE TABLE IF NOT EXISTS ${refs.migrationLocksTable} (
      collection VARCHAR(191) NOT NULL,
      lock_id VARCHAR(191) NOT NULL,
      acquired_at BIGINT NOT NULL,
      PRIMARY KEY (collection)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
  `);

  await client.query(`
    CREATE TABLE IF NOT EXISTS ${refs.migrationCheckpointsTable} (
      collection VARCHAR(191) NOT NULL,
      checkpoint_cursor TEXT NOT NULL,
      PRIMARY KEY (collection)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
  `);
}

async function withTransaction<T>(
  client: MySqlPoolLike,
  work: (tx: MySqlConnectionLike) => Promise<T>,
): Promise<T> {
  const tx = await client.getConnection();

  try {
    await tx.beginTransaction();

    const result = await work(tx);

    await tx.commit();

    return result;
  } catch (error) {
    try {
      await tx.rollback();
    } catch {
      // Ignore rollback failures and preserve original error.
    }

    throw error;
  } finally {
    tx.release();
  }
}

async function ensureIndex(
  client: MySqlQueryableLike,
  tableName: string,
  indexName: string,
  columnsSql: string,
): Promise<void> {
  try {
    await client.query(
      `ALTER TABLE ${tableName} ADD INDEX ${quoteIdentifier(indexName)} ${columnsSql}`,
    );
  } catch (error) {
    if (isMySqlDuplicateIndexError(error)) {
      return;
    }

    throw error;
  }
}

async function ensureColumn(
  client: MySqlQueryableLike,
  tableName: string,
  columnName: string,
  definitionSql: string,
): Promise<void> {
  try {
    await client.query(
      `ALTER TABLE ${tableName} ADD COLUMN ${quoteIdentifier(columnName)} ${definitionSql}`,
    );
  } catch (error) {
    if (isMySqlDuplicateColumnError(error)) {
      return;
    }

    throw error;
  }
}

interface SqlBuilder {
  values: (string | number)[];
  push(value: string | number): string;
}

function createSqlBuilder(): SqlBuilder {
  const values: (string | number)[] = [];

  return {
    values,
    push(value) {
      values.push(value);
      return "?";
    },
  };
}

function buildFilterSql(filter: string | number | FieldCondition, builder: SqlBuilder): string {
  if (typeof filter === "string" || typeof filter === "number") {
    return `ix.index_value = ${builder.push(String(filter))}`;
  }

  const clauses: string[] = [];

  if (filter.$eq !== undefined) {
    clauses.push(`ix.index_value = ${builder.push(String(filter.$eq as string | number))}`);
  }

  if (filter.$gt !== undefined) {
    clauses.push(`ix.index_value > ${builder.push(String(filter.$gt as string | number))}`);
  }

  if (filter.$gte !== undefined) {
    clauses.push(`ix.index_value >= ${builder.push(String(filter.$gte as string | number))}`);
  }

  if (filter.$lt !== undefined) {
    clauses.push(`ix.index_value < ${builder.push(String(filter.$lt as string | number))}`);
  }

  if (filter.$lte !== undefined) {
    clauses.push(`ix.index_value <= ${builder.push(String(filter.$lte as string | number))}`);
  }

  if (filter.$begins !== undefined) {
    clauses.push(
      `ix.index_value LIKE ${builder.push(`${escapeLikePattern(filter.$begins)}%`)} ESCAPE '\\\\'`,
    );
  }

  if (filter.$between !== undefined) {
    const [low, high] = filter.$between as [string | number, string | number];

    clauses.push(
      `(ix.index_value >= ${builder.push(String(low))} AND ix.index_value <= ${builder.push(String(high))})`,
    );
  }

  if (clauses.length === 0) {
    return "";
  }

  return clauses.map((clause) => `(${clause})`).join(" AND ");
}

function escapeLikePattern(input: string): string {
  return input.replace(/[\\%_]/g, "\\$&");
}

async function fetchRows(
  client: MySqlQueryableLike,
  options: {
    sql: string;
    params: unknown[];
    errorMessage: string;
  },
): Promise<Record<string, unknown>[]> {
  const [rows] = await client.query(options.sql, options.params);

  if (!Array.isArray(rows)) {
    throw new Error(options.errorMessage);
  }

  return rows.map((row) => parseRecord(row, options.errorMessage));
}

async function fetchOptionalRow(
  client: MySqlQueryableLike,
  options: {
    sql: string;
    params: unknown[];
    errorMessage: string;
  },
): Promise<Record<string, unknown> | null> {
  const rows = await fetchRows(client, options);

  if (rows.length === 0) {
    return null;
  }

  return rows[0] ?? null;
}

function parseQueryRow(raw: Record<string, unknown>, collection: string): QueryRow {
  const key = readStringField(raw, "key", "MySQL returned an invalid query result");

  return {
    key,
    doc: parseStoredDocument(raw.doc_json, collection, key),
  };
}

function parseStoredDocument(
  raw: unknown,
  collection: string,
  key: string,
): Record<string, unknown> {
  if (isRecord(raw)) {
    return raw;
  }

  if (typeof raw === "string") {
    try {
      const parsed = JSON.parse(raw);

      if (!isRecord(parsed)) {
        throw new Error("not-an-object");
      }

      return parsed;
    } catch {
      throw new Error(
        `Stored document for collection "${collection}" and key "${key}" is invalid JSON object`,
      );
    }
  }

  if (ArrayBuffer.isView(raw)) {
    const decoded = new TextDecoder().decode(raw as Uint8Array);

    try {
      const parsed = JSON.parse(decoded);

      if (!isRecord(parsed)) {
        throw new Error("not-an-object");
      }

      return parsed;
    } catch {
      throw new Error(
        `Stored document for collection "${collection}" and key "${key}" is invalid JSON object`,
      );
    }
  }

  throw new Error(
    `Stored document for collection "${collection}" and key "${key}" is invalid JSON object`,
  );
}

function serializeDocument(doc: unknown): string {
  const cloned = structuredClone(doc);

  if (!isRecord(cloned)) {
    throw new Error("MySQL received a non-object document");
  }

  const encoded = JSON.stringify(cloned);

  if (encoded === undefined) {
    throw new Error("MySQL received a non-serializable document");
  }

  return encoded;
}

function normalizeIndexes(indexes: ResolvedIndexKeys): ResolvedIndexKeys {
  const normalized: ResolvedIndexKeys = {};

  for (const [name, value] of Object.entries(indexes)) {
    if (typeof value !== "string") {
      throw new Error("MySQL received invalid index values");
    }

    normalized[name] = value;
  }

  return normalized;
}

async function resolveCursorId(
  client: MySqlQueryableLike,
  refs: TableRefs,
  collection: string,
  cursor: string | undefined,
): Promise<number> {
  if (!cursor) {
    return 0;
  }

  const row = await fetchOptionalRow(client, {
    sql: `
      SELECT id
      FROM ${refs.documentsTable}
      WHERE collection = ? AND doc_key = ?
      LIMIT 1
    `,
    params: [collection, cursor],
    errorMessage: "MySQL returned an invalid cursor row",
  });

  if (!row) {
    return 0;
  }

  return readFiniteInteger(row, "id", "MySQL returned an invalid cursor row");
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

async function getOutdatedDocuments(
  client: MySqlQueryableLike,
  refs: TableRefs,
  collection: string,
  criteria: MigrationCriteria,
  cursor?: string,
): Promise<EngineQueryResult> {
  if (criteria.skipMetadataSyncHint !== true) {
    await syncMissingMigrationMetadata(client as MySqlPoolLike, refs, collection, criteria);
  }
  return getOutdatedDocumentsByMetadata(client, refs, collection, criteria, cursor);
}

async function getOutdatedDocumentsByMetadata(
  client: MySqlQueryableLike,
  refs: TableRefs,
  collection: string,
  criteria: MigrationCriteria,
  cursor?: string,
): Promise<EngineQueryResult> {
  const pageLimit = normalizeOutdatedPageLimit(criteria.pageSizeHint);
  const startKey = cursor ?? "";
  const expectedSignature = computeIndexSignature(criteria.indexes);
  const rows = await fetchRows(client, {
    sql: `
      SELECT d.doc_key AS \`key\`, d.doc_json, d.write_version
      FROM ${refs.migrationMetadataTable} m
      INNER JOIN ${refs.documentsTable} d
        ON d.collection = m.collection
       AND d.doc_key = m.doc_key
      WHERE m.collection = ?
        AND m.doc_key > ?
        AND (
          m.version_state = 'stale'
          OR (
            m.version_state = 'current'
            AND (m.index_signature IS NULL OR m.index_signature <> ?)
          )
        )
        AND m.target_version = ?
      ORDER BY m.doc_key ASC
      LIMIT ?
    `,
    params: [collection, startKey, expectedSignature, criteria.version, pageLimit + 1],
    errorMessage: "MySQL returned an invalid outdated query result",
  });

  const hasMore = rows.length > pageLimit;
  const pageRows = hasMore ? rows.slice(0, pageLimit) : rows;
  const documents: KeyedDocument[] = [];

  for (const row of pageRows) {
    const key = readStringField(row, "key", "MySQL returned an invalid outdated query result");
    const writeVersion = readFiniteInteger(
      row,
      "write_version",
      "MySQL returned an invalid outdated query result",
    );
    const doc = parseStoredDocument(row.doc_json, collection, key);

    documents.push({
      key,
      doc: structuredClone(doc),
      writeToken: String(writeVersion),
    });
  }

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

async function syncMissingMigrationMetadata(
  client: MySqlPoolLike,
  refs: TableRefs,
  collection: string,
  criteria: MigrationCriteria,
): Promise<void> {
  while (true) {
    const rows = await fetchRows(client, {
      sql: `
        SELECT d.doc_key AS \`key\`, d.doc_json
        FROM ${refs.documentsTable} d
        LEFT JOIN ${refs.migrationMetadataTable} m
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
      `,
      params: [collection, criteria.version, OUTDATED_SCAN_CHUNK_SIZE],
      errorMessage: "MySQL returned an invalid migration metadata sync result",
    });

    if (rows.length === 0) {
      return;
    }

    await withTransaction(client, async (tx) => {
      for (const row of rows) {
        const key = readStringField(
          row,
          "key",
          "MySQL returned an invalid migration metadata sync result",
        );
        const doc = parseStoredDocument(row.doc_json, collection, key);

        await upsertMigrationMetadata(
          tx,
          refs,
          collection,
          key,
          deriveMetadataForCriteria(doc, criteria),
        );
      }
    });

    if (rows.length < OUTDATED_SCAN_CHUNK_SIZE) {
      return;
    }
  }
}

function deriveMetadataForCriteria(
  doc: Record<string, unknown>,
  criteria: MigrationCriteria,
): MigrationMetadata {
  const parseVersion = criteria.parseVersion ?? defaultParseVersion;
  const compareVersions = criteria.compareVersions ?? defaultCompareVersions;
  const parsedVersion = parseVersion(doc[criteria.versionField]);

  return {
    targetVersion: criteria.version,
    versionState: classifyVersionState(parsedVersion, criteria.version, compareVersions),
    indexSignature: computeIndexSignatureFromUnknown(doc[criteria.indexesField]),
  };
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

function parseRecord(value: unknown, message: string): Record<string, unknown> {
  if (!isRecord(value)) {
    throw new Error(message);
  }

  return value;
}

function readStringField(record: Record<string, unknown>, field: string, message: string): string {
  const value = record[field];

  if (typeof value !== "string") {
    throw new Error(message);
  }

  return value;
}

function readFiniteInteger(
  record: Record<string, unknown>,
  field: string,
  message: string,
): number {
  const raw = record[field];

  if (typeof raw === "number") {
    if (!Number.isFinite(raw)) {
      throw new Error(message);
    }

    return raw;
  }

  if (typeof raw === "bigint") {
    const asNumber = Number(raw);

    if (!Number.isFinite(asNumber)) {
      throw new Error(message);
    }

    return asNumber;
  }

  if (typeof raw === "string") {
    if (!/^-?\d+$/.test(raw)) {
      throw new Error(message);
    }

    const parsed = Number(raw);

    if (!Number.isFinite(parsed)) {
      throw new Error(message);
    }

    return parsed;
  }

  throw new Error(message);
}

function parseLockRow(row: Record<string, unknown>, message: string): LockRow {
  return {
    lockId: readStringField(row, "lock_id", message),
    acquiredAt: readFiniteInteger(row, "acquired_at", message),
  };
}

function parseWriteToken(raw: string): number {
  const value = Number(raw);

  if (!Number.isFinite(value) || Math.floor(value) !== value || value <= 0) {
    throw new Error("MySQL received an invalid write token");
  }

  return value;
}

function readAffectedRows(result: unknown, message: string): number {
  if (!isRecord(result)) {
    throw new Error(message);
  }

  const affectedRows = result.affectedRows;

  if (typeof affectedRows !== "number" || !Number.isFinite(affectedRows)) {
    throw new Error(message);
  }

  return affectedRows;
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

function createPlaceholders(count: number): string {
  return Array.from({ length: count }, () => "?").join(", ");
}

function normalizeIdentifier(value: string, field: string): string {
  if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(value)) {
    throw new Error(
      `MySQL ${field} must be a valid SQL identifier (letters, numbers, underscores)`,
    );
  }

  return value;
}

function quoteIdentifier(value: string): string {
  return `\`${value.replace(/`/g, "``")}\``;
}

function qualifyTableName(database: string | null, table: string): string {
  const tableName = quoteIdentifier(normalizeIdentifier(table, "table"));

  if (!database) {
    return tableName;
  }

  return `${quoteIdentifier(database)}.${tableName}`;
}

function buildTableRefs(options: MySqlEngineOptions): TableRefs {
  const database = options.database ? normalizeIdentifier(options.database, "database") : null;

  return {
    documentsTable: qualifyTableName(database, options.documentsTable ?? DEFAULT_DOCUMENTS_TABLE),
    indexesTable: qualifyTableName(database, options.indexesTable ?? DEFAULT_INDEXES_TABLE),
    migrationMetadataTable: qualifyTableName(
      database,
      options.migrationMetadataTable ?? DEFAULT_MIGRATION_METADATA_TABLE,
    ),
    migrationLocksTable: qualifyTableName(
      database,
      options.migrationLocksTable ?? DEFAULT_MIGRATION_LOCKS_TABLE,
    ),
    migrationCheckpointsTable: qualifyTableName(
      database,
      options.migrationCheckpointsTable ?? DEFAULT_MIGRATION_CHECKPOINTS_TABLE,
    ),
  };
}

function isMySqlDuplicateKeyError(error: unknown): boolean {
  if (!isRecord(error)) {
    return false;
  }

  return error.code === "ER_DUP_ENTRY" || error.errno === 1062;
}

function isMySqlDuplicateIndexError(error: unknown): boolean {
  if (!isRecord(error)) {
    return false;
  }

  return error.code === "ER_DUP_KEYNAME" || error.errno === 1061;
}

function isMySqlDuplicateColumnError(error: unknown): boolean {
  if (!isRecord(error)) {
    return false;
  }

  return error.code === "ER_DUP_FIELDNAME" || error.errno === 1060;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

function randomId(): string {
  if (typeof crypto !== "undefined" && typeof crypto.randomUUID === "function") {
    return crypto.randomUUID();
  }

  return `${Date.now().toString(16)}-${Math.random().toString(16).slice(2)}`;
}
