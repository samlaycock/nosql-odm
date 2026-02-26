import { DefaultMigrator } from "../migrator";
import {
  encodeQueryPageCursor,
  resolveQueryPageCursorPosition,
  type QueryCursorPosition,
} from "./query-cursor";
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

const DEFAULT_SCHEMA = "public";
const DEFAULT_DOCUMENTS_TABLE = "nosql_odm_documents";
const DEFAULT_INDEXES_TABLE = "nosql_odm_index_entries";
const DEFAULT_MIGRATION_METADATA_TABLE = "nosql_odm_migration_metadata";
const DEFAULT_MIGRATION_LOCKS_TABLE = "nosql_odm_migration_locks";
const DEFAULT_MIGRATION_CHECKPOINTS_TABLE = "nosql_odm_migration_checkpoints";
const OUTDATED_PAGE_LIMIT = 100;
const OUTDATED_SCAN_CHUNK_SIZE = 256;

interface PostgresQueryResultLike {
  rows?: unknown[];
  rowCount?: number | null;
}

interface PostgresQueryableLike {
  query(text: string, values?: unknown[]): Promise<PostgresQueryResultLike>;
}

interface PostgresSessionLike extends PostgresQueryableLike {
  release?(): void;
}

interface PostgresPoolLike extends PostgresQueryableLike {
  connect(): Promise<PostgresSessionLike>;
}

export interface PostgresEngineOptions {
  client: PostgresQueryableLike | PostgresPoolLike;
  schema?: string;
  documentsTable?: string;
  indexesTable?: string;
  migrationMetadataTable?: string;
  migrationLocksTable?: string;
  migrationCheckpointsTable?: string;
}

export interface PostgresQueryEngine extends QueryEngine<never> {}

interface QueryRow {
  key: string;
  doc: Record<string, unknown>;
  writeToken?: string;
}

interface LockRow {
  lockId: string;
  acquiredAt: number;
}

interface TableRefs {
  schema: string;
  documentsTable: string;
  indexesTable: string;
  migrationMetadataTable: string;
  migrationLocksTable: string;
  migrationCheckpointsTable: string;
}

export function postgresEngine(options: PostgresEngineOptions): PostgresQueryEngine {
  const client = options.client;
  const refs: TableRefs = {
    schema: quoteIdentifier(normalizeIdentifier(options.schema ?? DEFAULT_SCHEMA, "schema")),
    documentsTable: qualifyTableName(
      options.schema ?? DEFAULT_SCHEMA,
      options.documentsTable ?? DEFAULT_DOCUMENTS_TABLE,
      "documentsTable",
    ),
    indexesTable: qualifyTableName(
      options.schema ?? DEFAULT_SCHEMA,
      options.indexesTable ?? DEFAULT_INDEXES_TABLE,
      "indexesTable",
    ),
    migrationMetadataTable: qualifyTableName(
      options.schema ?? DEFAULT_SCHEMA,
      options.migrationMetadataTable ?? DEFAULT_MIGRATION_METADATA_TABLE,
      "migrationMetadataTable",
    ),
    migrationLocksTable: qualifyTableName(
      options.schema ?? DEFAULT_SCHEMA,
      options.migrationLocksTable ?? DEFAULT_MIGRATION_LOCKS_TABLE,
      "migrationLocksTable",
    ),
    migrationCheckpointsTable: qualifyTableName(
      options.schema ?? DEFAULT_SCHEMA,
      options.migrationCheckpointsTable ?? DEFAULT_MIGRATION_CHECKPOINTS_TABLE,
      "migrationCheckpointsTable",
    ),
  };

  const ready = ensureSchema(client, refs);

  const engine: PostgresQueryEngine = {
    capabilities: {
      uniqueConstraints: "atomic",
    },

    async get(collection, key) {
      await ready;

      const row = await fetchOptionalRow(client, {
        sql: `
          SELECT doc_key AS key, doc_json, write_version
          FROM ${refs.documentsTable}
          WHERE collection = $1 AND doc_key = $2
          LIMIT 1
        `,
        params: [collection, key],
        errorMessage: "Postgres returned an invalid document row",
      });

      if (!row) {
        return null;
      }

      return parseQueryRow(row, collection).doc;
    },

    async getWithMetadata(collection, key) {
      await ready;

      const row = await fetchOptionalRow(client, {
        sql: `
          SELECT doc_key AS key, doc_json, write_version
          FROM ${refs.documentsTable}
          WHERE collection = $1 AND doc_key = $2
          LIMIT 1
        `,
        params: [collection, key],
        errorMessage: "Postgres returned an invalid document row",
      });

      if (!row) {
        return null;
      }

      const parsed = parseQueryRow(row, collection);

      return {
        doc: structuredClone(parsed.doc),
        writeToken: parsed.writeToken,
      };
    },

    async create(collection, key, doc, indexes, _options, migrationMetadata) {
      await ready;

      const docJson = serializeDocument(doc);
      const normalizedIndexes = normalizeIndexes(indexes);
      const metadata = normalizeMigrationMetadata(migrationMetadata) ?? deriveLegacyMetadata(doc);

      try {
        await withTransaction(client, async (tx) => {
          await tx.query(
            `INSERT INTO ${refs.documentsTable} (collection, doc_key, doc_json) VALUES ($1, $2, $3::jsonb)`,
            [collection, key, docJson],
          );

          await replaceIndexes(tx, refs, collection, key, normalizedIndexes, false);
          await upsertMigrationMetadata(tx, refs, collection, key, metadata);
        });
      } catch (error) {
        if (isPostgresUniqueViolation(error)) {
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
        await tx.query(
          `
            INSERT INTO ${refs.documentsTable} (collection, doc_key, doc_json)
            VALUES ($1, $2, $3::jsonb)
            ON CONFLICT (collection, doc_key)
            DO UPDATE SET
              doc_json = EXCLUDED.doc_json,
              write_version = ${refs.documentsTable}.write_version + 1
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
        const updateResult = await tx.query(
          `
            UPDATE ${refs.documentsTable}
            SET
              doc_json = $1::jsonb,
              write_version = write_version + 1
            WHERE collection = $2 AND doc_key = $3
          `,
          [docJson, collection, key],
        );

        if (readRowCount(updateResult, "Postgres returned an invalid update result") === 0) {
          throw new EngineDocumentNotFoundError(collection, key);
        }

        await replaceIndexes(tx, refs, collection, key, normalizedIndexes, true);
        await upsertMigrationMetadata(tx, refs, collection, key, metadata);
      });
    },

    async delete(collection, key) {
      await ready;

      await client.query(
        `DELETE FROM ${refs.documentsTable} WHERE collection = $1 AND doc_key = $2`,
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

    async queryWithMetadata(collection, params) {
      await ready;

      if (!params.index || !params.filter) {
        return queryByCollectionScan(client, refs, collection, params, true);
      }

      return queryByIndex(client, refs, collection, params, true);
    },

    async batchGet(collection, keys) {
      await ready;

      const uniqueKeys = uniqueStrings(keys);

      if (uniqueKeys.length === 0) {
        return [];
      }

      const rows = await fetchRows(client, {
        sql: `
          SELECT doc_key AS key, doc_json, write_version
          FROM ${refs.documentsTable}
          WHERE collection = $1
            AND doc_key = ANY($2::text[])
        `,
        params: [collection, uniqueKeys],
        errorMessage: "Postgres returned an invalid batch get result",
      });

      const fetched = new Map<string, QueryRow>();

      for (const row of rows) {
        const parsed = parseQueryRow(row, collection);
        fetched.set(parsed.key, parsed);
      }

      const results: KeyedDocument[] = [];

      for (const key of keys) {
        const parsed = fetched.get(key);

        if (!parsed) {
          continue;
        }

        results.push({
          key,
          doc: structuredClone(parsed.doc),
        });
      }

      return results;
    },

    async batchGetWithMetadata(collection, keys) {
      await ready;

      const uniqueKeys = uniqueStrings(keys);

      if (uniqueKeys.length === 0) {
        return [];
      }

      const rows = await fetchRows(client, {
        sql: `
          SELECT doc_key AS key, doc_json, write_version
          FROM ${refs.documentsTable}
          WHERE collection = $1
            AND doc_key = ANY($2::text[])
        `,
        params: [collection, uniqueKeys],
        errorMessage: "Postgres returned an invalid batch get result",
      });

      const fetched = new Map<string, QueryRow>();

      for (const row of rows) {
        const parsed = parseQueryRow(row, collection);
        fetched.set(parsed.key, parsed);
      }

      const results: KeyedDocument[] = [];

      for (const key of keys) {
        const parsed = fetched.get(key);

        if (!parsed) {
          continue;
        }

        results.push({
          key,
          doc: structuredClone(parsed.doc),
          writeToken: parsed.writeToken,
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

      await client.query(
        `DELETE FROM ${refs.documentsTable} WHERE collection = $1 AND doc_key = ANY($2::text[])`,
        [collection, uniqueKeys],
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
              WHERE collection = $1
              FOR UPDATE
            `,
            params: [collection],
            errorMessage: "Postgres returned an invalid migration lock row",
          });

          if (existing) {
            const parsedExisting = parseLockRow(
              existing,
              "Postgres returned an invalid migration lock row",
            );
            const ttl = normalizeTtl(options?.ttl);

            if (ttl === null || now - parsedExisting.acquiredAt < ttl) {
              return null;
            }

            await tx.query(
              `
                UPDATE ${refs.migrationLocksTable}
                SET lock_id = $1, acquired_at = $2
                WHERE collection = $3
              `,
              [lockId, now, collection],
            );
          } else {
            await tx.query(
              `
                INSERT INTO ${refs.migrationLocksTable} (collection, lock_id, acquired_at)
                VALUES ($1, $2, $3)
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

        await client.query(
          `DELETE FROM ${refs.migrationLocksTable} WHERE collection = $1 AND lock_id = $2`,
          [lock.collection, lock.id],
        );
      },

      async getOutdated(collection, criteria, cursor) {
        await ready;

        return getOutdatedDocuments(client, refs, collection, criteria, cursor);
      },

      async saveCheckpoint(lock, cursor) {
        await ready;

        await client.query(
          `
            INSERT INTO ${refs.migrationCheckpointsTable} (collection, cursor)
            SELECT $1, $2
            WHERE EXISTS (
              SELECT 1
              FROM ${refs.migrationLocksTable}
              WHERE collection = $1 AND lock_id = $3
            )
            ON CONFLICT (collection)
            DO UPDATE SET cursor = EXCLUDED.cursor
          `,
          [lock.collection, cursor, lock.id],
        );
      },

      async loadCheckpoint(collection) {
        await ready;

        const row = await fetchOptionalRow(client, {
          sql: `
            SELECT cursor
            FROM ${refs.migrationCheckpointsTable}
            WHERE collection = $1
            LIMIT 1
          `,
          params: [collection],
          errorMessage: "Postgres returned an invalid migration checkpoint row",
        });

        if (!row) {
          return null;
        }

        return readStringField(
          row,
          "cursor",
          "Postgres returned an invalid migration checkpoint row",
        );
      },

      async clearCheckpoint(collection) {
        await ready;

        await client.query(`DELETE FROM ${refs.migrationCheckpointsTable} WHERE collection = $1`, [
          collection,
        ]);
      },

      async getStatus(collection) {
        await ready;

        const row = await fetchOptionalRow(client, {
          sql: `
            SELECT l.lock_id, l.acquired_at, c.cursor
            FROM ${refs.migrationLocksTable} l
            LEFT JOIN ${refs.migrationCheckpointsTable} c
              ON c.collection = l.collection
            WHERE l.collection = $1
            LIMIT 1
          `,
          params: [collection],
          errorMessage: "Postgres returned an invalid migration status row",
        });

        if (!row) {
          return null;
        }

        const lock = parseLockRow(row, "Postgres returned an invalid migration status row");
        const cursorValue = row.cursor;

        if (cursorValue !== null && typeof cursorValue !== "string") {
          throw new Error("Postgres returned an invalid migration status row");
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
  client: PostgresQueryableLike,
  refs: TableRefs,
  collection: string,
  params: QueryParams,
  includeWriteTokens = false,
): Promise<EngineQueryResult> {
  const limit = normalizeLimit(params.limit);

  if (limit === 0) {
    return {
      documents: [],
      cursor: null,
    };
  }

  const cursorPosition = resolveQueryPageCursorPosition(collection, params);
  const cursorId =
    cursorPosition === null
      ? 0
      : cursorPosition.kind === "key"
        ? await resolveCursorId(client, refs, collection, cursorPosition.key)
        : readSqlCursorRowId(cursorPosition);
  const values: (string | number)[] = [collection, cursorId];

  let sql = `
    SELECT id AS cursor_id, doc_key AS key, doc_json, write_version
    FROM ${refs.documentsTable}
    WHERE collection = $1 AND id > $2
    ORDER BY id ASC
  `;

  if (limit !== null) {
    values.push(limit + 1);
    sql += ` LIMIT $3`;
  }

  const rows = await fetchRows(client, {
    sql,
    params: values,
    errorMessage: "Postgres returned an invalid query result",
  });

  return formatPage(rows, params, collection, includeWriteTokens);
}

async function queryByIndex(
  client: PostgresQueryableLike,
  refs: TableRefs,
  collection: string,
  params: QueryParams,
  includeWriteTokens = false,
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

  const cursorPosition = resolveQueryPageCursorPosition(collection, params);

  if (cursorPosition) {
    if (sort === "asc" || sort === "desc") {
      if (cursorPosition.kind !== "sorted-index") {
        throw new Error("Query cursor is missing sorted index metadata");
      }

      if (cursorPosition.createdAt === undefined) {
        throw new Error("Query cursor is missing SQL row metadata");
      }

      const valueArg = builder.push(cursorPosition.indexValue);
      const valueTieArg = builder.push(cursorPosition.indexValue);
      const idArg = builder.push(cursorPosition.createdAt);

      if (sort === "asc") {
        whereClauses.push(
          `(ix.index_value > ${valueArg} OR (ix.index_value = ${valueTieArg} AND d.id > ${idArg}))`,
        );
      } else {
        whereClauses.push(
          `(ix.index_value < ${valueArg} OR (ix.index_value = ${valueTieArg} AND d.id > ${idArg}))`,
        );
      }
    } else {
      const cursorId =
        cursorPosition.kind === "key"
          ? await resolveCursorId(client, refs, collection, cursorPosition.key)
          : readSqlCursorRowId(cursorPosition);
      const idArg = builder.push(cursorId);

      whereClauses.push(`d.id > ${idArg}`);
    }
  }

  const orderBy =
    sort === "asc"
      ? `ORDER BY ix.index_value ASC, d.id ASC`
      : sort === "desc"
        ? // Keep id ascending in both directions for deterministic tie-breaking
          // when multiple rows share the same index value.
          `ORDER BY ix.index_value DESC, d.id ASC`
        : `ORDER BY d.id ASC`;

  let sql = `
    SELECT d.id AS cursor_id, ix.index_value AS cursor_index_value, d.doc_key AS key, d.doc_json, d.write_version
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
    errorMessage: "Postgres returned an invalid query result",
  });

  return formatPage(rows, params, collection, includeWriteTokens);
}

function formatPage(
  rows: Record<string, unknown>[],
  params: QueryParams,
  collection: string,
  includeWriteTokens = false,
): EngineQueryResult {
  const limit = normalizeLimit(params.limit);
  const hasLimit = limit !== null;
  const hasMore = hasLimit && rows.length > limit;
  const pageRows = hasMore ? rows.slice(0, limit) : rows;
  const cursor = hasMore
    ? encodeQueryPageCursor(collection, params, {
        key: readStringField(
          pageRows[pageRows.length - 1]!,
          "key",
          "Postgres returned an invalid query result",
        ),
        createdAt: readFiniteInteger(
          pageRows[pageRows.length - 1]!,
          "cursor_id",
          "Postgres returned an invalid query result",
        ),
        indexValue: params.index
          ? readStringField(
              pageRows[pageRows.length - 1]!,
              "cursor_index_value",
              "Postgres returned an invalid query result",
            )
          : undefined,
      })
    : null;

  return {
    documents: pageRows.map((row) => {
      const parsed = parseQueryRow(row, collection);
      const document = {
        key: parsed.key,
        doc: structuredClone(parsed.doc),
      };

      if (!includeWriteTokens || parsed.writeToken === undefined) {
        return document;
      }

      return {
        ...document,
        writeToken: parsed.writeToken,
      };
    }),
    cursor,
  };
}

async function replaceIndexes(
  client: PostgresQueryableLike,
  refs: TableRefs,
  collection: string,
  key: string,
  indexes: ResolvedIndexKeys,
  clearExisting: boolean,
): Promise<void> {
  if (clearExisting) {
    await client.query(`DELETE FROM ${refs.indexesTable} WHERE collection = $1 AND doc_key = $2`, [
      collection,
      key,
    ]);
  }

  for (const [indexName, indexValue] of Object.entries(indexes)) {
    await client.query(
      `
        INSERT INTO ${refs.indexesTable} (collection, doc_key, index_name, index_value)
        VALUES ($1, $2, $3, $4)
      `,
      [collection, key, indexName, indexValue],
    );
  }
}

function readSqlCursorRowId(cursorPosition: Exclude<QueryCursorPosition, { kind: "key" }>): number {
  if (cursorPosition.createdAt === undefined) {
    throw new Error("Query cursor is missing SQL row metadata");
  }

  return cursorPosition.createdAt;
}

interface MigrationMetadata {
  targetVersion: number;
  versionState: MigrationVersionState;
  indexSignature: string | null;
}

async function upsertMigrationMetadata(
  client: PostgresQueryableLike,
  refs: TableRefs,
  collection: string,
  key: string,
  metadata: MigrationMetadata,
): Promise<void> {
  await client.query(
    `
      INSERT INTO ${refs.migrationMetadataTable} (
        collection,
        doc_key,
        target_version,
        version_state,
        index_signature
      )
      VALUES ($1, $2, $3, $4, $5)
      ON CONFLICT (collection, doc_key)
      DO UPDATE SET
        target_version = EXCLUDED.target_version,
        version_state = EXCLUDED.version_state,
        index_signature = EXCLUDED.index_signature
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
    throw new Error("Postgres received invalid migration metadata target version");
  }

  if (!isMigrationVersionState(raw.versionState)) {
    throw new Error("Postgres received invalid migration metadata state");
  }

  if (raw.indexSignature !== null && typeof raw.indexSignature !== "string") {
    throw new Error("Postgres received invalid migration metadata index signature");
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
  tx: PostgresSessionLike,
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
      const updateResult = await tx.query(
        `
          UPDATE ${refs.documentsTable}
          SET
            doc_json = $1::jsonb,
            write_version = write_version + 1
          WHERE collection = $2
            AND doc_key = $3
            AND write_version = $4
        `,
        [docJson, collection, item.key, expectedWriteVersion],
      );

      if (
        readRowCount(updateResult, "Postgres returned an invalid conditional batch-set result") ===
        0
      ) {
        conflictedKeys.push(item.key);
        continue;
      }

      await replaceIndexes(tx, refs, collection, item.key, normalizedIndexes, true);
      await upsertMigrationMetadata(tx, refs, collection, item.key, metadata);
      persistedKeys.push(item.key);
      continue;
    }

    await tx.query(
      `
        INSERT INTO ${refs.documentsTable} (collection, doc_key, doc_json)
        VALUES ($1, $2, $3::jsonb)
        ON CONFLICT (collection, doc_key)
        DO UPDATE SET
          doc_json = EXCLUDED.doc_json,
          write_version = ${refs.documentsTable}.write_version + 1
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

async function ensureSchema(client: PostgresQueryableLike, refs: TableRefs): Promise<void> {
  const session = await acquireSession(client);
  const lockKey = toAdvisoryLockKey(
    `${refs.schema}|${refs.documentsTable}|${refs.indexesTable}|${refs.migrationMetadataTable}|${refs.migrationLocksTable}|${refs.migrationCheckpointsTable}`,
  );

  // A process-scoped advisory lock prevents concurrent instances from racing
  // DDL statements during lazy schema bootstrap.
  await session.query(`SELECT pg_advisory_lock($1)`, [lockKey]);

  try {
    const documentsCollectionIdIndex = quoteIdentifier(
      createIndexName(refs.documentsTable, "collection_id_idx"),
    );
    const indexesLookupIndex = quoteIdentifier(createIndexName(refs.indexesTable, "lookup_idx"));
    const indexesScanIndex = quoteIdentifier(createIndexName(refs.indexesTable, "scan_idx"));
    const migrationMetadataTargetStateDocIndex = quoteIdentifier(
      createIndexName(refs.migrationMetadataTable, "target_state_doc_idx"),
    );
    const migrationMetadataTargetStateSignatureIndex = quoteIdentifier(
      createIndexName(refs.migrationMetadataTable, "target_state_signature_idx"),
    );

    await session.query(`CREATE SCHEMA IF NOT EXISTS ${refs.schema}`);

    await session.query(`
    CREATE TABLE IF NOT EXISTS ${refs.documentsTable} (
      id BIGSERIAL NOT NULL,
      collection TEXT NOT NULL,
      doc_key TEXT NOT NULL,
      doc_json JSONB NOT NULL,
      write_version BIGINT NOT NULL DEFAULT 1,
      PRIMARY KEY (collection, doc_key)
    )
  `);

    await session.query(
      `ALTER TABLE ${refs.documentsTable} ADD COLUMN IF NOT EXISTS write_version BIGINT NOT NULL DEFAULT 1`,
    );

    await session.query(
      `CREATE INDEX IF NOT EXISTS ${documentsCollectionIdIndex} ON ${refs.documentsTable} (collection, id)`,
    );

    await session.query(`
    CREATE TABLE IF NOT EXISTS ${refs.indexesTable} (
      collection TEXT NOT NULL,
      doc_key TEXT NOT NULL,
      index_name TEXT NOT NULL,
      index_value TEXT NOT NULL,
      PRIMARY KEY (collection, doc_key, index_name),
      FOREIGN KEY (collection, doc_key)
        REFERENCES ${refs.documentsTable} (collection, doc_key)
        ON DELETE CASCADE
    )
  `);

    await session.query(
      `CREATE INDEX IF NOT EXISTS ${indexesLookupIndex} ON ${refs.indexesTable} (collection, index_name, index_value, doc_key)`,
    );
    await session.query(
      `CREATE INDEX IF NOT EXISTS ${indexesScanIndex} ON ${refs.indexesTable} (collection, index_name, doc_key)`,
    );

    await session.query(`
    CREATE TABLE IF NOT EXISTS ${refs.migrationMetadataTable} (
      collection TEXT NOT NULL,
      doc_key TEXT NOT NULL,
      target_version INTEGER NULL,
      version_state TEXT NULL,
      index_signature TEXT NULL,
      PRIMARY KEY (collection, doc_key),
      FOREIGN KEY (collection, doc_key)
        REFERENCES ${refs.documentsTable} (collection, doc_key)
        ON DELETE CASCADE
    )
  `);

    await session.query(
      `ALTER TABLE ${refs.migrationMetadataTable} ADD COLUMN IF NOT EXISTS target_version INTEGER NULL`,
    );
    await session.query(
      `ALTER TABLE ${refs.migrationMetadataTable} ADD COLUMN IF NOT EXISTS version_state TEXT NULL`,
    );
    await session.query(
      `ALTER TABLE ${refs.migrationMetadataTable} ADD COLUMN IF NOT EXISTS index_signature TEXT NULL`,
    );

    await session.query(
      `CREATE INDEX IF NOT EXISTS ${migrationMetadataTargetStateDocIndex} ON ${refs.migrationMetadataTable} (collection, target_version, version_state, doc_key)`,
    );
    await session.query(
      `CREATE INDEX IF NOT EXISTS ${migrationMetadataTargetStateSignatureIndex} ON ${refs.migrationMetadataTable} (collection, target_version, version_state, index_signature, doc_key)`,
    );

    await session.query(`
    CREATE TABLE IF NOT EXISTS ${refs.migrationLocksTable} (
      collection TEXT PRIMARY KEY,
      lock_id TEXT NOT NULL,
      acquired_at BIGINT NOT NULL
    )
  `);

    await session.query(`
    CREATE TABLE IF NOT EXISTS ${refs.migrationCheckpointsTable} (
      collection TEXT PRIMARY KEY,
      cursor TEXT NOT NULL
    )
  `);
  } finally {
    try {
      await session.query(`SELECT pg_advisory_unlock($1)`, [lockKey]);
    } finally {
      session.release?.();
    }
  }
}

function parseWriteToken(raw: string): number {
  const value = Number(raw);

  if (!Number.isFinite(value) || Math.floor(value) !== value || value <= 0) {
    throw new Error("Postgres received an invalid write token");
  }

  return value;
}

async function withTransaction<T>(
  client: PostgresQueryableLike | PostgresPoolLike,
  work: (tx: PostgresSessionLike) => Promise<T>,
): Promise<T> {
  const tx = await acquireSession(client);

  try {
    await tx.query("BEGIN");

    const result = await work(tx);

    await tx.query("COMMIT");

    return result;
  } catch (error) {
    try {
      await tx.query("ROLLBACK");
    } catch {
      // Ignore rollback errors and surface original failure.
    }

    throw error;
  } finally {
    tx.release?.();
  }
}

async function acquireSession(
  client: PostgresQueryableLike | PostgresPoolLike,
): Promise<PostgresSessionLike> {
  if ("connect" in client && typeof client.connect === "function") {
    return client.connect();
  }

  return client;
}

interface SqlBuilder {
  values: (string | number)[];
  push(value: string | number): string;
}

function toAdvisoryLockKey(value: string): number {
  // FNV-1a 32-bit hash for a stable advisory-lock key.
  let hash = 2166136261;

  for (let i = 0; i < value.length; i++) {
    hash ^= value.charCodeAt(i);
    hash = Math.imul(hash, 16777619);
  }

  return hash | 0;
}

function createSqlBuilder(): SqlBuilder {
  const values: (string | number)[] = [];

  return {
    values,
    push(value) {
      values.push(value);
      return `$${String(values.length)}`;
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
      `ix.index_value LIKE ${builder.push(`${escapeLikePattern(filter.$begins)}%`)} ESCAPE '\\'`,
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
  client: PostgresQueryableLike,
  options: {
    sql: string;
    params: unknown[];
    errorMessage: string;
  },
): Promise<Record<string, unknown>[]> {
  const result = await client.query(options.sql, options.params);

  if (!Array.isArray(result.rows)) {
    throw new Error(options.errorMessage);
  }

  return result.rows.map((row) => parseRecord(row, options.errorMessage));
}

async function fetchOptionalRow(
  client: PostgresQueryableLike,
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
  const key = readStringField(raw, "key", "Postgres returned an invalid query result");
  const writeToken =
    raw.write_version === undefined || raw.write_version === null
      ? undefined
      : String(
          readFiniteInteger(raw, "write_version", "Postgres returned an invalid query result"),
        );

  return {
    key,
    doc: parseStoredDocument(raw.doc_json, collection, key),
    writeToken,
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
        throw new Error("not an object");
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
    throw new Error("Postgres received a non-object document");
  }

  const encoded = JSON.stringify(cloned);

  if (encoded === undefined) {
    throw new Error("Postgres received a non-serializable document");
  }

  return encoded;
}

function normalizeIndexes(indexes: ResolvedIndexKeys): ResolvedIndexKeys {
  const normalized: ResolvedIndexKeys = {};

  for (const [name, value] of Object.entries(indexes)) {
    if (typeof value !== "string") {
      throw new Error("Postgres received invalid index values");
    }

    normalized[name] = value;
  }

  return normalized;
}

async function resolveCursorId(
  client: PostgresQueryableLike,
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
      WHERE collection = $1 AND doc_key = $2
      LIMIT 1
    `,
    params: [collection, cursor],
    errorMessage: "Postgres returned an invalid cursor row",
  });

  if (!row) {
    return 0;
  }

  return readFiniteInteger(row, "id", "Postgres returned an invalid cursor row");
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
  client: PostgresQueryableLike,
  refs: TableRefs,
  collection: string,
  criteria: MigrationCriteria,
  cursor?: string,
): Promise<EngineQueryResult> {
  if (criteria.skipMetadataSyncHint !== true) {
    // Refresh metadata only when starting a scan; continuation pages reuse
    // the same targetVersion/signature window for consistency and cost control.
    await syncMissingMigrationMetadata(client, refs, collection, criteria);
  }
  return getOutdatedDocumentsByMetadata(client, refs, collection, criteria, cursor);
}

async function getOutdatedDocumentsByMetadata(
  client: PostgresQueryableLike,
  refs: TableRefs,
  collection: string,
  criteria: MigrationCriteria,
  cursor?: string,
): Promise<EngineQueryResult> {
  const pageLimit = normalizeOutdatedPageLimit(criteria.pageSizeHint);
  const startId = await resolveCursorId(client, refs, collection, cursor);
  const expectedSignature = computeIndexSignature(criteria.indexes);
  const rows = await fetchRows(client, {
    sql: `
      SELECT d.id, d.doc_key AS key, d.doc_json, d.write_version
      FROM ${refs.migrationMetadataTable} m
      INNER JOIN ${refs.documentsTable} d
        ON d.collection = m.collection
       AND d.doc_key = m.doc_key
      WHERE m.collection = $1
        AND d.id > $2
        AND (
          m.version_state = 'stale'
          OR (
            m.version_state = 'current'
            AND m.index_signature IS DISTINCT FROM $3
          )
        )
        AND m.target_version = $4
      ORDER BY d.id ASC
      LIMIT $5
    `,
    params: [collection, startId, expectedSignature, criteria.version, pageLimit + 1],
    errorMessage: "Postgres returned an invalid outdated query result",
  });
  const hasMore = rows.length > pageLimit;
  const pageRows = hasMore ? rows.slice(0, pageLimit) : rows;
  const documents: KeyedDocument[] = [];

  for (const row of pageRows) {
    const key = readStringField(row, "key", "Postgres returned an invalid outdated query result");
    const writeVersion = readFiniteInteger(
      row,
      "write_version",
      "Postgres returned an invalid outdated query result",
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
  client: PostgresQueryableLike,
  refs: TableRefs,
  collection: string,
  criteria: MigrationCriteria,
): Promise<void> {
  while (true) {
    // Sync in chunks so large collections do not hold long transactions.
    const rows = await fetchRows(client, {
      sql: `
        SELECT d.doc_key AS key, d.doc_json
        FROM ${refs.documentsTable} d
        LEFT JOIN ${refs.migrationMetadataTable} m
          ON m.collection = d.collection
         AND m.doc_key = d.doc_key
        WHERE d.collection = $1
          AND (
            m.doc_key IS NULL
            OR m.target_version IS NULL
            OR m.target_version <> $2
          )
        ORDER BY d.id ASC
        LIMIT $3
      `,
      params: [collection, criteria.version, OUTDATED_SCAN_CHUNK_SIZE],
      errorMessage: "Postgres returned an invalid migration metadata sync result",
    });

    if (rows.length === 0) {
      return;
    }

    await withTransaction(client, async (tx) => {
      for (const row of rows) {
        const key = readStringField(
          row,
          "key",
          "Postgres returned an invalid migration metadata sync result",
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

function readRowCount(result: PostgresQueryResultLike, message: string): number {
  const rowCount = result.rowCount;

  if (typeof rowCount !== "number" || !Number.isFinite(rowCount)) {
    throw new Error(message);
  }

  return rowCount;
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

function normalizeIdentifier(value: string, field: string): string {
  if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(value)) {
    throw new Error(
      `Postgres ${field} must be a valid SQL identifier (letters, numbers, underscores)`,
    );
  }

  return value;
}

function quoteIdentifier(value: string): string {
  return `"${value.replace(/"/g, '""')}"`;
}

function qualifyTableName(schema: string, table: string, field: string): string {
  const normalizedSchema = normalizeIdentifier(schema, "schema");
  const normalizedTable = normalizeIdentifier(table, field);

  return `${quoteIdentifier(normalizedSchema)}.${quoteIdentifier(normalizedTable)}`;
}

function createIndexName(qualifiedTable: string, suffix: string): string {
  const base = `${qualifiedTable.replace(/"/g, "").replace(/\./g, "_")}_${suffix}`;

  if (base.length <= 63) {
    return base;
  }

  const hash = simpleHash(base).slice(0, 8);
  const prefixLength = 63 - hash.length - 1;

  return `${base.slice(0, prefixLength)}_${hash}`;
}

function simpleHash(value: string): string {
  let hash = 2166136261;

  for (let i = 0; i < value.length; i++) {
    hash ^= value.charCodeAt(i);
    hash = Math.imul(hash, 16777619);
  }

  return (hash >>> 0).toString(16);
}

function isPostgresUniqueViolation(error: unknown): boolean {
  return isRecord(error) && error.code === "23505";
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
