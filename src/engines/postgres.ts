import {
  EngineDocumentAlreadyExistsError,
  EngineDocumentNotFoundError,
  type ComparableVersion,
  type EngineQueryResult,
  type FieldCondition,
  type KeyedDocument,
  type MigrationCriteria,
  type QueryEngine,
  type QueryParams,
  type ResolvedIndexKeys,
} from "./types";

const DEFAULT_SCHEMA = "public";
const DEFAULT_DOCUMENTS_TABLE = "nosql_odm_documents";
const DEFAULT_INDEXES_TABLE = "nosql_odm_index_entries";
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
  migrationLocksTable?: string;
  migrationCheckpointsTable?: string;
}

export interface PostgresQueryEngine extends QueryEngine<never> {}

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
  schema: string;
  documentsTable: string;
  indexesTable: string;
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
    async get(collection, key) {
      await ready;

      const row = await fetchOptionalRow(client, {
        sql: `
          SELECT doc_key AS key, doc_json
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

    async create(collection, key, doc, indexes) {
      await ready;

      const docJson = serializeDocument(doc);
      const normalizedIndexes = normalizeIndexes(indexes);

      try {
        await withTransaction(client, async (tx) => {
          await tx.query(
            `INSERT INTO ${refs.documentsTable} (collection, doc_key, doc_json) VALUES ($1, $2, $3::jsonb)`,
            [collection, key, docJson],
          );

          await replaceIndexes(tx, refs, collection, key, normalizedIndexes, false);
        });
      } catch (error) {
        if (isPostgresUniqueViolation(error)) {
          throw new EngineDocumentAlreadyExistsError(collection, key);
        }

        throw error;
      }
    },

    async put(collection, key, doc, indexes) {
      await ready;

      const docJson = serializeDocument(doc);
      const normalizedIndexes = normalizeIndexes(indexes);

      await withTransaction(client, async (tx) => {
        await tx.query(
          `
            INSERT INTO ${refs.documentsTable} (collection, doc_key, doc_json)
            VALUES ($1, $2, $3::jsonb)
            ON CONFLICT (collection, doc_key)
            DO UPDATE SET doc_json = EXCLUDED.doc_json
          `,
          [collection, key, docJson],
        );

        await replaceIndexes(tx, refs, collection, key, normalizedIndexes, true);
      });
    },

    async update(collection, key, doc, indexes) {
      await ready;

      const docJson = serializeDocument(doc);
      const normalizedIndexes = normalizeIndexes(indexes);

      await withTransaction(client, async (tx) => {
        const updateResult = await tx.query(
          `
            UPDATE ${refs.documentsTable}
            SET doc_json = $1::jsonb
            WHERE collection = $2 AND doc_key = $3
          `,
          [docJson, collection, key],
        );

        if (readRowCount(updateResult, "Postgres returned an invalid update result") === 0) {
          throw new EngineDocumentNotFoundError(collection, key);
        }

        await replaceIndexes(tx, refs, collection, key, normalizedIndexes, true);
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

    async batchGet(collection, keys) {
      await ready;

      const uniqueKeys = uniqueStrings(keys);

      if (uniqueKeys.length === 0) {
        return [];
      }

      const rows = await fetchRows(client, {
        sql: `
          SELECT doc_key AS key, doc_json
          FROM ${refs.documentsTable}
          WHERE collection = $1
            AND doc_key = ANY($2::text[])
        `,
        params: [collection, uniqueKeys],
        errorMessage: "Postgres returned an invalid batch get result",
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
        for (const item of items) {
          const docJson = serializeDocument(item.doc);
          const normalizedIndexes = normalizeIndexes(item.indexes);

          await tx.query(
            `
              INSERT INTO ${refs.documentsTable} (collection, doc_key, doc_json)
              VALUES ($1, $2, $3::jsonb)
              ON CONFLICT (collection, doc_key)
              DO UPDATE SET doc_json = EXCLUDED.doc_json
            `,
            [collection, item.key, docJson],
          );

          await replaceIndexes(tx, refs, collection, item.key, normalizedIndexes, true);
        }
      });
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

  return engine;
}

async function queryByCollectionScan(
  client: PostgresQueryableLike,
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
    SELECT doc_key AS key, doc_json
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

  return formatPage(rows, limit, collection);
}

async function queryByIndex(
  client: PostgresQueryableLike,
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
    SELECT d.doc_key AS key, d.doc_json
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

  return formatPage(rows, limit, collection);
}

async function resolveIndexedCursor(
  client: PostgresQueryableLike,
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
    errorMessage: "Postgres returned an invalid indexed cursor row",
  });

  if (!row) {
    return null;
  }

  return {
    id: readFiniteInteger(row, "id", "Postgres returned an invalid indexed cursor row"),
    indexValue: readStringField(
      row,
      "index_value",
      "Postgres returned an invalid indexed cursor row",
    ),
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
          "Postgres returned an invalid query result",
        )
      : null,
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

async function ensureSchema(client: PostgresQueryableLike, refs: TableRefs): Promise<void> {
  const documentsCollectionIdIndex = quoteIdentifier(
    createIndexName(refs.documentsTable, "collection_id_idx"),
  );
  const indexesLookupIndex = quoteIdentifier(createIndexName(refs.indexesTable, "lookup_idx"));
  const indexesScanIndex = quoteIdentifier(createIndexName(refs.indexesTable, "scan_idx"));

  await client.query(`CREATE SCHEMA IF NOT EXISTS ${refs.schema}`);

  await client.query(`
    CREATE TABLE IF NOT EXISTS ${refs.documentsTable} (
      id BIGSERIAL NOT NULL,
      collection TEXT NOT NULL,
      doc_key TEXT NOT NULL,
      doc_json JSONB NOT NULL,
      PRIMARY KEY (collection, doc_key)
    )
  `);

  await client.query(
    `CREATE INDEX IF NOT EXISTS ${documentsCollectionIdIndex} ON ${refs.documentsTable} (collection, id)`,
  );

  await client.query(`
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

  await client.query(
    `CREATE INDEX IF NOT EXISTS ${indexesLookupIndex} ON ${refs.indexesTable} (collection, index_name, index_value, doc_key)`,
  );
  await client.query(
    `CREATE INDEX IF NOT EXISTS ${indexesScanIndex} ON ${refs.indexesTable} (collection, index_name, doc_key)`,
  );

  await client.query(`
    CREATE TABLE IF NOT EXISTS ${refs.migrationLocksTable} (
      collection TEXT PRIMARY KEY,
      lock_id TEXT NOT NULL,
      acquired_at BIGINT NOT NULL
    )
  `);

  await client.query(`
    CREATE TABLE IF NOT EXISTS ${refs.migrationCheckpointsTable} (
      collection TEXT PRIMARY KEY,
      cursor TEXT NOT NULL
    )
  `);
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
  const parseVersion = criteria.parseVersion ?? defaultParseVersion;
  const compareVersions = criteria.compareVersions ?? defaultCompareVersions;
  const startId = await resolveCursorId(client, refs, collection, cursor);

  const outdated: KeyedDocument[] = [];
  let scanCursorId = startId;

  while (outdated.length < OUTDATED_PAGE_LIMIT) {
    const rows = await fetchRows(client, {
      sql: `
        SELECT id, doc_key AS key, doc_json
        FROM ${refs.documentsTable}
        WHERE collection = $1 AND id > $2
        ORDER BY id ASC
        LIMIT $3
      `,
      params: [collection, scanCursorId, OUTDATED_SCAN_CHUNK_SIZE],
      errorMessage: "Postgres returned an invalid outdated query result",
    });

    if (rows.length === 0) {
      break;
    }

    for (const row of rows) {
      const id = readFiniteInteger(row, "id", "Postgres returned an invalid outdated query result");
      const key = readStringField(row, "key", "Postgres returned an invalid outdated query result");
      const doc = parseStoredDocument(row.doc_json, collection, key);

      scanCursorId = id;

      if (isOutdated(doc, criteria, parseVersion, compareVersions)) {
        outdated.push({ key, doc: structuredClone(doc) });
      }

      if (outdated.length >= OUTDATED_PAGE_LIMIT) {
        break;
      }
    }

    if (rows.length < OUTDATED_SCAN_CHUNK_SIZE) {
      break;
    }
  }

  if (outdated.length < OUTDATED_PAGE_LIMIT) {
    return {
      documents: outdated,
      cursor: null,
    };
  }

  const hasMore = await hasAdditionalOutdatedAfter(
    client,
    refs,
    collection,
    scanCursorId,
    criteria,
    parseVersion,
    compareVersions,
  );

  return {
    documents: outdated,
    cursor: hasMore ? (outdated[outdated.length - 1]?.key ?? null) : null,
  };
}

async function hasAdditionalOutdatedAfter(
  client: PostgresQueryableLike,
  refs: TableRefs,
  collection: string,
  startId: number,
  criteria: MigrationCriteria,
  parseVersion: (raw: unknown) => ComparableVersion | null,
  compareVersions: (a: ComparableVersion, b: ComparableVersion) => number,
): Promise<boolean> {
  let cursorId = startId;

  while (true) {
    const rows = await fetchRows(client, {
      sql: `
        SELECT id, doc_key AS key, doc_json
        FROM ${refs.documentsTable}
        WHERE collection = $1 AND id > $2
        ORDER BY id ASC
        LIMIT $3
      `,
      params: [collection, cursorId, OUTDATED_SCAN_CHUNK_SIZE],
      errorMessage: "Postgres returned an invalid outdated query result",
    });

    if (rows.length === 0) {
      return false;
    }

    for (const row of rows) {
      cursorId = readFiniteInteger(row, "id", "Postgres returned an invalid outdated query result");
      const key = readStringField(row, "key", "Postgres returned an invalid outdated query result");
      const doc = parseStoredDocument(row.doc_json, collection, key);

      if (isOutdated(doc, criteria, parseVersion, compareVersions)) {
        return true;
      }
    }

    if (rows.length < OUTDATED_SCAN_CHUNK_SIZE) {
      return false;
    }
  }
}

function isOutdated(
  doc: Record<string, unknown>,
  criteria: MigrationCriteria,
  parseVersion: (raw: unknown) => ComparableVersion | null,
  compareVersions: (a: ComparableVersion, b: ComparableVersion) => number,
): boolean {
  const parsedVersion = parseVersion(doc[criteria.versionField]);
  const versionState = classifyVersionState(parsedVersion, criteria.version, compareVersions);

  if (versionState === "stale") {
    return true;
  }

  if (versionState !== "current") {
    return false;
  }

  const storedIndexes = doc[criteria.indexesField];

  if (!Array.isArray(storedIndexes) || storedIndexes.length !== criteria.indexes.length) {
    return true;
  }

  for (let i = 0; i < criteria.indexes.length; i++) {
    if (storedIndexes[i] !== criteria.indexes[i]) {
      return true;
    }
  }

  return false;
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
