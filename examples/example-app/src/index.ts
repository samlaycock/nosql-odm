import type { Context } from "hono";

import { Redis as UpstashRedis } from "@upstash/redis";
import { Client as WorkflowClient } from "@upstash/workflow";
import { serve } from "@upstash/workflow/hono";
import { Hono } from "hono";
import { ZodError, z } from "zod";

import {
  DocumentAlreadyExistsError,
  MigrationAlreadyRunningError,
  MissingMigratorError,
  UniqueConstraintError,
  ValidationError,
  createStore,
  model,
  type MigrationNextPageResult,
} from "../../../src";
import { redisEngine } from "../../../src/engines/redis";

const TODO_STATUSES = ["open", "in_progress", "completed"] as const;
const TODO_PRIORITIES = ["low", "medium", "high"] as const;

const DEFAULT_PORT = 3000;
const DEFAULT_LOCK_TTL_MS = 30_000;
const DEFAULT_MAX_PAGES_PER_RUN = 20;
const DEFAULT_BUSY_BACKOFF_SECONDS = 5;
const DEFAULT_CONTINUE_DELAY_SECONDS = 0;
const DEFAULT_REDIS_KEY_PREFIX = "nosql_odm_example";
const DEFAULT_UPSTASH_REDIS_REST_URL = "http://localhost:8079";
const DEFAULT_UPSTASH_REDIS_REST_TOKEN = "example_token";
const DEFAULT_QSTASH_URL = "http://localhost:8081";

const env = {
  upstashRedisRestUrl: envWithDefault("UPSTASH_REDIS_REST_URL", DEFAULT_UPSTASH_REDIS_REST_URL),
  upstashRedisRestToken: envWithDefault(
    "UPSTASH_REDIS_REST_TOKEN",
    DEFAULT_UPSTASH_REDIS_REST_TOKEN,
  ),
  qstashUrl: envWithDefault("QSTASH_URL", DEFAULT_QSTASH_URL),
};

const redisKeyPrefix = process.env.UPSTASH_REDIS_KEY_PREFIX ?? DEFAULT_REDIS_KEY_PREFIX;

const todoV1Schema = z.object({
  id: z.string(),
  title: z.string().min(1),
  completed: z.boolean(),
  createdAt: z.iso.datetime(),
});

const todoV2Schema = z.object({
  id: z.string(),
  title: z.string().min(1),
  status: z.enum(["open", "completed"]),
  createdAt: z.iso.datetime(),
  updatedAt: z.iso.datetime(),
});

const todoV3Schema = z.object({
  id: z.string(),
  title: z.string().min(1),
  status: z.enum(TODO_STATUSES),
  priority: z.enum(TODO_PRIORITIES),
  tags: z.array(z.string().min(1)).max(20),
  createdAt: z.iso.datetime(),
  updatedAt: z.iso.datetime(),
  dueAt: z.iso.datetime().nullable().optional(),
});

const Todo = model("todo", {
  migration: "eager",
  migrationErrors: "ignore",
})
  .schema(1, todoV1Schema)
  .schema(2, todoV2Schema, {
    migrate(old) {
      const status = old.completed ? "completed" : "open";

      return {
        id: old.id,
        title: old.title,
        status,
        createdAt: old.createdAt,
        updatedAt: old.createdAt,
      };
    },
  })
  .schema(3, todoV3Schema, {
    migrate(old) {
      const normalizedStatus: (typeof TODO_STATUSES)[number] =
        old.status === "completed" ? "completed" : "open";

      return {
        id: old.id,
        title: old.title,
        status: normalizedStatus,
        priority: "medium" as const,
        tags: [],
        createdAt: old.createdAt,
        updatedAt: old.updatedAt,
        dueAt: null,
      };
    },
  })
  .index({ name: "primary", value: "id" })
  .index({
    name: "byStatusUpdated",
    value: (todo) => `${todo.status}#${todo.updatedAt}`,
  })
  .index({
    name: "byPriorityUpdated",
    value: (todo) => `${todo.priority}#${todo.updatedAt}`,
  })
  .build();

const upstashRedis = new UpstashRedis({
  url: env.upstashRedisRestUrl,
  token: env.upstashRedisRestToken,
});

const scriptCache = new Map<string, UpstashScript>();

const redisClientAdapter = {
  get(key: string): Promise<unknown> {
    return upstashRedis.get(key);
  },
  del(keys: string | string[]): Promise<unknown> {
    if (Array.isArray(keys)) {
      if (keys.length === 0) {
        return Promise.resolve(0);
      }

      return upstashRedis.del(...keys);
    }

    return upstashRedis.del(keys);
  },
  async hGetAll(key: string): Promise<unknown> {
    const fields = await upstashRedis.hgetall<Record<string, unknown>>(key);
    return fields ?? {};
  },
  zRange(key: string, start: number, stop: number): Promise<unknown> {
    return upstashRedis.zrange(key, start, stop);
  },
  zRem(key: string, members: string | string[]): Promise<unknown> {
    const normalizedMembers = Array.isArray(members) ? members : [members];

    if (normalizedMembers.length === 0) {
      return Promise.resolve(0);
    }

    return upstashRedis.zrem(key, ...normalizedMembers);
  },
  incr(key: string): Promise<unknown> {
    return upstashRedis.incr(key);
  },
  eval(script: string, options: { keys: string[]; arguments: string[] }): Promise<unknown> {
    const cachedScript = getOrCreateScript(script);
    return cachedScript.eval(options.keys, options.arguments);
  },
};

const store = createStore(
  redisEngine({
    client: redisClientAdapter,
    keyPrefix: redisKeyPrefix,
  }),
  [Todo] as const,
  {
    migrationHooks: {
      onMigrationCreated: ({ progress }) => {
        console.info("[migration] created", {
          runId: progress.id,
          model: progress.models,
        });
      },
      onPageCommitted: ({ runId, model, migrated, skipped, hasMore, telemetry }) => {
        console.info("[migration] page committed", {
          runId,
          model,
          migrated,
          skipped,
          hasMore,
          durationMs: telemetry.durationMs,
          recordsPerSecond: telemetry.recordsPerSecond,
          writebackFailures: telemetry.writebackFailures,
          skipReasons: telemetry.skipReasons,
        });
      },
      onDocumentSkipped: ({ runId, model, key, reason }) => {
        console.warn("[migration] document skipped", {
          runId,
          model,
          key,
          reason,
        });
      },
      onMigrationCompleted: ({ progress }) => {
        console.info("[migration] completed", {
          runId: progress.id,
          totals: progress.totals,
          telemetry: Object.fromEntries(
            Object.entries(progress.progressByModel).map(([model, modelProgress]) => [
              model,
              {
                totalDurationMs: modelProgress.telemetry.totalDurationMs,
                recordsPerSecond: modelProgress.telemetry.recordsPerSecond,
                writebackFailures: modelProgress.telemetry.writebackFailures,
                recentPages: modelProgress.telemetry.recentPages,
              },
            ]),
          ),
        });
      },
      onMigrationFailed: ({ runId, error, progress }) => {
        console.error("[migration] failed", { runId, error, progress });
      },
    },
  },
);

const createTodoBodySchema = z.object({
  title: z.string().min(1).max(200),
  status: z.enum(TODO_STATUSES).optional(),
  priority: z.enum(TODO_PRIORITIES).optional(),
  tags: z.array(z.string().min(1).max(64)).max(20).optional(),
  dueAt: z.iso.datetime().nullable().optional(),
});

const updateTodoBodySchema = z
  .object({
    title: z.string().min(1).max(200).optional(),
    status: z.enum(TODO_STATUSES).optional(),
    priority: z.enum(TODO_PRIORITIES).optional(),
    tags: z.array(z.string().min(1).max(64)).max(20).optional(),
    dueAt: z.iso.datetime().nullable().optional(),
  })
  .refine((value) => Object.keys(value).length > 0, {
    message: "At least one field is required.",
  });

const listTodosQuerySchema = z.object({
  status: z.enum(TODO_STATUSES).optional(),
  limit: z.coerce.number().int().min(1).max(100).default(20),
  cursor: z.string().min(1).optional(),
});

const migrationWorkflowPayloadSchema = z.object({
  lockTtlMs: z.coerce.number().int().min(1_000).max(300_000).optional(),
  maxPagesPerRun: z.coerce.number().int().min(1).max(200).optional(),
  busyBackoffSeconds: z.coerce.number().int().min(1).max(120).optional(),
  continueDelaySeconds: z.coerce.number().int().min(0).max(120).optional(),
});

interface ResolvedMigrationWorkflowPayload {
  lockTtlMs: number;
  maxPagesPerRun: number;
  busyBackoffSeconds: number;
  continueDelaySeconds: number;
}

type ErrorStatus = 400 | 404 | 409 | 500;

class HttpError extends Error {
  readonly status: ErrorStatus;
  readonly details?: unknown;

  constructor(status: ErrorStatus, message: string, details?: unknown) {
    super(message);
    this.name = "HttpError";
    this.status = status;
    this.details = details;
  }
}

const app = new Hono();

app.onError((error, c) => {
  const response = mapErrorToHttpResponse(error);
  return c.newResponse(JSON.stringify(response.body), response.status, {
    "content-type": "application/json",
  });
});

app.notFound((c) => c.json({ error: "Not Found" }, 404));

app.get("/health", (c) => {
  return c.json({
    status: "ok",
    timestamp: new Date().toISOString(),
    redisConfigured: true,
    qstashConfigured: Boolean(process.env.QSTASH_TOKEN),
    workflowBaseUrl:
      process.env.APP_BASE_URL ?? process.env.UPSTASH_WORKFLOW_URL ?? new URL(c.req.url).origin,
  });
});

app.post("/todos", async (c) => {
  const body = createTodoBodySchema.parse(await parseJsonBody(c));
  const now = new Date().toISOString();
  const id = `todo_${crypto.randomUUID()}`;

  const created = await store.todo.create(id, {
    id,
    title: body.title,
    status: body.status ?? "open",
    priority: body.priority ?? "medium",
    tags: body.tags ?? [],
    createdAt: now,
    updatedAt: now,
    dueAt: body.dueAt ?? null,
  });

  return c.json({ item: created }, 201);
});

app.get("/todos/:id", async (c) => {
  const id = c.req.param("id");
  const item = await store.todo.findByKey(id);

  if (!item) {
    throw new HttpError(404, `Todo "${id}" was not found.`);
  }

  return c.json({ item });
});

app.get("/todos", async (c) => {
  const query = listTodosQuerySchema.parse(c.req.query());
  const result =
    query.status === undefined
      ? await store.todo.query({
          index: "primary",
          limit: query.limit,
          cursor: query.cursor,
          sort: "asc",
        })
      : await store.todo.query({
          index: "byStatusUpdated",
          filter: { value: { $begins: `${query.status}#` } },
          limit: query.limit,
          cursor: query.cursor,
          sort: "desc",
        });

  return c.json({
    items: result.documents,
    cursor: result.cursor,
  });
});

app.patch("/todos/:id", async (c) => {
  const id = c.req.param("id");
  const existing = await store.todo.findByKey(id);

  if (!existing) {
    throw new HttpError(404, `Todo "${id}" was not found.`);
  }

  const patch = updateTodoBodySchema.parse(await parseJsonBody(c));
  const updatedAt = new Date().toISOString();

  const updated = await store.todo.update(id, {
    ...(patch.title !== undefined ? { title: patch.title } : {}),
    ...(patch.status !== undefined ? { status: patch.status } : {}),
    ...(patch.priority !== undefined ? { priority: patch.priority } : {}),
    ...(patch.tags !== undefined ? { tags: patch.tags } : {}),
    ...(patch.dueAt !== undefined ? { dueAt: patch.dueAt } : {}),
    updatedAt,
  });

  return c.json({ item: updated });
});

app.delete("/todos/:id", async (c) => {
  const id = c.req.param("id");
  const existing = await store.todo.findByKey(id);

  if (!existing) {
    throw new HttpError(404, `Todo "${id}" was not found.`);
  }

  await store.todo.delete(id);
  return c.body(null, 204);
});

const migrateTodosWorkflow = serve<unknown>(
  async (workflowContext) => {
    const payload = resolveMigrationWorkflowPayload(workflowContext.requestPayload);
    const migrationOptions = { lockTtlMs: payload.lockTtlMs };
    const initialProgress = await workflowContext.run("get-or-create-run", async () => {
      return store.todo.getOrCreateMigration(migrationOptions);
    });

    let lastPage: MigrationNextPageResult | null = null;

    for (let index = 0; index < payload.maxPagesPerRun; index++) {
      const stepNumber = index + 1;
      lastPage = await workflowContext.run(`migrate-page-${stepNumber}`, async () => {
        return store.todo.migrateNextPage(migrationOptions);
      });

      if (lastPage.status === "busy") {
        await workflowContext.sleep(`busy-backoff-${stepNumber}`, payload.busyBackoffSeconds);
        continue;
      }

      if (lastPage.completed) {
        return {
          outcome: "completed",
          initialProgress,
          page: lastPage,
        };
      }

      if (lastPage.hasMore && payload.continueDelaySeconds > 0) {
        await workflowContext.sleep(`continue-delay-${stepNumber}`, payload.continueDelaySeconds);
      }
    }

    return {
      outcome: lastPage?.status === "busy" ? "busy" : "partial",
      initialProgress,
      page: lastPage,
      hasMore: lastPage?.hasMore ?? true,
      progress: lastPage?.progress ?? (await store.todo.getMigrationProgress()),
    };
  },
  {
    env: {
      ...process.env,
      QSTASH_URL: env.qstashUrl,
    } as Record<string, string | undefined>,
    failureFunction: async ({ context, failStatus, failResponse, failStack }) => {
      console.error("[workflow:migrate-todos] failed", {
        runId: context.workflowRunId,
        failStatus,
        failResponse,
        failStack,
      });
    },
  },
);

app.post("/workflows/migrate-todos", migrateTodosWorkflow);

app.post("/migrations/start", async (c) => {
  const rawBody = await parseOptionalJsonBody(c);
  const payload = resolveMigrationWorkflowPayload(rawBody);
  const qstashToken = process.env.QSTASH_TOKEN;

  if (!qstashToken) {
    throw new HttpError(
      500,
      "QSTASH_TOKEN is required to trigger workflows through /migrations/start.",
    );
  }

  const workflowBaseUrl = resolveWorkflowBaseUrl(c.req.url);
  const workflowUrl = `${workflowBaseUrl}/workflows/migrate-todos`;
  const workflowClient = new WorkflowClient({ baseUrl: env.qstashUrl, token: qstashToken });
  const triggerResult = await workflowClient.trigger({
    url: workflowUrl,
    body: payload,
    retries: 3,
  });

  return c.json({
    workflowRunId: triggerResult.workflowRunId,
    workflowUrl,
    payload,
  });
});

app.get("/migrations/progress", async (c) => {
  const progress = await store.todo.getMigrationProgress();
  return c.json({ progress });
});

app.get("/migrations/status", async (c) => {
  const status = await store.todo.getMigrationStatus();
  return c.json({ status });
});

app.post("/migrations/tick", async (c) => {
  const rawBody = await parseOptionalJsonBody(c);
  const payload = resolveMigrationWorkflowPayload(rawBody);
  const migrationOptions = { lockTtlMs: payload.lockTtlMs };

  await store.todo.getOrCreateMigration(migrationOptions);
  const page = await store.todo.migrateNextPage(migrationOptions);

  return c.json({ page });
});

const port = normalizePort(process.env.PORT);

if (import.meta.main) {
  const server = Bun.serve({
    port,
    fetch: app.fetch,
  });

  console.log(`[example-app] listening at http://localhost:${String(server.port)}`);
  console.log(`[example-app] redis key prefix: ${redisKeyPrefix}`);
  console.log(`[example-app] redis rest url: ${env.upstashRedisRestUrl}`);
  console.log(`[example-app] qstash url: ${env.qstashUrl}`);
  console.log(
    `[example-app] workflow base url: ${
      process.env.APP_BASE_URL ?? process.env.UPSTASH_WORKFLOW_URL ?? "derived-per-request"
    }`,
  );
}

export { app, store, Todo };

type UpstashScript = {
  eval(keys: string[], args: string[]): Promise<unknown>;
};

function getOrCreateScript(scriptSource: string): UpstashScript {
  const existing = scriptCache.get(scriptSource);

  if (existing) {
    return existing;
  }

  const created = upstashRedis.createScript(scriptSource) as UpstashScript;
  scriptCache.set(scriptSource, created);
  return created;
}

function envWithDefault(name: string, fallback: string): string {
  const value = process.env[name]?.trim();

  return value || fallback;
}

function normalizePort(rawPort: string | undefined): number {
  const parsed = Number(rawPort ?? DEFAULT_PORT);

  if (!Number.isFinite(parsed) || parsed <= 0) {
    return DEFAULT_PORT;
  }

  return Math.floor(parsed);
}

function resolveWorkflowBaseUrl(requestUrl: string): string {
  const explicit = process.env.APP_BASE_URL?.trim();

  if (explicit) {
    return stripTrailingSlash(explicit);
  }

  const workflowUrl = process.env.UPSTASH_WORKFLOW_URL?.trim();

  if (workflowUrl) {
    return stripTrailingSlash(workflowUrl);
  }

  return stripTrailingSlash(new URL(requestUrl).origin);
}

function stripTrailingSlash(url: string): string {
  return url.replace(/\/+$/, "");
}

function resolveMigrationWorkflowPayload(rawPayload: unknown): ResolvedMigrationWorkflowPayload {
  const parsed = migrationWorkflowPayloadSchema.parse(rawPayload ?? {});

  return {
    lockTtlMs: parsed.lockTtlMs ?? DEFAULT_LOCK_TTL_MS,
    maxPagesPerRun: parsed.maxPagesPerRun ?? DEFAULT_MAX_PAGES_PER_RUN,
    busyBackoffSeconds: parsed.busyBackoffSeconds ?? DEFAULT_BUSY_BACKOFF_SECONDS,
    continueDelaySeconds: parsed.continueDelaySeconds ?? DEFAULT_CONTINUE_DELAY_SECONDS,
  };
}

async function parseJsonBody(c: Context): Promise<unknown> {
  try {
    return await c.req.json();
  } catch (error) {
    if (error instanceof SyntaxError) {
      throw new HttpError(400, "Request body must be valid JSON.");
    }

    throw error;
  }
}

async function parseOptionalJsonBody(c: Context): Promise<unknown> {
  const contentLength = c.req.header("content-length");

  if (!contentLength || contentLength === "0") {
    return {};
  }

  return parseJsonBody(c);
}

function mapErrorToHttpResponse(error: unknown): {
  status: ErrorStatus;
  body: Record<string, unknown>;
} {
  if (error instanceof HttpError) {
    return {
      status: error.status,
      body: {
        error: error.message,
        ...(error.details !== undefined ? { details: error.details } : {}),
      },
    };
  }

  if (error instanceof ZodError) {
    return {
      status: 400,
      body: {
        error: "Validation failed.",
        issues: error.issues,
      },
    };
  }

  if (error instanceof ValidationError) {
    return {
      status: 400,
      body: {
        error: error.message,
      },
    };
  }

  if (
    error instanceof DocumentAlreadyExistsError ||
    error instanceof UniqueConstraintError ||
    error instanceof MigrationAlreadyRunningError
  ) {
    return {
      status: 409,
      body: {
        error: error.message,
      },
    };
  }

  if (error instanceof MissingMigratorError) {
    return {
      status: 500,
      body: {
        error: error.message,
      },
    };
  }

  if (error instanceof Error && error.message.toLowerCase().includes("not found")) {
    return {
      status: 404,
      body: {
        error: error.message,
      },
    };
  }

  console.error("[example-app] unhandled error", error);

  return {
    status: 500,
    body: {
      error: "Internal Server Error",
    },
  };
}
