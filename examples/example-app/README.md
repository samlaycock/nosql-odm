# nosql-odm example app

This is a Bun + Hono example API using `nosql-odm` with the Redis engine.

## Run locally

Start the local Redis HTTP and QStash-compatible services:

```sh
bun run services:up
```

Start the API:

```sh
bun run dev
```

The app defaults to the local Docker Compose services, so a `.env` file is optional for normal local development. Copy `.env.example` to `.env` only when you need to override ports, tokens, key prefixes, or public workflow URLs.

## Endpoints

- `GET /health`
- `GET /todos`
- `POST /todos`
- `GET /todos/:id`
- `PATCH /todos/:id`
- `DELETE /todos/:id`
- `GET /migrations/status`
- `GET /migrations/progress`
- `POST /migrations/tick`
- `POST /migrations/start`
- `POST /workflows/migrate-todos`

`POST /migrations/tick` runs one migration page directly and does not require QStash. `POST /migrations/start` triggers the Upstash Workflow endpoint through QStash and requires `QSTASH_TOKEN`.

## Example request

```sh
curl -X POST http://localhost:3000/todos \
  -H 'content-type: application/json' \
  -d '{"title":"Try nosql-odm","priority":"high","tags":["example"]}'
```
