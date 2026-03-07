# nimtra

`nimtra` is an async-first ORM foundation for Nim with a native libSQL (Turso) HTTP driver.

## Implemented

- Async libSQL HTTP pipeline driver (`openLibSQL`, `execute`, `query`, `close`)
- Batch execution (`executeBatch`) for transaction-safe multi-statement flows
- Connection helpers (`openLibSQLEnv`, `withLibSQL`, `withLibSQLEnv`) and retry config
- Hrana-like typed value encode/decode and result parsing
- Dialect abstraction (`SQLite`, `Postgres`, `MySQL`) with placeholder rewriting
- Compile-time `where` macro:
  - Example: `select(User).where(it.age >= 18 and it.status == "active")`
- Query builder (`select`, `where`, multi-`orderBy`, `limit`, `offset`, `paginate`, `all`, `first`, `oneRow`, `count`, `exists`)
- Basic CRUD helpers (`insert`, `updateById`, `deleteById`, `findById`, `findAll`, `findAllModels`, `existsById`)
- Row-to-model mapping (`rowToModel`, `rowsToModels`, `allModels`, `firstModel`, `findByIdModel`) with snake_case/case-insensitive column matching
- Model pragmas and compile-time metadata extraction (`modelMeta`)
- Schema SQL generation from models (`createTableSql`, `createSchemaSql`)
- Migration manager (`newMigration`, `migrationFromModel`, `migrate`)
- Schema diff planner (`tableSnapshot`, `planModelDiff`, `ensureModelSchemaDiff`)
- Migration warning persistence (`_nimtra_migrations.warnings` as JSON array)
- Native libSQL C replica sync hook (`libsql_embedded`)

## Quick example

```nim
import std/asyncdispatch
import nimtra

type
  User = ref object
    id {.primary, autoincrement.}: int
    name {.maxLength: 50.}: string
    email {.unique.}: string
    age: int

proc main() {.async.} =
  let db = await openLibSQLEnv()

  discard await db.insert(User(name: "Alice", email: "alice@example.com", age: 22))

  let users = await db
    .select(User)
    .where(it.age >= 18 and it.email != nil)
    .orderBy("age", descending = true)
    .paginate(page = 1, perPage = 20)
    .allModels()

  echo users.len
  await db.close()

waitFor main()
```

## Schema and migration example

```nim
import std/asyncdispatch
import nimtra

type
  User {.table: "users".} = ref object
    id {.primary, autoincrement.}: int
    email {.unique.}: string
    age {.index.}: int

proc main() {.async.} =
  let db = await openLibSQL(
    url = "libsql://your-db.turso.io",
    authToken = "YOUR_TOKEN"
  )

  let migration = migrationFromModel(User, 2026030701, migrationName = "create_users")
  await db.migrate([migration])

  await db.close()

waitFor main()
```

## Auto diff migration (from current DB schema)

```nim
import std/asyncdispatch
import nimtra

type
  User = ref object
    id {.primary, autoincrement.}: int
    email {.unique.}: string
    age {.index.}: int

proc main() {.async.} =
  let db = await openLibSQL(
    url = "libsql://your-db.turso.io",
    authToken = "YOUR_TOKEN"
  )

  let plan = await db.planModelDiff(User, autoRebuild = true)
  for warning in plan.warnings:
    echo "warning: ", warning

  discard await db.ensureModelSchemaDiff(User, autoRebuild = true)
  await db.close()

waitFor main()
```

## Native embedded sync hook (libsql C API)

```nim
import std/asyncdispatch
import nimtra

proc main() {.async.} =
  let db = await openLibSQLWithEmbeddedSync(
    url = "libsql://your-db.turso.io",
    replicaPath = "local.db",
    authToken = "YOUR_TOKEN",
    # set this when auto-loading fails:
    # libraryPath = "/opt/homebrew/lib/libsql.dylib"
  )

  # Uses libsql_database_sync() under the hood
  await db.sync()
  await db.close()

waitFor main()
```

## Test

```bash
for t in tests/test_*.nim; do
  nim c -r --nimcache:.nimcache --path:src "$t"
done
```

## Notes

- `sync()` behavior:
  - If `syncHook` is supplied in `openLibSQL`, nimtra calls that hook (recommended for embedded/local replica integration).
  - If `syncUrl` is supplied, nimtra sends `POST` to that endpoint (or to `syncPath` when URL has no path).
  - If neither is supplied, `sync()` runs a lightweight `SELECT 1` checkpoint.
- HTTP driver currently targets the `/v2/pipeline` flow first.
- HTTP retry behavior:
  - `openLibSQL` supports `maxRetries` and `retryBackoffMs`.
  - Retries are applied on transport errors and `408/429/5xx` responses.
- Schema diff notes:
  - `autoRebuild = false` keeps migration conservative and only applies compatible changes (e.g. add column/index).
  - `autoRebuild = true` generates a SQLite rebuild flow (`CREATE temp -> copy -> drop -> rename`) for incompatible changes.
- Embedded sync notes:
  - Requires `libsql` shared library installed (`libsql.dylib` / `libsql.so` / `libsql.dll`).
  - Pass `libraryPath` explicitly if auto-discovery cannot find your library.
