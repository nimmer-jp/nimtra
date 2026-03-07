# nimtra

`nimtra` is an async-first ORM foundation for Nim with a native libSQL (Turso) HTTP driver.

## Install

```bash
nimble install nimtra
```

`nimtra` is published as a hybrid package, so one install gives you both:

- the library modules (`import nimtra`)
- the migration CLI binaries (`nimtra`, `nimtra_cli`)

## Implemented

- Async libSQL HTTP pipeline driver (`openLibSQL`, `execute`, `query`, `close`)
- Batch execution (`executeBatch`) for transaction-safe multi-statement flows
- Connection helpers (`openLibSQLEnv`, `withLibSQL`, `withLibSQLEnv`) and retry config
- HTTP transport fallback to `curl` for environments where Nim TLS handshake is unavailable/unstable
- Hrana-like typed value encode/decode and result parsing
- Dialect abstraction (`SQLite`, `Postgres`, `MySQL`) with placeholder rewriting
- Compile-time `where` macro:
  - Example: `select(User).where(it.age >= 18 and it.status == "active")`
- Query builder (`select`, `fromRaw`, `columnsRaw`, `join`, `leftJoin`, `joinRaw`, `where`, `LIKE` helpers via `contains` / `startsWith` / `endsWith` / `like`, multi-`orderBy`, `limit`, `offset`, `paginate`, `all`, `first`, `oneRow`, `count`, `exists`)
- Basic CRUD helpers (`insert`, `upsert`, `upsertReturningId`, `updateById`, `deleteById`, `findById`, `findAll`, `findAllModels`, `existsById`)
- Row-to-model mapping (`rowToModel`, `rowsToModels`, `allModels`, `firstModel`, `findByIdModel`) with snake_case/case-insensitive column matching
- Model pragmas and compile-time metadata extraction (`modelMeta`) including exported model/field symbols
- Schema SQL generation from models (`createTableSql`, `createSchemaSql`)
- Migration manager (`newMigration`, `migrationFromModel`, `migrate`)
- Migration safety helpers (`migrationChecksum`, `listAppliedMigrations`, `pendingMigrations`, `verifyMigrationHistory`, `migrateTo`)
- CLI for migration workflows (`nimtra_cli migrate new/status/up/to/verify/list`)
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

## Migration status and checksum verification

```nim
let applied = await db.listAppliedMigrations()
let pending = await db.pendingMigrations([m1, m2, m3])
await db.verifyMigrationHistory([m1, m2, m3], allowUnknownApplied = false)
await db.migrateTo([m1, m2, m3], targetVersion = 2)
```

## CLI migration workflow

`drizzle`のように、SQLファイルをディレクトリで管理してCLI実行できます。  
推奨構成:

```text
db/
  migrations/
    20260307120000_create_users.sql
    20260307121000_add_user_index.sql
```

CLI例:

```bash
# 0) インストール
nimble install nimtra

# 1) マイグレーション雛形を作成
nimtra migrate new "create users"

# 2) 適用状況を確認
nimtra migrate status --strict

# 3) 未適用を実行
nimtra migrate up

# 4) 特定バージョンまで適用
nimtra migrate to 20260307121000
```

主なオプション:
- `--dir` (`db/migrations` がデフォルト)
- `--table` (`_nimtra_migrations` がデフォルト)
- `--url`, `--token`（未指定時は `TURSO_*` 環境変数）
- `--prefer-curl`, `--strict`
- 互換コマンドとして `nimtra_cli` も利用可能

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
nimble test
```

## Publish to Nimble

Nimble は Git タグを配布単位として扱います。公開時は次の流れです。

```bash
git tag v0.1.0
git push origin v0.1.0
nimble publish
```

公開前のローカル確認:

```bash
nimble --nimbleDir:.nimble-publish-test install -y
nimble --nimbleDir:.nimble-publish-test build -y
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
  - `useCurlFallback = true` (default) retries via `curl` when Nim HTTP/TLS transport fails.
  - `preferCurlTransport = true` forces `curl` transport for all requests.
- Environment helper names:
  - `openLibSQLEnv` reads `TURSO_DATABASE_URL` / `TURSO_AUTH_TOKEN` by default.
  - It also accepts `TURSO_URL` / `TURSO_TOKEN` as fallback aliases.
- Schema diff notes:
  - `autoRebuild = false` keeps migration conservative and only applies compatible changes (e.g. add column/index).
  - `autoRebuild = true` generates a SQLite rebuild flow (`CREATE temp -> copy -> drop -> rename`) for incompatible changes.
- Migration history notes:
  - Applied rows now keep a deterministic `checksum` to detect local-vs-applied migration drift (`pendingMigrations` / `verifyMigrationHistory`).
- Embedded sync notes:
  - Requires `libsql` shared library installed (`libsql.dylib` / `libsql.so` / `libsql.dll`).
  - Pass `libraryPath` explicitly if auto-discovery cannot find your library.
