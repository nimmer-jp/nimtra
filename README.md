<div align="center">
  <h1>nimtra</h1>
  <p><strong>Async-first ORM and libSQL client for Nim</strong></p>
  <p>モデル定義、クエリビルダ、マイグレーション、Turso/libSQL 接続を 1 パッケージにまとめた Nim 向け ORM 基盤です。</p>
  <p>
    <img alt="Nim &gt;= 2.2.0" src="https://img.shields.io/badge/Nim-%3E%3D%202.2.0-FFC200?style=flat-square&amp;logo=nim&amp;logoColor=111827" />
    <img alt="version 0.1.2" src="https://img.shields.io/badge/version-0.1.2-111827?style=flat-square" />
    <img alt="license MIT" src="https://img.shields.io/badge/license-MIT-059669?style=flat-square" />
  </p>
  <p>
    <a href="#why-nimtra">Why</a> ·
    <a href="#install">Install</a> ·
    <a href="#quick-start">Quick Start</a> ·
    <a href="#migrations">Migrations</a> ·
    <a href="#cli-workflow">CLI</a> ·
    <a href="#development">Development</a>
  </p>
</div>

## Why nimtra

`nimtra` は、Nim で Turso/libSQL を扱うときに必要になりやすい層をまとめて提供します。HTTP ドライバだけでも、ORM だけでもなく、その間の移行コストまで含めて扱えるのが狙いです。

- Async-first な libSQL HTTP ドライバ
- `where(it.age >= 18)` のような compile-time 指向のクエリ記述
- モデルからの schema SQL 生成と migration 実行
- SQL ファイル管理にも対応した drizzle-like CLI
- ローカルレプリカ向けの native `libsql` sync hook

## Feature Snapshot

| Layer | Highlights |
| --- | --- |
| Driver | HTTP: `openLibSQL`, `LibSQLAsyncPool`, `withLibSQLFromPool` / マルチスレッド同期: `openLibSQLSync`, `newLibSQLSyncPool`, `initLibSQLSyncThreadPool`, `withLibSQLSyncThread`, … |
| Connection helpers | `withLibSQL`, `withLibSQLEnv`, `withLibSQLSync`, `withLibSQLSyncEnv`, `normalizeLibSqlUrl`, `validateLibSqlUrl`, retry config |
| Query builder | `select`, `fromRaw`, `columnsRaw`, `join`, `leftJoin`, `groupBy`, `where`, `orderBy`, `limit`, `offset`, `paginate`, `count`, `exists`, `update().set().where().exec()` |
| CRUD | `insert`, `upsert`, `upsertReturningId`, `updateById`, `deleteById`, `findById`, `findAll`, `existsById` |
| Mapper | `rowToModel`, `rowsToModels`, `allModels`, `allInto`, `queryInto`, `firstModel`, `findByIdModel` |
| Schema | `modelMeta`, `createTableSql`, `createSchemaSql` |
| Migration | `newMigration`, `migrationFromModel`, `migrate`, `migrateTo`, `pendingMigrations`, `verifyMigrationHistory` |
| Schema diff | `tableSnapshot`, `planModelDiff`, `ensureModelSchemaDiff` |
| Embedded sync | `openLibSQLWithEmbeddedSync`, `sync()` |

## Install

```bash
nimble install nimtra
```

`nimtra` は hybrid package として公開されています。1 回のインストールで次が入ります。

- ライブラリ本体: `import nimtra`
- CLI バイナリ: `nimtra`, `nimtra_cli`

## Database Drivers

`nimtra` は libSQL, PostgreSQL, MySQL をサポートしています。どのドライバを使っても、その後のクエリ記述やモデル操作は共通です。

### libSQL (Turso)

`openLibSQL` が返す `LibSQLConnection` は **`AsyncHttpClient`** を共有し、**`asyncdispatch` の 1 本のイベントループ（1 dispatcher）上与えて順番に処理する設計前提**です。**単一ハンドルを全リクエストで共有すると、並行している `await` が互いをブロックするだけのことが多く、HTTP 側の並行度も上がりにくくなります。** ORM での既定の構成としては、`LibSQLAsyncPool` と `withLibSQLFromPool` で複数接続を先に張り、その中から自動で借用する運用が推奨です（処理はすべて同じイベントループ上だけが対象であり、複数 CPU への分散ではありません）。

同じ **`LibSQLConnection` を複数 OS スレッドから同時に使わないでください**。Basolato + httpbeast のように **OS スレッドごとにリクエストを処理する**構成では、`openLibSQL`（async）ではなく **`initLibSQLSyncThreadPool` + `withLibSQLSyncThread`**（または `withLibSQLSyncThreadLocal`）を使ってください。各ワーカースレッドが専用の同期接続プールを持ち、`borrow` / `release` をスレッド内で完結できます。手動でプールを回す場合は **`openLibSQLSync`** / **`newLibSQLSyncPool`** でも構いませんが、接続オブジェクトは **スレッド間で共有しないでください**。

#### Async プール（ORM 推奨）

```nim
import std/asyncdispatch
import nimtra

proc handle(pool: LibSQLAsyncPool): Future[string] {.async.} =
  await withLibSQLFromPool(pool, proc(db: DbConnection): Future[string] {.async.} =
    discard await db.execute("SELECT 1")
    return "ok"
  )

proc main() {.async.} =
  let pool = await newLibSQLAsyncPoolEnv(size = 8)
  discard await handle(pool)
  await closeLibSQLAsyncPool(pool)

waitFor main()
```

`borrowLibSQLAsync` / `releaseLibSQLAsync` で手動管理も可能です。この非同期プールは **`asyncdispatch` のシングルスレッド協調モデル専用**です（OS ロックを `await` 越えで掴む必要がないように実装してあります）。マルチスレッドサーバ本体からは **`initLibSQLSyncThreadPool`** 側を使ってください。

#### httpbeast / Basolato（スレッドローカル同期プール）

```nim
import std/asyncdispatch
import nimtra

proc main() =
  initLibSQLSyncThreadPoolEnv(poolSize = 2)
  # httpbeast ワーカー内:
  waitFor withLibSQLSyncThread(proc(db: DbConnection): Future[void] {.async.} =
    discard await db.execute("SELECT 1")
  )

main()
```

プロセス終了時は各ワーカースレッドで `closeLibSQLSyncThreadLocal()` を呼ぶと、そのスレッドのプールだけを閉じられます。

#### Async（単一接続）


```nim
import nimtra

# HTTP 接続
let db = await openLibSQL(url = "libsql://...", authToken = "...")

# 環境変数 (TURSO_DATABASE_URL / TURSO_AUTH_TOKEN) から接続
let db = await openLibSQLEnv()
```

#### 同期 (`DbConnection` 互換、`waitFor` で利用)

```nim
import std/asyncdispatch
import nimtra

let dbSync = openLibSQLSync(url = "libsql://...", authToken = "...")

discard waitFor dbSync.execute("SELECT 1")

# 環境変数から開く
let dbFromEnv = openLibSQLSyncEnv()

discard waitFor dbSync.close()
```

#### 簡単な同期プール

```nim
import std/asyncdispatch
import nimtra

let pool = newLibSQLSyncPool(size = 4, url = "libsql://...", authToken = "...")

proc run(pool: LibSQLSyncPool) =
  let cx = borrowLibSQLSync(pool)
  try:
    discard waitFor cx.execute("SELECT 1")
  finally:
    releaseLibSQLSync(pool, cx)

# シャットダウン: borrow されていない状態ですべて返却してから:
closeLibSQLSyncPool(pool)
```

`TURSO_DATABASE_URL` は `normalizeLibSqlUrl` / `validateLibSqlUrl` で正規化・検証されます（`libsql://` の付与、scheme 重複、`libsql` ホスト誤設定など）。接続失敗時は `normalizeDbOpenError` により DNS / タイムアウト等が読みやすいメッセージに変換されます。

### PostgreSQL

```nim
import nimtra

# 接続文字列で接続
let db = await openPostgres("postgres://user:pass@localhost:5432/dbname")

# 環境変数 (PG_DATABASE_URL) から接続
let db = await openPostgresEnv()
```

### MySQL

```nim
import nimtra

# パラメータを指定して接続
let db = await openMySQL(host = "127.0.0.1", user = "root", pass = "", dbname = "test")

# 環境変数 (MYSQL_DATABASE_URL) から接続
# (mysql://user:pass@host:port/dbname 形式をパースします)
let db = await openMySQLEnv()
```

## Quick Start

一度 `db` をオープンすれば、データベースの種類に関係なく共通の API を利用できます。

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

  discard await db.insert(User(
    name: "Alice",
    email: "alice@example.com",
    age: 22
  ))

  let users = await db
    .select(User)
    .where(it.age >= 18)
    .orderBy("age", descending = true)
    .paginate(page = 1, perPage = 20)
    .allModels()

  echo users.len
  await db.close()

waitFor main()
```

部分更新と DTO マッピング:

```nim
discard await db
  .update(FeatureRequest)
  .set(status = "done")
  .where(it.id == requestId)
  .exec()

let summaries = await db
  .select(User)
  .columnsRaw("users.id", "COUNT(documents.id) AS document_count")
  .leftJoin("documents", "documents.user_id = users.id")
  .groupBy("users.id")
  .allInto(UserSummary)
```

## Migrations

モデル定義からそのまま migration を作る構成です。小さく始めるならこの流れが最短です。

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

Migration の適用状況や checksum 検証も API から扱えます。

```nim
let applied = await db.listAppliedMigrations()
let pending = await db.pendingMigrations([m1, m2, m3])
await db.verifyMigrationHistory([m1, m2, m3], allowUnknownApplied = false)
await db.migrateTo([m1, m2, m3], targetVersion = 2)
```

## CLI Workflow

SQL ファイルをディレクトリで管理する drizzle-like な運用にも対応しています。
接続先は `--url` オプションまたは環境変数（`DATABASE_URL`, `TURSO_DATABASE_URL`, `PG_DATABASE_URL`, `MYSQL_DATABASE_URL`）から自動判別されます。

```bash
# 0) インストール
nimble install nimtra

# 接続先の設定例
export DATABASE_URL="libsql://your-db.turso.io"         # libSQL
# export DATABASE_URL="postgres://user:pass@host/db"   # PostgreSQL
# export DATABASE_URL="mysql://user:pass@host/db"      # MySQL

# 1) マイグレーション雛形を作成
nimtra migrate new "create users"

# 2) 適用状況を確認
nimtra migrate status --strict

# 3) 未適用を実行
nimtra migrate up

# 4) 特定バージョンまで適用
nimtra migrate to 20260307121000
```

主なオプションは次のとおりです。

| Option | Meaning |
| --- | --- |
| `--dir`, `-d` | Migration SQL directory. デフォルトは `db/migrations` |
| `--table`, `-t` | Migration table 名. デフォルトは `_nimtra_migrations` |
| `--url`, `--token` | 接続先を環境変数より優先して上書き |
| `--strict` | 厳密な verification を有効化 |
| `--version`, `-v` | `migrate new` 作成時の version を明示 |

互換コマンドとして `nimtra_cli` も利用できます。

## Auto Diff From Current DB Schema

既存 DB とモデル定義の差分を見て、追加 SQL や rebuild が必要かを確認できます。

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

## Native Embedded Sync Hook

ローカルレプリカや embedded 運用では、`libsql` C API の sync hook を使えます。

```nim
import std/asyncdispatch
import nimtra

proc main() {.async.} =
  let db = await openLibSQLWithEmbeddedSync(
    url = "libsql://your-db.turso.io",
    replicaPath = "local.db",
    authToken = "YOUR_TOKEN",
    # libraryPath = "/opt/homebrew/lib/libsql.dylib"
  )

  await db.sync()
  await db.close()

waitFor main()
```

## Development

```bash
nimble test
```

<details>
  <summary>Operational notes</summary>

- `sync()` は `syncHook` を優先し、未指定なら `syncUrl`、さらに未指定なら軽量な `SELECT 1` checkpoint を実行します。
- HTTP retry は transport error と `408` / `429` / `5xx` response に適用されます。
- **`LibSQLConnection` は `AsyncHttpClient` と `asyncdispatch` の 1 本のイベントループ専用**です。**非同期 libSQL での ORM は `LibSQLAsyncPool` + `withLibSQLFromPool` を検討**してください（ロックをイベントループ外で長時間ブロックしないプールです）。別 OS スレッドから並列に読み書きするときは **`openLibSQLSync` / `newLibSQLSyncPool`**（同期）を使い、接続オブジェクトはスレッド間で共有しないでください。
- `LibSQLSyncConnection.sync()` は **`syncHook` / `syncCloseHook` を解釈しません**（`syncUrl` または `SELECT 1` のみ）。カスタム hook が必要なら async の `openLibSQL` を使います。
- `openLibSQLEnv` は `TURSO_DATABASE_URL` / `TURSO_AUTH_TOKEN` を優先し、`TURSO_URL` / `TURSO_TOKEN` も fallback として受け付けます。
- `autoRebuild = false` は安全寄りの差分適用に留め、`autoRebuild = true` は SQLite の table rebuild flow を生成します。
- 適用済み migration には deterministic な `checksum` が保存され、`pendingMigrations` と `verifyMigrationHistory` で drift を検出できます。
- Embedded sync には `libsql.dylib` / `libsql.so` / `libsql.dll` のいずれかが必要です。自動検出できない場合は `libraryPath` を指定してください。

</details>

<details>
  <summary>Release flow</summary>

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

</details>
