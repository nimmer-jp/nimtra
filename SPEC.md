# nimtra - Modern, Async-First ORM for Nim

nimtra は、Nim言語のための非同期ファーストかつ型安全なORM（Object-Relational Mapper）です。
エッジデータベース（libSQL/Turso）のネイティブサポートから始まり、PostgreSQLやMySQLなどの従来のリレーショナルデータベースにも拡張可能なマルチダイアレクトアーキテクチャを採用しています。

## 🎯 コアコンセプト

1. **Async-First**: すべてのI/O操作は非同期（`asyncdispatch`）で実行されます。非同期通信を前提としているため、BasolatoなどのWebフレームワークと組み合わせた際も、スレッドをブロックすることなく高いパフォーマンスを発揮します。
2. **Edge-Ready**: libSQLのHranaプロトコルやローカルレプリカ同期（`sync`）をトップレベルでサポートします。
3. **Compile-Time Safety**: Nimの強力なマクロを活用し、実行時のオーバーヘッドなしで型安全なSQLクエリを生成します。
4. **Multi-Dialect**: 方言（Dialect）レイヤーを分離し、将来的なPostgreSQL/MySQL対応を容易にするプラグインアーキテクチャ。

## 🏛️ アーキテクチャ

システムは大きく3つの独立したレイヤーで構成されます。

### 1. Model Layer (ORM)
開発者が直接触れるインターフェースです。Nimのオブジェクトとプラグマ（`{. .}`）を使用してデータベースのスキーマを定義します。

```nim
type
  User = ref object
    id {.primary, autoincrement.}: int
    name {.maxLength: 50.}: string
    email {.unique.}: string
    createdAt {.default: "CURRENT_TIMESTAMP".}: DateTime
```

### 2. Query Builder Layer
NimのAST（抽象構文木）を解析し、各データベースエンジンの方言（Dialect）に応じたSQL文字列とパラメータに変換します。

* **Dialect Interface**: `SQLiteDialect`, `PostgresDialect`, `MySQLDialect` が共通のインターフェースを実装。
* 開発者はデータベースの違いを意識することなく、Nimの構文でクエリを構築できます。

```nim
# コンパイル時にプレースホルダー付きのSQLとパラメータリストに変換される
let activeUsers = await db.select(User).where(it.age >= 18 and it.status == "active").all()
```

### 3. Driver Layer
実際にデータベースと通信を行う低レイヤーモジュールです。

* **libSQL/Turso Driver**: Hranaプロトコル（HTTP/WebSocket）およびローカルC APIのラッパー。バッチ実行や `sync()` を提供。
* **PostgreSQL Driver**: （将来実装）非同期ソケット通信によるネイティブドライバ。
* **MySQL Driver**: （将来実装）非同期ソケット通信によるネイティブドライバ。

## 🚀 libSQL (Turso) 固有の機能

Tura は標準のSQLiteドライバでは実現できない、Tursoの強力な機能をネイティブに引き出します。

```nim
# ローカルファイルを使用しつつ、バックグラウンドでリモート(Turso)と同期する
let db = await openLibSQL(url = "file:local.db", syncUrl = "libsql://...", authToken = "...")

# データの読み書き
await db.insert(User(name: "Alice", email: "alice@example.com"))

# リモートとの明示的な同期
await db.sync()
```

## 🗺️ 開発ロードマップ

* [ ] **Phase 1: Core libSQL Driver**
  * Hranaプロトコル（HTTP）のNimネイティブ実装
  * 非同期クエリ実行とJSON結果のパース
* [ ] **Phase 2: Query Builder & Macros**
  * ASTからSQLへの変換エンジンの実装
  * プレースホルダーによるSQLインジェクション対策
  * 基本的なCRUDオペレーションのマクロ実装
* [ ] **Phase 3: ORM Features & Sync**
  * モデル定義用のプラグマ実装
  * libSQLのローカルレプリカと `sync()` APIの実装
* [ ] **Phase 4: Multi-Database Support**
  * Dialectアーキテクチャの抽象化
  * PostgreSQL用非同期ドライバの実装
