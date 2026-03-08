import std/asyncdispatch

type
  Dialect = ref object of RootObj
  SQLiteDialect = ref object of Dialect
  PostgresDialect = ref object of Dialect
  DbConnection = ref object
    dialect: Dialect

method tableSnapshot(dialect: Dialect, db: DbConnection, name: string): Future[string] {.base, async.} =
  return "base"

method tableSnapshot(dialect: SQLiteDialect, db: DbConnection, name: string): Future[string] {.async.} =
  return "sqlite"

method tableSnapshot(dialect: PostgresDialect, db: DbConnection, name: string): Future[string] {.async.} =
  return "postgres"

proc test() {.async.} =
  let db1 = DbConnection(dialect: SQLiteDialect())
  let db2 = DbConnection(dialect: PostgresDialect())
  echo await tableSnapshot(db1.dialect, db1, "t1")
  echo await tableSnapshot(db2.dialect, db2, "t2")

waitFor test()
