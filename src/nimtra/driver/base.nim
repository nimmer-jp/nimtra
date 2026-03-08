import std/asyncdispatch
import ../[dialects, values]

type
  NimtraDbError* = object of CatchableError

  DbConnection* = ref object of RootObj
    dialect*: Dialect

method execute*(db: DbConnection, sql: string, args: openArray[SqlValue] = []): Future[SqlResult] {.base.} =
  raise newException(CatchableError, "execute must be implemented")

method execute*(db: DbConnection, statement: SqlStatement): Future[SqlResult] {.base, async.} =
  raise newException(CatchableError, "execute must be implemented")

method executeBatch*(db: DbConnection, statements: openArray[SqlStatement]): Future[seq[SqlResult]] {.base.} =
  raise newException(CatchableError, "executeBatch must be implemented")

method query*(db: DbConnection, sql: string, args: openArray[SqlValue] = []): Future[SqlResult] {.base.} =
  raise newException(CatchableError, "query must be implemented")

method query*(db: DbConnection, statement: SqlStatement): Future[SqlResult] {.base, async.} =
  raise newException(CatchableError, "query must be implemented")

method sync*(db: DbConnection): Future[void] {.base, async.} =
  raise newException(CatchableError, "sync must be implemented")

method close*(db: DbConnection): Future[void] {.base, async.} =
  raise newException(CatchableError, "close must be implemented")
