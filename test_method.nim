import std/asyncdispatch

type
  DbConnection* = ref object of RootObj

method execute*(db: DbConnection): Future[int] {.base, async.} =
  return 1

type
  MyDb = ref object of DbConnection

method execute*(db: MyDb): Future[int] {.async.} =
  return 2

proc main() {.async.} =
  var db: DbConnection = MyDb()
  let res = await db.execute()
  echo res

waitFor main()
