import std/asyncdispatch

type
  Dialect = ref object of RootObj
  SQLiteDialect = ref object of Dialect
  PostgresDialect = ref object of Dialect

method getColType(d: Dialect, dbType: string): string {.base.} = dbType

method getColType(d: PostgresDialect, dbType: string): string =
  if dbType == "INTEGER": return "SERIAL"
  return dbType

let s = SQLiteDialect()
let p = PostgresDialect()

echo s.getColType("INTEGER")
echo p.getColType("INTEGER")
