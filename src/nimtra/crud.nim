import std/[asyncdispatch, options, sequtils, strutils, typetraits]

import ./[utils, values]
import ./driver/libsql_http

proc defaultCrudTable[T](): string =
  defaultTableName(name(T))

template eachModelField(entity: untyped, body: untyped) =
  when entity is ref object:
    if entity.isNil:
      raise newException(ValueError, "Entity is nil")
    for fieldName, fieldValue in fieldPairs(entity[]):
      body
  else:
    for fieldName, fieldValue in fieldPairs(entity):
      body

proc insert* [T](
  db: LibSQLConnection,
  entity: T,
  tableName = "",
  idField = "id"
): Future[SqlResult] {.async.} =
  if db.isNil:
    raise newException(LibSQLError, "Database handle is nil")

  let targetTable = if tableName.len > 0: tableName else: defaultCrudTable[T]()
  var cols: seq[string]
  var params: seq[SqlValue]

  eachModelField(entity):
    when fieldValue is SomeInteger:
      if fieldName == idField and fieldValue == 0:
        continue
    cols.add(fieldName)
    params.add(toSqlValue(fieldValue))

  if cols.len == 0:
    raise newException(ValueError, "No columns found for INSERT")

  let quoted = cols.mapIt(db.dialect.quoteIdent(it)).join(", ")
  let placeholders = cols.mapIt("?").join(", ")
  let sql = "INSERT INTO " & db.dialect.quoteIdent(targetTable) &
    " (" & quoted & ") VALUES (" & placeholders & ")"

  await db.execute(sql, params)

proc updateById* [T](
  db: LibSQLConnection,
  entity: T,
  tableName = "",
  idField = "id"
): Future[SqlResult] {.async.} =
  if db.isNil:
    raise newException(LibSQLError, "Database handle is nil")

  let targetTable = if tableName.len > 0: tableName else: defaultCrudTable[T]()

  var setClauses: seq[string]
  var params: seq[SqlValue]
  var idValue = nullValue()
  var hasId = false

  eachModelField(entity):
    if fieldName == idField:
      idValue = toSqlValue(fieldValue)
      hasId = true
      continue

    setClauses.add(db.dialect.quoteIdent(fieldName) & " = ?")
    params.add(toSqlValue(fieldValue))

  if not hasId:
    raise newException(ValueError, "Entity does not contain id field: " & idField)

  if setClauses.len == 0:
    raise newException(ValueError, "No update columns found")

  params.add(idValue)

  let sql = "UPDATE " & db.dialect.quoteIdent(targetTable) &
    " SET " & setClauses.join(", ") &
    " WHERE " & db.dialect.quoteIdent(idField) & " = ?"

  await db.execute(sql, params)

proc deleteById* [T, I: SomeInteger](
  db: LibSQLConnection,
  _: typedesc[T],
  id: I,
  tableName = "",
  idField = "id"
): Future[SqlResult] {.async.} =
  if db.isNil:
    raise newException(LibSQLError, "Database handle is nil")

  let targetTable = if tableName.len > 0: tableName else: defaultCrudTable[T]()
  let sql = "DELETE FROM " & db.dialect.quoteIdent(targetTable) &
    " WHERE " & db.dialect.quoteIdent(idField) & " = ?"

  await db.execute(sql, @[toSqlValue(id)])

proc findById* [T, I: SomeInteger](
  db: LibSQLConnection,
  _: typedesc[T],
  id: I,
  tableName = "",
  idField = "id"
): Future[Option[SqlRow]] {.async.} =
  if db.isNil:
    raise newException(LibSQLError, "Database handle is nil")

  let targetTable = if tableName.len > 0: tableName else: defaultCrudTable[T]()
  let sql = "SELECT * FROM " & db.dialect.quoteIdent(targetTable) &
    " WHERE " & db.dialect.quoteIdent(idField) & " = ? LIMIT 1"
  let res = await db.query(sql, @[toSqlValue(id)])
  if res.rows.len == 0:
    return none(SqlRow)
  some(res.rows[0])
