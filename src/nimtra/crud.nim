import std/[asyncdispatch, options, sequtils, strutils]

import ./[mapper, model, values]
import ./driver/base

proc defaultCrudTable[T](): string =
  modelTableName(T)

template eachModelField(entity: untyped, body: untyped) =
  when entity is ref object:
    if entity.isNil:
      raise newException(ValueError, "Entity is nil")
    for fieldName, fieldValue in fieldPairs(entity[]):
      body
  else:
    for fieldName, fieldValue in fieldPairs(entity):
      body

proc eqFieldName(a, b: string): bool =
  a.toLowerAscii() == b.toLowerAscii()

proc containsFieldName(fields: openArray[string], candidate: string): bool =
  for field in fields:
    if eqFieldName(field, candidate):
      return true
  false

proc collectInsertData[T](entity: T, idField: string): tuple[cols: seq[string], params: seq[SqlValue]] =
  when T is ref object:
    if entity.isNil:
      raise newException(ValueError, "Entity is nil")
    for fieldName, fieldValue in fieldPairs(entity[]):
      var includeField = true
      when fieldValue is SomeInteger:
        if fieldName == idField and fieldValue == 0:
          includeField = false
      if includeField:
        result.cols.add(fieldName)
        result.params.add(toSqlValue(fieldValue))
  else:
    for fieldName, fieldValue in fieldPairs(entity):
      var includeField = true
      when fieldValue is SomeInteger:
        if fieldName == idField and fieldValue == 0:
          includeField = false
      if includeField:
        result.cols.add(fieldName)
        result.params.add(toSqlValue(fieldValue))

proc insert* [T](
  db: DbConnection,
  entity: T,
  tableName = "",
  idField = "id"
): Future[SqlResult] {.async.} =
  if db.isNil:
    raise newException(NimtraDbError, "Database handle is nil")

  let targetTable = if tableName.len > 0: tableName else: defaultCrudTable[T]()
  let insertData = collectInsertData(entity, idField)
  let cols = insertData.cols
  let params = insertData.params

  if cols.len == 0:
    raise newException(ValueError, "No columns found for INSERT")

  let quoted = cols.mapIt(db.dialect.quoteIdent(it)).join(", ")
  let placeholders = cols.mapIt("?").join(", ")
  let sql = "INSERT INTO " & db.dialect.quoteIdent(targetTable) &
    " (" & quoted & ") VALUES (" & placeholders & ")"

  await db.execute(sql, params)

proc upsertInternal[T](
  db: DbConnection,
  entity: T,
  conflictFields: seq[string],
  updateFields: seq[string],
  tableName = "",
  idField = "id"
): Future[SqlResult] {.async.} =
  if db.isNil:
    raise newException(NimtraDbError, "Database handle is nil")

  var normalizedConflict: seq[string]
  for field in conflictFields:
    let trimmed = field.strip()
    if trimmed.len == 0:
      continue
    normalizedConflict.add(trimmed)

  if normalizedConflict.len == 0:
    raise newException(ValueError, "conflictFields must contain at least one field")

  let targetTable = if tableName.len > 0: tableName else: defaultCrudTable[T]()
  let insertData = collectInsertData(entity, idField)
  let cols = insertData.cols
  let params = insertData.params

  if cols.len == 0:
    raise newException(ValueError, "No columns found for UPSERT")

  for conflictField in normalizedConflict:
    if not containsFieldName(cols, conflictField):
      raise newException(
        ValueError,
        "Conflict field '" & conflictField & "' is not present in UPSERT columns"
      )

  var finalUpdateFields: seq[string]
  if updateFields.len > 0:
    for field in updateFields:
      let trimmed = field.strip()
      if trimmed.len == 0:
        continue
      if eqFieldName(trimmed, idField):
        continue
      if not containsFieldName(cols, trimmed):
        raise newException(
          ValueError,
          "Update field '" & trimmed & "' is not present in UPSERT columns"
        )
      if not containsFieldName(finalUpdateFields, trimmed):
        finalUpdateFields.add(trimmed)
  else:
    for col in cols:
      if eqFieldName(col, idField):
        continue
      if containsFieldName(normalizedConflict, col):
        continue
      finalUpdateFields.add(col)

  let quotedCols = cols.mapIt(db.dialect.quoteIdent(it)).join(", ")
  let placeholders = cols.mapIt("?").join(", ")
  let conflictSql = normalizedConflict.mapIt(db.dialect.quoteIdent(it)).join(", ")

  var sql = "INSERT INTO " & db.dialect.quoteIdent(targetTable) &
    " (" & quotedCols & ") VALUES (" & placeholders & ") ON CONFLICT (" & conflictSql & ") "

  if finalUpdateFields.len == 0:
    sql.add("DO NOTHING")
  else:
    let updateSql = finalUpdateFields.mapIt(
      db.dialect.quoteIdent(it) & " = excluded." & db.dialect.quoteIdent(it)
    ).join(", ")
    sql.add("DO UPDATE SET " & updateSql)

  await db.execute(sql, params)

proc upsertReturningIdInternal[T](
  db: DbConnection,
  entity: T,
  conflictFields: seq[string],
  updateFields: seq[string],
  tableName = "",
  idField = "id"
): Future[Option[int64]] {.async.} =
  let res = await upsertInternal(db, entity, conflictFields, updateFields, tableName, idField)
  res.lastInsertRowId

proc upsert* [T](
  db: DbConnection,
  entity: T,
  conflictFields: openArray[string],
  updateFields: openArray[string] = [],
  tableName = "",
  idField = "id"
): Future[SqlResult] =
  var conflictCopy = newSeqOfCap[string](conflictFields.len)
  for field in conflictFields:
    conflictCopy.add(field)
  var updateCopy = newSeqOfCap[string](updateFields.len)
  for field in updateFields:
    updateCopy.add(field)
  upsertInternal(db, entity, conflictCopy, updateCopy, tableName, idField)

proc upsert* [T](
  db: DbConnection,
  entity: T,
  conflictField: string,
  updateFields: openArray[string] = [],
  tableName = "",
  idField = "id"
): Future[SqlResult] =
  var updateCopy = newSeqOfCap[string](updateFields.len)
  for field in updateFields:
    updateCopy.add(field)
  upsertInternal(db, entity, @[conflictField], updateCopy, tableName, idField)

proc upsertReturningId* [T](
  db: DbConnection,
  entity: T,
  conflictFields: openArray[string],
  updateFields: openArray[string] = [],
  tableName = "",
  idField = "id"
): Future[Option[int64]] =
  var conflictCopy = newSeqOfCap[string](conflictFields.len)
  for field in conflictFields:
    conflictCopy.add(field)
  var updateCopy = newSeqOfCap[string](updateFields.len)
  for field in updateFields:
    updateCopy.add(field)
  upsertReturningIdInternal(db, entity, conflictCopy, updateCopy, tableName, idField)

proc insertReturningId* [T](
  db: DbConnection,
  entity: T,
  tableName = "",
  idField = "id"
): Future[Option[int64]] {.async.} =
  let res = await db.insert(entity, tableName, idField)
  res.lastInsertRowId

proc updateById* [T](
  db: DbConnection,
  entity: T,
  tableName = "",
  idField = "id"
): Future[SqlResult] {.async.} =
  if db.isNil:
    raise newException(NimtraDbError, "Database handle is nil")

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

proc deleteById* [T, I](
  db: DbConnection,
  _: typedesc[T],
  id: I,
  tableName = "",
  idField = "id"
): Future[SqlResult] {.async.} =
  if db.isNil:
    raise newException(NimtraDbError, "Database handle is nil")

  let targetTable = if tableName.len > 0: tableName else: defaultCrudTable[T]()
  let sql = "DELETE FROM " & db.dialect.quoteIdent(targetTable) &
    " WHERE " & db.dialect.quoteIdent(idField) & " = ?"

  await db.execute(sql, @[toSqlValue(id)])

proc findById* [T, I](
  db: DbConnection,
  _: typedesc[T],
  id: I,
  tableName = "",
  idField = "id"
): Future[Option[SqlRow]] {.async.} =
  if db.isNil:
    raise newException(NimtraDbError, "Database handle is nil")

  let targetTable = if tableName.len > 0: tableName else: defaultCrudTable[T]()
  let sql = "SELECT * FROM " & db.dialect.quoteIdent(targetTable) &
    " WHERE " & db.dialect.quoteIdent(idField) & " = ? LIMIT 1"
  let res = await db.query(sql, @[toSqlValue(id)])
  if res.rows.len == 0:
    return none(SqlRow)
  some(res.rows[0])

proc findByIdModel* [T, I](
  db: DbConnection,
  modelType: typedesc[T],
  id: I,
  tableName = "",
  idField = "id"
): Future[Option[T]] {.async.} =
  let row = await db.findById(modelType, id, tableName, idField)
  rowOptionToModel[T](row)

proc findAll* [T](
  db: DbConnection,
  _: typedesc[T],
  tableName = ""
): Future[seq[SqlRow]] {.async.} =
  if db.isNil:
    raise newException(NimtraDbError, "Database handle is nil")

  let targetTable = if tableName.len > 0: tableName else: defaultCrudTable[T]()
  let sql = "SELECT * FROM " & db.dialect.quoteIdent(targetTable)
  let res = await db.query(sql)
  res.rows

proc findAllModels* [T](
  db: DbConnection,
  modelType: typedesc[T],
  tableName = ""
): Future[seq[T]] {.async.} =
  let rows = await db.findAll(modelType, tableName)
  rowsToModels[T](rows)

proc existsById* [T, I](
  db: DbConnection,
  modelType: typedesc[T],
  id: I,
  tableName = "",
  idField = "id"
): Future[bool] {.async.} =
  let row = await db.findById(modelType, id, tableName, idField)
  row.isSome
