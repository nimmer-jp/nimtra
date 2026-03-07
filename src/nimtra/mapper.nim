import std/[options, strutils, tables]

import ./utils
import ./values

type
  ModelMappingError* = object of ValueError

proc resolveColumnKey(row: SqlRow, fieldName: string): Option[string] =
  if row.hasKey(fieldName):
    return some(fieldName)

  let snakeCase = camelToSnake(fieldName)
  if snakeCase != fieldName and row.hasKey(snakeCase):
    return some(snakeCase)

  let lowerField = fieldName.toLowerAscii()
  let lowerSnake = snakeCase.toLowerAscii()
  for key in row.keys:
    let lowerKey = key.toLowerAscii()
    if lowerKey == lowerField or lowerKey == lowerSnake:
      return some(key)

  none(string)

proc fieldValueFromRow[T](row: SqlRow, columnKey, fieldName: string): T =
  if not row.hasKey(columnKey):
    raise newException(ModelMappingError, "Missing column in row: " & columnKey)

  try:
    fromSqlValue(row[columnKey], T)
  except CatchableError as e:
    raise newException(
      ModelMappingError,
      "Failed to map column '" & columnKey & "' to field '" & fieldName & "': " & e.msg
    )

proc rowToModel*[T](row: SqlRow): T =
  when T is ref object:
    new(result)
    for fieldName, fieldValue in fieldPairs(result[]):
      let columnKey = resolveColumnKey(row, fieldName)
      if columnKey.isSome:
        fieldValue = fieldValueFromRow[type(fieldValue)](row, columnKey.get(), fieldName)
  else:
    var model: T
    for fieldName, fieldValue in fieldPairs(model):
      let columnKey = resolveColumnKey(row, fieldName)
      if columnKey.isSome:
        fieldValue = fieldValueFromRow[type(fieldValue)](row, columnKey.get(), fieldName)
    result = model

proc rowsToModels*[T](rows: openArray[SqlRow]): seq[T] =
  result = newSeqOfCap[T](rows.len)
  for row in rows:
    result.add(rowToModel[T](row))

proc rowOptionToModel*[T](row: Option[SqlRow]): Option[T] =
  if row.isNone:
    return none(T)
  some(rowToModel[T](row.get()))
