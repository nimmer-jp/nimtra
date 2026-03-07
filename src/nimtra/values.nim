import std/[json, options, strutils, tables, times]

type
  SqlValueKind* = enum
    svNull,
    svInteger,
    svFloat,
    svText,
    svBlob

  SqlValue* = object
    case kind*: SqlValueKind
    of svNull:
      discard
    of svInteger:
      intValue*: int64
    of svFloat:
      floatValue*: float64
    of svText:
      textValue*: string
    of svBlob:
      blobValue*: seq[byte]

  SqlColumn* = object
    name*: string
    decltype*: string

  SqlRow* = OrderedTable[string, SqlValue]

  SqlResult* = object
    columns*: seq[SqlColumn]
    rows*: seq[SqlRow]
    affectedRowCount*: int
    lastInsertRowId*: Option[int64]

  SqlStatement* = object
    sql*: string
    params*: seq[SqlValue]

proc nullValue*(): SqlValue =
  SqlValue(kind: svNull)

proc toSqlValue*(value: SqlValue): SqlValue =
  value

proc toSqlValue*[T: SomeInteger](value: T): SqlValue =
  SqlValue(kind: svInteger, intValue: int64(value))

proc toSqlValue*[T: SomeFloat](value: T): SqlValue =
  SqlValue(kind: svFloat, floatValue: float64(value))

proc toSqlValue*(value: bool): SqlValue =
  SqlValue(kind: svInteger, intValue: (if value: 1'i64 else: 0'i64))

proc toSqlValue*(value: string): SqlValue =
  SqlValue(kind: svText, textValue: value)

proc toSqlValue*(value: cstring): SqlValue =
  SqlValue(kind: svText, textValue: $value)

proc toSqlValue*(value: seq[byte]): SqlValue =
  SqlValue(kind: svBlob, blobValue: value)

proc toSqlValue*(value: DateTime): SqlValue =
  SqlValue(kind: svText, textValue: value.format("yyyy-MM-dd'T'HH:mm:sszzz"))

proc toSqlValue*[T](value: Option[T]): SqlValue =
  if value.isSome:
    toSqlValue(value.get)
  else:
    nullValue()

proc asInt64*(value: SqlValue): int64 =
  case value.kind
  of svInteger:
    value.intValue
  of svFloat:
    int64(value.floatValue)
  of svText:
    parseInt(value.textValue).int64
  else:
    raise newException(ValueError, "SqlValue is not numeric")

proc asString*(value: SqlValue): string =
  case value.kind
  of svText:
    value.textValue
  of svInteger:
    $value.intValue
  of svFloat:
    $value.floatValue
  of svNull:
    ""
  of svBlob:
    "<blob>"

proc isNull*(value: SqlValue): bool =
  value.kind == svNull

proc toJson*(value: SqlValue): JsonNode =
  case value.kind
  of svNull:
    newJNull()
  of svInteger:
    %value.intValue
  of svFloat:
    %value.floatValue
  of svText:
    %value.textValue
  of svBlob:
    var arr = newJArray()
    for b in value.blobValue:
      arr.add(%int(b))
    arr

proc fromJsonScalar*(node: JsonNode): SqlValue =
  case node.kind
  of JNull:
    nullValue()
  of JInt:
    SqlValue(kind: svInteger, intValue: node.getBiggestInt().int64)
  of JFloat:
    SqlValue(kind: svFloat, floatValue: node.getFloat())
  of JString:
    SqlValue(kind: svText, textValue: node.getStr())
  of JBool:
    SqlValue(kind: svInteger, intValue: (if node.getBool(): 1'i64 else: 0'i64))
  else:
    SqlValue(kind: svText, textValue: $node)

proc `$`*(value: SqlValue): string =
  case value.kind
  of svNull:
    "NULL"
  of svInteger:
    $value.intValue
  of svFloat:
    $value.floatValue
  of svText:
    value.textValue
  of svBlob:
    "<blob:" & $value.blobValue.len & ">"
