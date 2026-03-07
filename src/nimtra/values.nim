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

proc parseDateTimeFlexible(value: string): DateTime =
  let trimmed = value.strip()
  if trimmed.len == 0:
    raise newException(ValueError, "Cannot parse empty datetime")

  let formats = [
    "yyyy-MM-dd'T'HH:mm:sszzz",
    "yyyy-MM-dd'T'HH:mm:ss'.'fffzzz",
    "yyyy-MM-dd'T'HH:mm:ss'Z'",
    "yyyy-MM-dd'T'HH:mm:ss",
    "yyyy-MM-dd HH:mm:ss",
    "yyyy-MM-dd"
  ]

  for fmt in formats:
    try:
      return parse(trimmed, fmt)
    except CatchableError:
      discard

  try:
    return fromUnix(parseInt(trimmed)).utc
  except CatchableError:
    raise newException(ValueError, "Invalid datetime format: " & value)

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

proc asFloat64*(value: SqlValue): float64 =
  case value.kind
  of svFloat:
    value.floatValue
  of svInteger:
    float64(value.intValue)
  of svText:
    parseFloat(value.textValue)
  else:
    raise newException(ValueError, "SqlValue is not float")

proc asBool*(value: SqlValue): bool =
  case value.kind
  of svInteger:
    value.intValue != 0
  of svFloat:
    value.floatValue != 0.0
  of svText:
    let normalized = value.textValue.strip().toLowerAscii()
    normalized in ["1", "t", "true", "yes", "y", "on"]
  of svNull:
    false
  of svBlob:
    raise newException(ValueError, "SqlValue blob cannot convert to bool")

proc asDateTime*(value: SqlValue): DateTime =
  case value.kind
  of svText:
    parseDateTimeFlexible(value.textValue)
  of svInteger:
    fromUnix(value.intValue).utc
  of svFloat:
    fromUnix(int64(value.floatValue)).utc
  else:
    raise newException(ValueError, "SqlValue cannot convert to DateTime")

proc isNull*(value: SqlValue): bool =
  value.kind == svNull

proc fromSqlValue*(value: SqlValue, _: typedesc[SqlValue]): SqlValue =
  value

proc fromSqlValue*[T: SomeInteger](value: SqlValue, _: typedesc[T]): T =
  let n = value.asInt64()
  when T is uint or T is uint8 or T is uint16 or T is uint32 or T is uint64:
    if n < 0:
      raise newException(ValueError, "Negative SQL value cannot convert to unsigned integer")
  T(n)

proc fromSqlValue*[T: SomeFloat](value: SqlValue, _: typedesc[T]): T =
  T(value.asFloat64())

proc fromSqlValue*(value: SqlValue, _: typedesc[bool]): bool =
  value.asBool()

proc fromSqlValue*(value: SqlValue, _: typedesc[string]): string =
  value.asString()

proc fromSqlValue*(value: SqlValue, _: typedesc[cstring]): cstring =
  value.asString().cstring

proc fromSqlValue*(value: SqlValue, _: typedesc[DateTime]): DateTime =
  value.asDateTime()

proc fromSqlValue*(value: SqlValue, _: typedesc[seq[byte]]): seq[byte] =
  case value.kind
  of svBlob:
    value.blobValue
  of svText:
    var bytesOut: seq[byte]
    bytesOut.setLen(value.textValue.len)
    for i, ch in value.textValue:
      bytesOut[i] = byte(ch)
    bytesOut
  else:
    raise newException(ValueError, "SqlValue cannot convert to seq[byte]")

proc fromSqlValue*[T](value: SqlValue, _: typedesc[Option[T]]): Option[T] =
  if value.isNull:
    return none(T)
  some(fromSqlValue(value, T))

proc fromSqlValue*[T](value: SqlValue, _: typedesc[T]): T =
  raise newException(ValueError, "Unsupported fromSqlValue target type")

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
