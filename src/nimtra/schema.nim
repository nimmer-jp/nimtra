import std/[options, sequtils, strutils, typetraits]

import ./[dialects, model]

const
  DefaultIndexPrefix* = "idx"

proc sqlStringLiteral(value: string): string =
  "'" & value.replace("'", "''") & "'"

proc looksLikeSqlExpression(value: string): bool =
  if value.len == 0:
    return false

  let trimmed = value.strip()
  if trimmed.len == 0:
    return false

  let upper = trimmed.toUpperAscii()
  if upper in ["NULL", "TRUE", "FALSE", "CURRENT_TIMESTAMP", "CURRENT_DATE", "CURRENT_TIME"]:
    return true

  if trimmed[0] == '(' and trimmed[^1] == ')':
    return true

  let numericChars = {'0'..'9', '.', '+', '-', 'e', 'E'}
  if trimmed.allCharsInSet(numericChars):
    return true

  false

proc renderDefaultClause(field: FieldMeta): string =
  if field.defaultValue.isNone:
    return ""

  let raw = field.defaultValue.get().strip()
  if raw.len == 0:
    return ""

  if looksLikeSqlExpression(raw):
    " DEFAULT " & raw
  else:
    " DEFAULT " & sqlStringLiteral(raw)

method columnDefinitionSqlImpl*(dialect: Dialect, field: FieldMeta, includePrimaryAndUnique: bool): string {.base.} =
  let colName = dialect.quoteIdent(field.name)
  result = colName & " " & field.dbType
  if includePrimaryAndUnique and field.primary:
    result.add(" PRIMARY KEY")
  if includePrimaryAndUnique and field.unique:
    result.add(" UNIQUE")
  if field.maxLength.isSome and field.dbType == "TEXT":
    result.add(" CHECK (length(" & colName & ") <= " & $field.maxLength.get() & ")")
  result.add(renderDefaultClause(field))

method columnDefinitionSqlImpl*(dialect: SQLiteDialect, field: FieldMeta, includePrimaryAndUnique: bool): string =
  let colName = dialect.quoteIdent(field.name)
  if includePrimaryAndUnique and field.primary and field.autoincrement and field.dbType == "INTEGER":
    return colName & " INTEGER PRIMARY KEY AUTOINCREMENT"
  result = colName & " " & field.dbType
  if includePrimaryAndUnique and field.primary:
    result.add(" PRIMARY KEY")
  if includePrimaryAndUnique and field.unique:
    result.add(" UNIQUE")
  if field.maxLength.isSome and field.dbType == "TEXT":
    result.add(" CHECK (length(" & colName & ") <= " & $field.maxLength.get() & ")")
  result.add(renderDefaultClause(field))

method columnDefinitionSqlImpl*(dialect: PostgresDialect, field: FieldMeta, includePrimaryAndUnique: bool): string =
  let colName = dialect.quoteIdent(field.name)
  var dbType = field.dbType
  if includePrimaryAndUnique and field.primary and field.autoincrement and dbType == "INTEGER":
    dbType = "SERIAL"
  elif dbType == "DATETIME":
    dbType = "TIMESTAMP WITH TIME ZONE"

  result = colName & " " & dbType
  if includePrimaryAndUnique and field.primary:
    result.add(" PRIMARY KEY")
  if includePrimaryAndUnique and field.unique:
    result.add(" UNIQUE")
  if field.maxLength.isSome and field.dbType == "TEXT":
    result.add(" CHECK (length(" & colName & ") <= " & $field.maxLength.get() & ")")
  result.add(renderDefaultClause(field))

method columnDefinitionSqlImpl*(dialect: MySQLDialect, field: FieldMeta, includePrimaryAndUnique: bool): string =
  let colName = dialect.quoteIdent(field.name)
  var dbType = field.dbType
  if includePrimaryAndUnique and field.primary and field.autoincrement and dbType == "INTEGER":
    dbType = "INT AUTO_INCREMENT"

  result = colName & " " & dbType
  if includePrimaryAndUnique and field.primary:
    result.add(" PRIMARY KEY")
  if includePrimaryAndUnique and field.unique:
    result.add(" UNIQUE")
  if field.maxLength.isSome and field.dbType == "TEXT":
    result.add(" VARCHAR(" & $field.maxLength.get() & ")") # MySQL check constraints on text are tricky, standard is varchar
  result.add(renderDefaultClause(field))

proc columnDefinitionSql*(
  field: FieldMeta,
  dialect: Dialect = nil,
  includePrimaryAndUnique = true
): string =
  let d = if dialect.isNil: newSQLiteDialect() else: dialect
  return columnDefinitionSqlImpl(d, field, includePrimaryAndUnique)

proc createTableSql*(
  meta: ModelMeta,
  dialect: Dialect = nil,
  ifNotExists = true
): string =
  if meta.fields.len == 0:
    raise newException(ValueError, "Model has no fields: " & meta.name)

  let d = if dialect.isNil: newSQLiteDialect() else: dialect
  let fieldSql = meta.fields.mapIt(columnDefinitionSql(it, d)).join(", ")

  "CREATE TABLE " &
    (if ifNotExists: "IF NOT EXISTS " else: "") &
    d.quoteIdent(meta.table) &
    " (" & fieldSql & ")"

proc createTableSql*[T](
  modelType: typedesc[T],
  dialect: Dialect = nil,
  ifNotExists = true
): string =
  createTableSql(modelMeta(modelType), dialect, ifNotExists)

proc createIndexSql*(
  meta: ModelMeta,
  field: FieldMeta,
  dialect: Dialect = nil,
  ifNotExists = true,
  indexPrefix = DefaultIndexPrefix
): string =
  if not field.indexed:
    raise newException(ValueError, "Field is not marked with {.index.}: " & field.name)

  let d = if dialect.isNil: newSQLiteDialect() else: dialect
  let indexName = indexPrefix & "_" & meta.table & "_" & field.name

  "CREATE INDEX " &
    (if ifNotExists: "IF NOT EXISTS " else: "") &
    d.quoteIdent(indexName) &
    " ON " & d.quoteIdent(meta.table) &
    " (" & d.quoteIdent(field.name) & ")"

proc createIndexesSql*(
  meta: ModelMeta,
  dialect: Dialect = nil,
  ifNotExists = true,
  indexPrefix = DefaultIndexPrefix
): seq[string] =
  let d = if dialect.isNil: newSQLiteDialect() else: dialect
  for field in meta.fields:
    if field.indexed and not field.unique:
      result.add(createIndexSql(meta, field, d, ifNotExists, indexPrefix))

proc createIndexesSql*[T](
  modelType: typedesc[T],
  dialect: Dialect = nil,
  ifNotExists = true,
  indexPrefix = DefaultIndexPrefix
): seq[string] =
  createIndexesSql(modelMeta(modelType), dialect, ifNotExists, indexPrefix)

proc createSchemaSql*(
  meta: ModelMeta,
  dialect: Dialect = nil,
  ifNotExists = true,
  indexPrefix = DefaultIndexPrefix
): seq[string] =
  let d = if dialect.isNil: newSQLiteDialect() else: dialect
  result = @[createTableSql(meta, d, ifNotExists)]
  result.add(createIndexesSql(meta, d, ifNotExists, indexPrefix))

proc createSchemaSql*[T](
  modelType: typedesc[T],
  dialect: Dialect = nil,
  ifNotExists = true,
  indexPrefix = DefaultIndexPrefix
): seq[string] =
  createSchemaSql(modelMeta(modelType), dialect, ifNotExists, indexPrefix)
