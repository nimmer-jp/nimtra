import std/[asyncdispatch, macros, options, sequtils, strutils, typetraits]

import ./[dialects, utils, values]
import ./driver/libsql_http

type
  Query* [T] = object
    db*: LibSQLConnection
    dialect*: Dialect
    table*: string
    selectedColumns*: seq[string]
    whereClauses*: seq[string]
    params*: seq[SqlValue]
    orderByClause*: string
    limitValue*: Option[int]

proc quotedTableName[T](query: Query[T]): string =
  query.dialect.quoteIdent(query.table)

proc defaultModelTable[T](): string =
  defaultTableName(name(T))

proc select* [T](
  db: LibSQLConnection,
  _: typedesc[T],
  tableName = ""
): Query[T] =
  let resolvedTable = if tableName.len > 0: tableName else: defaultModelTable[T]()
  result = Query[T](
    db: db,
    dialect: if not db.isNil and not db.dialect.isNil: db.dialect else: newSQLiteDialect(),
    table: resolvedTable,
    selectedColumns: @[],
    whereClauses: @[],
    params: @[],
    orderByClause: "",
    limitValue: none(int)
  )

proc select* [T](
  _: typedesc[T],
  tableName = "",
  dialect: Dialect = nil
): Query[T] =
  let resolvedTable = if tableName.len > 0: tableName else: defaultModelTable[T]()
  result = Query[T](
    db: nil,
    dialect: if not dialect.isNil: dialect else: newSQLiteDialect(),
    table: resolvedTable,
    selectedColumns: @[],
    whereClauses: @[],
    params: @[],
    orderByClause: "",
    limitValue: none(int)
  )

proc columns* [T](query: Query[T], cols: varargs[string]): Query[T] =
  result = query
  for col in cols:
    result.selectedColumns.add(col)

proc whereRaw* [T](query: Query[T], clause: string, args: openArray[SqlValue] = []): Query[T] =
  result = query
  result.whereClauses.add(clause)
  result.params.add(args)

proc orderBy* [T](query: Query[T], column: string, descending = false): Query[T] =
  result = query
  result.orderByClause = query.dialect.quoteIdent(column) & (if descending: " DESC" else: " ASC")

proc limit* [T](query: Query[T], n: int): Query[T] =
  if n < 0:
    raise newException(ValueError, "LIMIT must be >= 0")
  result = query
  result.limitValue = some(n)

proc build* [T](query: Query[T]): SqlStatement =
  if query.table.len == 0:
    raise newException(ValueError, "Query table name is empty")

  let selectPart =
    if query.selectedColumns.len == 0:
      "*"
    else:
      query.selectedColumns.mapIt(query.dialect.quoteIdent(it)).join(", ")

  var sql = "SELECT " & selectPart & " FROM " & query.quotedTableName()

  if query.whereClauses.len > 0:
    sql.add(" WHERE " & query.whereClauses.join(" AND "))

  if query.orderByClause.len > 0:
    sql.add(" ORDER BY " & query.orderByClause)

  if query.limitValue.isSome:
    sql.add(" LIMIT " & $query.limitValue.get())

  sql = query.dialect.applyPlaceholders(sql)
  SqlStatement(sql: sql, params: query.params)

type
  CompiledPredicate = tuple
    sql: string
    args: seq[NimNode]

proc extractFieldName(node: NimNode): string {.compileTime.} =
  case node.kind
  of nnkIdent, nnkSym:
    $node
  of nnkPragmaExpr:
    extractFieldName(node[0])
  else:
    ""

proc objectFields(typeImpl: NimNode): seq[string] {.compileTime.} =
  var ty = typeImpl[2]
  if ty.kind == nnkRefTy:
    ty = ty[0]

  if ty.kind != nnkObjectTy:
    error("where() currently supports object/ref object model types", typeImpl)

  let recList = ty[2]
  for item in recList:
    if item.kind != nnkIdentDefs:
      continue
    for i in 0 ..< item.len - 2:
      let name = extractFieldName(item[i])
      if name.len > 0:
        result.add(name)

proc modelFieldNames(modelType: NimNode): seq[string] {.compileTime.} =
  let impl = modelType.getImpl
  if impl.kind != nnkTypeDef:
    error("Could not resolve model type for where()", modelType)
  objectFields(impl)

proc isModelField(node: NimNode): bool {.compileTime.} =
  node.kind == nnkDotExpr and
    node.len == 2 and
    node[0].kind in {nnkIdent, nnkSym} and
    $node[0] == "it" and
    node[1].kind in {nnkIdent, nnkSym}

proc escapeField(field: string): string {.compileTime.} =
  "\"" & field.replace("\"", "\"\"") & "\""

proc mergePred(a, b: CompiledPredicate, joiner: string): CompiledPredicate {.compileTime.} =
  result.sql = "(" & a.sql & " " & joiner & " " & b.sql & ")"
  result.args = a.args
  result.args.add(b.args)

proc compileComparison(node: NimNode, fields: seq[string]): CompiledPredicate {.compileTime.} =
  let op = $node[0]
  let lhs = node[1]
  let rhs = node[2]

  proc ensureField(name: string) {.compileTime.} =
    if name notin fields:
      error("Unknown field '" & name & "' in where()", node)

  var sqlOp = op
  case op
  of "==":
    sqlOp = "="
  of "!=":
    sqlOp = "<>"
  of ">", ">=", "<", "<=":
    discard
  else:
    error("Unsupported comparison operator: " & op, node)

  if isModelField(lhs):
    let field = $lhs[1]
    ensureField(field)
    if rhs.kind == nnkNilLit and sqlOp in ["=", "<>"]:
      result.sql = escapeField(field) & (if sqlOp == "=": " IS NULL" else: " IS NOT NULL")
      return
    result.sql = escapeField(field) & " " & sqlOp & " ?"
    result.args = @[rhs]
    return

  if isModelField(rhs):
    let field = $rhs[1]
    ensureField(field)
    if lhs.kind == nnkNilLit and sqlOp in ["=", "<>"]:
      result.sql = escapeField(field) & (if sqlOp == "=": " IS NULL" else: " IS NOT NULL")
      return
    result.sql = "? " & sqlOp & " " & escapeField(field)
    result.args = @[lhs]
    return

  error("A comparison must include at least one model field like it.field", node)

proc compilePredicate(node: NimNode, fields: seq[string]): CompiledPredicate {.compileTime.} =
  case node.kind
  of nnkPar:
    result = compilePredicate(node[0], fields)
  of nnkInfix:
    let op = $node[0]
    case op
    of "and", "or":
      let left = compilePredicate(node[1], fields)
      let right = compilePredicate(node[2], fields)
      result = mergePred(left, right, op.toUpperAscii())
    of "==", "!=", ">", ">=", "<", "<=":
      result = compileComparison(node, fields)
    else:
      error("Unsupported infix operator in where(): " & op, node)
  of nnkPrefix:
    if $node[0] != "not":
      error("Unsupported prefix operator in where()", node)
    let inner = compilePredicate(node[1], fields)
    result.sql = "(NOT " & inner.sql & ")"
    result.args = inner.args
  else:
    error("Unsupported expression in where()", node)

macro where*(query: typed, predicate: untyped): untyped =
  let qType = query.getTypeInst
  if qType.kind != nnkBracketExpr or qType.len < 2 or $qType[0] != "Query":
    error("where() must be called on Query[T]", query)

  let modelType = qType[1]
  let fields = modelFieldNames(modelType)
  let compiled = compilePredicate(predicate, fields)

  let sqlNode = newLit(compiled.sql)
  var argConversions = newSeq[NimNode]()
  for arg in compiled.args:
    argConversions.add(quote do: toSqlValue(`arg`))

  let argsNode = nnkPrefix.newTree(ident("@"), nnkBracket.newTree(argConversions))

  result = quote do:
    block:
      var nextQuery = `query`
      nextQuery.whereClauses.add(`sqlNode`)
      nextQuery.params.add(`argsNode`)
      nextQuery

proc all* [T](query: Query[T]): Future[seq[SqlRow]] {.async.} =
  if query.db.isNil:
    raise newException(LibSQLError, "Query has no bound database. Use db.select(Model)")
  let stmt = query.build()
  let res = await query.db.query(stmt)
  res.rows

proc first* [T](query: Query[T]): Future[Option[SqlRow]] {.async.} =
  let rows = await query.limit(1).all()
  if rows.len == 0:
    return none(SqlRow)
  some(rows[0])

proc count* [T](query: Query[T]): Future[int] {.async.} =
  if query.db.isNil:
    raise newException(LibSQLError, "Query has no bound database. Use db.select(Model)")

  let built = query.build()
  let countSql = "SELECT COUNT(*) AS \"count\" FROM (" & built.sql & ") AS __nimtra_count"
  let res = await query.db.query(countSql, built.params)
  if res.rows.len == 0:
    return 0
  if not res.rows[0].hasKey("count"):
    return 0
  int(res.rows[0]["count"].asInt64())
