import std/[asyncdispatch, macros, options, sequtils, strutils]

import ./[dialects, mapper, model, values]
import ./driver/base

type
  JoinType* = enum
    jtInner,
    jtLeft,
    jtRight,
    jtFull,
    jtCross

  Query* [T] = object
    db*: DbConnection
    dialect*: Dialect
    table*: string
    tableIsRaw*: bool
    selectedColumns*: seq[string]
    rawSelectedColumns*: seq[string]
    joinClauses*: seq[string]
    whereClauses*: seq[string]
    params*: seq[SqlValue]
    orderByClauses*: seq[string]
    limitValue*: Option[int]
    offsetValue*: Option[int]

proc quotedTableName[T](query: Query[T]): string =
  if query.tableIsRaw:
    query.table
  else:
    query.dialect.quoteIdent(query.table)

proc defaultModelTable[T](): string =
  modelTableName(T)

proc select* [T](
  db: DbConnection,
  _: typedesc[T],
  tableName = ""
): Query[T] =
  let resolvedTable = if tableName.len > 0: tableName else: defaultModelTable[T]()
  result = Query[T](
    db: db,
    dialect: if not db.isNil and not db.dialect.isNil: db.dialect else: newSQLiteDialect(),
    table: resolvedTable,
    tableIsRaw: false,
    selectedColumns: @[],
    rawSelectedColumns: @[],
    joinClauses: @[],
    whereClauses: @[],
    params: @[],
    orderByClauses: @[],
    limitValue: none(int),
    offsetValue: none(int)
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
    tableIsRaw: false,
    selectedColumns: @[],
    rawSelectedColumns: @[],
    joinClauses: @[],
    whereClauses: @[],
    params: @[],
    orderByClauses: @[],
    limitValue: none(int),
    offsetValue: none(int)
  )

proc columns* [T](query: Query[T], cols: varargs[string]): Query[T] =
  result = query
  for col in cols:
    result.selectedColumns.add(col)

proc columnsRaw* [T](query: Query[T], cols: varargs[string]): Query[T] =
  result = query
  for col in cols:
    let trimmed = col.strip()
    if trimmed.len == 0:
      raise newException(ValueError, "Raw column expression cannot be empty")
    result.rawSelectedColumns.add(trimmed)

proc fromRaw* [T](query: Query[T], tableExpr: string): Query[T] =
  let trimmed = tableExpr.strip()
  if trimmed.len == 0:
    raise newException(ValueError, "FROM expression cannot be empty")
  result = query
  result.table = trimmed
  result.tableIsRaw = true

proc joinKeyword(joinType: JoinType): string =
  case joinType
  of jtInner:
    "INNER JOIN"
  of jtLeft:
    "LEFT JOIN"
  of jtRight:
    "RIGHT JOIN"
  of jtFull:
    "FULL JOIN"
  of jtCross:
    "CROSS JOIN"

proc join* [T](
  query: Query[T],
  table: string,
  onClause: string,
  joinType: JoinType = jtInner,
  rawTable = false
): Query[T] =
  let tableExpr = table.strip()
  let onExpr = onClause.strip()
  if tableExpr.len == 0:
    raise newException(ValueError, "JOIN table cannot be empty")
  if joinType != jtCross and onExpr.len == 0:
    raise newException(ValueError, "JOIN ON clause cannot be empty")

  result = query
  let renderedTable = if rawTable: tableExpr else: query.dialect.quoteIdent(tableExpr)
  var clause = joinKeyword(joinType) & " " & renderedTable
  if joinType != jtCross:
    clause.add(" ON " & onExpr)
  result.joinClauses.add(clause)

proc leftJoin* [T](
  query: Query[T],
  table: string,
  onClause: string,
  rawTable = false
): Query[T] =
  query.join(table, onClause, jtLeft, rawTable)

proc joinRaw* [T](query: Query[T], clause: string): Query[T] =
  let trimmed = clause.strip()
  if trimmed.len == 0:
    raise newException(ValueError, "Raw JOIN clause cannot be empty")
  result = query
  result.joinClauses.add(trimmed)

proc whereRaw* [T](query: Query[T], clause: string, args: openArray[SqlValue] = []): Query[T] =
  result = query
  result.whereClauses.add(clause)
  result.params.add(args)

proc orderBy* [T](query: Query[T], column: string, descending = false): Query[T] =
  result = query
  result.orderByClauses.add(
    query.dialect.quoteIdent(column) & (if descending: " DESC" else: " ASC")
  )

proc orderByRaw* [T](query: Query[T], clause: string): Query[T] =
  let trimmed = clause.strip()
  if trimmed.len == 0:
    raise newException(ValueError, "ORDER BY clause cannot be empty")
  result = query
  result.orderByClauses.add(trimmed)

proc limit* [T](query: Query[T], n: int): Query[T] =
  if n < 0:
    raise newException(ValueError, "LIMIT must be >= 0")
  result = query
  result.limitValue = some(n)

proc offset* [T](query: Query[T], n: int): Query[T] =
  if n < 0:
    raise newException(ValueError, "OFFSET must be >= 0")
  result = query
  result.offsetValue = some(n)

proc paginate* [T](query: Query[T], page, perPage: int): Query[T] =
  if page <= 0:
    raise newException(ValueError, "page must be >= 1")
  if perPage <= 0:
    raise newException(ValueError, "perPage must be >= 1")
  result = query
  result.limitValue = some(perPage)
  result.offsetValue = some((page - 1) * perPage)

proc build* [T](query: Query[T]): SqlStatement =
  if query.table.len == 0:
    raise newException(ValueError, "Query table name is empty")

  var selectParts: seq[string]
  for col in query.selectedColumns:
    selectParts.add(query.dialect.quoteIdent(col))
  for expr in query.rawSelectedColumns:
    selectParts.add(expr)

  let selectPart =
    if selectParts.len == 0:
      "*"
    else:
      selectParts.join(", ")

  var sql = "SELECT " & selectPart & " FROM " & query.quotedTableName()

  if query.joinClauses.len > 0:
    sql.add(" " & query.joinClauses.join(" "))

  if query.whereClauses.len > 0:
    sql.add(" WHERE " & query.whereClauses.join(" AND "))

  if query.orderByClauses.len > 0:
    sql.add(" ORDER BY " & query.orderByClauses.join(", "))

  if query.limitValue.isSome:
    sql.add(" LIMIT " & $query.limitValue.get())

  if query.offsetValue.isSome:
    if query.limitValue.isNone:
      case query.dialect.name()
      of "postgres":
        sql.add(" LIMIT ALL")
      of "mysql":
        sql.add(" LIMIT 18446744073709551615")
      else:
        sql.add(" LIMIT -1")
    sql.add(" OFFSET " & $query.offsetValue.get())

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
  of nnkPostfix:
    if node.len >= 2:
      extractFieldName(node[1])
    else:
      ""
  of nnkPragmaExpr:
    extractFieldName(node[0])
  else:
    ""

proc resolveTypeDefNode(typeNode: NimNode): NimNode {.compileTime.} =
  let directImpl = typeNode.getImpl
  if directImpl.kind == nnkTypeDef:
    return directImpl

  let typeInst = typeNode.getTypeInst
  if typeInst.kind == nnkBracketExpr and typeInst.len >= 2:
    let candidate = typeInst[^1]
    let candidateImpl = candidate.getImpl
    if candidateImpl.kind == nnkTypeDef:
      return candidateImpl

  error("Could not resolve model type for where()", typeNode)

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
  objectFields(resolveTypeDefNode(modelType))

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

proc compileLikeCall(node: NimNode, fields: seq[string]): CompiledPredicate {.compileTime.} =
  proc ensureField(name: string) {.compileTime.} =
    if name notin fields:
      error("Unknown field '" & name & "' in where()", node)

  var methodName = ""
  var fieldExpr: NimNode = nil
  var argExpr: NimNode = nil

  if node.len == 2 and node[0].kind == nnkDotExpr:
    let callee = node[0]
    if callee.len == 2 and isModelField(callee[0]):
      methodName = $callee[1]
      fieldExpr = callee[0]
      argExpr = node[1]
  elif node.len == 3 and node[0].kind in {nnkIdent, nnkSym} and isModelField(node[1]):
    methodName = $node[0]
    fieldExpr = node[1]
    argExpr = node[2]

  if fieldExpr.isNil or argExpr.isNil:
    error("LIKE helpers must be called on a model field like it.title.contains(term)", node)

  let fieldName = $fieldExpr[1]
  ensureField(fieldName)

  result.sql = escapeField(fieldName) & " LIKE ?"

  case methodName
  of "contains":
    result.args = @[
      quote do:
        "%" & $`argExpr` & "%"
    ]
  of "startsWith":
    result.args = @[
      quote do:
        $`argExpr` & "%"
    ]
  of "endsWith":
    result.args = @[
      quote do:
        "%" & $`argExpr`
    ]
  of "like":
    result.args = @[argExpr]
  else:
    error("Unsupported call in where(): " & methodName, node)

proc compilePredicate(node: NimNode, fields: seq[string]): CompiledPredicate {.compileTime.} =
  case node.kind
  of nnkPar:
    result = compilePredicate(node[0], fields)
  of nnkCall:
    result = compileLikeCall(node, fields)
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
    raise newException(NimtraDbError, "Query has no bound database. Use db.select(Model)")
  let stmt = query.build()
  let res = await query.db.query(stmt)
  res.rows

proc allRows* [T](query: Query[T]): Future[seq[SqlRow]] {.async.} =
  await query.all()

proc allModels* [T](query: Query[T]): Future[seq[T]] {.async.} =
  let rows = await query.all()
  rowsToModels[T](rows)

proc first* [T](query: Query[T]): Future[Option[SqlRow]] {.async.} =
  let rows = await query.limit(1).all()
  if rows.len == 0:
    return none(SqlRow)
  some(rows[0])

proc firstRow* [T](query: Query[T]): Future[Option[SqlRow]] {.async.} =
  await query.first()

proc firstModel* [T](query: Query[T]): Future[Option[T]] {.async.} =
  let row = await query.first()
  rowOptionToModel[T](row)

proc oneRow* [T](query: Query[T]): Future[SqlRow] {.async.} =
  let rows = await query.limit(2).all()
  if rows.len == 0:
    raise newException(KeyError, "Expected one row but query returned none")
  if rows.len > 1:
    raise newException(ValueError, "Expected one row but query returned multiple rows")
  rows[0]

proc oneModel* [T](query: Query[T]): Future[T] {.async.} =
  let rows = await query.limit(2).all()
  if rows.len == 0:
    raise newException(KeyError, "Expected one row but query returned none")
  if rows.len > 1:
    raise newException(ValueError, "Expected one row but query returned multiple rows")
  rowToModel[T](rows[0])

proc count* [T](query: Query[T]): Future[int] {.async.} =
  if query.db.isNil:
    raise newException(NimtraDbError, "Query has no bound database. Use db.select(Model)")

  let built = query.build()
  let countSql = "SELECT COUNT(*) AS \"count\" FROM (" & built.sql & ") AS __nimtra_count"
  let res = await query.db.query(countSql, built.params)
  if res.rows.len == 0:
    return 0
  if not res.rows[0].hasKey("count"):
    return 0
  int(res.rows[0]["count"].asInt64())

proc exists* [T](query: Query[T]): Future[bool] {.async.} =
  if query.db.isNil:
    raise newException(NimtraDbError, "Query has no bound database. Use db.select(Model)")

  let built = query.limit(1).build()
  let existsSql = "SELECT EXISTS(SELECT 1 FROM (" & built.sql & ") AS __nimtra_exists) AS \"exists\""
  let res = await query.db.query(existsSql, built.params)
  if res.rows.len == 0:
    return false
  if not res.rows[0].hasKey("exists"):
    return false
  res.rows[0]["exists"].asBool()
