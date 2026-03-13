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
  of nnkAccQuoted:
    if node.len > 0:
      extractFieldName(node[0])
    else:
      ""
  else:
    ""

proc resolveTypeDefNode(typeNode: NimNode): NimNode {.compileTime.} =
  let directImpl = typeNode.getImpl
  if directImpl.kind == nnkTypeDef:
    return directImpl

  var current = typeNode.getTypeInst
  if current.kind == nnkBracketExpr and current.len >= 2:
    current = current[^1]

  let instImpl = current.getImpl
  if instImpl.kind == nnkTypeDef:
    return instImpl

  let tImpl = current.getTypeImpl
  if tImpl.kind == nnkRefTy or tImpl.kind == nnkObjectTy:
    return newTree(nnkTypeDef, newEmptyNode(), newEmptyNode(), tImpl)

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

proc unwrapModelField(node: NimNode): tuple[isField: bool, fieldName: string, isLower: bool] {.compileTime.} =
  if node.kind == nnkDotExpr and node.len == 2 and
     node[0].kind in {nnkIdent, nnkSym} and $node[0] == "it" and
     node[1].kind in {nnkIdent, nnkSym}:
    return (true, $node[1], false)
  
  if node.kind == nnkCall and node.len == 1 and node[0].kind == nnkDotExpr and node[0].len == 2:
    let innerDot = node[0]
    if innerDot[1].kind in {nnkIdent, nnkSym} and $innerDot[1] in ["lower", "toLowerAscii"]:
      let fieldDot = innerDot[0]
      if fieldDot.kind == nnkDotExpr and fieldDot.len == 2 and
         fieldDot[0].kind in {nnkIdent, nnkSym} and $fieldDot[0] == "it" and
         fieldDot[1].kind in {nnkIdent, nnkSym}:
        return (true, $fieldDot[1], true)
        
  if node.kind == nnkCall and node.len == 2 and node[0].kind in {nnkIdent, nnkSym} and $node[0] in ["lower", "toLowerAscii"]:
    let fieldDot = node[1]
    if fieldDot.kind == nnkDotExpr and fieldDot.len == 2 and
       fieldDot[0].kind in {nnkIdent, nnkSym} and $fieldDot[0] == "it" and
       fieldDot[1].kind in {nnkIdent, nnkSym}:
      return (true, $fieldDot[1], true)

  return (false, "", false)

proc isModelField(node: NimNode): bool {.compileTime.} =
  unwrapModelField(node).isField

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
  of "like", "ilike":
    sqlOp = "LIKE"
  else:
    error("Unsupported comparison operator: " & op, node)

  let lhsInfo = unwrapModelField(lhs)
  let rhsInfo = unwrapModelField(rhs)

  if lhsInfo.isField:
    let field = lhsInfo.fieldName
    ensureField(field)
    var leftSql = escapeField(field)
    if lhsInfo.isLower or op == "ilike": leftSql = "LOWER(" & leftSql & ")"
    
    if rhs.kind == nnkNilLit and sqlOp in ["=", "<>"]:
      result.sql = leftSql & (if sqlOp == "=": " IS NULL" else: " IS NOT NULL")
      return
    
    result.sql = leftSql & " " & sqlOp & " ?"
    if lhsInfo.isLower or op == "ilike":
      result.args = @[quote do: (`rhs`).toLowerAscii()]
    else:
      result.args = @[rhs]
    return

  if rhsInfo.isField:
    let field = rhsInfo.fieldName
    ensureField(field)
    var rightSql = escapeField(field)
    if rhsInfo.isLower or op == "ilike": rightSql = "LOWER(" & rightSql & ")"
    
    if lhs.kind == nnkNilLit and sqlOp in ["=", "<>"]:
      result.sql = rightSql & (if sqlOp == "=": " IS NULL" else: " IS NOT NULL")
      return
      
    result.sql = "? " & sqlOp & " " & rightSql
    if rhsInfo.isLower or op == "ilike":
      result.args = @[quote do: (`lhs`).toLowerAscii()]
    else:
      result.args = @[lhs]
    return

  error("A comparison must include at least one model field like it.field", node)

proc compileLikeCall(node: NimNode, fields: seq[string]): CompiledPredicate {.compileTime.} =
  proc ensureField(name: string) {.compileTime.} =
    if name notin fields:
      error("Unknown field '" & name & "' in where()", node)

  var methodName = ""
  var argExpr: NimNode = nil
  var fieldInfo: tuple[isField: bool, fieldName: string, isLower: bool] = (false, "", false)

  if node.len == 2 and node[0].kind == nnkDotExpr:
    let callee = node[0]
    if callee.len == 2:
      let checkField = unwrapModelField(callee[0])
      if checkField.isField:
        methodName = $callee[1]
        fieldInfo = checkField
        argExpr = node[1]
  elif node.len == 3 and node[0].kind in {nnkIdent, nnkSym}:
    let checkField = unwrapModelField(node[1])
    if checkField.isField:
      methodName = $node[0]
      fieldInfo = checkField
      argExpr = node[2]

  if not fieldInfo.isField or argExpr.isNil:
    error("LIKE helpers must be called on a model field like it.title.contains(term)", node)

  let fieldName = fieldInfo.fieldName
  ensureField(fieldName)

  var leftSql = escapeField(fieldName)
  if fieldInfo.isLower or methodName in ["icontains", "ilike"]:
    leftSql = "LOWER(" & leftSql & ")"
    
  result.sql = leftSql & " LIKE ?"

  case methodName
  of "contains":
    if fieldInfo.isLower:
      result.args = @[quote do: "%" & (`argExpr`).toLowerAscii() & "%"]
    else:
      result.args = @[quote do: "%" & $`argExpr` & "%"]
  of "icontains":
    result.args = @[quote do: "%" & (`argExpr`).toLowerAscii() & "%"]
  of "startsWith":
    if fieldInfo.isLower:
      result.args = @[quote do: (`argExpr`).toLowerAscii() & "%"]
    else:
      result.args = @[quote do: $`argExpr` & "%"]
  of "endsWith":
    if fieldInfo.isLower:
      result.args = @[quote do: "%" & (`argExpr`).toLowerAscii()]
    else:
      result.args = @[quote do: "%" & $`argExpr`]
  of "like":
    if fieldInfo.isLower:
      result.args = @[quote do: (`argExpr`).toLowerAscii()]
    else:
      result.args = @[argExpr]
  of "ilike":
    result.args = @[quote do: (`argExpr`).toLowerAscii()]
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
    of "==", "!=", ">", ">=", "<", "<=", "like", "ilike":
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
