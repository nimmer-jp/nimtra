import std/[asyncdispatch, options, os, strutils, tables]
import db_connector/postgres

import ../[dialects, values]
import ./base

type
  PostgresConnection* = ref object of DbConnection
    conn: PPGconn
    closed: bool

proc toHex(b: byte): string =
  const hexChars = "0123456789abcdef"
  result = newString(2)
  result[0] = hexChars[int(b) shr 4]
  result[1] = hexChars[int(b) and 0x0f]

proc blobToHex(blob: seq[byte]): string =
  result = "\\x"
  for b in blob:
    result.add(toHex(b))

proc formatParam(val: SqlValue): string =
  case val.kind
  of svNull:
    "" # CString handles null as nil
  of svBlob:
    blobToHex(val.blobValue)
  of svText:
    val.textValue
  of svInteger:
    $val.intValue
  of svFloat:
    $val.floatValue

proc parseResult(res: PPGresult): SqlResult =
  if res.isNil:
    raise newException(NimtraDbError, "Missing execute result from Postgres")

  let status = pqresultStatus(res)
  case status
  of PGRES_COMMAND_OK, PGRES_TUPLES_OK, PGRES_SINGLE_TUPLE:
    discard
  of PGRES_FATAL_ERROR:
    let msg = pqresultErrorMessage(res)
    raise newException(NimtraDbError, "Postgres error: " & $msg)
  else:
    raise newException(NimtraDbError, "Unexpected Postgres status: " & $status)

  let nFields = pqnfields(res)
  for i in 0 ..< nFields:
    let colName = $pqfname(res, i)
    let oid = pqftype(res, i)
    result.columns.add(SqlColumn(name: colName, decltype: $oid))

  let nTuples = pqntuples(res)
  for i in 0 ..< nTuples:
    var row: SqlRow
    for j in 0 ..< nFields:
      let colName = result.columns[j].name
      if pqgetisnull(res, i, j) == 1:
        row[colName] = nullValue()
      else:
        let valStr = $pqgetvalue(res, i, j)
        row[colName] = toSqlValue(valStr)
    result.rows.add(row)

  let affected = pqcmdTuples(res)
  if affected != nil and len($affected) > 0:
    try:
      result.affectedRowCount = parseInt($affected)
    except ValueError:
      discard

proc executeInternal(db: PostgresConnection, sql: string, params: seq[SqlValue]): Future[SqlResult] {.async.} =
  if db.closed or db.conn.isNil:
    raise newException(NimtraDbError, "PostgresConnection is closed")

  var strParams: seq[string]
  var cParams: seq[cstring]

  for p in params:
    if p.isNull():
      strParams.add("")
      cParams.add(nil)
    else:
      strParams.add(formatParam(p))
      cParams.add(strParams[^1].cstring)

  var cParamsPtr = if cParams.len > 0: cast[cstringArray](addr cParams[0]) else: nil

  let sendRes = pqsendQueryParams(
    db.conn,
    sql.cstring,
    cParams.len.int32,
    nil, # paramTypes
    cParamsPtr,
    nil, # paramLengths
    nil, # paramFormats
    0 # resultFormat (0=text)
  )

  if sendRes != 1:
    let msg = pqerrorMessage(db.conn)
    raise newException(NimtraDbError, "Postgres send query failed: " & $msg)

  var finalRes: PPGresult = nil

  while true:
    let consumeRes = pqconsumeInput(db.conn)
    if consumeRes != 1:
      let msg = pqerrorMessage(db.conn)
      raise newException(NimtraDbError, "Postgres consume input failed: " & $msg)

    if pqisBusy(db.conn) == 1:
      await sleepAsync(1)
      continue

    let res = pqgetResult(db.conn)
    if res.isNil:
      break

    let status = pqresultStatus(res)
    if status == PGRES_FATAL_ERROR:
      if finalRes != nil: pqclear(finalRes)
      finalRes = res
      break # Process error result
    
    # We only care about the last result or any error
    if finalRes != nil: pqclear(finalRes)
    finalRes = res

  if finalRes.isNil:
    return SqlResult()

  defer: pqclear(finalRes)
  return parseResult(finalRes)

method execute*(db: PostgresConnection, statement: SqlStatement): Future[SqlResult] {.async.} =
  return await executeInternal(db, statement.sql, statement.params)

method execute*(db: PostgresConnection, sql: string, args: openArray[SqlValue] = []): Future[SqlResult] =
  var paramsSeq: seq[SqlValue]
  for arg in args: paramsSeq.add(arg)
  return executeInternal(db, sql, paramsSeq)

proc executeBatchInternal(db: PostgresConnection, statements: seq[SqlStatement]): Future[seq[SqlResult]] {.async.} =
  var results: seq[SqlResult]
  for stmt in statements:
    if stmt.sql.len == 0: continue
    results.add(await executeInternal(db, stmt.sql, stmt.params))
  return results

method executeBatch*(db: PostgresConnection, statements: openArray[SqlStatement]): Future[seq[SqlResult]] =
  var stmtsSeq: seq[SqlStatement]
  for stmt in statements: stmtsSeq.add(stmt)
  return executeBatchInternal(db, stmtsSeq)

method query*(db: PostgresConnection, statement: SqlStatement): Future[SqlResult] {.async.} =
  return await executeInternal(db, statement.sql, statement.params)

method query*(db: PostgresConnection, sql: string, args: openArray[SqlValue] = []): Future[SqlResult] =
  var paramsSeq: seq[SqlValue]
  for arg in args: paramsSeq.add(arg)
  return executeInternal(db, sql, paramsSeq)

method sync*(db: PostgresConnection): Future[void] {.async.} =
  # No-op for Postgres
  discard

method close*(db: PostgresConnection): Future[void] {.async.} =
  if not db.closed and not db.conn.isNil:
    pqfinish(db.conn)
    db.conn = nil
    db.closed = true

proc openPostgres*(
  connStr: string,
  timeoutMs = 30_000
): Future[PostgresConnection] {.async.} =
  let conn = pqconnectdb(connStr.cstring)
  if pqstatus(conn) != CONNECTION_OK:
    let msg = pqerrorMessage(conn)
    pqfinish(conn)
    raise newException(NimtraDbError, "Postgres connection failed: " & $msg)

  if pqsetnonblocking(conn, 1) == -1:
    pqfinish(conn)
    raise newException(NimtraDbError, "Failed to set Postgres connection to non-blocking")

  result = PostgresConnection(
    conn: conn,
    closed: false,
    dialect: newPostgresDialect()
  )

proc openPostgresEnv*(
  connStrEnv = "PG_DATABASE_URL",
  timeoutMs = 30_000
): Future[PostgresConnection] {.async.} =
  var url = getEnv(connStrEnv).strip()
  if url.len == 0:
    url = getEnv("DATABASE_URL").strip()
  if url.len == 0:
    raise newException(
      ValueError,
      "Environment variable not set: " & connStrEnv & " or DATABASE_URL"
    )

  return await openPostgres(connStr = url, timeoutMs = timeoutMs)
