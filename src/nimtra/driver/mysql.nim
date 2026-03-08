import std/[asyncdispatch, options, os, strutils, tables, times]
import db_connector/mysql

import ../[dialects, values]
import ./base

type
  MySQLConnection* = ref object of DbConnection
    conn: PMySQL
    closed: bool

proc parseResult(res: PRES, affectedRows: int64): SqlResult =
  if res != nil:
    let numFields = int(num_fields(res))
    let fields = fetch_fields(res)
    var fieldsArr = cast[ptr UncheckedArray[FIELD]](fields)
    
    for i in 0 ..< numFields:
      result.columns.add(SqlColumn(name: $fieldsArr[i].name, decltype: "unknown"))

    var row = fetch_row(res)
    while row != nil:
      var lengths = fetch_lengths(res)
      var lengthsArr = cast[ptr UncheckedArray[culong]](lengths)
      var sqlRow: SqlRow
      for i in 0 ..< numFields:
        let colName = result.columns[i].name
        let valPtr = row[i]
        if valPtr == nil:
          sqlRow[colName] = nullValue()
        else:
          let length = lengthsArr[i]
          let valStr = newString(length)
          if length > 0:
            copyMem(addr valStr[0], valPtr, length)
          sqlRow[colName] = toSqlValue(valStr)
      result.rows.add(sqlRow)
      row = fetch_row(res)
    
    free_result(res)

  result.affectedRowCount = int(affectedRows)

proc executeInternal(db: MySQLConnection, sql: string, params: seq[SqlValue]): Future[SqlResult] {.async.} =
  if db.closed or db.conn == nil:
    raise newException(NimtraDbError, "MySQLConnection is closed")

  var finalSql = ""
  var pIdx = 0
  for ch in sql:
    if ch == '?':
      if pIdx < params.len:
        let val = params[pIdx]
        case val.kind
        of svNull:
          finalSql.add("NULL")
        of svInteger:
          finalSql.add($val.intValue)
        of svFloat:
          finalSql.add($val.floatValue)
        of svText:
          var esc = newString(val.textValue.len * 2 + 1)
          let len = real_escape_string(db.conn, cast[cstring](addr esc[0]), val.textValue.cstring, val.textValue.len)
          esc.setLen(len)
          finalSql.add("'" & esc & "'")
        of svBlob:
          var esc = newString(val.blobValue.len * 2 + 1)
          var tempStr = newString(val.blobValue.len)
          if val.blobValue.len > 0:
            copyMem(addr tempStr[0], unsafeAddr val.blobValue[0], val.blobValue.len)
          let len = real_escape_string(db.conn, cast[cstring](addr esc[0]), tempStr.cstring, tempStr.len)
          esc.setLen(len)
          finalSql.add("'" & esc & "'")
        inc pIdx
      else:
        finalSql.add('?') # Missing param
    else:
      finalSql.add(ch)

  if real_query(db.conn, finalSql.cstring, finalSql.len) != 0:
    let msg = error(db.conn)
    raise newException(NimtraDbError, "MySQL query failed: " & $msg)

  let res = store_result(db.conn)
  let affected = affected_rows(db.conn)
  
  var sqlResult = parseResult(res, affected)
  
  let insertId = insert_id(db.conn)
  if insertId > 0:
    sqlResult.lastInsertRowId = some(int64(insertId))

  return sqlResult

method execute*(db: MySQLConnection, statement: SqlStatement): Future[SqlResult] {.async.} =
  return await executeInternal(db, statement.sql, statement.params)

method execute*(db: MySQLConnection, sql: string, args: openArray[SqlValue] = []): Future[SqlResult] =
  var paramsSeq: seq[SqlValue]
  for arg in args: paramsSeq.add(arg)
  return executeInternal(db, sql, paramsSeq)

proc executeBatchInternal(db: MySQLConnection, statements: seq[SqlStatement]): Future[seq[SqlResult]] {.async.} =
  var results: seq[SqlResult]
  for stmt in statements:
    if stmt.sql.len == 0: continue
    results.add(await executeInternal(db, stmt.sql, stmt.params))
  return results

method executeBatch*(db: MySQLConnection, statements: openArray[SqlStatement]): Future[seq[SqlResult]] =
  var stmtsSeq: seq[SqlStatement]
  for stmt in statements: stmtsSeq.add(stmt)
  return executeBatchInternal(db, stmtsSeq)

method query*(db: MySQLConnection, statement: SqlStatement): Future[SqlResult] {.async.} =
  return await executeInternal(db, statement.sql, statement.params)

method query*(db: MySQLConnection, sql: string, args: openArray[SqlValue] = []): Future[SqlResult] =
  var paramsSeq: seq[SqlValue]
  for arg in args: paramsSeq.add(arg)
  return executeInternal(db, sql, paramsSeq)

method sync*(db: MySQLConnection): Future[void] {.async.} =
  # No-op for MySQL
  discard

method close*(db: MySQLConnection): Future[void] {.async.} =
  if not db.closed and db.conn != nil:
    close(db.conn)
    db.conn = nil
    db.closed = true

proc openMySQL*(
  host: string,
  user: string,
  pass: string,
  dbname: string,
  port: int = 3306
): Future[MySQLConnection] {.async.} =
  let conn = init(nil)
  if conn == nil:
    raise newException(NimtraDbError, "Failed to initialize MySQL connection")

  if real_connect(conn, host.cstring, user.cstring, pass.cstring, dbname.cstring, mysql.cuint(port), nil, 0) == nil:
    let msg = error(conn)
    close(conn)
    raise newException(NimtraDbError, "MySQL connection failed: " & $msg)

  discard set_character_set(conn, "utf8mb4")

  result = MySQLConnection(
    conn: conn,
    closed: false,
    dialect: newMySQLDialect()
  )

proc openMySQLEnv*(
  connStrEnv = "MYSQL_DATABASE_URL",
  timeoutMs = 30_000
): Future[MySQLConnection] {.async.} =
  var url = getEnv(connStrEnv).strip()
  if url.len == 0:
    url = getEnv("DATABASE_URL").strip()
  if url.len == 0:
    raise newException(
      ValueError,
      "Environment variable not set: " & connStrEnv & " or DATABASE_URL"
    )

  # A simplistic parser for mysql://user:pass@host:port/dbname
  var host = "127.0.0.1"
  var port = 3306
  var user = "root"
  var pass = ""
  var dbname = ""

  let cleanUrl = url.replace("mysql://", "")
  let parts = cleanUrl.split('@')
  if parts.len == 2:
    let credentials = parts[0].split(':')
    user = credentials[0]
    if credentials.len > 1: pass = credentials[1]
    
    let hostDb = parts[1].split('/')
    if hostDb.len == 2:
      dbname = hostDb[1]
    
    let hostPort = hostDb[0].split(':')
    host = hostPort[0]
    if hostPort.len > 1:
      try:
        port = parseInt(hostPort[1])
      except ValueError:
        discard
  else:
    raise newException(ValueError, "Invalid MySQL URL format: " & url)

  return await openMySQL(host, user, pass, dbname, port)
