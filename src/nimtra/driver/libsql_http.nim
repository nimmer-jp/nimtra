import std/[asyncdispatch, base64, deques, httpclient, json, locks, options, os, strutils, tables,
    uri]

import ../[dialects, values]
import ./base

const
  LibSqlUrlEmptyMsg* = "libSQL URL is empty"
  LibSqlUrlDuplicateSchemeMsg* =
    "libSQL URL has a duplicated scheme prefix (for example libsql://libsql://...)"
  LibSqlUrlInvalidHostMsg* =
    "libSQL URL host is 'libsql'. Set TURSO_DATABASE_URL to libsql://YOUR-DB.turso.io (not libsql://libsql/...)"
  LibSqlUrlInvalidSchemeMsg* =
    "libSQL URL must start with libsql://, https://, or http://"

type
  LibSQLError* = object of NimtraDbError

  SyncHook* = proc(): Future[void] {.closure.}
  SyncCloseHook* = proc() {.closure.}

  LibSQLConfig* = object
    url*: string
    authToken*: string
    syncUrl*: string
    syncPath*: string
    timeoutMs*: int
    maxRetries*: int
    retryBackoffMs*: int
    closeAfterExecute*: bool
    syncHook*: SyncHook
    syncCloseHook*: SyncCloseHook

  LibSQLConnection* = ref object of DbConnection
    config*: LibSQLConfig
    client*: AsyncHttpClient
    pipelineUrl*: string
    baton*: Option[string]
    baseUrl*: Option[string]

  LibSQLSyncConnection* = ref object of DbConnection
    config*: LibSQLConfig
    syncClient*: HttpClient
    pipelineUrl*: string
    baton*: Option[string]
    baseUrl*: Option[string]

  LibSQLSyncPool* = ref object
    lock: Lock
    capacity*: int
    available: seq[LibSQLSyncConnection]

  LibSQLAsyncPool* = ref object
    ## Multiple libSQL AsyncHttp handles for overlapping ``await db.*`` workloads on **one**
    ## ``asyncdispatch`` loop. Do **not** call ``borrowLibSQLAsync`` / ``releaseLibSQLAsync``
    ## from distinct OS threads; use ``initLibSQLSyncThreadPool`` instead.
    capacity*: int
    borrowed*: int
    closed*: bool
    available: seq[LibSQLConnection]
    pendingWaiters: Deque[Future[LibSQLConnection]]

  LibSQLSyncThreadConfig* = object
    poolSize*: int
    url*: string
    authToken*: string
    syncUrl*: string
    syncPath*: string
    timeoutMs*: int
    maxRetries*: int
    retryBackoffMs*: int
    closeAfterExecute*: bool

var
  libSQLSyncThreadConfig*: LibSQLSyncThreadConfig
  libSQLSyncThreadConfigReady* = false

var tlsLibSQLSyncPool {.threadvar.}: LibSQLSyncPool

proc normalizeLibSqlUrl*(raw: string): string =
  ## Normalizes Turso/libSQL URLs for HTTP drivers.
  ##
  ## - Trims whitespace
  ## - Adds ``libsql://`` when no scheme is present
  ## - Detects duplicated scheme prefixes
  var url = raw.strip()
  if url.len == 0:
    raise newException(ValueError, LibSqlUrlEmptyMsg)

  let schemeCount = url.count("://")
  if schemeCount > 1:
    raise newException(LibSQLError, LibSqlUrlDuplicateSchemeMsg & ": " & url)
  if schemeCount == 1 and url.startsWith("libsql://libsql://"):
    raise newException(LibSQLError, LibSqlUrlDuplicateSchemeMsg & ": " & url)

  if not (
    url.startsWith("libsql://") or
    url.startsWith("https://") or
    url.startsWith("http://")
  ):
    url = "libsql://" & url

  url

proc validateLibSqlUrl*(url: string) =
  ## Validates common Turso URL misconfigurations after normalization.
  let normalized = normalizeLibSqlUrl(url)
  var httpLike = normalized
  if httpLike.startsWith("libsql://"):
    httpLike = "https://" & httpLike[9 .. ^1]

  if not (httpLike.startsWith("https://") or httpLike.startsWith("http://")):
    raise newException(LibSQLError, LibSqlUrlInvalidSchemeMsg)

  let parsed = parseUri(httpLike)
  let host = parsed.hostname
  if host.len > 0 and host.toLowerAscii() == "libsql":
    raise newException(LibSQLError, LibSqlUrlInvalidHostMsg)

proc normalizeDbOpenError*(msg: string): string =
  ## Converts low-level transport errors into stable, operator-friendly messages.
  let lower = msg.toLowerAscii()
  if "no address associated with hostname" in lower or
     "nodename nor servname provided" in lower or
     "name or service not known" in lower or
     "could not resolve host" in lower:
    return "データベースのホスト名を解決できません。TURSO_DATABASE_URL のホスト名を確認してください。"
  if "timed out" in lower or "timeout" in lower or "deadline exceeded" in lower:
    return "データベースへの接続がタイムアウトしました。ネットワークと TURSO_DATABASE_URL を確認してください。"
  if "connection refused" in lower:
    return "データベースへの接続が拒否されました。URL とネットワーク設定を確認してください。"
  if "certificate" in lower or "ssl" in lower or "tls" in lower:
    return "データベースへの TLS 接続に失敗しました。URL と証明書設定を確認してください。"
  msg

proc makePipelineUrl(url: string): string =
  let normalized = normalizeLibSqlUrl(url)
  validateLibSqlUrl(normalized)

  var httpUrl = normalized
  if httpUrl.startsWith("libsql://"):
    httpUrl = "https://" & httpUrl[9 .. ^1]
  elif not (httpUrl.startsWith("https://") or httpUrl.startsWith("http://")):
    raise newException(LibSQLError, LibSqlUrlInvalidSchemeMsg)

  if httpUrl.endsWith("/"):
    httpUrl = httpUrl[0 .. ^2]

  httpUrl & "/v2/pipeline"

proc bytesToString(data: seq[byte]): string =
  result = newString(data.len)
  for i, b in data:
    result[i] = char(b)

proc stringToBytes(value: string): seq[byte] =
  result = newSeq[byte](value.len)
  for i, ch in value:
    result[i] = byte(ch)

proc encodeHranaValue(value: SqlValue): JsonNode =
  case value.kind
  of svNull:
    %*{"type": "null"}
  of svInteger:
    %*{"type": "integer", "value": $value.intValue}
  of svFloat:
    %*{"type": "float", "value": $value.floatValue}
  of svText:
    %*{"type": "text", "value": value.textValue}
  of svBlob:
    %*{"type": "blob", "base64": encode(bytesToString(value.blobValue))}

proc parseIntFlexible(node: JsonNode): int64 =
  case node.kind
  of JInt:
    node.getBiggestInt().int64
  of JString:
    parseBiggestInt(node.getStr()).int64
  of JFloat:
    int64(node.getFloat())
  else:
    raise newException(LibSQLError, "Expected integer value in Hrana response")

proc parseFloatFlexible(node: JsonNode): float64 =
  case node.kind
  of JFloat:
    node.getFloat()
  of JInt:
    float64(node.getBiggestInt())
  of JString:
    parseFloat(node.getStr())
  else:
    raise newException(LibSQLError, "Expected float value in Hrana response")

proc decodeHranaValue(node: JsonNode): SqlValue =
  if node.isNil:
    return nullValue()

  if node.kind == JObject and node.hasKey("type"):
    let kind = node["type"].getStr()
    case kind
    of "null":
      nullValue()
    of "integer":
      if node.hasKey("value"):
        SqlValue(kind: svInteger, intValue: parseIntFlexible(node["value"]))
      else:
        nullValue()
    of "float":
      if node.hasKey("value"):
        SqlValue(kind: svFloat, floatValue: parseFloatFlexible(node["value"]))
      else:
        nullValue()
    of "text":
      SqlValue(kind: svText, textValue: node{"value"}.getStr())
    of "blob":
      if node.hasKey("base64"):
        SqlValue(kind: svBlob, blobValue: stringToBytes(decode(node["base64"].getStr())))
      else:
        nullValue()
    else:
      fromJsonScalar(node)
  else:
    fromJsonScalar(node)

proc getField(node: JsonNode, names: openArray[string]): JsonNode =
  if node.isNil or node.kind != JObject:
    return nil
  for name in names:
    if node.hasKey(name):
      return node[name]
  nil

proc mergePipelineTransportState*(baton: var Option[string], baseUrl: var Option[string], parsed: JsonNode) =
  if parsed.kind != JObject:
    return
  let nextBaton = getField(parsed, ["baton"])
  if not nextBaton.isNil and nextBaton.kind == JString:
    baton = some(nextBaton.getStr())
  let baseUrlNode = getField(parsed, ["base_url", "baseUrl"])
  if not baseUrlNode.isNil and baseUrlNode.kind == JString:
    baseUrl = some(baseUrlNode.getStr())

proc parseExecuteResult*(resultNode: JsonNode): SqlResult =
  if resultNode.isNil:
    raise newException(LibSQLError, "Missing execute result")

  let colsNode = getField(resultNode, ["cols", "columns"])
  if not colsNode.isNil and colsNode.kind == JArray:
    for col in colsNode:
      var name = ""
      var decl = ""
      if col.kind == JObject:
        if col.hasKey("name"):
          name = col["name"].getStr()
        decl = getField(col, ["decltype", "decl_type", "type"]).getStr("")
      result.columns.add(SqlColumn(name: name, decltype: decl))

  let rowsNode = getField(resultNode, ["rows"])
  if not rowsNode.isNil and rowsNode.kind == JArray:
    for rowNode in rowsNode:
      var row: SqlRow
      if rowNode.kind == JArray:
        for idx in 0 ..< rowNode.len:
          let valueNode = rowNode[idx]
          let key = if idx < result.columns.len and result.columns[idx].name.len > 0:
            result.columns[idx].name
          else:
            $idx
          row[key] = decodeHranaValue(valueNode)
      elif rowNode.kind == JObject:
        for key, valueNode in rowNode:
          row[key] = decodeHranaValue(valueNode)
      result.rows.add(row)

  let affectedNode = getField(resultNode, ["affected_row_count", "affectedRowCount", "rows_affected"])
  if not affectedNode.isNil:
    result.affectedRowCount = parseIntFlexible(affectedNode).int

  let lastIdNode = getField(resultNode, ["last_insert_rowid", "lastInsertRowid", "lastInsertRowId"])
  if not lastIdNode.isNil and lastIdNode.kind != JNull:
    result.lastInsertRowId = some(parseIntFlexible(lastIdNode))

proc parsePipelineResults*(payload: JsonNode): seq[SqlResult]

proc parsePipelineResult*(payload: JsonNode): SqlResult =
  let results = parsePipelineResults(payload)
  if results.len == 0:
    raise newException(LibSQLError, "No execute result found in libSQL response")
  results[0]

proc parsePipelineResults*(payload: JsonNode): seq[SqlResult] =
  if payload.isNil:
    raise newException(LibSQLError, "Empty response from libSQL")

  if payload.kind == JObject and payload.hasKey("error"):
    raise newException(LibSQLError, payload["error"].pretty())

  let resultsNode = getField(payload, ["results"])
  if not resultsNode.isNil and resultsNode.kind == JArray:
    for item in resultsNode:
      if item.kind != JObject:
        continue

      if item.hasKey("type"):
        let t = item["type"].getStr()
        case t
        of "error":
          let msg = getField(item, ["error", "message"])
          if not msg.isNil and msg.kind == JString:
            raise newException(LibSQLError, msg.getStr())
          raise newException(LibSQLError, item.pretty())
        of "ok":
          let resp = getField(item, ["response"])
          if not resp.isNil and resp.kind == JObject:
            if resp.hasKey("type") and resp["type"].getStr() == "execute":
              let executeNode = getField(resp, ["result"])
              if executeNode.isNil:
                raise newException(LibSQLError, "Missing execute result in ok response")
              result.add(parseExecuteResult(executeNode))
            elif resp.hasKey("result"):
              result.add(parseExecuteResult(resp["result"]))
        else:
          discard
      elif item.hasKey("result"):
        result.add(parseExecuteResult(item["result"]))
      elif item.hasKey("rows") or item.hasKey("cols"):
        result.add(parseExecuteResult(item))

  if result.len == 0 and payload.kind == JObject:
    if payload.hasKey("result"):
      result.add(parseExecuteResult(payload["result"]))
    elif payload.hasKey("rows") or payload.hasKey("cols"):
      result.add(parseExecuteResult(payload))

proc buildExecuteRequest(sql: string, args: openArray[SqlValue]): JsonNode =
  result = %*{
    "type": "execute",
    "stmt": {
      "sql": sql,
      "args": newJArray()
    }
  }
  for arg in args:
    result["stmt"]["args"].add(encodeHranaValue(arg))

proc pipeline(db: LibSQLConnection, requests: seq[JsonNode]): Future[JsonNode] {.async.} =
  var body = %*{"requests": requests}
  if db.baton.isSome:
    body["baton"] = %db.baton.get()

  var headers = newHttpHeaders({"Content-Type": "application/json"})
  if db.config.authToken.len > 0:
    headers["Authorization"] = "Bearer " & db.config.authToken

  proc isRetriableStatus(code: int): bool =
    code == 408 or code == 429 or code >= 500

  proc parseAndStorePipelineState(responseBody: string): JsonNode =
    result = parseJson(responseBody)
    mergePipelineTransportState(db.baton, db.baseUrl, result)

  var attempt = 0
  while true:
    try:
      let requestBody = $body
      let response = await db.client.request(
        db.pipelineUrl,
        httpMethod = HttpPost,
        headers = headers,
        body = requestBody
      )
      let responseBody = await response.body
      let code = int(response.code)

      if code >= 400:
        if isRetriableStatus(code) and attempt < db.config.maxRetries:
          inc attempt
          let delay = max(0, db.config.retryBackoffMs) * attempt
          if delay > 0:
            await sleepAsync(delay)
          continue
        raise newException(LibSQLError, "libSQL HTTP error " & $code & ": " & responseBody)

      result = parseAndStorePipelineState(responseBody)
      return
    except CatchableError as e:
      if (e of LibSQLError) or attempt >= db.config.maxRetries:
        raise
      inc attempt
      let delay = max(0, db.config.retryBackoffMs) * attempt
      if delay > 0:
        await sleepAsync(delay)

proc pipelineSync(db: LibSQLSyncConnection, requests: seq[JsonNode]): JsonNode =
  var body = %*{"requests": requests}
  if db.baton.isSome:
    body["baton"] = %db.baton.get()

  var headers = newHttpHeaders({"Content-Type": "application/json"})
  if db.config.authToken.len > 0:
    headers["Authorization"] = "Bearer " & db.config.authToken

  proc isRetriableStatus(code: int): bool =
    code == 408 or code == 429 or code >= 500

  proc parseStored(responseBody: string): JsonNode =
    result = parseJson(responseBody)
    mergePipelineTransportState(db.baton, db.baseUrl, result)

  var attempt = 0
  while true:
    try:
      let requestBody = $body
      let response = db.syncClient.request(
        db.pipelineUrl,
        httpMethod = HttpPost,
        headers = headers,
        body = requestBody
      )
      let responseBody = response.body
      let code = int(response.code)

      if code >= 400:
        if isRetriableStatus(code) and attempt < db.config.maxRetries:
          inc attempt
          let delay = max(0, db.config.retryBackoffMs) * attempt
          if delay > 0:
            sleep(delay)
          continue
        raise newException(LibSQLError, "libSQL HTTP error " & $code & ": " & responseBody)

      return parseStored(responseBody)
    except CatchableError as e:
      if (e of LibSQLError) or attempt >= db.config.maxRetries:
        raise
      inc attempt
      let delay = max(0, db.config.retryBackoffMs) * attempt
      if delay > 0:
        sleep(delay)

proc openLibSQL*(
  url: string,
  authToken = "",
  syncUrl = "",
  syncPath = "/v1/sync",
  timeoutMs = 30_000,
  maxRetries = 2,
  retryBackoffMs = 200,
  closeAfterExecute = true,
  syncHook: SyncHook = nil,
  syncCloseHook: SyncCloseHook = nil
): Future[DbConnection] {.async.} =
  let normalizedUrl = normalizeLibSqlUrl(url)
  validateLibSqlUrl(normalizedUrl)
  var client = newAsyncHttpClient()
  client.timeout = timeoutMs

  result = LibSQLConnection(
    config: LibSQLConfig(
      url: normalizedUrl,
      authToken: authToken,
      syncUrl: syncUrl,
      syncPath: syncPath,
      timeoutMs: timeoutMs,
      maxRetries: max(0, maxRetries),
      retryBackoffMs: max(0, retryBackoffMs),
      closeAfterExecute: closeAfterExecute,
      syncHook: syncHook,
      syncCloseHook: syncCloseHook
    ),
    client: client,
    pipelineUrl: makePipelineUrl(normalizedUrl),
    baton: none(string),
    baseUrl: none(string),
    dialect: newSQLiteDialect()
  )

proc executeBatchInternal(
  db: LibSQLConnection,
  statements: seq[SqlStatement]
): Future[seq[SqlResult]]

method executeBatch*(
  db: LibSQLConnection,
  statements: openArray[SqlStatement]
): Future[seq[SqlResult]] {.async.}

method execute*(
  db: LibSQLConnection,
  statement: SqlStatement
): Future[SqlResult] {.async.} =
  let results = await db.executeBatch(@[statement])
  if results.len == 0:
    raise newException(LibSQLError, "No execute result found for statement")
  results[0]

method execute*(
  db: LibSQLConnection,
  sql: string,
  args: openArray[SqlValue] = []
): Future[SqlResult] =
  var params: seq[SqlValue]
  for arg in args:
    params.add(arg)
  return db.execute(SqlStatement(sql: sql, params: params))

proc executeBatchInternal(
  db: LibSQLConnection,
  statements: seq[SqlStatement]
): Future[seq[SqlResult]] {.async.} =
  if db.isNil:
    raise newException(LibSQLError, "Database handle is nil")

  if statements.len == 0:
    return @[]

  var requests: seq[JsonNode]
  for statement in statements:
    if statement.sql.len == 0:
      continue
    requests.add(buildExecuteRequest(statement.sql, statement.params))

  if requests.len == 0:
    return @[]

  let expectedResults = requests.len
  if db.config.closeAfterExecute and db.baton.isSome:
    requests.add(%*{"type": "close"})

  let payload = await db.pipeline(requests)
  let results = parsePipelineResults(payload)
  if results.len < expectedResults:
    raise newException(LibSQLError, "Incomplete batch response from libSQL")
  results

method executeBatch*(
  db: LibSQLConnection,
  statements: openArray[SqlStatement]
): Future[seq[SqlResult]] =
  var copied = newSeqOfCap[SqlStatement](statements.len)
  for statement in statements:
    copied.add(statement)
  return executeBatchInternal(db, copied)

method query*(db: LibSQLConnection, statement: SqlStatement): Future[SqlResult] =
  return db.execute(statement.sql, statement.params)

method query*(db: LibSQLConnection, sql: string, args: openArray[SqlValue] = []): Future[SqlResult] =
  var params: seq[SqlValue]
  for arg in args:
    params.add(arg)
  return db.execute(sql, params)

method sync*(db: LibSQLConnection): Future[void] {.async.} =
  if db.isNil:
    raise newException(LibSQLError, "Database handle is nil")

  if db.config.syncHook != nil:
    await db.config.syncHook()
    return

  if db.config.syncUrl.len == 0:
    ## For remote-only connections, writes are already strongly coordinated by libSQL.
    ## Keep sync() as a lightweight consistency checkpoint.
    discard await db.execute("SELECT 1")
    return

  var endpoint = db.config.syncUrl.strip()
  if endpoint.startsWith("libsql://"):
    endpoint = "https://" & endpoint[9 .. ^1]

  if not (endpoint.startsWith("https://") or endpoint.startsWith("http://")):
    raise newException(LibSQLError, "syncUrl must be HTTP(S) or libsql:// URL")

  var parsed = parseUri(endpoint)
  if parsed.path.len == 0 or parsed.path == "/":
    parsed.path = db.config.syncPath
    endpoint = $parsed

  var headers = newHttpHeaders({"Content-Type": "application/json"})
  if db.config.authToken.len > 0:
    headers["Authorization"] = "Bearer " & db.config.authToken

  let response = await db.client.request(
    endpoint,
    httpMethod = HttpPost,
    headers = headers,
    body = "{}"
  )
  let responseBody = await response.body
  if int(response.code) >= 400:
    raise newException(
      LibSQLError,
      "sync() HTTP error " & $response.code & " from " & endpoint & ": " & responseBody
    )

method close*(db: LibSQLConnection): Future[void] {.async.} =
  if db.isNil:
    return

  if db.baton.isSome:
    try:
      discard await db.pipeline(@[%*{"type": "close"}])
    except CatchableError:
      discard

  db.client.close()

  if db.config.syncCloseHook != nil:
    db.config.syncCloseHook()

proc executeBatchInternalSync(db: LibSQLSyncConnection, statements: seq[SqlStatement]): seq[SqlResult] =
  if db.isNil:
    raise newException(LibSQLError, "Database handle is nil")

  if statements.len == 0:
    return @[]

  var requests: seq[JsonNode]
  for statement in statements:
    if statement.sql.len == 0:
      continue
    requests.add(buildExecuteRequest(statement.sql, statement.params))

  if requests.len == 0:
    return @[]

  let expectedResults = requests.len
  if db.config.closeAfterExecute and db.baton.isSome:
    requests.add(%*{"type": "close"})

  let payload = pipelineSync(db, requests)
  let results = parsePipelineResults(payload)
  if results.len < expectedResults:
    raise newException(LibSQLError, "Incomplete batch response from libSQL")
  results

proc syncExecuteBatchAsFuture(db: LibSQLSyncConnection, statements: seq[SqlStatement]): Future[seq[SqlResult]] {.async.} =
  result = executeBatchInternalSync(db, statements)

method executeBatch*(db: LibSQLSyncConnection, statements: openArray[SqlStatement]): Future[seq[SqlResult]] =
  var copied = newSeqOfCap[SqlStatement](statements.len)
  for statement in statements:
    copied.add(statement)
  return syncExecuteBatchAsFuture(db, copied)

method execute*(db: LibSQLSyncConnection, statement: SqlStatement): Future[SqlResult] {.async.} =
  let results = await db.executeBatch(@[statement])
  if results.len == 0:
    raise newException(LibSQLError, "No execute result found for statement")
  results[0]

method execute*(db: LibSQLSyncConnection, sql: string, args: openArray[SqlValue] = []): Future[SqlResult] =
  var params: seq[SqlValue]
  for arg in args:
    params.add(arg)
  return db.execute(SqlStatement(sql: sql, params: params))

method query*(db: LibSQLSyncConnection, statement: SqlStatement): Future[SqlResult] =
  return db.execute(statement.sql, statement.params)

method query*(db: LibSQLSyncConnection, sql: string, args: openArray[SqlValue] = []): Future[SqlResult] =
  var params: seq[SqlValue]
  for arg in args:
    params.add(arg)
  return db.execute(sql, params)

method sync*(db: LibSQLSyncConnection): Future[void] {.async.} =
  if db.isNil:
    raise newException(LibSQLError, "Database handle is nil")

  ## syncHook/syncCloseHook are unsupported for synchronous HTTP handles;
  ## use Async openLibSQL if you need custom sync hooks.

  if db.config.syncUrl.len == 0:
    discard await db.execute("SELECT 1")
    return

  var endpoint = db.config.syncUrl.strip()
  if endpoint.startsWith("libsql://"):
    endpoint = "https://" & endpoint[9 .. ^1]

  if not (endpoint.startsWith("https://") or endpoint.startsWith("http://")):
    raise newException(LibSQLError, "syncUrl must be HTTP(S) or libsql:// URL")

  var parsed = parseUri(endpoint)
  if parsed.path.len == 0 or parsed.path == "/":
    parsed.path = db.config.syncPath
    endpoint = $parsed

  var headers = newHttpHeaders({"Content-Type": "application/json"})
  if db.config.authToken.len > 0:
    headers["Authorization"] = "Bearer " & db.config.authToken

  let response = db.syncClient.request(
    endpoint,
    httpMethod = HttpPost,
    headers = headers,
    body = "{}"
  )
  let responseBody = response.body
  if int(response.code) >= 400:
    raise newException(
      LibSQLError,
      "sync() HTTP error " & $response.code & " from " & endpoint & ": " & responseBody
    )

method close*(db: LibSQLSyncConnection): Future[void] {.async.} =
  if db.isNil:
    return

  if db.baton.isSome:
    try:
      discard pipelineSync(db, @[%*{"type": "close"}])
    except CatchableError:
      discard

  db.syncClient.close()

proc openLibSQLSync*(
  url: string,
  authToken = "",
  syncUrl = "",
  syncPath = "/v1/sync",
  timeoutMs = 30_000,
  maxRetries = 2,
  retryBackoffMs = 200,
  closeAfterExecute = true
): LibSQLSyncConnection =
  try:
    let normalizedUrl = normalizeLibSqlUrl(url)
    validateLibSqlUrl(normalizedUrl)
    let syncClient = newHttpClient()
    syncClient.timeout = timeoutMs
    LibSQLSyncConnection(
      config: LibSQLConfig(
        url: normalizedUrl,
        authToken: authToken,
        syncUrl: syncUrl,
        syncPath: syncPath,
        timeoutMs: timeoutMs,
        maxRetries: max(0, maxRetries),
        retryBackoffMs: max(0, retryBackoffMs),
        closeAfterExecute: closeAfterExecute,
        syncHook: nil,
        syncCloseHook: nil
      ),
      syncClient: syncClient,
      pipelineUrl: makePipelineUrl(normalizedUrl),
      baton: none(string),
      baseUrl: none(string),
      dialect: newSQLiteDialect()
    )
  except LibSQLError as e:
    raise e
  except CatchableError as e:
    raise newException(LibSQLError, normalizeDbOpenError(e.msg))

proc openLibSQLSyncWithRetry*(
  url: string,
  authToken = "",
  syncUrl = "",
  syncPath = "/v1/sync",
  timeoutMs = 30_000,
  maxRetries = 2,
  retryBackoffMs = 200,
  closeAfterExecute = true
): LibSQLSyncConnection =
  var attempt = 0
  var lastError = ""
  while attempt <= max(0, maxRetries):
    try:
      return openLibSQLSync(
        url = url,
        authToken = authToken,
        syncUrl = syncUrl,
        syncPath = syncPath,
        timeoutMs = timeoutMs,
        maxRetries = maxRetries,
        retryBackoffMs = retryBackoffMs,
        closeAfterExecute = closeAfterExecute
      )
    except CatchableError as e:
      lastError = normalizeDbOpenError(e.msg)
      if attempt >= max(0, maxRetries):
        raise newException(LibSQLError, lastError)
      inc attempt
      let delay = max(0, retryBackoffMs) * attempt
      if delay > 0:
        sleep(delay)
  raise newException(LibSQLError, lastError)

proc openLibSQLSyncEnv*(
  urlEnv = "TURSO_DATABASE_URL",
  authTokenEnv = "TURSO_AUTH_TOKEN",
  syncUrlEnv = "TURSO_SYNC_URL",
  syncPath = "/v1/sync",
  timeoutMs = 30_000,
  maxRetries = 2,
  retryBackoffMs = 200,
  closeAfterExecute = true
): LibSQLSyncConnection =
  var url = getEnv(urlEnv).strip()
  if url.len == 0 and urlEnv == "TURSO_DATABASE_URL":
    url = getEnv("TURSO_URL").strip()
  if url.len == 0:
    raise newException(
      ValueError,
      "Environment variable not set: " & urlEnv &
      " (or TURSO_URL)"
    )

  var authToken = getEnv(authTokenEnv)
  if authToken.len == 0 and authTokenEnv == "TURSO_AUTH_TOKEN":
    authToken = getEnv("TURSO_TOKEN")
  let syncUrl = getEnv(syncUrlEnv)

  openLibSQLSyncWithRetry(
    url = url,
    authToken = authToken,
    syncUrl = syncUrl,
    syncPath = syncPath,
    timeoutMs = timeoutMs,
    maxRetries = maxRetries,
    retryBackoffMs = retryBackoffMs,
    closeAfterExecute = closeAfterExecute
  )

proc newLibSQLSyncPool*(
  size: int,
  url: string,
  authToken = "",
  syncUrl = "",
  syncPath = "/v1/sync",
  timeoutMs = 30_000,
  maxRetries = 2,
  retryBackoffMs = 200,
  closeAfterExecute = true
): LibSQLSyncPool =
  if size <= 0:
    raise newException(ValueError, "LibSQL sync pool size must be positive")
  new(result)
  initLock(result.lock)
  result.capacity = size
  result.available = newSeqOfCap[LibSQLSyncConnection](size)
  for _ in 0 ..< size:
    result.available.add(openLibSQLSyncWithRetry(
      url = url,
      authToken = authToken,
      syncUrl = syncUrl,
      syncPath = syncPath,
      timeoutMs = timeoutMs,
      maxRetries = maxRetries,
      retryBackoffMs = retryBackoffMs,
      closeAfterExecute = closeAfterExecute
    ))

proc borrowLibSQLSync*(pool: LibSQLSyncPool): LibSQLSyncConnection =
  acquire(pool.lock)
  defer: release(pool.lock)
  if pool.available.len == 0:
    raise newException(LibSQLError, "LibSQL sync pool exhausted (borrow without release?)")
  result = pool.available.pop()

proc releaseLibSQLSync*(pool: LibSQLSyncPool, conn: LibSQLSyncConnection) =
  acquire(pool.lock)
  defer: release(pool.lock)
  pool.available.add(conn)

proc closeLibSQLSyncPool*(pool: LibSQLSyncPool) =
  acquire(pool.lock)
  try:
    if pool.available.len != pool.capacity:
      raise newException(
        LibSQLError,
        "closeLibSQLSyncPool: pool still has borrowed connections (" &
        $(pool.capacity - pool.available.len) & ")"
      )
    for c in pool.available:
      waitFor c.close()
    pool.available.setLen(0)
  finally:
    release(pool.lock)
  deinitLock(pool.lock)

proc initLibSQLSyncThreadPool*(
  poolSize: int,
  url: string,
  authToken = "",
  syncUrl = "",
  syncPath = "/v1/sync",
  timeoutMs = 30_000,
  maxRetries = 2,
  retryBackoffMs = 200,
  closeAfterExecute = true
) =
  ## Registers configuration for per-OS-thread sync pools.
  ##
  ## Call once at process startup (for example before httpbeast workers start).
  ## Each worker thread lazily creates its own ``LibSQLSyncPool`` on first use via
  ## ``threadLocalLibSQLSyncPool`` / ``withLibSQLSyncThread``.
  ##
  ## Do **not** share a single ``LibSQLSyncConnection`` across OS threads.
  ## Basolato + httpbeast: prefer this over ``openLibSQL`` (async) on worker threads.
  if poolSize <= 0:
    raise newException(ValueError, "LibSQL sync thread pool size must be positive")
  libSQLSyncThreadConfig = LibSQLSyncThreadConfig(
    poolSize: poolSize,
    url: url,
    authToken: authToken,
    syncUrl: syncUrl,
    syncPath: syncPath,
    timeoutMs: timeoutMs,
    maxRetries: maxRetries,
    retryBackoffMs: retryBackoffMs,
    closeAfterExecute: closeAfterExecute
  )
  libSQLSyncThreadConfigReady = true

proc initLibSQLSyncThreadPoolEnv*(
  poolSize: int,
  urlEnv = "TURSO_DATABASE_URL",
  authTokenEnv = "TURSO_AUTH_TOKEN",
  syncUrlEnv = "TURSO_SYNC_URL",
  syncPath = "/v1/sync",
  timeoutMs = 30_000,
  maxRetries = 2,
  retryBackoffMs = 200,
  closeAfterExecute = true
) =
  var url = getEnv(urlEnv).strip()
  if url.len == 0 and urlEnv == "TURSO_DATABASE_URL":
    url = getEnv("TURSO_URL").strip()
  if url.len == 0:
    raise newException(
      ValueError,
      "Environment variable not set: " & urlEnv & " (or TURSO_URL)"
    )

  var authToken = getEnv(authTokenEnv)
  if authToken.len == 0 and authTokenEnv == "TURSO_AUTH_TOKEN":
    authToken = getEnv("TURSO_TOKEN")
  let syncUrl = getEnv(syncUrlEnv)

  initLibSQLSyncThreadPool(
    poolSize = poolSize,
    url = url,
    authToken = authToken,
    syncUrl = syncUrl,
    syncPath = syncPath,
    timeoutMs = timeoutMs,
    maxRetries = maxRetries,
    retryBackoffMs = retryBackoffMs,
    closeAfterExecute = closeAfterExecute
  )

proc threadLocalLibSQLSyncPool*(): LibSQLSyncPool =
  if not libSQLSyncThreadConfigReady:
    raise newException(
      LibSQLError,
      "initLibSQLSyncThreadPool() must be called before threadLocalLibSQLSyncPool()"
    )
  if tlsLibSQLSyncPool.isNil:
    let cfg = libSQLSyncThreadConfig
    tlsLibSQLSyncPool = newLibSQLSyncPool(
      size = cfg.poolSize,
      url = cfg.url,
      authToken = cfg.authToken,
      syncUrl = cfg.syncUrl,
      syncPath = cfg.syncPath,
      timeoutMs = cfg.timeoutMs,
      maxRetries = cfg.maxRetries,
      retryBackoffMs = cfg.retryBackoffMs,
      closeAfterExecute = cfg.closeAfterExecute
    )
  tlsLibSQLSyncPool

proc withLibSQLSyncThread*[T](
  body: proc(db: DbConnection): Future[T] {.closure.}
): Future[T] {.async.} =
  let pool = threadLocalLibSQLSyncPool()
  let cx = borrowLibSQLSync(pool)
  try:
    return await body(cx)
  finally:
    releaseLibSQLSync(pool, cx)

proc withLibSQLSyncThreadLocal*[T](
  body: proc(db: LibSQLSyncConnection): T {.closure.}
): T =
  let pool = threadLocalLibSQLSyncPool()
  let cx = borrowLibSQLSync(pool)
  try:
    return body(cx)
  finally:
    releaseLibSQLSync(pool, cx)

proc closeLibSQLSyncThreadLocal*() =
  ## Closes the sync pool for the **current** OS thread only.
  if not tlsLibSQLSyncPool.isNil:
    closeLibSQLSyncPool(tlsLibSQLSyncPool)
    tlsLibSQLSyncPool = nil

proc newLibSQLAsyncPool*(
  size: int,
  url: string,
  authToken = "",
  syncUrl = "",
  syncPath = "/v1/sync",
  timeoutMs = 30_000,
  maxRetries = 2,
  retryBackoffMs = 200,
  closeAfterExecute = true,
  syncHook: SyncHook = nil,
  syncCloseHook: SyncCloseHook = nil,
): Future[LibSQLAsyncPool] {.async.} =
  if size <= 0:
    raise newException(ValueError, "LibSQL async pool size must be positive")
  result = LibSQLAsyncPool(
    capacity: size,
    borrowed: 0,
    closed: false,
    available: newSeqOfCap[LibSQLConnection](size)
  )
  result.pendingWaiters = initDeque[Future[LibSQLConnection]]()
  try:
    for _ in 0 ..< size:
      let raw = await openLibSQL(
        url = url,
        authToken = authToken,
        syncUrl = syncUrl,
        syncPath = syncPath,
        timeoutMs = timeoutMs,
        maxRetries = maxRetries,
        retryBackoffMs = retryBackoffMs,
        closeAfterExecute = closeAfterExecute,
        syncHook = syncHook,
        syncCloseHook = syncCloseHook
      )
      result.available.add(LibSQLConnection(raw))
  except CatchableError as e:
    for c in result.available:
      try:
        await c.close()
      except CatchableError:
        discard
    result.available.setLen(0)
    raise e

proc borrowLibSQLAsync*(pool: LibSQLAsyncPool): Future[LibSQLConnection] {.async.} =
  if pool.isNil:
    raise newException(LibSQLError, "LibSQL async pool is nil")
  if pool.closed:
    raise newException(LibSQLError, "LibSQL async pool is closed")

  var conn: LibSQLConnection
  if pool.available.len > 0:
    conn = pool.available.pop()
  else:
    let fut = newFuture[LibSQLConnection]("borrowLibSQLAsync")
    pool.pendingWaiters.addLast(fut)
    conn = await fut
  inc pool.borrowed
  return conn

proc releaseLibSQLAsync*(pool: LibSQLAsyncPool, conn: LibSQLConnection): Future[void] {.async.} =
  if pool.isNil or conn.isNil:
    return
  if pool.closed:
    return
  if pool.borrowed <= 0:
    raise newException(LibSQLError, "LibSQL async pool release without matching borrow")

  dec pool.borrowed
  if pool.pendingWaiters.len > 0:
    let waiter = pool.pendingWaiters.popFirst()
    complete(waiter, conn)
    inc pool.borrowed
  else:
    pool.available.add(conn)

proc withLibSQLFromPool*[T](
  pool: LibSQLAsyncPool,
  body: proc(db: DbConnection): Future[T] {.closure.}
): Future[T] {.async.} =
  let cx = await borrowLibSQLAsync(pool)
  try:
    return await body(cx)
  finally:
    await releaseLibSQLAsync(pool, cx)

proc closeLibSQLAsyncPool*(pool: LibSQLAsyncPool): Future[void] {.async.} =
  if pool.isNil:
    return
  if pool.closed:
    return
  if pool.pendingWaiters.len > 0 or pool.borrowed != 0 or pool.available.len != pool.capacity:
    raise newException(
      LibSQLError,
      "closeLibSQLAsyncPool: pool is not idle (borrowed != 0 or waiters stuck or missing connections)"
    )
  pool.closed = true
  while pool.available.len > 0:
    let cx = pool.available.pop()
    await cx.close()
  pool.available.setLen(0)

proc newLibSQLAsyncPoolEnv*(
  size: int,
  urlEnv = "TURSO_DATABASE_URL",
  authTokenEnv = "TURSO_AUTH_TOKEN",
  syncUrlEnv = "TURSO_SYNC_URL",
  syncPath = "/v1/sync",
  timeoutMs = 30_000,
  maxRetries = 2,
  retryBackoffMs = 200,
  closeAfterExecute = true,
): Future[LibSQLAsyncPool] {.async.} =
  var url = getEnv(urlEnv).strip()
  if url.len == 0 and urlEnv == "TURSO_DATABASE_URL":
    url = getEnv("TURSO_URL").strip()
  if url.len == 0:
    raise newException(
      ValueError,
      "Environment variable not set: " & urlEnv &
      " (or TURSO_URL)"
    )

  var authToken = getEnv(authTokenEnv)
  if authToken.len == 0 and authTokenEnv == "TURSO_AUTH_TOKEN":
    authToken = getEnv("TURSO_TOKEN")
  let syncUrl = getEnv(syncUrlEnv)

  await newLibSQLAsyncPool(
    size,
    url = url,
    authToken = authToken,
    syncUrl = syncUrl,
    syncPath = syncPath,
    timeoutMs = timeoutMs,
    maxRetries = maxRetries,
    retryBackoffMs = retryBackoffMs,
    closeAfterExecute = closeAfterExecute
  )

proc withLibSQLSync*[T](
  url: string,
  authToken = "",
  syncUrl = "",
  syncPath = "/v1/sync",
  timeoutMs = 30_000,
  maxRetries = 2,
  retryBackoffMs = 200,
  closeAfterExecute = true,
  body: proc(db: LibSQLSyncConnection): T {.closure.}
): T =
  let db = openLibSQLSync(
    url = url,
    authToken = authToken,
    syncUrl = syncUrl,
    syncPath = syncPath,
    timeoutMs = timeoutMs,
    maxRetries = maxRetries,
    retryBackoffMs = retryBackoffMs,
    closeAfterExecute = closeAfterExecute
  )
  try:
    return body(db)
  finally:
    waitFor db.close()

proc withLibSQLSyncEnv*[T](
  body: proc(db: LibSQLSyncConnection): T {.closure.},
  urlEnv = "TURSO_DATABASE_URL",
  authTokenEnv = "TURSO_AUTH_TOKEN",
  syncUrlEnv = "TURSO_SYNC_URL",
  syncPath = "/v1/sync",
  timeoutMs = 30_000,
  maxRetries = 2,
  retryBackoffMs = 200,
  closeAfterExecute = true
): T =
  let db = openLibSQLSyncEnv(
    urlEnv = urlEnv,
    authTokenEnv = authTokenEnv,
    syncUrlEnv = syncUrlEnv,
    syncPath = syncPath,
    timeoutMs = timeoutMs,
    maxRetries = maxRetries,
    retryBackoffMs = retryBackoffMs,
    closeAfterExecute = closeAfterExecute
  )
  try:
    return body(db)
  finally:
    waitFor db.close()

proc openLibSQLEnv*(
  urlEnv = "TURSO_DATABASE_URL",
  authTokenEnv = "TURSO_AUTH_TOKEN",
  syncUrlEnv = "TURSO_SYNC_URL",
  syncPath = "/v1/sync",
  timeoutMs = 30_000,
  maxRetries = 2,
  retryBackoffMs = 200,
  closeAfterExecute = true
): Future[DbConnection] {.async.} =
  var url = getEnv(urlEnv).strip()
  if url.len == 0 and urlEnv == "TURSO_DATABASE_URL":
    url = getEnv("TURSO_URL").strip()
  if url.len == 0:
    raise newException(
      ValueError,
      "Environment variable not set: " & urlEnv &
      " (or TURSO_URL)"
    )

  var authToken = getEnv(authTokenEnv)
  if authToken.len == 0 and authTokenEnv == "TURSO_AUTH_TOKEN":
    authToken = getEnv("TURSO_TOKEN")
  let syncUrl = getEnv(syncUrlEnv)

  await openLibSQL(
    url = url,
    authToken = authToken,
    syncUrl = syncUrl,
    syncPath = syncPath,
    timeoutMs = timeoutMs,
    maxRetries = maxRetries,
    retryBackoffMs = retryBackoffMs,
    closeAfterExecute = closeAfterExecute
  )

proc withLibSQL*[T](
  url: string,
  authToken = "",
  syncUrl = "",
  syncPath = "/v1/sync",
  timeoutMs = 30_000,
  maxRetries = 2,
  retryBackoffMs = 200,
  closeAfterExecute = true,
  body: proc(db: DbConnection): Future[T] {.closure.}
): Future[T] {.async.} =
  let db = await openLibSQL(
    url = url,
    authToken = authToken,
    syncUrl = syncUrl,
    syncPath = syncPath,
    timeoutMs = timeoutMs,
    maxRetries = maxRetries,
    retryBackoffMs = retryBackoffMs,
    closeAfterExecute = closeAfterExecute
  )
  try:
    return await body(db)
  finally:
    await db.close()

proc withLibSQLEnv*[T](
  body: proc(db: DbConnection): Future[T] {.closure.},
  urlEnv = "TURSO_DATABASE_URL",
  authTokenEnv = "TURSO_AUTH_TOKEN",
  syncUrlEnv = "TURSO_SYNC_URL",
  syncPath = "/v1/sync",
  timeoutMs = 30_000,
  maxRetries = 2,
  retryBackoffMs = 200,
  closeAfterExecute = true
): Future[T] {.async.} =
  let db = await openLibSQLEnv(
    urlEnv = urlEnv,
    authTokenEnv = authTokenEnv,
    syncUrlEnv = syncUrlEnv,
    syncPath = syncPath,
    timeoutMs = timeoutMs,
    maxRetries = maxRetries,
    retryBackoffMs = retryBackoffMs,
    closeAfterExecute = closeAfterExecute
  )
  try:
    return await body(db)
  finally:
    await db.close()
