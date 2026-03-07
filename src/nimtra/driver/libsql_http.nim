import std/[asyncdispatch, base64, httpclient, json, options, strutils, tables, uri]

import ../[dialects, values]

type
  LibSQLError* = object of CatchableError

  SyncHook* = proc(): Future[void] {.closure.}
  SyncCloseHook* = proc() {.closure.}

  LibSQLConfig* = object
    url*: string
    authToken*: string
    syncUrl*: string
    syncPath*: string
    timeoutMs*: int
    closeAfterExecute*: bool
    syncHook*: SyncHook
    syncCloseHook*: SyncCloseHook

  LibSQLConnection* = ref object
    config*: LibSQLConfig
    client*: AsyncHttpClient
    pipelineUrl*: string
    baton*: Option[string]
    baseUrl*: Option[string]
    dialect*: Dialect

proc makePipelineUrl(url: string): string =
  var normalized = url.strip()
  if normalized.startsWith("libsql://"):
    normalized = "https://" & normalized[9 .. ^1]
  elif not (normalized.startsWith("https://") or normalized.startsWith("http://")):
    raise newException(LibSQLError, "libSQL URL must start with libsql://, https://, or http://")

  if normalized.endsWith("/"):
    normalized = normalized[0 .. ^2]

  normalized & "/v2/pipeline"

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

  let response = await db.client.request(
    db.pipelineUrl,
    httpMethod = HttpPost,
    headers = headers,
    body = $body
  )

  let responseBody = await response.body
  if int(response.code) >= 400:
    raise newException(LibSQLError, "libSQL HTTP error " & $response.code & ": " & responseBody)

  result = parseJson(responseBody)

  if result.kind == JObject:
    let nextBaton = getField(result, ["baton"])
    if not nextBaton.isNil and nextBaton.kind == JString:
      db.baton = some(nextBaton.getStr())

    let baseUrlNode = getField(result, ["base_url", "baseUrl"])
    if not baseUrlNode.isNil and baseUrlNode.kind == JString:
      db.baseUrl = some(baseUrlNode.getStr())

proc openLibSQL*(
  url: string,
  authToken = "",
  syncUrl = "",
  syncPath = "/v1/sync",
  timeoutMs = 30_000,
  closeAfterExecute = true,
  syncHook: SyncHook = nil,
  syncCloseHook: SyncCloseHook = nil
): Future[LibSQLConnection] {.async.} =
  var client = newAsyncHttpClient()
  client.timeout = timeoutMs

  result = LibSQLConnection(
    config: LibSQLConfig(
      url: url,
      authToken: authToken,
      syncUrl: syncUrl,
      syncPath: syncPath,
      timeoutMs: timeoutMs,
      closeAfterExecute: closeAfterExecute,
      syncHook: syncHook,
      syncCloseHook: syncCloseHook
    ),
    client: client,
    pipelineUrl: makePipelineUrl(url),
    baton: none(string),
    baseUrl: none(string),
    dialect: newSQLiteDialect()
  )

proc executeBatch*(
  db: LibSQLConnection,
  statements: openArray[SqlStatement]
): Future[seq[SqlResult]]

proc execute*(
  db: LibSQLConnection,
  statement: SqlStatement
): Future[SqlResult] {.async.} =
  let results = await db.executeBatch(@[statement])
  if results.len == 0:
    raise newException(LibSQLError, "No execute result found for statement")
  results[0]

proc execute*(
  db: LibSQLConnection,
  sql: string,
  args: openArray[SqlValue] = []
): Future[SqlResult] {.async.} =
  var params: seq[SqlValue]
  for arg in args:
    params.add(arg)
  await db.execute(SqlStatement(sql: sql, params: params))

proc executeBatch*(
  db: LibSQLConnection,
  statements: openArray[SqlStatement]
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

proc query*(db: LibSQLConnection, statement: SqlStatement): Future[SqlResult] {.async.} =
  await db.execute(statement.sql, statement.params)

proc query*(db: LibSQLConnection, sql: string, args: openArray[SqlValue] = []): Future[SqlResult] {.async.} =
  await db.execute(sql, args)

proc sync*(db: LibSQLConnection): Future[void] {.async.} =
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

proc close*(db: LibSQLConnection): Future[void] {.async.} =
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
