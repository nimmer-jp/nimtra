import std/[asyncdispatch, dynlib, strutils]

import ./libsql_http

type
  LibsqlErrPtr = pointer

  LibsqlCypher {.size: sizeof(cint).} = enum
    LIBSQL_CYPHER_DEFAULT = 0,
    LIBSQL_CYPHER_AES256 = 1

  LibsqlDatabaseHandle {.bycopy.} = object
    err: LibsqlErrPtr
    inner: pointer

  LibsqlSyncResult {.bycopy.} = object
    err: LibsqlErrPtr
    frame_no: uint64
    frames_synced: uint64

  LibsqlDatabaseDesc {.bycopy.} = object
    url: cstring
    path: cstring
    auth_token: cstring
    encryption_key: cstring
    sync_interval: uint64
    cypher: LibsqlCypher
    disable_read_your_writes: bool
    webpki: bool
    synced: bool
    disable_safety_assert: bool
    `namespace`: cstring

  LibsqlConfig {.bycopy.} = object
    logger: pointer
    version: cstring

  FnSetup = proc(config: LibsqlConfig): LibsqlErrPtr {.cdecl.}
  FnErrorMessage = proc(self: LibsqlErrPtr): cstring {.cdecl.}
  FnErrorDeinit = proc(self: LibsqlErrPtr) {.cdecl.}
  FnDatabaseInit = proc(desc: LibsqlDatabaseDesc): LibsqlDatabaseHandle {.cdecl.}
  FnDatabaseSync = proc(self: LibsqlDatabaseHandle): LibsqlSyncResult {.cdecl.}
  FnDatabaseDeinit = proc(self: LibsqlDatabaseHandle) {.cdecl.}

  LibsqlApi* = ref object
    handle: LibHandle
    setup: FnSetup
    errorMessage: FnErrorMessage
    errorDeinit: FnErrorDeinit
    databaseInit: FnDatabaseInit
    databaseSync: FnDatabaseSync
    databaseDeinit: FnDatabaseDeinit
    setupDone: bool

  EmbeddedReplicaConfig* = object
    path*: string
    primaryUrl*: string
    authToken*: string
    encryptionKey*: string
    namespace*: string
    syncIntervalMs*: uint64
    webpki*: bool
    synced*: bool
    disableReadYourWrites*: bool
    disableSafetyAssert*: bool
    libraryPath*: string

  EmbeddedReplica* = ref object
    api*: LibsqlApi
    db*: LibsqlDatabaseHandle
    config*: EmbeddedReplicaConfig
    closed*: bool

proc defaultLibraryCandidates(): seq[string] =
  when defined(macosx):
    @["libsql.dylib", "libsql", "/opt/homebrew/lib/libsql.dylib", "/usr/local/lib/libsql.dylib"]
  elif defined(windows):
    @["libsql.dll"]
  else:
    @["libsql.so", "libsql.so.0", "libsql"]

proc loadSymbol[T](handle: LibHandle, name: string): T =
  let symbol = symAddr(handle, name)
  if symbol.isNil:
    raise newException(LibSQLError, "Missing symbol in libsql C library: " & name)
  cast[T](symbol)

proc loadLibsqlApi*(libraryPath = ""): LibsqlApi =
  var handle: LibHandle

  if libraryPath.len > 0:
    handle = loadLib(libraryPath)
    if handle.isNil:
      raise newException(LibSQLError, "Failed to load libsql C library from: " & libraryPath)
  else:
    for candidate in defaultLibraryCandidates():
      handle = loadLib(candidate)
      if not handle.isNil:
        break
    if handle.isNil:
      raise newException(
        LibSQLError,
        "Failed to load libsql C library. Set EmbeddedReplicaConfig.libraryPath to your installed libsql shared library."
      )

  result = LibsqlApi(
    handle: handle,
    setup: loadSymbol[FnSetup](handle, "libsql_setup"),
    errorMessage: loadSymbol[FnErrorMessage](handle, "libsql_error_message"),
    errorDeinit: loadSymbol[FnErrorDeinit](handle, "libsql_error_deinit"),
    databaseInit: loadSymbol[FnDatabaseInit](handle, "libsql_database_init"),
    databaseSync: loadSymbol[FnDatabaseSync](handle, "libsql_database_sync"),
    databaseDeinit: loadSymbol[FnDatabaseDeinit](handle, "libsql_database_deinit"),
    setupDone: false
  )

proc raiseIfError(api: LibsqlApi, err: LibsqlErrPtr, context: string) =
  if err.isNil:
    return

  var msg = "libsql error"
  if not api.errorMessage.isNil:
    let raw = api.errorMessage(err)
    if not raw.isNil:
      msg = $raw

  if not api.errorDeinit.isNil:
    api.errorDeinit(err)

  raise newException(LibSQLError, context & ": " & msg)

proc initSetup(api: LibsqlApi) =
  if api.setupDone:
    return
  let err = api.setup(LibsqlConfig(logger: nil, version: nil))
  api.raiseIfError(err, "libsql_setup failed")
  api.setupDone = true

proc openEmbeddedReplica*(
  config: EmbeddedReplicaConfig,
  api: LibsqlApi = nil
): EmbeddedReplica =
  if config.path.len == 0:
    raise newException(ValueError, "EmbeddedReplicaConfig.path must not be empty")

  let runtimeApi = if api.isNil: loadLibsqlApi(config.libraryPath) else: api
  runtimeApi.initSetup()

  var desc = LibsqlDatabaseDesc(
    url: if config.primaryUrl.len > 0: config.primaryUrl.cstring else: nil,
    path: config.path.cstring,
    auth_token: if config.authToken.len > 0: config.authToken.cstring else: nil,
    encryption_key: if config.encryptionKey.len > 0: config.encryptionKey.cstring else: nil,
    sync_interval: config.syncIntervalMs,
    cypher: LIBSQL_CYPHER_DEFAULT,
    disable_read_your_writes: config.disableReadYourWrites,
    webpki: config.webpki,
    synced: config.synced,
    disable_safety_assert: config.disableSafetyAssert,
    `namespace`: if config.namespace.len > 0: config.namespace.cstring else: nil
  )

  let db = runtimeApi.databaseInit(desc)
  runtimeApi.raiseIfError(db.err, "libsql_database_init failed")

  result = EmbeddedReplica(
    api: runtimeApi,
    db: db,
    config: config,
    closed: false
  )

proc openEmbeddedReplica*(
  path: string,
  primaryUrl = "",
  authToken = "",
  encryptionKey = "",
  namespace = "",
  syncIntervalMs = 0'u64,
  webpki = false,
  synced = true,
  disableReadYourWrites = false,
  disableSafetyAssert = false,
  libraryPath = "",
  api: LibsqlApi = nil
): EmbeddedReplica =
  openEmbeddedReplica(
    EmbeddedReplicaConfig(
      path: path,
      primaryUrl: primaryUrl,
      authToken: authToken,
      encryptionKey: encryptionKey,
      namespace: namespace,
      syncIntervalMs: syncIntervalMs,
      webpki: webpki,
      synced: synced,
      disableReadYourWrites: disableReadYourWrites,
      disableSafetyAssert: disableSafetyAssert,
      libraryPath: libraryPath
    ),
    api
  )

proc sync*(replica: EmbeddedReplica): Future[void] {.async.} =
  if replica.isNil:
    raise newException(ValueError, "EmbeddedReplica is nil")
  if replica.closed:
    raise newException(LibSQLError, "EmbeddedReplica is closed")

  let syncResult = replica.api.databaseSync(replica.db)
  replica.api.raiseIfError(syncResult.err, "libsql_database_sync failed")

proc close*(replica: EmbeddedReplica) =
  if replica.isNil or replica.closed:
    return

  replica.api.databaseDeinit(replica.db)
  replica.closed = true

proc syncHook*(replica: EmbeddedReplica): SyncHook =
  if replica.isNil:
    raise newException(ValueError, "EmbeddedReplica is nil")

  proc hook(): Future[void] {.closure, async.} =
    await replica.sync()

  hook

proc syncCloseHook*(replica: EmbeddedReplica): SyncCloseHook =
  if replica.isNil:
    raise newException(ValueError, "EmbeddedReplica is nil")

  proc hook() {.closure.} =
    replica.close()

  hook

proc openLibSQLWithEmbeddedSync*(
  url: string,
  replicaPath: string,
  authToken = "",
  primaryUrl = "",
  syncIntervalMs = 0'u64,
  libraryPath = "",
  timeoutMs = 30_000,
  closeAfterExecute = true,
  webpki = false,
  synced = true
): Future[LibSQLConnection] {.async.} =
  let replica = openEmbeddedReplica(
    path = replicaPath,
    primaryUrl = if primaryUrl.len > 0: primaryUrl else: url,
    authToken = authToken,
    syncIntervalMs = syncIntervalMs,
    libraryPath = libraryPath,
    webpki = webpki,
    synced = synced
  )

  await openLibSQL(
    url = url,
    authToken = authToken,
    timeoutMs = timeoutMs,
    closeAfterExecute = closeAfterExecute,
    syncHook = replica.syncHook(),
    syncCloseHook = replica.syncCloseHook()
  )
