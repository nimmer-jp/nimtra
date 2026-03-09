import std/[asyncdispatch, options, os, osproc, strformat, strutils, tables, json, times, sequtils]

import ../migrations
import ../driver/base
import ../driver/libsql_http
import ../driver/postgres
import ../driver/mysql
import ../values
import ../model
import ./migration_files
import ./runner

type
  CliError = object of CatchableError


  CliConfig = object
    migrationsDir: string
    migrationsTable: string
    url: string
    token: string
    preferCurl: bool
    strict: bool
    version: Option[int64]

const
  DefaultMigrationsDir = "db/migrations"

proc defaultConfig(): CliConfig =
  CliConfig(
    migrationsDir: DefaultMigrationsDir,
    migrationsTable: DefaultMigrationsTable,
    url: "",
    token: "",
    preferCurl: true,
    strict: false,
    version: none(int64)
  )

proc parseBoolFlag(value: string): bool =
  let normalized = value.strip().toLowerAscii()
  if normalized.len == 0:
    return true
  if normalized in ["1", "true", "yes", "y", "on"]:
    return true
  if normalized in ["0", "false", "no", "n", "off"]:
    return false
  raise newException(CliError, "Invalid boolean value: " & value)

proc parseVersionOpt(value: string): int64 =
  try:
    result = parseBiggestInt(value).int64
  except CatchableError:
    raise newException(CliError, "Invalid version: " & value)
  if result <= 0:
    raise newException(CliError, "Version must be greater than zero: " & value)

proc splitLongOption(arg: string): tuple[key: string, value: string, hasEq: bool] =
  let body = arg[2 .. ^1]
  let eqPos = body.find('=')
  if eqPos >= 0:
    result.key = body[0 ..< eqPos]
    result.value = body[eqPos + 1 .. ^1]
    result.hasEq = true
  else:
    result.key = body
    result.value = ""
    result.hasEq = false

proc requireNextValue(args: seq[string], idx: var int, optName: string): string =
  if idx + 1 >= args.len:
    raise newException(CliError, "Option requires a value: " & optName)
  inc idx
  args[idx]

proc setOptionValue(cfg: var CliConfig, key: string, value: string) =
  case key
  of "dir", "d":
    cfg.migrationsDir = value
  of "table", "t":
    cfg.migrationsTable = value
  of "url":
    cfg.url = value
  of "token":
    cfg.token = value
  of "version", "v":
    cfg.version = some(parseVersionOpt(value))
  else:
    raise newException(CliError, "Unknown option: --" & key)

proc printHelp(programName: string) =
  echo &"""{programName} - drizzle-like migration CLI for nimtra

Usage:
  {programName} migrate new <name> [options]
  {programName} migrate status [options]
  {programName} migrate up [options]
  {programName} migrate to <version> [options]
  {programName} migrate verify [options]
  {programName} migrate list [options]
  {programName} generate <schema-file> [options]
  {programName} push <schema-file> [options]


Options:
  --dir, -d <path>         Migration SQL directory (default: db/migrations)
  --table, -t <name>       Migrations table (default: _nimtra_migrations)
  --url <libsql-url>       Override DB URL (otherwise env is used)
  --token <token>          Override auth token
  --prefer-curl[=bool]     Use curl transport (default: true)
  --strict[=bool]          Strict verification mode (default: false)
  --version, -v <number>   Explicit migration version (for `new`)

Environment fallback:
  TURSO_DATABASE_URL / TURSO_AUTH_TOKEN
  TURSO_URL / TURSO_TOKEN
"""

proc parseMigrateArgs(args: seq[string]): tuple[subcommand: string, cfg: CliConfig, positionals: seq[string]] =
  if args.len == 0:
    raise newException(CliError, "Missing migrate subcommand")

  result.cfg = defaultConfig()
  result.subcommand = args[0].toLowerAscii()

  var i = 1
  while i < args.len:
    let arg = args[i]
    if arg == "--":
      if i + 1 <= args.high:
        for item in args[i + 1 .. ^1]:
          result.positionals.add(item)
      break

    if arg.startsWith("--"):
      let (key, rawValue, hasEq) = splitLongOption(arg)
      case key
      of "prefer-curl":
        if hasEq:
          result.cfg.preferCurl = parseBoolFlag(rawValue)
        else:
          result.cfg.preferCurl = true
      of "strict":
        if hasEq:
          result.cfg.strict = parseBoolFlag(rawValue)
        else:
          result.cfg.strict = true
      else:
        var value = rawValue
        if not hasEq:
          value = requireNextValue(args, i, "--" & key)
        setOptionValue(result.cfg, key, value)
      inc i
      continue

    if arg.startsWith("-") and arg.len >= 2:
      let key = arg[1 .. ^1]
      if key in ["d", "t", "v"]:
        let value = requireNextValue(args, i, "-" & key)
        setOptionValue(result.cfg, key, value)
      else:
        raise newException(CliError, "Unknown option: " & arg)
      inc i
      continue

    result.positionals.add(arg)
    inc i

proc detectDbUrl(cfg: CliConfig): string =
  if cfg.url.len > 0: return cfg.url
  for env in ["DATABASE_URL", "TURSO_DATABASE_URL", "TURSO_URL", "PG_DATABASE_URL", "MYSQL_DATABASE_URL"]:
    let val = getEnv(env).strip()
    if val.len > 0: return val
  ""

proc openDb(cfg: CliConfig): Future[DbConnection] {.async.} =
  let url = detectDbUrl(cfg)
  if url.len == 0:
    raise newException(CliError, "No database URL provided via --url or environment variables (DATABASE_URL, etc.)")

  if url.startsWith("postgres://") or url.startsWith("postgresql://"):
    return await openPostgres(url)
  elif url.startsWith("mysql://"):
    let cleanUrl = url.replace("mysql://", "")
    var host = "127.0.0.1"
    var port = 3306
    var user = "root"
    var pass = ""
    var dbname = ""
    
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
      raise newException(CliError, "Invalid MySQL URL format: " & url)
    return await openMySQL(host, user, pass, dbname, port)
  else:
    # Default to libSQL
    var token = cfg.token
    if token.len == 0:
      token = getEnv("TURSO_AUTH_TOKEN").strip()
      if token.len == 0: token = getEnv("TURSO_TOKEN").strip()
    
    return await openLibSQL(
      url = url,
      authToken = token,
      preferCurlTransport = cfg.preferCurl
    )

proc extractModelMeta(schemaFile: string): seq[ModelMeta] =
  if not fileExists(schemaFile):
    raise newException(CliError, "Schema file not found: " & schemaFile)

  let absPath = absolutePath(schemaFile)
  let tmpDir = getTempDir()
  let runnerFile = tmpDir / "nimtra_runner.nim"
  let moduleName = absPath.splitFile.name
  
  # Copy the user's schema to the temp directory alongside the runner so it can be imported cleanly
  # Or better, just import it by absolute path
  let code = runnerTemplate.replace("$1", "\"" & absPath.replace("\\", "/") & "\"")
  writeFile(runnerFile, code)

  echo "Compiling schema definition... (" & schemaFile & ")"
  let (output, exitCode) = execCmdEx("nim c -r --hints:off --warnings:off " & runnerFile)
  if exitCode != 0:
    removeFile(runnerFile)
    raise newException(CliError, "Failed to compile schema file:\n" & output)

  removeFile(runnerFile)

  # Try to find JSON output
  try:
    let jNode = parseJson(output.strip().splitLines()[^1])
    for jm in jNode.elems:
      var meta: ModelMeta
      meta.table = jm["table"].getStr()
      for jf in jm["fields"].elems:
        var field: FieldMeta
        field.name = jf["name"].getStr()
        field.nimType = jf["nimType"].getStr()
        field.dbType = jf["dbType"].getStr()
        field.primary = jf["primary"].getBool()
        field.autoincrement = jf["autoincrement"].getBool()
        field.unique = jf["unique"].getBool()
        field.indexed = jf["indexed"].getBool()
        if jf.hasKey("maxLength"): field.maxLength = some(jf["maxLength"].getInt())
        if jf.hasKey("defaultValue"): field.defaultValue = some(jf["defaultValue"].getStr())
        meta.fields.add(field)
      result.add(meta)
  except CatchableError as e:
    raise newException(CliError, "Failed to parse schema output: " & e.msg & "\nOutput was: " & output)
proc printApplied(applied: seq[AppliedMigration]) =
  if applied.len == 0:
    echo "No applied migrations."
    return

  echo "Applied migrations:"
  for item in applied:
    let warningsSummary =
      if item.warnings.len == 0:
        ""
      else:
        " warnings=" & $item.warnings.len
    echo &"  {item.version} {item.name} checksum={item.checksum}{warningsSummary} applied_at={item.appliedAt}"

proc printStatus(local: seq[Migration], applied: seq[AppliedMigration], pending: seq[Migration]) =
  var appliedByVersion: Table[int64, AppliedMigration]
  for item in applied:
    appliedByVersion[item.version] = item

  var pendingSet: Table[int64, bool]
  for item in pending:
    pendingSet[item.version] = true

  echo "Local migration status:"
  if local.len == 0:
    echo "  (none)"
  for migration in local:
    let status =
      if pendingSet.hasKey(migration.version):
        "pending"
      elif appliedByVersion.hasKey(migration.version):
        "applied"
      else:
        "unknown"
    let checksum = migrationChecksum(migration)
    echo &"  {migration.version} {migration.name} [{status}] checksum={checksum}"

  var unknownApplied: seq[AppliedMigration]
  var localVersions: Table[int64, bool]
  for migration in local:
    localVersions[migration.version] = true
  for item in applied:
    if not localVersions.hasKey(item.version):
      unknownApplied.add(item)

  if unknownApplied.len > 0:
    echo "Applied but missing locally:"
    for item in unknownApplied:
      echo &"  {item.version} {item.name} checksum={item.checksum}"
  else:
    echo "Applied but missing locally: none"

  echo &"Summary: local={local.len} applied={applied.len} pending={pending.len}"

proc runMigrate(subcommand: string, cfg: CliConfig, positionals: seq[string]): Future[void] {.async.} =
  case subcommand
  of "new":
    if positionals.len == 0:
      raise newException(CliError, "migrate new requires <name>")
    let path = createMigrationFile(
      cfg.migrationsDir,
      positionals.join("_"),
      cfg.version
    )
    echo "Created migration file: " & path

  of "status":
    let local = loadMigrationsFromDir(cfg.migrationsDir)
    let db = await openDb(cfg)
    try:
      let applied = await db.listAppliedMigrations(cfg.migrationsTable)
      let pending = await db.pendingMigrations(local, cfg.migrationsTable)
      printStatus(local, applied, pending)

      if cfg.strict:
        await db.verifyMigrationHistory(
          local,
          cfg.migrationsTable,
          allowUnknownApplied = false
        )
    finally:
      await db.close()

  of "up":
    let local = loadMigrationsFromDir(cfg.migrationsDir)
    let db = await openDb(cfg)
    try:
      let pending = await db.pendingMigrations(local, cfg.migrationsTable)
      if pending.len == 0:
        echo "No pending migrations."
      else:
        await db.migrate(local, cfg.migrationsTable)
        echo &"Applied {pending.len} migration(s)."
    finally:
      await db.close()

  of "to":
    var target: int64
    if cfg.version.isSome:
      target = cfg.version.get()
    elif positionals.len > 0:
      target = parseVersionOpt(positionals[0])
    else:
      raise newException(CliError, "migrate to requires <version> or --version")

    let local = loadMigrationsFromDir(cfg.migrationsDir)
    let db = await openDb(cfg)
    try:
      await db.migrateTo(local, target, cfg.migrationsTable)
      echo &"Migrated up to version {target}."
    finally:
      await db.close()

  of "verify":
    let local = loadMigrationsFromDir(cfg.migrationsDir)
    let db = await openDb(cfg)
    try:
      await db.verifyMigrationHistory(
        local,
        cfg.migrationsTable,
        allowUnknownApplied = not cfg.strict
      )
      echo "Migration history verification passed."
    finally:
      await db.close()

  of "list":
    let db = await openDb(cfg)
    try:
      let applied = await db.listAppliedMigrations(cfg.migrationsTable)
      printApplied(applied)
    finally:
      await db.close()

  else:
    raise newException(
      CliError,
      "Unknown migrate subcommand: " & subcommand &
      " (expected: new, status, up, to, verify, list)"
    )

proc runCli*(
  programName = "nimtra",
  rawArgs: seq[string] = commandLineParams()
): Future[void] {.async.} =
  var args = rawArgs
  if args.len > 0 and args[0] == "--":
    args = args[1 .. ^1]

  if args.len == 0 or args[0] in ["help", "--help", "-h"]:
    printHelp(programName)
    return

  let command = args[0].toLowerAscii()
  case command
  of "migrate":
    if args.len == 1:
      raise newException(CliError, "Missing migrate subcommand")
    let (subcommand, cfg, positionals) = parseMigrateArgs(args[1 .. ^1])
    await runMigrate(subcommand, cfg, positionals)
  of "generate":
    let (_, cfg, positionals) = parseMigrateArgs(args)
    if positionals.len < 1:
      raise newException(CliError, "generate requires <schema-file>")
    let schemaFile = positionals[0]
    let metas = extractModelMeta(schemaFile)
    let db = await openDb(cfg)
    try:
      let migration = await migrationFromModels(metas, db, cfg.version.get(defaultVersionFromClock()), "schema_update")
      if migration.statements.len == 0:
        echo "No schema changes detected."
        return
      
      createDir(cfg.migrationsDir)
      let fileName = $migration.version & "_schema_update.sql"
      let target = cfg.migrationsDir / fileName
      var content = "-- nimtra migration\n-- name: schema_update\n-- created_at: " & now().utc.format("yyyy-MM-dd'T'HH:mm:ss'Z'") & "\n--\n\n"
      for stmt in migration.statements:
        content.add(stmt.sql & ";\n")
      writeFile(target, content)
      echo "Generated migration file: " & target
      if migration.warnings.len > 0:
        echo "Warnings:"
        for w in migration.warnings: echo "  - " & w
    finally:
      await db.close()

  of "push":
    let (_, cfg, positionals) = parseMigrateArgs(args)
    if positionals.len < 1:
      raise newException(CliError, "push requires <schema-file>")
    let schemaFile = positionals[0]
    let metas = extractModelMeta(schemaFile)
    let db = await openDb(cfg)
    try:
      let plan = await planFullSchemaDiff(metas, db, autoRebuild = true)
      if plan.statements.len == 0:
        echo "Database schema is already up to date."
        return
      
      echo "The following statements will be executed:"
      for stmt in plan.statements:
        echo "  " & stmt & ";"
      
      if plan.warnings.len > 0:
        echo "\nWARNINGS:"
        for w in plan.warnings: echo "  ! " & w
        echo ""
      
      stdout.write("Apply these changes to the database? [y/N]: ")
      let answer = stdin.readLine().strip().toLowerAscii()
      if answer == "y" or answer == "yes":
        let stmts = plan.statements.mapIt(SqlStatement(sql: it))
        discard await db.executeBatch(stmts)
        echo "Changes applied successfully."
      else:
        echo "Aborted."
    finally:
      await db.close()
  else:
    raise newException(CliError, "Unknown command: " & command)

proc runCliMain*(programName = "nimtra") =
  try:
    waitFor runCli(programName = programName)
  except CliError as e:
    stderr.writeLine(programName & " error: " & e.msg)
    quit(2)
  except CatchableError as e:
    stderr.writeLine(programName & " failed: " & e.msg)
    quit(1)
