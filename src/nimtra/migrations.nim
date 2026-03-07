import std/[algorithm, asyncdispatch, json, options, sequtils, sets, strutils, tables, typetraits]

import ./[dialects, model, schema, values]
import ./driver/libsql_http

type
  Migration* = object
    version*: int64
    name*: string
    statements*: seq[SqlStatement]
    warnings*: seq[string]

  ExistingColumn* = object
    name*: string
    dbType*: string
    notNull*: bool
    defaultValue*: Option[string]
    primaryKey*: bool

  ExistingIndex* = object
    name*: string
    columns*: seq[string]
    unique*: bool

  TableSnapshot* = object
    table*: string
    columns*: seq[ExistingColumn]
    indexes*: seq[ExistingIndex]

  SchemaDiffPlan* = object
    statements*: seq[string]
    warnings*: seq[string]

  AppliedMigration* = object
    version*: int64
    name*: string
    checksum*: string
    warnings*: seq[string]
    appliedAt*: string

const
  DefaultMigrationsTable* = "_nimtra_migrations"
  MigrationChecksumOffsetBasis = 14695981039346656037'u64
  MigrationChecksumPrime = 1099511628211'u64

proc newMigration*(
  version: SomeInteger,
  name: string,
  sqlStatements: openArray[string],
  warnings: openArray[string] = []
): Migration =
  if version <= 0:
    raise newException(ValueError, "Migration version must be greater than zero")

  result.version = int64(version)
  result.name = name
  for sql in sqlStatements:
    if sql.len > 0:
      result.statements.add(SqlStatement(sql: sql, params: @[]))
  for warning in warnings:
    if warning.len > 0:
      result.warnings.add(warning)

proc sortedMigrations*(migrations: openArray[Migration]): seq[Migration] =
  result = @migrations
  result.sort(proc(a, b: Migration): int = cmp(a.version, b.version))

proc validateMigrations*(migrations: openArray[Migration]) =
  var seen: HashSet[int64]
  for migration in migrations:
    if migration.version <= 0:
      raise newException(ValueError, "Migration version must be greater than zero")
    if migration.version in seen:
      raise newException(ValueError, "Duplicate migration version: " & $migration.version)
    seen.incl(migration.version)

proc parseWarningsPayload(raw: string): seq[string] =
  let trimmed = raw.strip()
  if trimmed.len == 0:
    return @[]

  try:
    let node = parseJson(trimmed)
    case node.kind
    of JArray:
      for entry in node:
        if entry.kind == JString:
          result.add(entry.getStr())
    of JString:
      result.add(node.getStr())
    else:
      result.add(trimmed)
  except CatchableError:
    result.add(trimmed)

proc migrationPayload(migration: Migration): string =
  var chunks = @[
    "version:" & $migration.version,
    "name:" & migration.name
  ]
  for statement in migration.statements:
    chunks.add("sql:" & statement.sql.strip())
    if statement.params.len > 0:
      for value in statement.params:
        chunks.add("param:" & $value)
  for warning in migration.warnings:
    chunks.add("warning:" & warning.strip())
  chunks.join("\n")

proc migrationChecksum*(migration: Migration): string =
  var hash = MigrationChecksumOffsetBasis
  for ch in migrationPayload(migration):
    hash = hash xor uint64(ord(ch))
    hash = hash * MigrationChecksumPrime
  toHex(hash, 16).toLowerAscii()

proc copyMigrations(migrations: openArray[Migration]): seq[Migration] =
  result = newSeqOfCap[Migration](migrations.len)
  for migration in migrations:
    result.add(migration)

proc rowValue(row: SqlRow, keys: openArray[string]): Option[SqlValue] =
  for key in keys:
    if row.hasKey(key):
      return some(row[key])
  none(SqlValue)

proc rowString(row: SqlRow, keys: openArray[string]): string =
  let value = rowValue(row, keys)
  if value.isNone:
    return ""
  value.get().asString()

proc rowInt(row: SqlRow, keys: openArray[string]): int64 =
  let value = rowValue(row, keys)
  if value.isNone:
    return 0'i64
  value.get().asInt64()

proc ensureMigrationsTable*(
  db: LibSQLConnection,
  tableName = DefaultMigrationsTable
): Future[void] {.async.} =
  let table = db.dialect.quoteIdent(tableName)
  let warningsCol = db.dialect.quoteIdent("warnings")
  let checksumCol = db.dialect.quoteIdent("checksum")
  let sql = "CREATE TABLE IF NOT EXISTS " & table & " (" &
    db.dialect.quoteIdent("version") & " INTEGER PRIMARY KEY, " &
    db.dialect.quoteIdent("name") & " TEXT NOT NULL, " &
    checksumCol & " TEXT NOT NULL DEFAULT '', " &
    warningsCol & " TEXT NOT NULL DEFAULT '[]', " &
    db.dialect.quoteIdent("applied_at") & " TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP" &
    ")"
  discard await db.execute(sql)

  if db.dialect.name() == "sqlite":
    var hasWarnings = false
    var hasChecksum = false
    let pragmaRes = await db.query("PRAGMA table_info(" & table & ")")
    for row in pragmaRes.rows:
      let colName = rowString(row, ["name"]).toLowerAscii()
      if colName == "warnings":
        hasWarnings = true
      elif colName == "checksum":
        hasChecksum = true
    if not hasWarnings:
      let alterSql = "ALTER TABLE " & table & " ADD COLUMN " &
        warningsCol & " TEXT NOT NULL DEFAULT '[]'"
      discard await db.execute(alterSql)
    if not hasChecksum:
      let alterSql = "ALTER TABLE " & table & " ADD COLUMN " &
        checksumCol & " TEXT NOT NULL DEFAULT ''"
      discard await db.execute(alterSql)

proc listAppliedMigrations*(
  db: LibSQLConnection,
  tableName = DefaultMigrationsTable
): Future[seq[AppliedMigration]] {.async.} =
  await db.ensureMigrationsTable(tableName)

  let table = db.dialect.quoteIdent(tableName)
  let sql = "SELECT " &
    db.dialect.quoteIdent("version") & ", " &
    db.dialect.quoteIdent("name") & ", " &
    db.dialect.quoteIdent("checksum") & ", " &
    db.dialect.quoteIdent("warnings") & ", " &
    db.dialect.quoteIdent("applied_at") &
    " FROM " & table &
    " ORDER BY " & db.dialect.quoteIdent("version") & " ASC"

  let res = await db.query(sql)
  for row in res.rows:
    var applied = AppliedMigration(
      version: rowInt(row, ["version"]),
      name: rowString(row, ["name"]),
      checksum: rowString(row, ["checksum"]),
      appliedAt: rowString(row, ["applied_at"])
    )
    applied.warnings = parseWarningsPayload(rowString(row, ["warnings"]))
    result.add(applied)

proc appliedMigrationVersions*(
  db: LibSQLConnection,
  tableName = DefaultMigrationsTable
): Future[HashSet[int64]] {.async.} =
  let applied = await db.listAppliedMigrations(tableName)
  for migration in applied:
    result.incl(migration.version)

proc tableSnapshot*(
  db: LibSQLConnection,
  tableName: string
): Future[Option[TableSnapshot]] {.async.} =
  if db.dialect.name() != "sqlite":
    raise newException(LibSQLError, "tableSnapshot() currently supports sqlite/libSQL dialect only")

  let existsSql =
    "SELECT name FROM sqlite_master WHERE type = 'table' AND name = ? LIMIT 1"
  let existsRes = await db.query(existsSql, @[toSqlValue(tableName)])
  if existsRes.rows.len == 0:
    return none(TableSnapshot)

  var snapshot = TableSnapshot(table: tableName)
  let tableIdent = db.dialect.quoteIdent(tableName)

  let colRes = await db.query("PRAGMA table_info(" & tableIdent & ")")
  for row in colRes.rows:
    var column = ExistingColumn(
      name: rowString(row, ["name"]),
      dbType: rowString(row, ["type"]),
      notNull: rowInt(row, ["notnull"]) != 0,
      primaryKey: rowInt(row, ["pk"]) != 0
    )

    let defaultRaw = rowString(row, ["dflt_value", "default"])
    if defaultRaw.len > 0:
      column.defaultValue = some(defaultRaw)

    snapshot.columns.add(column)

  let indexListRes = await db.query("PRAGMA index_list(" & tableIdent & ")")
  for row in indexListRes.rows:
    let indexName = rowString(row, ["name"])
    if indexName.len == 0:
      continue
    if indexName.startsWith("sqlite_autoindex_"):
      continue

    var index = ExistingIndex(
      name: indexName,
      unique: rowInt(row, ["unique"]) != 0
    )

    var orderedCols: seq[tuple[seqNo: int64, column: string]]
    let indexRes = await db.query("PRAGMA index_info(" & db.dialect.quoteIdent(indexName) & ")")
    for idxRow in indexRes.rows:
      orderedCols.add((
        rowInt(idxRow, ["seqno"]),
        rowString(idxRow, ["name"])
      ))

    orderedCols.sort(proc(a, b: tuple[seqNo: int64, column: string]): int = cmp(a.seqNo, b.seqNo))
    for entry in orderedCols:
      if entry.column.len > 0:
        index.columns.add(entry.column)

    snapshot.indexes.add(index)

  some(snapshot)

proc normalizeType(value: string): string =
  value.strip().toUpperAscii()

proc normalizeDefault(value: string): string =
  value.strip().replace("\"", "'").toUpperAscii()

proc hasSingleColumnUniqueIndex(snapshot: TableSnapshot, columnName: string): bool =
  let target = columnName.toLowerAscii()
  for idx in snapshot.indexes:
    if idx.unique and idx.columns.len == 1 and idx.columns[0].toLowerAscii() == target:
      return true
  false

proc buildRebuildStatements(
  meta: ModelMeta,
  current: TableSnapshot,
  dialect: Dialect,
  indexPrefix: string
): seq[string] =
  let tempTable = "__nimtra_rebuild_" & meta.table
  var tempMeta = meta
  tempMeta.table = tempTable

  result.add(createTableSql(tempMeta, dialect, ifNotExists = false))

  var existingSet: HashSet[string]
  for col in current.columns:
    existingSet.incl(col.name.toLowerAscii())

  var copyColumns: seq[string]
  for field in meta.fields:
    if field.name.toLowerAscii() in existingSet:
      copyColumns.add(field.name)

  if copyColumns.len > 0:
    let targetCols = copyColumns.mapIt(dialect.quoteIdent(it)).join(", ")
    let sourceCols = copyColumns.mapIt(dialect.quoteIdent(it)).join(", ")
    result.add(
      "INSERT INTO " & dialect.quoteIdent(tempTable) &
      " (" & targetCols & ") " &
      "SELECT " & sourceCols & " FROM " & dialect.quoteIdent(meta.table)
    )

  result.add("DROP TABLE " & dialect.quoteIdent(meta.table))
  result.add(
    "ALTER TABLE " & dialect.quoteIdent(tempTable) &
    " RENAME TO " & dialect.quoteIdent(meta.table)
  )
  result.add(createIndexesSql(meta, dialect, ifNotExists = true, indexPrefix = indexPrefix))

proc planSchemaDiff*(
  meta: ModelMeta,
  snapshot: Option[TableSnapshot],
  dialect: Dialect = nil,
  indexPrefix = DefaultIndexPrefix,
  autoRebuild = false
): SchemaDiffPlan =
  let d = if dialect.isNil: newSQLiteDialect() else: dialect

  if snapshot.isNone:
    result.statements = createSchemaSql(meta, d, true, indexPrefix)
    return

  let current = snapshot.get()
  var rebuildReasons: seq[string]

  var existingCols: Table[string, ExistingColumn]
  for col in current.columns:
    existingCols[col.name.toLowerAscii()] = col

  var modelCols: HashSet[string]
  for field in meta.fields:
    let key = field.name.toLowerAscii()
    modelCols.incl(key)

    if not existingCols.hasKey(key):
      if field.primary or field.unique:
        rebuildReasons.add(
          "Constrained column '" & field.name & "' is missing (PRIMARY/UNIQUE)."
        )
      else:
        let addDef = columnDefinitionSql(field, d, includePrimaryAndUnique = false)
        result.statements.add(
          "ALTER TABLE " & d.quoteIdent(meta.table) & " ADD COLUMN " & addDef
        )
      continue

    let existing = existingCols[key]
    if normalizeType(existing.dbType) != normalizeType(field.dbType):
      rebuildReasons.add(
        "Column type differs for '" & field.name & "': existing=" &
        existing.dbType & ", model=" & field.dbType
      )

    if field.defaultValue.isSome and existing.defaultValue.isSome:
      if normalizeDefault(existing.defaultValue.get()) != normalizeDefault(field.defaultValue.get()):
        rebuildReasons.add(
          "Column default differs for '" & field.name & "': existing=" &
          existing.defaultValue.get() & ", model=" & field.defaultValue.get()
        )
    elif field.defaultValue.isSome != existing.defaultValue.isSome:
      rebuildReasons.add(
        "Column default presence differs for '" & field.name & "'."
      )

    if field.primary != existing.primaryKey:
      rebuildReasons.add(
        "Primary key setting differs for '" & field.name & "'."
      )

    if field.unique and not current.hasSingleColumnUniqueIndex(field.name):
      result.statements.add(
        "CREATE UNIQUE INDEX IF NOT EXISTS " &
        d.quoteIdent("uidx_" & meta.table & "_" & field.name) &
        " ON " & d.quoteIdent(meta.table) &
        " (" & d.quoteIdent(field.name) & ")"
      )

  for col in current.columns:
    if col.name.toLowerAscii() notin modelCols:
      rebuildReasons.add(
        "Existing column '" & col.name & "' is not in model."
      )

  var existingIndexNames: HashSet[string]
  for idx in current.indexes:
    existingIndexNames.incl(idx.name.toLowerAscii())

  for field in meta.fields:
    if field.indexed and not field.unique:
      let indexName = (indexPrefix & "_" & meta.table & "_" & field.name).toLowerAscii()
      if indexName notin existingIndexNames:
        result.statements.add(createIndexSql(meta, field, d, true, indexPrefix))

  if rebuildReasons.len > 0:
    result.warnings.add(rebuildReasons)
    if autoRebuild:
      result.warnings.add(
        "Using SQLite table rebuild strategy. Verify copied data and constraints before production rollout."
      )
      result.statements = buildRebuildStatements(meta, current, d, indexPrefix)
    else:
      result.warnings.add(
        "Set autoRebuild=true to generate rebuild SQL for incompatible schema changes."
      )

proc planSchemaDiff*[
  T
](
  modelType: typedesc[T],
  snapshot: Option[TableSnapshot],
  dialect: Dialect = nil,
  indexPrefix = DefaultIndexPrefix,
  autoRebuild = false
): SchemaDiffPlan =
  planSchemaDiff(modelMeta(modelType), snapshot, dialect, indexPrefix, autoRebuild)

proc planModelDiff*[
  T
](
  db: LibSQLConnection,
  modelType: typedesc[T],
  indexPrefix = DefaultIndexPrefix,
  autoRebuild = false
): Future[SchemaDiffPlan] {.async.} =
  let meta = modelMeta(modelType)
  let snapshot = await db.tableSnapshot(meta.table)
  planSchemaDiff(meta, snapshot, db.dialect, indexPrefix, autoRebuild)

proc migrationFromModel*[
  T
](
  modelType: typedesc[T],
  version: SomeInteger,
  migrationName = "",
  ifNotExists = true,
  dialect: Dialect = nil,
  indexPrefix = DefaultIndexPrefix
): Migration =
  let d = if dialect.isNil: newSQLiteDialect() else: dialect
  let statements = createSchemaSql(modelType, d, ifNotExists, indexPrefix)
  let resolvedMigrationName =
    if migrationName.len > 0:
      migrationName
    else:
      "create_" & name(T).toLowerAscii()
  result = newMigration(version, resolvedMigrationName, statements)

proc migrationFromModelDiff*[
  T
](
  db: LibSQLConnection,
  modelType: typedesc[T],
  version: SomeInteger,
  migrationName = "",
  indexPrefix = DefaultIndexPrefix,
  autoRebuild = false
): Future[Migration] {.async.} =
  let plan = await db.planModelDiff(modelType, indexPrefix, autoRebuild)
  let resolvedMigrationName =
    if migrationName.len > 0:
      migrationName
    else:
      "sync_" & name(T).toLowerAscii()
  result = newMigration(version, resolvedMigrationName, plan.statements, plan.warnings)

proc migrateModelDiff*[
  T
](
  db: LibSQLConnection,
  modelType: typedesc[T],
  version: SomeInteger,
  migrationName = "",
  indexPrefix = DefaultIndexPrefix,
  autoRebuild = false,
  tableName = DefaultMigrationsTable
): Future[SchemaDiffPlan] {.async.} =
  result = await db.planModelDiff(modelType, indexPrefix, autoRebuild)
  let resolvedMigrationName =
    if migrationName.len > 0:
      migrationName
    else:
      "sync_" & name(T).toLowerAscii()
  let migration = newMigration(version, resolvedMigrationName, result.statements, result.warnings)
  await db.applyMigration(migration, tableName)

proc applyMigration*(
  db: LibSQLConnection,
  migration: Migration,
  tableName = DefaultMigrationsTable
): Future[void] {.async.} =
  if migration.statements.len == 0 and migration.warnings.len == 0:
    return

  await db.ensureMigrationsTable(tableName)

  var batch: seq[SqlStatement]
  batch.add(SqlStatement(sql: "BEGIN", params: @[]))
  batch.add(migration.statements)
  let warningsJson = $(%migration.warnings)
  let checksum = migration.migrationChecksum()

  let insertSql = "INSERT INTO " & db.dialect.quoteIdent(tableName) &
    " (" & db.dialect.quoteIdent("version") & ", " &
    db.dialect.quoteIdent("name") & ", " &
    db.dialect.quoteIdent("checksum") & ", " &
    db.dialect.quoteIdent("warnings") & ")" &
    " VALUES (?, ?, ?, ?)"

  batch.add(SqlStatement(
    sql: insertSql,
    params: @[
      toSqlValue(migration.version),
      toSqlValue(migration.name),
      toSqlValue(checksum),
      toSqlValue(warningsJson)
    ]
  ))
  batch.add(SqlStatement(sql: "COMMIT", params: @[]))

  try:
    discard await db.executeBatch(batch)
  except CatchableError:
    try:
      discard await db.execute("ROLLBACK")
    except CatchableError:
      discard
    raise

proc verifyMigrationHistoryInternal(
  db: LibSQLConnection,
  migrations: seq[Migration],
  tableName = DefaultMigrationsTable,
  allowUnknownApplied = true
): Future[void] {.async.} =
  validateMigrations(migrations)

  var localByVersion: Table[int64, Migration]
  for migration in migrations:
    localByVersion[migration.version] = migration

  let applied = await db.listAppliedMigrations(tableName)
  for existing in applied:
    if localByVersion.hasKey(existing.version):
      let local = localByVersion[existing.version]
      let expectedChecksum = migrationChecksum(local)
      if existing.checksum.len > 0 and existing.checksum != expectedChecksum:
        raise newException(
          ValueError,
          "Migration checksum mismatch for version " & $existing.version &
          " (" & local.name & "): applied=" & existing.checksum &
          ", expected=" & expectedChecksum
        )
    elif not allowUnknownApplied:
      raise newException(
        ValueError,
        "Database contains applied migration version " & $existing.version &
        " that is not present in local migration set."
      )

proc verifyMigrationHistory*(
  db: LibSQLConnection,
  migrations: openArray[Migration],
  tableName = DefaultMigrationsTable,
  allowUnknownApplied = true
): Future[void] =
  let copied = copyMigrations(migrations)
  verifyMigrationHistoryInternal(db, copied, tableName, allowUnknownApplied)

proc pendingMigrationsInternal(
  db: LibSQLConnection,
  migrations: seq[Migration],
  tableName = DefaultMigrationsTable
): Future[seq[Migration]] {.async.} =
  validateMigrations(migrations)
  let ordered = sortedMigrations(migrations)
  let applied = await db.listAppliedMigrations(tableName)

  var appliedChecksums: Table[int64, string]
  for migration in applied:
    appliedChecksums[migration.version] = migration.checksum

  for migration in ordered:
    if appliedChecksums.hasKey(migration.version):
      let existingChecksum = appliedChecksums[migration.version]
      let expectedChecksum = migrationChecksum(migration)
      if existingChecksum.len > 0 and existingChecksum != expectedChecksum:
        raise newException(
          ValueError,
          "Migration checksum mismatch for version " & $migration.version &
          " (" & migration.name & "): applied=" & existingChecksum &
          ", expected=" & expectedChecksum
        )
      continue
    result.add(migration)

proc pendingMigrations*(
  db: LibSQLConnection,
  migrations: openArray[Migration],
  tableName = DefaultMigrationsTable
): Future[seq[Migration]] =
  let copied = copyMigrations(migrations)
  pendingMigrationsInternal(db, copied, tableName)

proc migrateInternal(
  db: LibSQLConnection,
  migrations: seq[Migration],
  tableName = DefaultMigrationsTable
): Future[void] {.async.} =
  let pending = await db.pendingMigrationsInternal(migrations, tableName)
  for migration in pending:
    await db.applyMigration(migration, tableName)

proc migrate*(
  db: LibSQLConnection,
  migrations: openArray[Migration],
  tableName = DefaultMigrationsTable
): Future[void] =
  let copied = copyMigrations(migrations)
  migrateInternal(db, copied, tableName)

proc migrateToInternal(
  db: LibSQLConnection,
  migrations: seq[Migration],
  targetVersion: SomeInteger,
  tableName = DefaultMigrationsTable
): Future[void] {.async.} =
  let target = int64(targetVersion)
  if target <= 0:
    raise newException(ValueError, "targetVersion must be greater than zero")

  var filtered: seq[Migration]
  for migration in migrations:
    if migration.version <= target:
      filtered.add(migration)

  let pending = await db.pendingMigrationsInternal(filtered, tableName)
  for migration in pending:
    await db.applyMigration(migration, tableName)

proc migrateTo*(
  db: LibSQLConnection,
  migrations: openArray[Migration],
  targetVersion: SomeInteger,
  tableName = DefaultMigrationsTable
): Future[void] =
  let copied = copyMigrations(migrations)
  migrateToInternal(db, copied, targetVersion, tableName)

proc ensureModelSchema*[
  T
](
  db: LibSQLConnection,
  modelType: typedesc[T],
  ifNotExists = true,
  indexPrefix = DefaultIndexPrefix
): Future[void] {.async.} =
  for sql in createSchemaSql(modelType, db.dialect, ifNotExists, indexPrefix):
    discard await db.execute(sql)

proc ensureModelSchemaDiff*[
  T
](
  db: LibSQLConnection,
  modelType: typedesc[T],
  indexPrefix = DefaultIndexPrefix,
  autoRebuild = false
): Future[SchemaDiffPlan] {.async.} =
  result = await db.planModelDiff(modelType, indexPrefix, autoRebuild)
  for sql in result.statements:
    discard await db.execute(sql)
