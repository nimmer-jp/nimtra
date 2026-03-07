import std/[algorithm, options, os, strutils, times]

import ../migrations

type
  LoadedMigration* = object
    migration*: Migration
    filePath*: string

proc parseVersion(value: string): int64 =
  if value.len == 0:
    raise newException(ValueError, "Missing migration version in filename")
  try:
    result = parseBiggestInt(value).int64
  except CatchableError:
    raise newException(ValueError, "Invalid migration version: " & value)

  if result <= 0:
    raise newException(ValueError, "Migration version must be greater than zero: " & value)

proc splitSqlStatements*(source: string): seq[string] =
  var current = newStringOfCap(source.len)
  var i = 0
  var inSingle = false
  var inDouble = false
  var inLineComment = false
  var inBlockComment = false

  while i < source.len:
    let ch = source[i]
    let next = if i + 1 < source.len: source[i + 1] else: '\0'

    if inLineComment:
      current.add(ch)
      if ch == '\n':
        inLineComment = false
      inc i
      continue

    if inBlockComment:
      current.add(ch)
      if ch == '*' and next == '/':
        current.add(next)
        inBlockComment = false
        inc i, 2
      else:
        inc i
      continue

    if not inSingle and not inDouble and ch == '-' and next == '-':
      inLineComment = true
      current.add(ch)
      current.add(next)
      inc i, 2
      continue

    if not inSingle and not inDouble and ch == '/' and next == '*':
      inBlockComment = true
      current.add(ch)
      current.add(next)
      inc i, 2
      continue

    if ch == '\'' and not inDouble:
      current.add(ch)
      if inSingle and next == '\'':
        current.add(next)
        inc i, 2
        continue
      inSingle = not inSingle
      inc i
      continue

    if ch == '"' and not inSingle:
      current.add(ch)
      if inDouble and next == '"':
        current.add(next)
        inc i, 2
        continue
      inDouble = not inDouble
      inc i
      continue

    if ch == ';' and not inSingle and not inDouble:
      let stmt = current.strip()
      if stmt.len > 0:
        result.add(stmt)
      current.setLen(0)
      inc i
      continue

    current.add(ch)
    inc i

  let tail = current.strip()
  if tail.len > 0:
    result.add(tail)

proc parseWarnings(source: string): seq[string] =
  const
    PrefixA = "-- nimtra: warning:"
    PrefixB = "-- nimtra:warning="

  for line in source.splitLines():
    let trimmed = line.strip()
    let lower = trimmed.toLowerAscii()
    if lower.startsWith(PrefixA):
      let text = trimmed[PrefixA.len .. ^1].strip()
      if text.len > 0:
        result.add(text)
    elif lower.startsWith(PrefixB):
      let text = trimmed[PrefixB.len .. ^1].strip()
      if text.len > 0:
        result.add(text)

proc splitFileStem(stem: string): tuple[version: int64, name: string] =
  let sep = stem.find('_')
  if sep < 0:
    raise newException(
      ValueError,
      "Migration filename must be '<version>_<name>.sql': " & stem
    )

  let verRaw = stem[0 ..< sep].strip()
  let nameRaw = stem[sep + 1 .. ^1].strip()
  if nameRaw.len == 0:
    raise newException(ValueError, "Migration filename has empty name: " & stem)

  result.version = parseVersion(verRaw)
  result.name = nameRaw

proc parseMigrationFile*(filePath: string): LoadedMigration =
  let filename = extractFilename(filePath)
  if filename.toLowerAscii().splitFile.ext != ".sql":
    raise newException(ValueError, "Migration file must have .sql extension: " & filename)

  let stem = filename.splitFile.name
  let (version, name) = splitFileStem(stem)
  let source = readFile(filePath)
  let statements = splitSqlStatements(source)
  let warnings = parseWarnings(source)

  result.filePath = filePath
  result.migration = newMigration(version, name, statements, warnings)

proc loadMigrationsFromDir*(dirPath: string): seq[Migration] =
  if not dirExists(dirPath):
    return @[]

  var loaded: seq[LoadedMigration]
  for kind, path in walkDir(dirPath):
    if kind != pcFile:
      continue
    if path.toLowerAscii().splitFile.ext != ".sql":
      continue
    loaded.add(parseMigrationFile(path))

  loaded.sort(proc(a, b: LoadedMigration): int = cmp(a.migration.version, b.migration.version))
  for item in loaded:
    result.add(item.migration)

  validateMigrations(result)

proc listMigrationFilesFromDir*(dirPath: string): seq[LoadedMigration] =
  if not dirExists(dirPath):
    return @[]

  for kind, path in walkDir(dirPath):
    if kind == pcFile and path.toLowerAscii().splitFile.ext == ".sql":
      result.add(parseMigrationFile(path))

  result.sort(proc(a, b: LoadedMigration): int = cmp(a.migration.version, b.migration.version))

proc slugifyMigrationName*(name: string): string =
  for ch in name.toLowerAscii():
    if ch in {'a' .. 'z', '0' .. '9'}:
      result.add(ch)
    elif result.len == 0 or result[^1] != '_':
      result.add('_')

  result = result.strip(chars = {'_'})
  if result.len == 0:
    result = "migration"

proc defaultVersionFromClock*(): int64 =
  let stamp = now().utc.format("yyyyMMddHHmmss")
  parseBiggestInt(stamp).int64

proc createMigrationFile*(
  dirPath: string,
  name: string,
  version = none(int64),
  force = false
): string =
  createDir(dirPath)

  var selectedVersion = if version.isSome: version.get() else: defaultVersionFromClock()
  if selectedVersion <= 0:
    raise newException(ValueError, "Migration version must be greater than zero")

  let slug = slugifyMigrationName(name)

  if version.isNone:
    let existing = loadMigrationsFromDir(dirPath)
    for migration in existing:
      if migration.version >= selectedVersion:
        selectedVersion = migration.version + 1

  let fileName = $selectedVersion & "_" & slug & ".sql"
  let target = dirPath / fileName
  if fileExists(target) and not force:
    raise newException(ValueError, "Migration file already exists: " & target)

  let content = [
    "-- nimtra migration",
    "-- name: " & slug,
    "-- created_at: " & now().utc.format("yyyy-MM-dd'T'HH:mm:ss'Z'"),
    "--",
    "-- Write SQL statements below. Terminate each statement with ';'.",
    "",
    ""
  ].join("\n")
  writeFile(target, content)
  target
