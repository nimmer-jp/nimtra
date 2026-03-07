import std/[options, os, strutils, times, unittest]

import ../src/nimtra/cli/migration_files

proc freshTmpDir(name: string): string =
  result = getTempDir() / ("nimtra_" & name & "_" & $epochTime().int64)
  createDir(result)

proc removeTree(path: string) =
  if not dirExists(path):
    return
  for child in walkDirRec(path):
    if fileExists(child):
      removeFile(child)
  removeDir(path)

suite "cli migration files":
  test "splitSqlStatements keeps quoted semicolons":
    let src = """
CREATE TABLE users (id INTEGER PRIMARY KEY, note TEXT);
INSERT INTO users(note) VALUES('a;still text');
-- comment;
INSERT INTO users(note) VALUES("b;still text");
"""
    let parts = splitSqlStatements(src)
    check parts.len == 3
    check parts[0].startsWith("CREATE TABLE")
    check parts[1].contains("'a;still text'")
    check parts[2].contains("\"b;still text\"")

  test "loadMigrationsFromDir sorts and parses":
    let dir = freshTmpDir("load")
    defer:
      removeTree(dir)

    writeFile(dir / "2_seed.sql", "INSERT INTO t(id) VALUES(1);")
    writeFile(dir / "1_create.sql", "CREATE TABLE t (id INTEGER PRIMARY KEY);")

    let migrations = loadMigrationsFromDir(dir)
    check migrations.len == 2
    check migrations[0].version == 1
    check migrations[0].name == "create"
    check migrations[1].version == 2

  test "createMigrationFile generates slug and version":
    let dir = freshTmpDir("new")
    defer:
      removeTree(dir)

    let path1 = createMigrationFile(dir, "Create Users")
    check fileExists(path1)
    check extractFilename(path1).contains("_create_users.sql")

    let path2 = createMigrationFile(dir, "Create Users")
    check fileExists(path2)
    check path1 != path2

  test "createMigrationFile accepts explicit version":
    let dir = freshTmpDir("explicit")
    defer:
      removeTree(dir)

    let path = createMigrationFile(dir, "init", some(20260307123456'i64))
    check extractFilename(path).startsWith("20260307123456_")
