import std/[strutils, unittest]

import ../src/nimtra/[migrations, model]

type
  User = ref object
    id {.primary, autoincrement.}: int
    name: string

suite "migrations":
  test "stores migration warnings payload":
    let m = newMigration(9, "warned", ["SELECT 1"], ["manual check required"])
    check m.warnings.len == 1
    check m.warnings[0] == "manual check required"

  test "computes deterministic migration checksum":
    let m = newMigration(10, "stable", ["SELECT 1", "SELECT 2"], ["review"])
    check migrationChecksum(m) == migrationChecksum(m)

  test "migration checksum changes with statement differences":
    let a = newMigration(11, "a", ["SELECT 1"])
    let b = newMigration(11, "a", ["SELECT 2"])
    check migrationChecksum(a) != migrationChecksum(b)

  test "sorts migrations by version":
    let m1 = newMigration(2, "second", ["SELECT 2"])
    let m2 = newMigration(1, "first", ["SELECT 1"])
    let ordered = sortedMigrations([m1, m2])
    check ordered[0].version == 1
    check ordered[1].version == 2

  test "rejects duplicate versions":
    let m1 = newMigration(1, "a", ["SELECT 1"])
    let m2 = newMigration(1, "b", ["SELECT 2"])
    expect(ValueError):
      validateMigrations([m1, m2])

  test "builds migration from model schema":
    let m = migrationFromModel(User, 2026030701)
    check m.version == 2026030701
    check m.name.len > 0
    check m.statements.len == 1
    check m.statements[0].sql.contains("CREATE TABLE")
