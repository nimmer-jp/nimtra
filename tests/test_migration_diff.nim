import std/[options, sequtils, strutils, unittest]

import ../src/nimtra/[migrations, model]

type
  User = ref object
    id {.primary, autoincrement.}: int
    email {.unique.}: string
    age {.index.}: int

suite "schema diff planner":
  test "creates full schema when table does not exist":
    let plan = planSchemaDiff(User, none(TableSnapshot))
    check plan.statements.len == 2
    check plan.warnings.len == 0

  test "adds missing columns and indexes":
    let snapshot = TableSnapshot(
      table: "users",
      columns: @[
        ExistingColumn(name: "id", dbType: "INTEGER", primaryKey: true),
        ExistingColumn(name: "email", dbType: "TEXT")
      ],
      indexes: @[]
    )

    let plan = planSchemaDiff(User, some(snapshot))
    check plan.statements.len == 3
    check plan.statements.anyIt(it.contains("ALTER TABLE \"users\" ADD COLUMN \"age\" INTEGER"))
    check plan.statements.anyIt(it.contains("CREATE UNIQUE INDEX IF NOT EXISTS \"uidx_users_email\""))
    check plan.statements.anyIt(it.contains("CREATE INDEX IF NOT EXISTS \"idx_users_age\""))

  test "emits warning for type changes":
    let snapshot = TableSnapshot(
      table: "users",
      columns: @[
        ExistingColumn(name: "id", dbType: "INTEGER", primaryKey: true),
        ExistingColumn(name: "email", dbType: "TEXT"),
        ExistingColumn(name: "age", dbType: "TEXT")
      ],
      indexes: @[
        ExistingIndex(name: "uidx_users_email", columns: @["email"], unique: true),
        ExistingIndex(name: "idx_users_age", columns: @["age"], unique: false)
      ]
    )

    let plan = planSchemaDiff(User, some(snapshot))
    check plan.statements.len == 0
    check plan.warnings.len > 0
    check plan.warnings[0].contains("Column type differs")
    check plan.warnings[^1].contains("autoRebuild=true")

  test "builds rebuild plan when autoRebuild is enabled":
    let snapshot = TableSnapshot(
      table: "users",
      columns: @[
        ExistingColumn(name: "id", dbType: "INTEGER", primaryKey: true),
        ExistingColumn(name: "email", dbType: "TEXT"),
        ExistingColumn(name: "legacy", dbType: "TEXT")
      ],
      indexes: @[
        ExistingIndex(name: "uidx_users_email", columns: @["email"], unique: true)
      ]
    )

    let plan = planSchemaDiff(User, some(snapshot), autoRebuild = true)
    check plan.statements.len >= 4
    check plan.statements[0].contains("CREATE TABLE \"__nimtra_rebuild_users\"")
    check plan.statements[1].contains("INSERT INTO \"__nimtra_rebuild_users\"")
    check plan.statements[2].contains("DROP TABLE \"users\"")
    check plan.statements[3].contains("ALTER TABLE \"__nimtra_rebuild_users\" RENAME TO \"users\"")
