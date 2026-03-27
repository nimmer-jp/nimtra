import std/[options, unittest]

import ../src/nimtra/[dialects, migrations, model, schema, uuid, values]

type
  Session = ref object
    id {.primary.}: UUID
    userId: UUID
    token: string

suite "uuid support":
  test "generates RFC-compliant uuid v4":
    let id = newUuidV4()
    check isValidUuid($id)
    check isUuidV4(id)
    check uuidVersion(id) == uv4

  test "generates RFC-compliant uuid v7":
    let id = newUuidV7()
    check isValidUuid($id)
    check isUuidV7(id)
    check uuidVersion(id) == uv7

  test "parses and normalizes uuid text":
    let parsed = parseUuid("550E8400-E29B-41D4-A716-446655440000")
    check $parsed == "550e8400-e29b-41d4-a716-446655440000"
    check isUuidV4(parsed)

  test "rejects invalid RFC variant":
    expect ValueError:
      discard parseUuid("550e8400-e29b-41d4-c716-446655440000")

  test "maps UUID model fields":
    let meta = modelMeta(Session)
    check meta.fields.len == 3
    check meta.fields[0].name == "id"
    check meta.fields[0].dbType == "UUID"
    check meta.fields[1].name == "userId"
    check meta.fields[1].dbType == "UUID"

  test "renders uuid column type by dialect":
    let sqliteSql = createTableSql(Session, newSQLiteDialect())
    check sqliteSql ==
      "CREATE TABLE IF NOT EXISTS \"sessions\" (\"id\" TEXT PRIMARY KEY, \"userId\" TEXT, \"token\" TEXT)"

    let postgresSql = createTableSql(Session, newPostgresDialect())
    check postgresSql ==
      "CREATE TABLE IF NOT EXISTS \"sessions\" (\"id\" UUID PRIMARY KEY, \"userId\" UUID, \"token\" TEXT)"

    let mysqlSql = createTableSql(Session, newMySQLDialect())
    check mysqlSql ==
      "CREATE TABLE IF NOT EXISTS `sessions` (`id` CHAR(36) PRIMARY KEY, `userId` CHAR(36), `token` TEXT)"

  test "converts UUID to and from SqlValue":
    let id = parseUuid("550e8400-e29b-41d4-a716-446655440000")
    let sqlValue = toSqlValue(id)
    check sqlValue.kind == svText
    check sqlValue.textValue == "550e8400-e29b-41d4-a716-446655440000"
    check $fromSqlValue(sqlValue, UUID) == $id

  test "sqlite diff treats UUID as TEXT-compatible":
    let snapshot = TableSnapshot(
      table: "sessions",
      columns: @[
        ExistingColumn(name: "id", dbType: "TEXT", primaryKey: true),
        ExistingColumn(name: "userId", dbType: "TEXT"),
        ExistingColumn(name: "token", dbType: "TEXT")
      ],
      indexes: @[]
    )

    let plan = planSchemaDiff(Session, some(snapshot))
    check plan.statements.len == 0
    check plan.warnings.len == 0
