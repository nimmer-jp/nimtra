import std/unittest

import ../src/nimtra/[model, query_builder, values]

type
  User = ref object
    id {.primary, autoincrement.}: int
    age: int
    status: string
    createdAt: string

  AuditLog {.table: "audit_log_entries".} = ref object
    id {.primary, autoincrement.}: int
    action: string

suite "query builder":
  test "builds a typed where clause from AST":
    let stmt =
      select(User)
        .where(it.age >= 18 and it.status == "active")
        .orderBy("createdAt", descending = true)
        .limit(10)
        .build()

    check stmt.sql ==
      "SELECT * FROM \"users\" WHERE (\"age\" >= ? AND \"status\" = ?) ORDER BY \"createdAt\" DESC LIMIT 10"

    check stmt.params.len == 2
    check stmt.params[0].kind == svInteger
    check stmt.params[0].intValue == 18
    check stmt.params[1].kind == svText
    check stmt.params[1].textValue == "active"

  test "supports null checks":
    let stmt = select(User).where(it.status == nil).build()
    check stmt.sql == "SELECT * FROM \"users\" WHERE \"status\" IS NULL"
    check stmt.params.len == 0

  test "supports multi-order and offset":
    let stmt =
      select(User)
        .orderBy("createdAt", descending = true)
        .orderBy("id")
        .offset(20)
        .build()

    check stmt.sql ==
      "SELECT * FROM \"users\" ORDER BY \"createdAt\" DESC, \"id\" ASC LIMIT -1 OFFSET 20"

  test "supports paginate helper":
    let stmt =
      select(User)
        .where(it.age >= 18)
        .paginate(page = 3, perPage = 25)
        .build()

    check stmt.sql == "SELECT * FROM \"users\" WHERE \"age\" >= ? LIMIT 25 OFFSET 50"
    check stmt.params.len == 1
    check stmt.params[0].asInt64() == 18

  test "uses table pragma name by default":
    let stmt = select(AuditLog).build()
    check stmt.sql == "SELECT * FROM \"audit_log_entries\""
