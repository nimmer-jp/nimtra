import std/unittest

import ../src/nimtra/[model, query_builder, values]

type
  User = ref object
    id {.primary, autoincrement.}: int
    age: int
    status: string
    createdAt: string

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
