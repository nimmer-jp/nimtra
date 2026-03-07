import std/unittest

import ../src/nimtra

type
  User = ref object
    id {.primary, autoincrement.}: int
    age: int
    status: string

suite "public api":
  test "ufcs chain compiles":
    var db: LibSQLConnection
    let stmt = db
      .select(User)
      .where(it.age >= 18 and it.status == "active")
      .build()

    check stmt.sql == "SELECT * FROM \"users\" WHERE (\"age\" >= ? AND \"status\" = ?)"
    check stmt.params.len == 2
