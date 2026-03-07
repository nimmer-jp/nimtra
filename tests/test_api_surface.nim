import std/unittest

import ../src/nimtra

type
  User = ref object
    id {.primary, autoincrement.}: int
    age: int
    status: string

  AccessLog {.table: "access_logs".} = ref object
    id {.primary, autoincrement.}: int
    userId: int

suite "public api":
  test "ufcs chain compiles":
    var db: LibSQLConnection
    let stmt = db
      .select(User)
      .where(it.age >= 18 and it.status == "active")
      .orderBy("age", descending = true)
      .offset(20)
      .build()

    check stmt.sql ==
      "SELECT * FROM \"users\" WHERE (\"age\" >= ? AND \"status\" = ?) ORDER BY \"age\" DESC LIMIT -1 OFFSET 20"
    check stmt.params.len == 2

  test "table pragma is reflected in select":
    let stmt = select(AccessLog).build()
    check stmt.sql == "SELECT * FROM \"access_logs\""

  test "crud supports non-integer id types":
    var db: LibSQLConnection
    check compiles(db.findById(User, "user_123"))
    check compiles(db.deleteById(User, "user_123"))

  test "query openArray overload compiles":
    var db: LibSQLConnection
    check compiles(db.query("SELECT 1"))
    check compiles(db.query("SELECT ?", [toSqlValue(1)]))

  test "upsert api compiles":
    var db: LibSQLConnection
    let user = User(id: 1, age: 20, status: "active")
    check compiles(db.upsert(user, ["id"]))
    check compiles(db.upsert(user, "id"))

  test "join api compiles":
    let stmt = select(User)
      .columnsRaw("\"users\".\"id\"", "\"profiles\".\"bio\"")
      .join("profiles", "\"profiles\".\"userId\" = \"users\".\"id\"")
      .build()
    check stmt.sql ==
      "SELECT \"users\".\"id\", \"profiles\".\"bio\" FROM \"users\" INNER JOIN \"profiles\" ON \"profiles\".\"userId\" = \"users\".\"id\""
