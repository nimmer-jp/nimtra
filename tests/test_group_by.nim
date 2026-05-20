import std/unittest

import ../src/nimtra/query_builder

type
  User = ref object
    id: int
    age: int

suite "group by":
  test "builds group by clause":
    let stmt =
      select(User)
        .columnsRaw("COUNT(*) AS total")
        .groupBy("age")
        .build()

    check stmt.sql == "SELECT COUNT(*) AS total FROM \"users\" GROUP BY \"age\""
