import std/[options, tables, times, unittest]

import ../src/nimtra/[mapper, values]

type
  UserRef = ref object
    id: int
    name: string
    active: bool
    score: float64
    createdAt: DateTime
    nickname: Option[string]

  UserObj = object
    id: int
    name: string
    active: bool

suite "model mapper":
  test "maps sql row to ref model":
    var row: SqlRow
    row["id"] = toSqlValue(1)
    row["name"] = toSqlValue("Alice")
    row["active"] = toSqlValue(1)
    row["score"] = toSqlValue(4.25)
    row["createdAt"] = toSqlValue("2026-03-07T12:34:56+09:00")
    row["nickname"] = nullValue()

    let user = rowToModel[UserRef](row)
    check not user.isNil
    check user.id == 1
    check user.name == "Alice"
    check user.active
    check abs(user.score - 4.25) < 0.0001
    check user.createdAt.year == 2026
    check user.nickname.isNone

  test "maps sql row to value object":
    var row: SqlRow
    row["id"] = toSqlValue(3)
    row["name"] = toSqlValue("Bob")
    row["active"] = toSqlValue(0)

    let user = rowToModel[UserObj](row)
    check user.id == 3
    check user.name == "Bob"
    check user.active == false

  test "maps multiple rows":
    var row1: SqlRow
    row1["id"] = toSqlValue(1)
    row1["name"] = toSqlValue("A")
    row1["active"] = toSqlValue(true)

    var row2: SqlRow
    row2["id"] = toSqlValue(2)
    row2["name"] = toSqlValue("B")
    row2["active"] = toSqlValue(false)

    let users = rowsToModels[UserObj]([row1, row2])
    check users.len == 2
    check users[0].id == 1
    check users[1].name == "B"

  test "maps snake_case and case-insensitive columns":
    var row: SqlRow
    row["ID"] = toSqlValue(10)
    row["name"] = toSqlValue("Casey")
    row["active"] = toSqlValue(1)
    row["created_at"] = toSqlValue("2026-03-07 12:34:56")

    let user = rowToModel[UserRef](row)
    check user.id == 10
    check user.createdAt.month == mMar
