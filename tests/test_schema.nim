import std/unittest

import ../src/nimtra/[model, schema]

type
  User {.table: "app_users".} = ref object
    id {.primary, autoincrement.}: int
    name {.maxLength: 50.}: string
    email {.unique.}: string
    age {.index.}: int
    role {.default: "guest".}: string

suite "schema sql generation":
  test "create table sql from model pragmas":
    let sql = createTableSql(User)
    check sql ==
      "CREATE TABLE IF NOT EXISTS \"app_users\" (\"id\" INTEGER PRIMARY KEY AUTOINCREMENT, \"name\" TEXT CHECK (length(\"name\") <= 50), \"email\" TEXT UNIQUE, \"age\" INTEGER, \"role\" TEXT DEFAULT 'guest')"

  test "create schema includes index definitions":
    let statements = createSchemaSql(User)
    check statements.len == 2
    check statements[1] ==
      "CREATE INDEX IF NOT EXISTS \"idx_app_users_age\" ON \"app_users\" (\"age\")"
