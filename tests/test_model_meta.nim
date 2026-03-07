import std/[options, unittest]

import ../src/nimtra/model

type
  User {.table: "app_users".} = ref object
    id {.primary, autoincrement.}: int
    name {.maxLength: 50.}: string
    email {.unique.}: string
    createdAt {.default: "CURRENT_TIMESTAMP".}: string

suite "model metadata":
  test "extracts fields and constraints":
    let meta = modelMeta(User)
    check meta.name == "User"
    check meta.table == "app_users"
    check meta.fields.len == 4

    check meta.fields[0].name == "id"
    check meta.fields[0].primary
    check meta.fields[0].autoincrement

    check meta.fields[1].name == "name"
    check meta.fields[1].maxLength == some(50)

    check meta.fields[2].name == "email"
    check meta.fields[2].unique
    check not meta.fields[2].indexed

    check meta.fields[3].name == "createdAt"
    check meta.fields[3].defaultValue == some("CURRENT_TIMESTAMP")
