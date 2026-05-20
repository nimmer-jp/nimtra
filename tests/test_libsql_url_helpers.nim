import std/[strutils, unittest]

import ../src/nimtra/driver/libsql_http

suite "libsql url helpers":
  test "normalizeLibSqlUrl adds libsql scheme":
    check normalizeLibSqlUrl("my-db.turso.io") == "libsql://my-db.turso.io"

  test "normalizeLibSqlUrl keeps https":
    check normalizeLibSqlUrl("https://my-db.turso.io") == "https://my-db.turso.io"

  test "rejects duplicated scheme":
    expect LibSQLError:
      discard normalizeLibSqlUrl("libsql://libsql://my-db.turso.io")

  test "validateLibSqlUrl rejects libsql host":
    expect LibSQLError:
      validateLibSqlUrl("libsql://libsql/my-db")

  test "normalizeDbOpenError maps dns failures":
    let msg = normalizeDbOpenError("nodename nor servname provided, or not known")
    check "ホスト名" in msg
