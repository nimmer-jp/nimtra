import std/unittest

import ../src/nimtra/dialects

suite "dialect placeholder rewriting":
  test "sqlite keeps question marks":
    let d = newSQLiteDialect()
    check d.applyPlaceholders("SELECT * FROM users WHERE id = ? AND status = ?") ==
      "SELECT * FROM users WHERE id = ? AND status = ?"

  test "postgres rewrites placeholders":
    let d = newPostgresDialect()
    check d.applyPlaceholders("SELECT * FROM users WHERE id = ? AND status = ?") ==
      "SELECT * FROM users WHERE id = $1 AND status = $2"
