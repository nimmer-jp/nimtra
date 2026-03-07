import std/[json, options, tables, unittest]

import ../src/nimtra/driver/libsql_http

suite "pipeline response parser":
  test "parses execute results wrapped in ok/response":
    let payload = parseJson("""
      {
        "results": [
          {
            "type": "ok",
            "response": {
              "type": "execute",
              "result": {
                "cols": [
                  {"name": "id", "decltype": "INTEGER"},
                  {"name": "name", "decltype": "TEXT"}
                ],
                "rows": [
                  [
                    {"type": "integer", "value": "1"},
                    {"type": "text", "value": "Alice"}
                  ]
                ],
                "affected_row_count": 1,
                "last_insert_rowid": "1"
              }
            }
          }
        ]
      }
    """)

    let result = parsePipelineResult(payload)
    check result.columns.len == 2
    check result.rows.len == 1
    check result.rows[0]["id"].intValue == 1
    check result.rows[0]["name"].textValue == "Alice"
    check result.affectedRowCount == 1
    check result.lastInsertRowId == some(1'i64)

  test "parses multiple execute responses":
    let payload = parseJson("""
      {
        "results": [
          {
            "type": "ok",
            "response": {
              "type": "execute",
              "result": {
                "cols": [{"name": "value", "decltype": "INTEGER"}],
                "rows": [[{"type": "integer", "value": "1"}]]
              }
            }
          },
          {
            "type": "ok",
            "response": {
              "type": "execute",
              "result": {
                "cols": [{"name": "value", "decltype": "INTEGER"}],
                "rows": [[{"type": "integer", "value": "2"}]]
              }
            }
          }
        ]
      }
    """)

    let results = parsePipelineResults(payload)
    check results.len == 2
    check results[0].rows[0]["value"].intValue == 1
    check results[1].rows[0]["value"].intValue == 2
