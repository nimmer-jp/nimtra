import std/unittest

import ../src/nimtra/[query_builder, values]

type
  FeatureRequestRow = ref object
    id: int
    status: string
    updatedAt: string

suite "partial update builder":
  test "builds snake_case set and where":
    let stmt =
      update(FeatureRequestRow)
        .set(status = "done")
        .where(it.id == 42)
        .buildUpdate()

    check stmt.sql == "UPDATE \"feature_request_rows\" SET \"status\" = ? WHERE \"id\" = ?"
    check stmt.params.len == 2
    check stmt.params[0].textValue == "done"
    check stmt.params[1].intValue == 42
