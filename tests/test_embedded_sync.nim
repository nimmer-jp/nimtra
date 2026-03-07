import std/[asyncdispatch, unittest]

import ../src/nimtra/driver/[libsql_embedded, libsql_http]

suite "embedded sync hook":
  test "invalid library path fails with LibSQLError":
    expect(LibSQLError):
      discard openEmbeddedReplica(
        path = ":memory:",
        libraryPath = "/tmp/definitely-missing/libsql.dylib"
      )

  test "openLibSQLWithEmbeddedSync bubbles load failures":
    expect(LibSQLError):
      discard waitFor openLibSQLWithEmbeddedSync(
        url = "https://example.com",
        replicaPath = ":memory:",
        libraryPath = "/tmp/definitely-missing/libsql.dylib"
      )
