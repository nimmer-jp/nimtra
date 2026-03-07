import std/[asyncdispatch, os, unittest]

import ../src/nimtra/driver/libsql_http

suite "connection helpers":
  test "openLibSQLEnv validates missing url env":
    expect(ValueError):
      discard waitFor openLibSQLEnv(
        urlEnv = "NIMTRA_MISSING_URL_ENV",
        authTokenEnv = "NIMTRA_MISSING_TOKEN_ENV",
        syncUrlEnv = "NIMTRA_MISSING_SYNC_ENV"
      )

  test "openLibSQLEnv reads values":
    putEnv("NIMTRA_TEST_URL_ENV", "https://example.com")
    putEnv("NIMTRA_TEST_TOKEN_ENV", "token")
    putEnv("NIMTRA_TEST_SYNC_ENV", "https://sync.example.com")

    let db = waitFor openLibSQLEnv(
      urlEnv = "NIMTRA_TEST_URL_ENV",
      authTokenEnv = "NIMTRA_TEST_TOKEN_ENV",
      syncUrlEnv = "NIMTRA_TEST_SYNC_ENV",
      maxRetries = 5,
      retryBackoffMs = 123
    )

    check db.config.url == "https://example.com"
    check db.config.authToken == "token"
    check db.config.syncUrl == "https://sync.example.com"
    check db.config.maxRetries == 5
    check db.config.retryBackoffMs == 123
    waitFor db.close()

    delEnv("NIMTRA_TEST_URL_ENV")
    delEnv("NIMTRA_TEST_TOKEN_ENV")
    delEnv("NIMTRA_TEST_SYNC_ENV")

  test "withLibSQL opens and closes around body":
    let value = waitFor withLibSQL(
      url = "https://example.com",
      body = proc(db: LibSQLConnection): Future[string] {.closure, async.} =
        return db.config.url
    )
    check value == "https://example.com"
