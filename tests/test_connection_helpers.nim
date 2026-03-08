import std/[asyncdispatch, os, unittest]

import ../src/nimtra/driver/libsql_http
import ../src/nimtra/driver/base

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
      retryBackoffMs = 123,
      useCurlFallback = false,
      preferCurlTransport = true
    )

    let conn = LibSQLConnection(db)
    check conn.config.url == "https://example.com"
    check conn.config.authToken == "token"
    check conn.config.syncUrl == "https://sync.example.com"
    check conn.config.maxRetries == 5
    check conn.config.retryBackoffMs == 123
    check conn.config.useCurlFallback == false
    check conn.config.preferCurlTransport
    waitFor db.close()

    delEnv("NIMTRA_TEST_URL_ENV")
    delEnv("NIMTRA_TEST_TOKEN_ENV")
    delEnv("NIMTRA_TEST_SYNC_ENV")

  test "withLibSQL opens and closes around body":
    let value = waitFor withLibSQL(
      url = "https://example.com",
      useCurlFallback = false,
      preferCurlTransport = true,
      body = proc(db: DbConnection): Future[string] {.closure, async.} =
        let conn = LibSQLConnection(db)
        check conn.config.useCurlFallback == false
        check conn.config.preferCurlTransport
        return conn.config.url
    )
    check value == "https://example.com"

  test "openLibSQLEnv supports TURSO_URL/TURSO_TOKEN fallback":
    putEnv("TURSO_URL", "https://fallback.example.com")
    putEnv("TURSO_TOKEN", "fallback-token")
    delEnv("TURSO_DATABASE_URL")
    delEnv("TURSO_AUTH_TOKEN")

    let db = waitFor openLibSQLEnv()
    let conn = LibSQLConnection(db)
    check conn.config.url == "https://fallback.example.com"
    check conn.config.authToken == "fallback-token"
    waitFor db.close()

    delEnv("TURSO_URL")
    delEnv("TURSO_TOKEN")
