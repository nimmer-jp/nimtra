import std/[asyncdispatch, os, strutils, unittest]

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
      retryBackoffMs = 123
    )

    let conn = LibSQLConnection(db)
    check conn.config.url == "https://example.com"
    check conn.config.authToken == "token"
    check conn.config.syncUrl == "https://sync.example.com"
    check conn.config.maxRetries == 5
    check conn.config.retryBackoffMs == 123
    waitFor db.close()

    delEnv("NIMTRA_TEST_URL_ENV")
    delEnv("NIMTRA_TEST_TOKEN_ENV")
    delEnv("NIMTRA_TEST_SYNC_ENV")

  test "withLibSQL opens and closes around body":
    let value = waitFor withLibSQL(
      url = "https://example.com",
      body = proc(db: DbConnection): Future[string] {.closure, async.} =
        let conn = LibSQLConnection(db)
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

  test "openLibSQLSyncEnv reads values":
    putEnv("NIMTRA_TEST_SYNC_URL_ENV2", "https://example.com")
    putEnv("NIMTRA_TEST_SYNC_TOKEN_ENV2", "token")
    putEnv("NIMTRA_TEST_SYNC_SYNC_ENV2", "https://sync.example.com")

    let db = openLibSQLSyncEnv(
      urlEnv = "NIMTRA_TEST_SYNC_URL_ENV2",
      authTokenEnv = "NIMTRA_TEST_SYNC_TOKEN_ENV2",
      syncUrlEnv = "NIMTRA_TEST_SYNC_SYNC_ENV2",
      maxRetries = 5,
      retryBackoffMs = 123
    )

    check db.config.url == "https://example.com"
    check db.config.authToken == "token"
    check db.config.syncUrl == "https://sync.example.com"
    check db.config.maxRetries == 5
    check db.config.retryBackoffMs == 123
    waitFor db.close()

    delEnv("NIMTRA_TEST_SYNC_URL_ENV2")
    delEnv("NIMTRA_TEST_SYNC_TOKEN_ENV2")
    delEnv("NIMTRA_TEST_SYNC_SYNC_ENV2")

  test "withLibSQLSync closes around body":
    let value = withLibSQLSync(
      url = "https://example.com",
      body = proc(db: LibSQLSyncConnection): string {.closure.} =
        db.config.url
    )
    check value == "https://example.com"

  test "sync pool borrows round-trip then closes":
    let pool = newLibSQLSyncPool(size = 1, url = "https://example.com")
    let cx = borrowLibSQLSync(pool)
    releaseLibSQLSync(pool, cx)
    closeLibSQLSyncPool(pool)

  test "sync pool rejects close while borrowed":
    let pool = newLibSQLSyncPool(size = 1, url = "https://example.com")
    discard borrowLibSQLSync(pool)
    expect(LibSQLError):
      closeLibSQLSyncPool(pool)

  test "thread-local sync pool config":
    initLibSQLSyncThreadPool(poolSize = 1, url = "https://example.com")
    let pool = threadLocalLibSQLSyncPool()
    check pool.capacity == 1
    let cx = borrowLibSQLSync(pool)
    releaseLibSQLSync(pool, cx)
    closeLibSQLSyncThreadLocal()
    libSQLSyncThreadConfigReady = false

  test "async pool lifecycle":
    proc run(): Future[bool] {.async.} =
      let pool = await newLibSQLAsyncPool(size = 2, url = "https://example.com")
      if pool.borrowed != 0 or pool.capacity != 2:
        return false
      let c1 = await borrowLibSQLAsync(pool)
      if pool.borrowed != 1:
        return false
      await releaseLibSQLAsync(pool, c1)
      if pool.borrowed != 0:
        return false
      await closeLibSQLAsyncPool(pool)
      result = true
    check waitFor run()

  test "async pool lends all slots":
    proc run(): Future[bool] {.async.} =
      let pool = await newLibSQLAsyncPool(2, url = "https://example.com")
      let c1 = await borrowLibSQLAsync(pool)
      let c2 = await borrowLibSQLAsync(pool)
      if pool.borrowed != 2:
        return false
      await releaseLibSQLAsync(pool, c2)
      if pool.borrowed != 1:
        return false
      await releaseLibSQLAsync(pool, c1)
      await closeLibSQLAsyncPool(pool)
      result = true
    check waitFor run()

  test "withLibSQLFromPool":
    proc run(): Future[bool] {.async.} =
      let pool = await newLibSQLAsyncPool(1, url = "https://example.com")
      let pip = await withLibSQLFromPool(pool, proc(db: DbConnection): Future[string] {.async.} =
        return LibSQLConnection(db).pipelineUrl
      )
      if not pip.endsWith("/v2/pipeline"):
        return false
      await closeLibSQLAsyncPool(pool)
      result = true
    check waitFor run()

  test "async pool close rejects borrowed":
    proc run(): Future[bool] {.async.} =
      let pool = await newLibSQLAsyncPool(1, url = "https://example.com")
      discard await borrowLibSQLAsync(pool)
      var ok = false
      try:
        await closeLibSQLAsyncPool(pool)
      except LibSQLError:
        ok = true
      result = ok
    check waitFor run()

  test "borrow from closed async pool fails":
    proc run(): Future[bool] {.async.} =
      let pool = await newLibSQLAsyncPool(1, url = "https://example.com")
      await closeLibSQLAsyncPool(pool)
      var ok = false
      try:
        discard await borrowLibSQLAsync(pool)
      except LibSQLError:
        ok = true
      result = ok
    check waitFor run()
