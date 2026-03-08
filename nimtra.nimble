version       = "0.1.2"
author        = "nimtra contributors"
description   = "Async-first ORM and libSQL client for Nim"
license       = "MIT"
srcDir        = "src"
bin           = @["nimtra", "nimtra_cli"]
installExt    = @["nim"]
skipDirs      = @["tests", ".git", ".github", ".nimble", ".nimcache"]

requires "nim >= 2.0.0"
requires "db_connector"

task test, "Run the package test suite":
  exec "for t in tests/test_*.nim; do nim c -r --hints:off --nimcache:.nimcache-tests --path:src \"$t\" || exit 1; done"
