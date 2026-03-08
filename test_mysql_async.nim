import std/[asyncdispatch, os, strutils]
import db_connector/mysql

proc main() {.async.} =
  let conn = mysql.init(nil)
  echo "compiled!"

waitFor main()
