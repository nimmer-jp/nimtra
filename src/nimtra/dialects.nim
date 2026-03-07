import std/strutils

type
  PlaceholderStyle* = enum
    phQuestionMark,
    phDollarNumber

  Dialect* = ref object of RootObj

  SQLiteDialect* = ref object of Dialect
  PostgresDialect* = ref object of Dialect
  MySQLDialect* = ref object of Dialect

method name*(dialect: Dialect): string {.base.} =
  raise newException(CatchableError, "name() must be implemented")

method quoteIdent*(dialect: Dialect, ident: string): string {.base.} =
  raise newException(CatchableError, "quoteIdent() must be implemented")

method placeholderStyle*(dialect: Dialect): PlaceholderStyle {.base.} =
  phQuestionMark

proc newSQLiteDialect*(): Dialect =
  SQLiteDialect()

proc newPostgresDialect*(): Dialect =
  PostgresDialect()

proc newMySQLDialect*(): Dialect =
  MySQLDialect()

method name*(dialect: SQLiteDialect): string =
  "sqlite"

method quoteIdent*(dialect: SQLiteDialect, ident: string): string =
  "\"" & ident.replace("\"", "\"\"") & "\""

method placeholderStyle*(dialect: SQLiteDialect): PlaceholderStyle =
  phQuestionMark

method name*(dialect: PostgresDialect): string =
  "postgres"

method quoteIdent*(dialect: PostgresDialect, ident: string): string =
  "\"" & ident.replace("\"", "\"\"") & "\""

method placeholderStyle*(dialect: PostgresDialect): PlaceholderStyle =
  phDollarNumber

method name*(dialect: MySQLDialect): string =
  "mysql"

method quoteIdent*(dialect: MySQLDialect, ident: string): string =
  "`" & ident.replace("`", "``") & "`"

method placeholderStyle*(dialect: MySQLDialect): PlaceholderStyle =
  phQuestionMark

proc applyPlaceholders*(dialect: Dialect, sql: string): string =
  ## Rewrites anonymous placeholders based on dialect requirements.
  if dialect.placeholderStyle() == phQuestionMark:
    return sql

  var inSingleQuote = false
  var escaped = false
  var count = 0
  for ch in sql:
    if escaped:
      result.add(ch)
      escaped = false
      continue

    if ch == '\\':
      escaped = true
      result.add(ch)
      continue

    if ch == '\'':
      inSingleQuote = not inSingleQuote
      result.add(ch)
      continue

    if ch == '?' and not inSingleQuote:
      inc count
      result.add("$" & $count)
    else:
      result.add(ch)
