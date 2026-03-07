import std/strutils

proc camelToSnake*(input: string): string =
  ## Converts CamelCase or mixedCase into snake_case.
  if input.len == 0:
    return ""

  result = newStringOfCap(input.len * 2)
  for i, ch in input:
    if ch in {'A'..'Z'}:
      if i > 0 and result.len > 0 and result[^1] != '_':
        result.add('_')
      result.add(ch.toLowerAscii())
    else:
      result.add(ch)

proc defaultTableName*(modelName: string): string =
  ## Uses a simple pluralization strategy for defaults.
  let snake = camelToSnake(modelName)
  if snake.len == 0:
    return ""
  if snake.endsWith("s"):
    return snake
  snake & "s"
