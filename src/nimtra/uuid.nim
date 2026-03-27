import std/[strutils, sysrand, times]

const
  UuidTextLength = 36
  UuidNilText* = "00000000-0000-0000-0000-000000000000"
  UuidHexDigits = "0123456789abcdef"

type
  UUID* = distinct string

  UuidVersion* = enum
    uvUnknown = 0
    uv1 = 1
    uv2 = 2
    uv3 = 3
    uv4 = 4
    uv5 = 5
    uv6 = 6
    uv7 = 7
    uv8 = 8

proc isHexChar(ch: char): bool {.inline.} =
  (ch >= '0' and ch <= '9') or (ch >= 'a' and ch <= 'f') or (ch >= 'A' and ch <= 'F')

proc hexNibble(ch: char): int {.inline.} =
  if ch >= '0' and ch <= '9':
    return ord(ch) - ord('0')
  let lower = ch.toLowerAscii()
  if lower >= 'a' and lower <= 'f':
    return ord(lower) - ord('a') + 10
  -1

proc isCanonicalUuidText(value: string): bool =
  if value.len != UuidTextLength:
    return false

  for i, ch in value:
    case i
    of 8, 13, 18, 23:
      if ch != '-':
        return false
    else:
      if not isHexChar(ch):
        return false
  true

proc hasRfcVariant(value: string): bool {.inline.} =
  if value.len != UuidTextLength:
    return false
  let variantNibble = value[19].toLowerAscii()
  variantNibble in {'8', '9', 'a', 'b'}

proc versionNibble(value: string): int {.inline.} =
  if value.len != UuidTextLength:
    return -1
  hexNibble(value[14])

proc normalizeUuidText(value: string): string =
  value.strip().toLowerAscii()

proc parseUuid*(value: string): UUID =
  ## Parses canonical UUID text and validates RFC variant bits.
  let normalized = normalizeUuidText(value)
  if not isCanonicalUuidText(normalized):
    raise newException(ValueError, "Invalid UUID format: " & value)
  if normalized != UuidNilText and not hasRfcVariant(normalized):
    raise newException(ValueError, "UUID does not use RFC variant bits: " & value)
  UUID(normalized)

proc isValidUuid*(value: string): bool =
  try:
    discard parseUuid(value)
    true
  except ValueError:
    false

proc `$`*(value: UUID): string {.inline.} =
  string(value)

proc uuidVersion*(value: string): UuidVersion =
  let normalized = normalizeUuidText(value)
  if not isCanonicalUuidText(normalized):
    return uvUnknown
  if normalized != UuidNilText and not hasRfcVariant(normalized):
    return uvUnknown

  let version = versionNibble(normalized)
  if version >= 1 and version <= 8:
    return UuidVersion(version)
  uvUnknown

proc uuidVersion*(value: UUID): UuidVersion =
  uuidVersion($value)

proc isUuidV4*(value: string): bool =
  uuidVersion(value) == uv4

proc isUuidV4*(value: UUID): bool =
  uuidVersion(value) == uv4

proc isUuidV7*(value: string): bool =
  uuidVersion(value) == uv7

proc isUuidV7*(value: UUID): bool =
  uuidVersion(value) == uv7

proc uuidTextFromBytes(bytes: openArray[byte]): string =
  if bytes.len != 16:
    raise newException(ValueError, "UUID requires 16 bytes")

  result = newStringOfCap(UuidTextLength)
  for i, b in bytes:
    if i == 4 or i == 6 or i == 8 or i == 10:
      result.add('-')
    result.add(UuidHexDigits[int((b shr 4) and 0x0F'u8)])
    result.add(UuidHexDigits[int(b and 0x0F'u8)])

proc newUuidV4*(): UUID =
  ## Generates a random RFC 9562 UUID version 4.
  var bytes = urandom(16)
  bytes[6] = (bytes[6] and 0x0F'u8) or 0x40'u8
  bytes[8] = (bytes[8] and 0x3F'u8) or 0x80'u8
  UUID(uuidTextFromBytes(bytes))

proc newUuidV7*(): UUID =
  ## Generates a time-ordered RFC 9562 UUID version 7.
  var bytes = urandom(16)
  let unixMs = uint64(int64(epochTime() * 1000.0))

  bytes[0] = byte((unixMs shr 40) and 0xFF'u64)
  bytes[1] = byte((unixMs shr 32) and 0xFF'u64)
  bytes[2] = byte((unixMs shr 24) and 0xFF'u64)
  bytes[3] = byte((unixMs shr 16) and 0xFF'u64)
  bytes[4] = byte((unixMs shr 8) and 0xFF'u64)
  bytes[5] = byte(unixMs and 0xFF'u64)

  bytes[6] = (bytes[6] and 0x0F'u8) or 0x70'u8
  bytes[8] = (bytes[8] and 0x3F'u8) or 0x80'u8
  UUID(uuidTextFromBytes(bytes))
