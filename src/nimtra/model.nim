import std/[macros, options]
import ./utils

template primary* {.pragma.}
template autoincrement* {.pragma.}
template unique* {.pragma.}
template index* {.pragma.}
template maxLength*(n: static[int]) {.pragma.}
template `default`*(value: static[string]) {.pragma.}
template table*(name: static[string]) {.pragma.}

type
  FieldMeta* = object
    name*: string
    nimType*: string
    dbType*: string
    primary*: bool
    autoincrement*: bool
    unique*: bool
    indexed*: bool
    maxLength*: Option[int]
    defaultValue*: Option[string]

  ModelMeta* = object
    name*: string
    table*: string
    fields*: seq[FieldMeta]

proc mapTypeToDbType(typeName: string): string {.compileTime.} =
  case typeName
  of "int", "int8", "int16", "int32", "int64", "uint", "uint8", "uint16", "uint32", "uint64", "bool":
    "INTEGER"
  of "float", "float32", "float64":
    "REAL"
  of "string", "cstring":
    "TEXT"
  of "seq[byte]":
    "BLOB"
  else:
    "TEXT"

proc extractPragmaName(node: NimNode): string {.compileTime.} =
  case node.kind
  of nnkSym, nnkIdent:
    $node
  of nnkCall:
    extractPragmaName(node[0])
  else:
    ""

proc extractFieldName(node: NimNode): string {.compileTime.} =
  case node.kind
  of nnkIdent, nnkSym:
    $node
  of nnkPostfix:
    if node.len >= 2:
      extractFieldName(node[1])
    else:
      ""
  of nnkPragmaExpr:
    extractFieldName(node[0])
  else:
    ""

proc extractPragmaList(fieldNode: NimNode): NimNode {.compileTime.} =
  if fieldNode.kind == nnkPragmaExpr and fieldNode.len > 1:
    fieldNode[1]
  else:
    newNimNode(nnkPragma)

proc parseTypeName(typeNode: NimNode): string {.compileTime.} =
  typeNode.repr

proc objectRecList(typeDef: NimNode): NimNode {.compileTime.} =
  var target = typeDef[2]
  if target.kind == nnkRefTy:
    target = target[0]
  if target.kind != nnkObjectTy:
    error("modelMeta() only supports object/ref object types", typeDef)
  target[2]

proc resolveTypeDefNode(typeNode: NimNode): NimNode {.compileTime.} =
  let directImpl = typeNode.getImpl
  if directImpl.kind == nnkTypeDef:
    return directImpl

  let typeInst = typeNode.getTypeInst
  if typeInst.kind == nnkBracketExpr and typeInst.len >= 2:
    let candidate = typeInst[^1]
    let candidateImpl = candidate.getImpl
    if candidateImpl.kind == nnkTypeDef:
      return candidateImpl

  error("modelMeta() expects a model type", typeNode)

macro modelMeta*(T: typedesc): untyped =
  let impl = resolveTypeDefNode(T)

  let typeHead = impl[0]
  var modelName = ""
  var tableName = ""

  if typeHead.kind == nnkPragmaExpr:
    modelName = extractFieldName(typeHead[0])
    let p = typeHead[1]
    for item in p:
      if extractPragmaName(item) == "table" and item.kind == nnkCall and item.len >= 2:
        tableName = item[1].strVal
  else:
    modelName = extractFieldName(typeHead)

  if tableName.len == 0:
    tableName = defaultTableName(modelName)

  var fieldNodes = newSeq[NimNode]()
  for entry in objectRecList(impl):
    if entry.kind != nnkIdentDefs:
      continue

    let typ = parseTypeName(entry[^2])
    for i in 0 ..< entry.len - 2:
      let rawName = extractFieldName(entry[i])
      if rawName.len == 0:
        continue

      var primaryField = false
      var autoincrementField = false
      var uniqueField = false
      var indexedField = false
      var maxLenNode = quote do: none(int)
      var defaultNode = quote do: none(string)

      let pragmas = extractPragmaList(entry[i])
      for pragmaNode in pragmas:
        let pname = extractPragmaName(pragmaNode)
        case pname
        of "primary":
          primaryField = true
        of "autoincrement":
          autoincrementField = true
        of "unique":
          uniqueField = true
        of "index":
          indexedField = true
        of "maxLength":
          if pragmaNode.kind == nnkCall and pragmaNode.len >= 2:
            let maxLenLit = newLit(pragmaNode[1].intVal.int)
            maxLenNode = quote do: some(`maxLenLit`)
        of "default":
          if pragmaNode.kind == nnkCall and pragmaNode.len >= 2:
            let value = pragmaNode[1]
            let defaultValueText =
              if value.kind == nnkStrLit:
                value.strVal
              else:
                value.repr
            let defaultLit = newLit(defaultValueText)
            defaultNode = quote do: some(`defaultLit`)
        else:
          discard

      let nameLit = newLit(rawName)
      let typeLit = newLit(typ)
      let dbTypeLit = newLit(mapTypeToDbType(typ))
      let primaryLit = newLit(primaryField)
      let autoincrementLit = newLit(autoincrementField)
      let uniqueLit = newLit(uniqueField)
      let indexedLit = newLit(indexedField)

      fieldNodes.add(
        quote do:
          FieldMeta(
            name: `nameLit`,
            nimType: `typeLit`,
            dbType: `dbTypeLit`,
            primary: `primaryLit`,
            autoincrement: `autoincrementLit`,
            unique: `uniqueLit`,
            indexed: `indexedLit`,
            maxLength: `maxLenNode`,
            defaultValue: `defaultNode`
          )
      )

  let fieldsArray = nnkPrefix.newTree(ident("@"), nnkBracket.newTree(fieldNodes))
  let modelNameLit = newLit(modelName)
  let tableNameLit = newLit(tableName)

  result = quote do:
    ModelMeta(
      name: `modelNameLit`,
      table: `tableNameLit`,
      fields: `fieldsArray`
    )

proc modelTableName*[T](_: typedesc[T]): string =
  modelMeta(T).table
