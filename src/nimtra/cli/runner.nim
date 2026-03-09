const runnerTemplate* = """
import std/[asyncdispatch, json, macros, options]
import $1 # Import user's schema file
import nimtra

macro dumpSchemaMeta(): untyped =
  result = newCall(bindSym("newJArray"))
  # In a real scenario, we'd need to parse the user's file or require them to export `let schema = @[ModelA, ModelB]`
  # For this MVP, we will require the user to define a specific proc `getNimtraSchema(): seq[ModelMeta]`
  # Let's call that proc directly and serialize it to JSON to print to stdout.
  discard

proc main() =
  # We assume the user's schema file exposes a proc: `proc exportedNimtraModels*(): seq[ModelMeta]`
  let metas = exportedNimtraModels()
  let jArr = newJArray()
  for m in metas:
    let jm = newJObject()
    jm["table"] = %m.table
    var fields = newJArray()
    for f in m.fields:
      let jf = newJObject()
      jf["name"] = %f.name
      jf["nimType"] = %f.nimType
      jf["dbType"] = %f.dbType
      jf["primary"] = %f.primary
      jf["autoincrement"] = %f.autoincrement
      jf["unique"] = %f.unique
      jf["indexed"] = %f.indexed
      if f.maxLength.isSome: jf["maxLength"] = %f.maxLength.get()
      if f.defaultValue.isSome: jf["defaultValue"] = %f.defaultValue.get()
      fields.add(jf)
    jm["fields"] = fields
    jArr.add(jm)
  
  echo $jArr

main()
"""
