import nimtra/model

type
  User* = ref object
    id* {.primary, autoincrement.}: int
    name* {.maxLength: 255.}: string
    age*: int

  Post* = ref object
    id* {.primary, autoincrement.}: int
    title*: string
    userId*: int

proc exportedNimtraModels*(): seq[ModelMeta] =
  @[
    modelMeta(User),
    modelMeta(Post)
  ]
