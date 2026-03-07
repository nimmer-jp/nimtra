import ../src/nimtra/model

type
  UserRecord* {.table: "users".} = ref object
    id* {.primary, autoincrement.}: int
    email* {.unique.}: string

  DocumentRecord* {.table: "documents".} = ref object
    id* {.primary, autoincrement.}: int
    title*: string
    content*: string
