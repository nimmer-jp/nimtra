import std/[tables, sequtils, algorithm]

type
  ExistingIndex = object
    name: string
    columns: seq[string]
    unique: bool

proc groupIndexes(rows: seq[tuple[index_name: string, is_unique: bool, column_name: string, seqno: int]]): seq[ExistingIndex] =
  var idxMap = initTable[string, tuple[unique: bool, cols: seq[tuple[seqno: int, col: string]]]]()
  for r in rows:
    if not idxMap.hasKey(r.index_name):
      idxMap[r.index_name] = (r.is_unique, newSeq[tuple[seqno: int, col: string]]())
    idxMap[r.index_name].cols.add((r.seqno, r.column_name))
  
  for k, v in idxMap:
    var cols = v.cols
    cols.sort(proc(a, b: tuple[seqno: int, col: string]): int = cmp(a.seqno, b.seqno))
    result.add(ExistingIndex(name: k, unique: v.unique, columns: cols.mapIt(it.col)))

let rows = @[
  (index_name: "idx1", is_unique: true, column_name: "b", seqno: 2),
  (index_name: "idx1", is_unique: true, column_name: "a", seqno: 1)
]

echo groupIndexes(rows)
