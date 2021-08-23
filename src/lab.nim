import sugar
import macros
import strutils
import strformat
import sequtils
import tables
import times

let rows = @[@[1,2,3,4],@[2,3],@[1,2,3,4,5,6]]
let colNumber = max(
    collect(newSeq) do:
        for row in rows:
            row.len
    )
echo colNumber
var a = 10
for i in 0..<a:
    echo i