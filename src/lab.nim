import sugar
import macros
import strutils
import strformat
import sequtils
import tables
import times
import sets

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

proc test1[T](a: T) =
    if typeof(T) is int:
        echo "int"
    elif typeof(T) is string:
        echo "string"
    else:
        echo "other"

test1(a)

echo "abc" < "abd"

echo toHashSet([@["1","1"],@["1","2"],@["1","1"]]).toSeq()

var t = initTable[seq[string], int]()
t[@["1","1"]] = 1
t[@["1","2"]] = 2
echo t

let aa = collect(newSeq):
    for x in [1,2,3,4,5,6,7,8]:
        var a: seq[int] = @[]
        for y in [1,2,3]:
            a.add(x)
        a
echo aa

for x in toHashSet([@["1","1"],@["1","2"],@["1","1"]]):
    echo x

proc test3(): Table[string,int] =
    result = {
        "a": 1,
        "b": 2,
    }.toTable()

echo test3()

for i in countup(0,10,3):
    echo i..<i+3

let xxx = @[1,2,3,4,5]
echo xxx[4..10]