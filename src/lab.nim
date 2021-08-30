import sugar
import macros
import strutils
import strformat
import sequtils
import tables
import times
import sets
import re
import math


var a = "1H"
var matches: array[2, string]

echo match(a, re"(\d+)([a-zA-Z]+)?", matches)
echo matches

var b = {"a":1,"b":3}.toTable()
b["a"] = 2
echo b
echo b["a"]

echo toHashSet([1,2,3,4]) - toHashSet([2,3,5])

echo false.ord

for i in 0..<0:
    echo i

echo "aaa".replace("c", "b")

echo toHashSet([1])