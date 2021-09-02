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
import encodings


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

echo "aaa" != "bbb"

let sep = ","
let regex = fmt"("".+?""({sep}|\n|$)|(?<!"")[^{sep}""]*?({sep}|\n|$))"
let text = r",a,b,c,""あ,あ"""
let ec = open("utf-8", "utf-8")
let textConverted = ec.convert(text)
ec.close()
echo textConverted.split("\n")[0].findAll(re(regex))