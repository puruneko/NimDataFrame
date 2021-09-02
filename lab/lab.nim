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

let text = """,a,b,c,"あ,
い"
,a,b,c,"あ,
い
う",え
,a,b,c,"あ,
い","う,え",お"""
let sep = ','
var dQuoteFlag = false
var cells: seq[seq[string]] = @[]
var cell = ""
cells.add(@[])
for i in 0..<text.len:
    if i != 0 and not dQuoteFlag and text[i-1] == '\r' and text[i] == '\n':
        continue
    elif not dQuoteFlag and (text[i] == sep or text[i] == '\n' or text[i] == '\r'):
        cells[^1].add(cell)
        cell = ""
        if text[i] == '\n' or text[i] == '\r':
            cells.add(@[])
    elif not dQuoteFlag and text[i] == '"':
        dQuoteFlag = true
    elif dQuoteFlag and text[i] == '"':
        dQuoteFlag = false
    else:
        cell.add(text[i])
if cell != "":
    cells[^1].add(cell)
    cell = ""
echo cells