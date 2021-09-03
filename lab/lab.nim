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

proc genIterator(a: string, b: int): iterator =
    result =
        iterator (): string =
            for i in 0..<b:
                yield a

let itr = genIterator("abc", 10)
for x in itr:
    echo x

template `[]`(tpl, key): untyped =
    tpl.key

var x: tuple[name: string, age: int]
x.name = "john"
x.age = 16

let n = "name"
echo x["name"]