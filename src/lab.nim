import sugar
import macros
import strutils
import strformat
import sequtils
import tables
import times
import sets
import re


var a = "1H"
var matches: array[2, string]

echo match(a, re"(\d+)([a-zA-Z]+)?", matches)
echo matches

echo high(int)