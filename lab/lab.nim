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

import threadpool
{.experimental: "parallel".}

var a = "1H"
var matches: array[2, string]

echo match(a, re"(\d+)([a-zA-Z]+)?", matches)
echo matches

const N = 1000000
proc f(x: float): float =
    sin(2*PI*x/N)
var tStart = cpuTime()
var s = newSeq[float](N)
tStart = cpuTime()
for i in 0..<N:
    s[i] = f(float(i))
echo cpuTime() - tStart

var s2: seq[float] = @[]
tStart = cpuTime()
for i in 0..<N:
    s2.add(f(float(i)))
echo cpuTime() - tStart

tStart = cpuTime()
parallel:
    for i in 0..<N:
        s[i] = spawn f(float(i))
echo cpuTime() - tStart

proc term(k: float): float = 4 * math.pow(-1, k) / (2*k + 1)

proc pi(n: int): float =
  var ch = newSeq[float](n+1)
  parallel:
    for k in 0..ch.high:
      ch[k] = spawn term(float(k))
  for k in 0..ch.high:
    result += ch[k]

echo formatFloat(pi(5000))