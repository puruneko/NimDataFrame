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

#import threadpool
{.experimental: "parallel".}

var a = "1H"
var matches: array[2, string]

echo match(a, re"(\d+)([a-zA-Z]+)?", matches)
echo matches

var tStart = cpuTime()
const N = 100000
#[
proc f(x: float): float =
    sin(2*PI*x/N)
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
proc pi2(n: int): float =
    var ch = newSeq[float](n+1)
    for k in 0..ch.high:
        ch[k] = term(float(k))
    for k in 0..ch.high:
        result += ch[k]

tStart = cpuTime()
echo formatFloat(pi(5000))
echo cpuTime() - tStart
tStart = cpuTime()
echo formatFloat(pi2(5000))
echo cpuTime() - tStart
]#

tStart = cpuTime()
for i in 0..<N:
    discard sum([1,2,3,4,5])
echo cpuTime() - tStart

var a2:seq[int] = @[]
tStart = cpuTime()
for i in 0..<N:
    a2.add(sum([1,2,3,4,5]))
echo cpuTime() - tStart

var a3:seq[int] = newSeq[int](N)
tStart = cpuTime()
for i in 0..<N:
    a3[i] = sum([1,2,3,4,5])
echo cpuTime() - tStart

var a4:seq[string] = @[]
tStart = cpuTime()
for i in 0..<N:
    a4.add($sum([1,2,3,4,5]))
echo cpuTime() - tStart

var a5:seq[string] = newSeq[string](N)
tStart = cpuTime()
for i in 0..<N:
    a5[i] = $sum([1,2,3,4,5])
echo cpuTime() - tStart

type DataFrame[T] = object
    data: T
    colTable: seq[string]
var tpl: (int,float,string) = (1,2.0,"3")

echo tpl[0]
echo tpl[1]
echo tpl[2]