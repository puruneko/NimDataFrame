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

type ColName = string
type ColType = enum
    IntCol, FloatCol, StringCol, DatetimeCol
type StringDataFrame[T] = object
    data: T
    columns: seq[ColName]
    colIndex: Table[ColName, int]
    colType: Table[ColName, ColType]

macro getDataFrameData(colTypes: static[openArray[ColType]]): untyped =
    var returnType = nnkPar.newTree()
    for colType in colTypes:
        let t = case colType
            of IntCol: bindSym"int"
            of FloatCol: bindSym"float"
            of StringCol: bindSym"string"
            of DatetimeCol: bindSym"DateTime"
        returnType.add(
            nnkBracketExpr.newTree(newIdentNode("seq"), t)
        )
    return returnType

proc initData[T](columnsWithType: openArray[(ColName, ColType)]): T =
    var res: getDataFrameData(columnsWithType)
    result = res

proc initStringDataFrame[T](columnsWithType: openArray[(ColName, ColType)]): StringDataFrame[T] =
    result = StringDataFrame[getDataFrameData(columnsWithType)]
    result.columns = @[]
    result.colIndex = initTable[ColName, int]()
    result.colType = initTable[ColName, ColType]()
    for i, (colName, colType) in columnsWithType.pairs():
        result.columns.add(colName)
        result.colIndex[colName] = i
        result.colType[colName] = colType

var columns = @["col1", "col2", "col3"]
var types: seq[ColType] = @[IntCol, FloatCol, StringCol]
var ct =
    collect(newSeq):
        for (c, t) in zip(columns, types):
            (c, t)
#var df = initStringDataFrame(ct)

var df: getDataFrameData(@[IntCol, FloatCol, StringCol])
echo typeof(df)
echo df

macro inspectType(df: typed): untyped =
    echo "--------"
    echo df.strVal
    echo df.getTypeImpl.repr
    for colType in df.getTypeImpl:
        echo colType.repr
        for cellType in colType.getTypeImpl:
            echo cellType.repr

inspectType(df)