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

import ../src/stringdataframe

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

var b1 = @[1,2,3]
var b2 = @[1,2,3]
echo b1 == b2

var xxx: string
echo xxx == ""

tStart = cpuTime()
var c1: seq[int] = @[]
for i in 0..<100000:
    c1.add(i)
echo cpuTime() - tStart
tStart = cpuTime()
var c2 =
    collect(newSeq):
    for i in 0..<100000:
        i
echo cpuTime() - tStart

echo toHashSet([1,2,3])-toHashSet([1,2,3,4])

echo `<`(1,2)

macro compareSeriesAndT(x: Series, y:typed, operator:untyped): untyped =
    template body(compExpression: untyped):untyped{.dirty.} =
        echo x, y
        when typeof(y) is int:
            result =
                collect(newSeq):
                    for z in x.toInt():
                        compExpression
        when typeof(y) is float:
            result =
                collect(newSeq):
                    for z in x.toFloat():
                        compExpression
        else:
            result =
                collect(newSeq):
                    for z in x:
                        compExpression
    var compExpression = newCall(
        nnkAccQuoted.newTree(
            operator
        ),
        newIdentNode("z"),
        newIdentNode("y"),
    )
    result = getAst(body(compExpression))

proc whenFunc[T](a: T) =
    when typeof(T) is int:
        echo fmt"a is int"
    else:
        when typeof(T) is float:
            echo fmt"a is float"
        else:
            echo fmt"a is something"

whenFunc(1)
whenFunc(1.0)
whenfunc("1")



proc `~===`*[T](a: Series, b: T): seq[bool] =
    let x = a
    let y = b
    compareSeriesAndT(x, y, `==`)

proc `~===`*[T](a: T, b: Series): FilterSeries =
    let x = b
    let y = a
    compareSeriesAndT(x, y, `==`)

echo (@["1","2","3"] > 1) | (3 > @["1","2","3"])
echo 2 > @["1","2","3"]
echo @["1","2","3"] < 2
echo @["1","2","3"] === "1"
echo "1" === @["1","2","3"]

echo "1" + 1