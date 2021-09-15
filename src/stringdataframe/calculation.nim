import sugar
import sequtils
import strutils
import tables
import stats
import math

import typedef
import core

proc `+`*(a: Cell, b: float): float =
    ## 左辺Cell型、右辺float型の加算を計算する.
    parseFloat(a) + b
proc `-`*(a: Cell, b: float): float =
    parseFloat(a) - b
proc `*`*(a: Cell, b: float): float =
    parseFloat(a) * b
proc `/`*(a: Cell, b: float): float =
    parseFloat(a) / b
proc `+`*(a: float, b: Cell): float =
    ## 左辺float型、右辺Cell型の加算を計算する.
    a + parseFloat(b)
proc `-`*(a: float, b: Cell): float =
    a - parseFloat(b)
proc `*`*(a: float, b: Cell): float =
    a * parseFloat(b)
proc `/`*(a: float, b: Cell): float =
    a / parseFloat(b)

#TODO: int版も作る
proc `==`*(a: Cell, b: float): bool =
    ## 左辺Cell型、右辺float型を等価比較する.
    result = a.parseFloat() == b
proc `!=`*(a: Cell, b: float): bool =
    result = a.parseFloat() != b
proc `>`*(a: Cell, b: float): bool =
    result = a.parseFloat() > b
proc `<`*(a: Cell, b: float): bool =
    result = a.parseFloat() < b
proc `>=`*(a: Cell, b: float): bool =
    result = a.parseFloat() >= b
proc `<=`*(a: Cell, b: float): bool =
    result = a.parseFloat() <= b
proc `==`*(a: float, b: Cell): bool =
    ## 左辺float型、右辺Cell型を等価比較する.
    result = a == b.parseFloat()
proc `!=`*(a: float, b: Cell): bool =
    result = a != b.parseFloat()
proc `>`*(a: float, b: Cell): bool =
    result = a > b.parseFloat()
proc `<`*(a: float, b: Cell): bool =
    result = a < b.parseFloat()
proc `>=`*(a: float, b: Cell): bool =
    result = a >= b.parseFloat()
proc `<=`*(a: float, b: Cell): bool =
    result = a <= b.parseFloat()

proc `===`*[T](a: Series, b: T): FilterSeries =
    let bString = b.parseString()
    result =
        collect(newSeq):
            for c in a:
                c == bString
proc `!==`*[T](a: Series, b: T): FilterSeries =
    let bString = b.parseString()
    result =
        collect(newSeq):
            for c in a:
                c != bString
proc `>`*[T](a: Series, b: T): FilterSeries =
    let bString = b.parseString()
    result =
        collect(newSeq):
            for c in a:
                c > bString
proc `<`*[T](a: Series, b: T): FilterSeries =
    let bString = b.parseString()
    result =
        collect(newSeq):
            for c in a:
                c < bString
proc `>=`*[T](a: Series, b: T): FilterSeries =
    let bString = b.parseString()
    result =
        collect(newSeq):
            for c in a:
                c >= bString
proc `<=`*[T](a: Series, b: T): FilterSeries =
    let bString = b.parseString()
    result =
        collect(newSeq):
            for c in a:
                c <= bString
#[
]#

proc agg*[T](s: Series, aggFn: Series -> T): Cell =
    ## SeriesをCellに変換する.
    ## aggFnにはSeriesをCell変換する関数を指定する.
    runnableExamples:
        proc f(s: Series): Cell =
            result = ""
            for c in s:
                result &= c
        df["col1"].agg(f)
    ##

    try:
        result = aggFn(s).parseString()
    except:
        result = dfEmpty  

proc aggMath*(s: Series, aggFn: openArray[float] -> float): Cell =
    ## Seriesの統計量を計算する.
    ## aggFnにはSeriesをfloat変換した配列の統計量を計算する関数を指定する.
    runnableExamples:
        df["col1"].aggMath(stats.mean)
    ##

    try:
        let f = s.toFloat()
        result = aggFn(f).parseString()
    except:
        result = dfEmpty

proc count*(s: Series): Cell =
    let cnt = proc(s: openArray[float]): float =
        float(s.len)
    s.aggMath(cnt)
proc sum*(s: Series): Cell =
    s.aggMath(sum)
proc mean*(s: Series): Cell =
    s.aggMath(stats.mean)
proc std*(s: Series): Cell =
    s.aggMath(stats.standardDeviation)
proc max*(s: Series): Cell =
    s.aggMath(max)
proc min*(s: Series): Cell =
    s.aggMath(min)
proc v*(s: Series): Cell =
    s.aggMath(stats.variance)

proc agg*[T](df: StringDataFrame, aggFn: Series -> T): Row =
    ## DataFrameの各列に対して統計量を計算する.
    ## aggFnにはSeriesの統計量を計算する関数を指定する.
    runnableExamples:
        df.agg(mean)
    ##

    result = initRow(df)
    for (colName, s) in zip(df.columns, df.data):
        result[colName] = aggFn(s).parseString()

proc count*(df: StringDataFrame): Row =
    df.agg(count)
proc sum*(df: StringDataFrame): Row =
    df.agg(sum)
proc mean*(df: StringDataFrame): Row =
    df.agg(mean)
proc std*(df: StringDataFrame): Row =
    df.agg(std)
proc max*(df: StringDataFrame): Row =
    df.agg(max)
proc min*(df: StringDataFrame): Row =
    df.agg(min)
proc v*(df: StringDataFrame): Row =
    df.agg(v)
