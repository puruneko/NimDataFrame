import sugar
import macros
import sequtils
import strutils
import strformat
import tables
import stats
import math
import times

import typedef
import core


macro operateCellAndT(a: typed, b: typed, operator: untyped): untyped =
    template body(opExpression: untyped): untyped{.dirty.} =
        when typeof(a) is int or typeof(b) is int:
            when typeof(a) is int:
                let left = a
                let right = b.parseInt()
            else:
                let left = a.parseInt()
                let right = b
        else:
            when typeof(a) is float or typeof(b) is float:
                when typeof(a) is float:
                    let left = a
                    let right = b.parseFloat()
                else:
                    let left = a.parseFloat()
                    let right = b
            else:
                when typeof(a) is DateTime or typeof(b) is DateTime:
                    when typeof(a) is DateTime:
                        let left = a
                        let right = b.parseDatetime()
                    else:
                        let left = a.parseDatetime()
                        let right = b
                else:
                    when typeof(a) is Cell:
                        let left = a
                        let right = b.parseString()
                    else:
                        let left = a.parseString()
                        let right = b
        result = opExpression
    var opExpression = newCall(
        nnkAccQuoted.newTree(
            operator
        ),
        newIdentNode("left"),
        newIdentNode("right"),
    )
    result = getAst(body(opExpression))

proc `+`*(a: Cell, b: StringDataFrameSafeTypes): StringDataFrameSafeTypes =
    ## 左辺Cell型、右辺T型の加算を計算する.
    operateCellAndT(a, b, `+`)

proc `-`*(a: Cell, b: StringDataFrameSafeTypes): StringDataFrameSafeTypes =
    operateCellAndT(a, b, `-`)

proc `*`*(a: Cell, b: StringDataFrameSafeTypes): StringDataFrameSafeTypes =
    operateCellAndT(a, b, `*`)

proc `/`*(a: Cell, b: StringDataFrameSafeTypes): StringDataFrameSafeTypes =
    operateCellAndT(a, b, `/`)

proc `+`*(a: StringDataFrameSafeTypes, b: Cell): StringDataFrameSafeTypes =
    ## 左辺T型、右辺Cell型の加算を計算する.
    operateCellAndT(b, a, `+`)

proc `-`*(a: StringDataFrameSafeTypes, b: Cell): StringDataFrameSafeTypes =
    operateCellAndT(b, a, `-`)

proc `*`*(a: StringDataFrameSafeTypes, b: Cell): StringDataFrameSafeTypes =
    operateCellAndT(b, a, `*`)

proc `/`*(a: StringDataFrameSafeTypes, b: Cell): StringDataFrameSafeTypes =
    operateCellAndT(b, a, `/`)

proc `===`*(a: Cell, b: StringDataFrameSafeTypes): bool =
    ## 左辺Cell型、右辺T型を等価比較する.
    operateCellAndT(a, b, `==`)

proc `!==`*(a: Cell, b: StringDataFrameSafeTypes): bool =
    operateCellAndT(a, b, `!=`)

proc `>`*(a: Cell, b: StringDataFrameSafeTypes): bool =
    operateCellAndT(a, b, `>`)

proc `<`*(a: Cell, b: StringDataFrameSafeTypes): bool =
    operateCellAndT(a, b, `<`)

proc `>=`*(a: Cell, b: StringDataFrameSafeTypes): bool =
    operateCellAndT(a, b, `>=`)

proc `<=`*(a: Cell, b: StringDataFrameSafeTypes): bool =
    operateCellAndT(a, b, `<=`)

proc `===`*(a: StringDataFrameSafeTypes, b: Cell): bool =
    ## 左辺T型、右辺Cell型を等価比較する.
    operateCellAndT(b, a, `==`)

proc `!==`*(a: StringDataFrameSafeTypes, b: Cell): bool =
    operateCellAndT(b, a, `!=`)
    
proc `>`*(a: StringDataFrameSafeTypes, b: Cell): bool =
    operateCellAndT(b, a, `>`)
    
proc `<`*(a: StringDataFrameSafeTypes, b: Cell): bool =
    operateCellAndT(b, a, `<`)
    
proc `>=`*(a: StringDataFrameSafeTypes, b: Cell): bool =
    operateCellAndT(b, a, `>=`)
    
proc `<=`*(a: StringDataFrameSafeTypes, b: Cell): bool =
    operateCellAndT(b, a, `<=`)


macro operateSeriesAndT(a: typed, b: typed, operator: untyped): untyped =
    template body(opExpression: untyped): untyped{.dirty.} =
        when typeof(a) is int or typeof(b) is int:
            when typeof(a) is int:
                let left = a
                result =
                    collect(newSeq):
                        for right in b.toInt():
                            opExpression
            else:
                let right = b
                result =
                    collect(newSeq):
                        for left in a.toInt():
                            opExpression
        else:
            when typeof(a) is float or typeof(b) is float:
                when typeof(a) is float:
                    let left = a
                    result =
                        collect(newSeq):
                            for right in b.toFloat():
                                opExpression
                else:
                    let right = b
                    result =
                        collect(newSeq):
                            for left in a.toFloat():
                                opExpression
            else:
                when typeof(a) is DateTime or typeof(b) is DateTime:
                    when typeof(a) is DateTime:
                        let left = a
                        result =
                            collect(newSeq):
                                for right in b.toDatetime():
                                    opExpression
                    else:
                        let right = b
                        result =
                            collect(newSeq):
                                for left in a.toDatetime():
                                    opExpression
                else:
                    when typeof(b) is Series:
                        let left = a.parseString()
                        result =
                            collect(newSeq):
                                for right in b:
                                    opExpression
                    else:
                        let right = b.parseString()
                        result =
                            collect(newSeq):
                                for left in a:
                                    opExpression
    var opExpression = newCall(
        nnkAccQuoted.newTree(
            operator
        ),
        newIdentNode("left"),
        newIdentNode("right"),
    )
    result = getAst(body(opExpression))

proc `===`*[T](a: Series, b: T): FilterSeries =
    operateSeriesAndT(a, b, `==`)

proc `!==`*[T](a: Series, b: T): FilterSeries =
    operateSeriesAndT(a, b, `!=`)

proc `>`*[T](a: Series, b: T): FilterSeries =
    operateSeriesAndT(a, b, `>`)
    
proc `<`*[T](a: Series, b: T): FilterSeries =
    operateSeriesAndT(a, b, `<`)

proc `>=`*[T](a: Series, b: T): FilterSeries =
    operateSeriesAndT(a, b, `>=`)
    
proc `<=`*[T](a: Series, b: T): FilterSeries =
    operateSeriesAndT(a, b, `<=`)
    
proc `===`*[T](a: T, b: Series): FilterSeries =
    operateSeriesAndT(b, a, `==`)

proc `!==`*[T](a: T, b: Series): FilterSeries =
    operateSeriesAndT(b, a, `!=`)

proc `>`*[T](a: T, b: Series): FilterSeries =
    operateSeriesAndT(b, a, `>`)

proc `<`*[T](a: T, b: Series): FilterSeries =
    operateSeriesAndT(b, a, `<`)

proc `>=`*[T](a: T, b: Series): FilterSeries =
    operateSeriesAndT(b, a, `>=`)

proc `<=`*[T](a: T, b: Series): FilterSeries =
    operateSeriesAndT(b, a, `<=`)

proc `&`*(a: FilterSeries, b: FilterSeries): FilterSeries =
    if a.len != b.len:
        raise newException(StringDataFrameError,
                fmt"& operator must be operated with FilterSeries of the same length")
    result =
        collect(newSeq):
            for i in 0..<a.len:
                a[i] and b[i]
                
proc `|`*(a: FilterSeries, b: FilterSeries): FilterSeries =
    if a.len != b.len:
        raise newException(StringDataFrameError,
                fmt"& operator must be operated with FilterSeries of the same length")
    result =
        collect(newSeq):
            for i in 0..<a.len:
                a[i] or b[i]
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
