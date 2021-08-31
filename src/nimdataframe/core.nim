import sugar
import macros
import strutils
import strformat
import sequtils
import tables
import times
import stats
import sets
import re

import typedef

###############################################################
type NimDataFrameError* = object of CatchableError
type UnimplementedError* = object of CatchableError


###############################################################
# 「_」で囲われた文字列を持つ列名は、基本的には禁止にする
const dfEmpty* = ""
const defaultIndexName* = "_idx_"
const mergeIndexName* = "_on_"
const defaultDateTimeFormat* = "yyyy-MM-dd HH:mm:ss"


###############################################################
#parse... : cellに対しての型変換
#to...    : seriesに対しての型変換
proc initRow*(): Row =
    result = initTable[string, Cell]()

proc initSeries*(): Series =
    result = @[]

proc initFilterSeries*(): FilterSeries =
    result = @[]


iterator columns*(df: DataFrame): string =
    for key in df.data.keys:
        yield key

iterator rows*(df: DataFrame): Row =
    let maxRowNumber = min(
        collect(newSeq) do:
            for colName in df.columns:
                df.data[colName].len
    )
    for i in 0..<maxRowNumber:
        var row = initRow()
        for colName in df.columns:
            row[colName] = df.data[colName][i]
        yield row

proc getColumns*(df: DataFrame): seq[string] =
    for column in df.columns:
        result.add(column)

proc getSeries*(df: DataFrame): seq[Series] =
    for value in df.data.values:
        result.add(value)

proc getRows*(df: DataFrame): seq[Row] =
    for row in df.rows:
        result.add(row)

proc initRow*(df: DataFrame): Row =
    result = initRow()
    for colName in df.columns:
        result[colName] = dfEmpty


proc `$`*(x: DateTime): string =
    x.format(defaultDateTimeFormat)

proc parseString*[T](x: T): Cell =
    when typeof(T) is DateTime:
        result = x.format(defaultDateTimeFormat)
    else:
        result = $(x)

proc parseDatetime*(c: Cell, format=defaultDateTimeFormat): DateTime =
    c.parse(format)

proc genParseDatetime*(format=defaultDateTimeFormat): Cell -> DateTime =
    result =
        proc(c:Cell): DateTime =
            c.parseDatetime(format)


proc to*[T](s: Series, parser: Cell -> T): seq[T] =
    result = collect(newSeq):
        for c in s:
            parser(c)

proc toInt*(s: Series): seq[int] =
    to(s, parseInt)

proc toFloat*(s: Series): seq[float] =
    to(s, parseFloat)

proc toDatetime*(s: Series, format=defaultDateTimeFormat): seq[DateTime] =
    to(s, genParseDatetime(format))

proc toString*[T](arr: openArray[T]): Series =
    result = initSeries()
    for a in arr:
        result.add(a.parseString())


proc `[]`*(df: DataFrame, colName: ColName): Series =
    ## DataFrameからSeriesを取り出す.
    df.data[colName]

proc `[]=`*[T](df: var DataFrame, colName: ColName, right: openArray[T]) =
    ## DataFrameのSeriesに代入する.
    ## 代入されるarrayの各値はstringにキャストされる.
    var newSeries = collect(newSeq):
        for c in right:
            c.parseString()
    df.data.del(colName)
    df.data.add(colName, newSeries)


proc initDataFrame*(): DataFrame =
    result.data = initTable[ColName, Series]()
    result.indexCol = defaultIndexName

proc initDataFrame*(df: DataFrame): DataFrame =
    result = initDataFrame()
    result.indexCol = df.indexCol
    for colName in df.columns:
        result.data[colName] = initSeries()

proc initDataFrameGroupBy*(): DataFrameGroupBy =
    result.data = initTable[seq[ColName], DataFrame]()
    result.columns = @[]


proc len*(df: DataFrame): int =
    ## DataFrameの長さを返す
    ## no healthCheck
    result = df.data[df.indexCol].len
    #[
    result = max(
        collect(newSeq) do:
            for colName in df.columns:
                df[colName].len
    )
    ]#

proc addRow*(df: var DataFrame, row: Row, autoIndex=false, fillEmpty=false) =
    var columns: seq[ColName] = @[]
    for colName in row.keys:
        columns.add(colName)
    let columnsHash = toHashSet(columns)
    let dfColumnsHash = toHashSet(df.getColumns())
    if dfColumnsHash == columnsHash or
        fillEmpty or
        (autoIndex and dfColumnsHash - toHashSet([df.indexCol]) == columnsHash):
        for colName in dfColumnsHash:
            #fillEmptyフラグ無し
            if not fillEmpty:
                #autoIndexフラグ無し
                if not autoIndex:
                    df.data[colName].add(row[colName])
                #autoIndexフラグあり
                else:
                    if colName == df.indexCol:
                        df.data[colName].add($(df.len))
                    else:
                        df.data[colName].add(row[colName])
            #fillEmptyフラグあり
            else:
                #autoIndexフラグ無し
                if not autoIndex:
                    if columnsHash.contains(colName):
                        df.data[colName].add(row[colName])
                    else:
                        df.data[colName].add(dfEmpty)
                #autoIndexフラグあり
                else:
                    if colName == df.indexCol:
                        df.data[colName].add($(df.len))
                    else:
                        if columnsHash.contains(colName):
                            df.data[colName].add(row[colName])
                        else:
                            df.data[colName].add(dfEmpty)
    else:
        raise newException(NimDataFrameError, fmt"not found {dfColumnsHash-columnsHash}")

proc addRow*[T](df: var DataFrame, row: openArray[(ColName, T)], autoIndex=false, fillEmpty=false) =
    var newRow: Row
    for (colName, value) in row:
        newRow[colName] = value.parseString()
    df.addRow(newRow, autoIndex, fillEmpty)

proc addRows*[T](df: var DataFrame, items: openArray[(ColName, seq[T])], autoIndex=false, fillEmptyRow=false, fillEmptyCol=false) =
    ##
    runnableExamples:
        var df = toDataFrame(
            columns = {
                "a": @[1,2,3,4],
                "b": @[10,20,30,40],
                "c": @[100,200,300,400],
            },
            indexCol = "a",
        )
        df.addRow(
            items = {
                "b": @[50,60],
                "c": @[500]
            },
            autoIndex=true,
            fillEmptyRow=true,
            fillEmptyCol=true,
        )
    ##

    let itemTable = items.toTable()
    var columns: seq[ColName] = @[]
    var lengths: seq[int] = @[]
    for (colName, s) in itemTable.pairs():
        columns.add(colName)
        lengths.add(s.len)
    let columnsHash = toHashSet(columns)
    let dfColumnsHash = toHashSet(df.getColumns())
    let lengthsHash = toHashSet(lengths)
    let length = max(toHashSet(lengths).toSeq())
    let dfLength = df.len
    if dfColumnsHash == columnsHash or
        fillEmptyCol or
        (autoIndex and dfColumnsHash - toHashSet([df.indexCol]) == columnsHash):
        if lengthsHash.len == 1 or fillEmptyRow:
            for colName in dfColumnsHash:
                #fillEmptyColフラグあり
                if fillEmptyCol:
                    #fillEmptyRowフラグあり
                    if fillEmptyRow:
                        #autoIndexフラグあり、かつ、colNameがindexCol
                        if autoIndex and colName == df.indexCol:
                            for i in 0..<length:
                                df.data[colName].add($(dfLength+i))
                        else:
                            #列名がない場合
                            if not columnsHash.contains(colName):
                                for i in 0..<length:
                                    df.data[colName].add(dfEmpty)
                            #列名がある場合
                            else:
                                for c in itemTable[colName]:
                                    df.data[colName].add(c.parseString())
                                for i in itemTable[colName].len..<length:
                                    df.data[colName].add(dfEmpty)
                    #fillEmptyRowフラグ無し
                    else:
                        #autoIndexフラグあり、かつ、colNameがindexCol
                        if autoIndex and colName == df.indexCol:
                            for i in 0..<length:
                                df.data[colName].add($(dfLength+i))
                        else:
                            #列名がない場合
                            if not columnsHash.contains(colName):
                                for i in 0..<length:
                                    df.data[colName].add(dfEmpty)
                            #列名がある場合
                            else:
                                for c in itemTable[colName]:
                                    df.data[colName].add(c.parseString())
                #fillEmptyColフラグ無し
                else:
                    #fillEmptyRowフラグあり
                    if fillEmptyRow:
                        #autoIndexフラグあり、かつ、colNameがindexCol
                        if autoIndex and colName == df.indexCol:
                            for i in 0..<length:
                                df.data[colName].add($(dfLength+i))
                        else:
                            for c in itemTable[colName]:
                                df.data[colName].add(c.parseString())
                            for i in itemTable[colName].len..<length:
                                df.data[colName].add(dfEmpty)
                    #fillEmptyRowフラグ無し
                    else:
                        #autoIndexフラグあり、かつ、colNameがindexCol
                        if autoIndex and colName == df.indexCol:
                            for i in 0..<length:
                                df.data[colName].add($(dfLength+i))
                        else:
                            for c in itemTable[colName]:
                                df.data[colName].add(c.parseString())
        else:
            raise newException(NimDataFrameError, fmt"items must be same length, but got '{lengthsHash}'")
    else:
        raise newException(NimDataFrameError, fmt"not found {dfColumnsHash-columnsHash}")

proc addColumns*[T](df: var DataFrame, columns: openArray[(ColName, seq[T])], fillEmpty=false) =
    let columnTable = columns.toTable()
    var lengths: seq[int] = @[]
    for (colName, s) in columnTable.pairs():
        lengths.add(s.len)
    let lengthHash = toHashSet(lengths)
    let colLength = lengthHash.toSeq()[0]
    let dfLength = df.len
    #すべての長さが一致していた場合
    if lengthHash.len == 1 and
        ((fillEmpty and colLength <= dfLength) or (not fillEmpty and colLength == dfLength)):
        for colName in columnTable.keys:
            df[colName] = initSeries()
        if fillEmpty:
            for colName in columnTable.keys:
                for i in 0..<dfLength:
                    if i < colLength:
                        df.data[colName].add(columnTable[colName][i].parseString())
                    else:
                        df.data[colName].add(dfEmpty)
        else:
            for colName in columnTable.keys:
                for i in 0..<dfLength:
                    df.data[colName].add(columnTable[colName][i].parseString())
    else:
        if lengthHash.len != 1:
            raise newException(NimDataFrameError, "argument 'columns' must be same length")
        elif fillEmpty:
            raise newException(NimDataFrameError, "each 'columns' must be shorter than length of DataFrame")
        else:
            raise newException(NimDataFrameError, "length of 'columns' must be the same as length of DataFrame")

proc deepCopy*(df: DataFrame): DataFrame =
    result = initDataFrame(df)
    for i in 0..<df.len:
        for colName in df.columns:
            result.data[colName].add(df[colName][i])

proc `[]`*(df: DataFrame, colNames: openArray[ColName]): DataFrame =
    ## 指定した列だけ返す.
    result = initDataFrame()
    let columns = df.getColumns()
    for colName in colNames:
        if not columns.contains(colName):
            raise newException(NimDataFrameError, fmt"df doesn't have column {colName}")
        result.data[colName] = df[colName]
    result.data[df.indexCol] = df[df.indexCol]

proc keep*(df: DataFrame, fs: FilterSeries): DataFrame =
    ## trueをkeepする（fsがtrueの行だけ返す）.
    result = initDataFrame(df)
    for colName in df.columns:
        for i, b in fs.pairs():
            if b:
                result.data[colName].add(df[colName][i])
proc drop*(df: DataFrame, fs: FilterSeries): DataFrame =
    ## trueをdropする（fsがtrueの行を落として返す）（fsがfalseの行だけ返す）.
    result = initDataFrame(df)
    for colName in df.columns:
        for i, b in fs.pairs():
            if not b:
                result.data[colName].add(df[colName][i])

proc `[]`*(df: DataFrame, fs: FilterSeries): DataFrame =
    ## fsがtrueの行だけ返す.
    df.keep(fs)

proc `[]`*(df: DataFrame, slice: HSlice[int, int]): DataFrame =
    ## sliceの範囲の行だけ返す.
    result = initDataFrame(df)
    let len = df.len
    for i in slice:
        if i < 0 or i >= len:
            continue
        for colName in df.columns:
            result.data[colName].add(df[colName][i])

proc `[]`*(df: DataFrame, indices: openArray[int]): DataFrame =
    ## indicesの行だけ返す.
    result = initDataFrame(df)
    let len = df.len
    for i in indices:
        if i < 0 or i >= len:
            continue
        for colName in df.columns:
            result.data[colName].add(df[colName][i])

proc iloc*(df: DataFrame, i: int): Row =
    ## index番目の行をRow形式で返す.
    result = initRow()
    for colName in df.columns:
        result[colName] = df.data[colName][i]

proc loc*(df: DataFrame, c: Cell): DataFrame =
    ## indexの行の値がcの値と一致する行を返す.
    result = initDataFrame(df)
    let columns = df.getColumns()
    for i in 0..<df.len:
        if df[df.indexCol][i] == c:
            for colName in columns:
                result.data[colName].add(df[colName][i])

proc head*(df: DataFrame, num: int): DataFrame =
    result = initDataFrame(df)
    for i in 0..<min(num,df.len):
        for colName in result.columns:
            result.data[colName].add(df[colName][i])
proc tail*(df: DataFrame, num: int): DataFrame =
    result = initDataFrame(df)
    for i in df.len-min(num,df.len)..<df.len:
        for colName in result.columns:
            result.data[colName].add(df[colName][i])

proc index*(df: DataFrame): Series =
    df[df.indexCol]

proc shape*(df: DataFrame): (int,int) =
    let colNumber = df.getColumns.len
    let rowNumber = df.len
    result = (rowNumber, colNumber)

proc size*(df: DataFrame, excludeIndex=false): int =
    result = df.len * (
        if excludeIndex:
            df.getColumns().len - 1
        else:
            df.getColumns().len
    )

