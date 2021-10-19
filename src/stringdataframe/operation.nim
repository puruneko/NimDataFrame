import sugar
import macros
import strutils
import sequtils
import strformat
import tables
import times
import algorithm
import sets
import re

import typedef
import core
import calculation


###############################################################
proc fillEmpty*[T](s: Series, fill: T): Series =
    result =
        collect(newSeq):
            for c in s:
                if c == dfEmpty:
                    fill.parseString()
                else:
                    c

proc fillEmpty*[T](df: StringDataFrame, fill: T): StringDataFrame =
    result = initStringDataFrame(df)
    for colIndex, colName in result.columns.pairs():
        result[colIndex] = fillEmpty(df[colIndex], fill)

proc dropEmpty*(s: Series): Series =
    result =
        collect(newSeq):
            for c in s:
                if c != dfEmpty:
                    c

proc dropEmpty*(df: StringDataFrame): StringDataFrame =
    result = initStringDataFrame(df)
    for i in 0..<df.len:
        var skip = false
        for colIndex, colName in result.columns.pairs():
            if df[colIndex][i] == dfEmpty:
                skip = true
                break
        if skip:
            continue
        for colIndex, colName in result.columns.pairs():
            result[colIndex].add(df[colIndex][i])


###############################################################
proc renameColumns*(df: StringDataFrame, renameMap: openArray[(ColName,ColName)]): StringDataFrame =
    ## DataFrameの列名を変更する.
    ## renameMapには変更前列名と変更後列名のペアを指定する.
    runnableExamples:
        df.renameColumns({"col1":"COL1","col2":"COL2"})
    ##

    result = df
    for renamePair in renameMap:
        if renamePair[1] == reservedColName:
            raise newException(
                    StringDataFrameReservedColNameError,
                    fmt"{reservedColName} is library-reserved name"
                )
        if result.columns.contains(renamePair[0]):
            result[renamePair[1]] = result[renamePair[0]]
            #インデックス列が書き換えられたときはインデックス情報を更新する
            if renamePair[0] == df.indexCol:
                result.indexCol = renamePair[1]
            #古い情報を削除する
            result.deleteColumn(renamePair[0])
        else:
            raise newException(StringDataFrameError, fmt"not found {renamePair[0]}")


proc resetIndex*[T](df: StringDataFrame, fn: int -> T): StringDataFrame =
    result = initStringDataFrame(df)
    for colIndex, colName in df.columns.pairs():
        if colName == df.indexCol:
            result[colName] =
                collect(newSeq):
                    for i in 0..<df.len:
                        fn(i).parseString()
        else:
            result[colIndex] = df[colIndex]

proc resetIndex*(df: StringDataFrame): StringDataFrame =
    let f = proc(i: int): Cell = $i
    result = df.resetIndex(f)

proc setIndex*(df: StringDataFrame, indexCol: ColName, delete=false): StringDataFrame =
    result = df
    if not df.columns.contains(indexCol):
        raise newException(StringDataFrameError, fmt"not found {indexCol}")
    if delete:
        result = result.dropColumn(result.indexCol, forceDropIndex=true)
    result.indexCol = indexCol


###############################################################
proc map*[T, U](s: Series, fn: U -> T, fromCell: Cell -> U): Series =
    ## Seriesの各セルに対して関数fnを適用する.
    ## 関数fnにはSeriesの各セルが渡され、関数fnは文字列に変換可能な任意の型を返す.
    ## 文字列型以外の操作を関数fn内で行う場合、fromCell関数にCell型から任意の型に変換する関数を渡す.
    runnableExamples:
        let triple = proc(c: int): int = c * 3
        df["col1"].map(triple, parseInt)
    ##

    result = initSeries()
    for c in s:
        result.add(fn(fromCell(c)))

proc stringMap*(s: Series, fn: string -> string): Series =
    let f = proc(c: Cell): string = c
    map(s, fn, f)
proc intMap*(s: Series, fn: int -> int): Series =
    map(s, fn, parseInt)
proc floatMap*(s: Series, fn: float -> float): Series =
    map(s, fn, parseFloat)
proc datetimeMap*(s: Series, fn: DateTime -> DateTime, format=defaultDatetimeFormat): Series =
    map(s, fn, genParseDatetime(format))

proc replace*(df: StringDataFrame, sub: string, by: string): StringDataFrame =
    result = initStringDataFrame(df)
    proc f(c: Cell): Cell =
        c.replace(sub, by)
    for colIndex, colName in df.columns.pairs():
        result[colIndex] = df[colIndex].stringMap(f)

proc replace*(df: StringDataFrame, sub: Regex, by: string): StringDataFrame =
    result = initStringDataFrame(df)
    proc f(c: Cell): Cell =
        c.replacef(sub, by)
    for colIndex, colName in df.columns.pairs():
        result.data[colIndex] = df[colIndex].stringMap(f)


###############################################################
proc filter*(df: StringDataFrame, fltr: Row -> bool): StringDataFrame =
    ## fltr関数に従ってDataFrameにフィルタをかける.
    ## fltr関数にはDataFrameの各列が渡され、fltr関数は論理値を返す.
    runnableExamples:
        df.filter(row => row["col1"] > 1000 and 3000 > row["col2"])
    ##

    var fs: FilterSeries = initFilterSeries()
    for row in df.rows:
        fs.add(fltr(row))
    result = df[fs]


###############################################################
proc cmpAsc[T](x: T, y: T): int =
    if x < y: -1
    elif x == y: 0
    else: 1
proc sort*[T](df: StringDataFrame, colName: ColName = reservedColName, fromCell: Cell -> T, ascending=true, ascFn: (T, T) -> int): StringDataFrame =
    ## DataFrameを指定列でソートする.
    ## 文字列以外のソートの場合はfromCellに文字列から指定型に変換する関数を指定する.
    ##
    result = initStringDataFrame(df)
    let cn =
        if colName != reservedColName:
            colName
        else:
            df.indexCol
    var sortSource =
        collect(newSeq):
            for rowNumber, cell in df[cn].pairs():
                (rowNumber, fromCell(cell))
    if ascending:
        let ascFn2 = proc(x: (int,T), y: (int,T)): int = ascFn(x[1], y[1])
        sortSource.sort(ascFn2)
    else:
        let desFn2 = proc(x: (int,T), y: (int,T)): int = ascFn(x[1], y[1]) * -1
        sortSource.sort(desFn2)
    #
    for sorted in sortSource:
        for colIndex, colName in df.columns.pairs():
            result[colIndex].add(df[colIndex][sorted[0]])

proc sort*[T](df: StringDataFrame, colName: ColName = reservedColName, fromCell: Cell -> T, ascending=true): StringDataFrame =
    result = sort(df, colName, fromCell, ascending, cmpAsc)

#TODO: x: openArray[T] or Tの適用
proc sort*[T](df: StringDataFrame, colNames: openArray[ColName], fromCells: openArray[Cell -> T], ascendings=openArray[bool], ascFns: openArray[(T, T) -> int]): StringDataFrame =
    if colNames.len != fromCells.len or colNames.len != ascendings.len or colNames.len != ascFns.len:
        raise newException(
                StringDataFrameError,
                "colNames and fromCells and asceindings and ascFns must have the same length"
        )
    result = initStringDataFrame(df, copy=true)
    for i in reversed(0..<colNames.len):
        result = result.sort(colNames[i], fromCells[i], ascendings[i], ascFns[i])

proc sort*[T](df: StringDataFrame, colNames: openArray[ColName], fromCell: Cell -> T, ascending=true, ascFn: (T, T) -> int): StringDataFrame =
    let fromCells =
        collect(newSeq):
            for _ in colNames:
                fromCell
    let ascFns =
        collect(newSeq):
            for _ in colNames:
                ascFn
    result = df.sort(colNames, fromCells, ascending, ascFns)

proc sort*[T](df: StringDataFrame, colNames: openArray[ColName], fromCell: Cell -> T, ascending=true): StringDataFrame =
    result = df.sort(colNames, fromCell, ascending, cmpAsc)

proc sort*(df: StringDataFrame, colName: ColName = reservedColName, ascending=true): StringDataFrame =
    let f = proc(c: Cell): Cell = c
    sort(df, colName, f, ascending)

proc sort*(df: StringDataFrame, colNames: openArray[ColName], ascending=true): StringDataFrame =
    result = initStringDataFrame(df, copy=true)
    for colName in reversed(colNames):
        result = result.sort(colName, ascending)

proc intSort*(df: StringDataFrame, colName: ColName = reservedColName, ascending=true): StringDataFrame =
    sort(df, colName, parseInt, ascending)
proc intSort*(df: StringDataFrame, colNames: openArray[ColName], ascending=true): StringDataFrame =
    result = df
    for colName in reversed(colNames):
        result = result.intSort(colName, ascending)

proc floatSort*(df: StringDataFrame, colName: ColName = reservedColName, ascending=true): StringDataFrame =
    sort(df, colName, parseFloat, ascending)
proc floatSort*(df: StringDataFrame, colNames: openArray[ColName], ascending=true): StringDataFrame =
    result = df
    for colName in reversed(colNames):
        result = result.floatSort(colName, ascending)

proc datetimeSort*(df: StringDataFrame, colName: ColName = reservedColName, format=defaultDatetimeFormat, ascending=true): StringDataFrame =
    sort(df, colName, genParseDatetime(format), ascending)
proc datetimeSort*(df: StringDataFrame, colNames: openArray[ColName], format=defaultDatetimeFormat, ascending=true): StringDataFrame =
    result = df
    for colName in reversed(colNames):
        result = result.datetimeSort(colName, format, ascending)


###############################################################
proc duplicated*(df: StringDataFrame, colNames: openArray[ColName] = []): FilterSeries =
    ## 重複した行はtrue、それ以外はfalse.
    ## 重複の評価行をcolNamesで指定する（指定なしの場合はインデックス列）.
    ##
    result = initFilterSeries()
    var columns = colNames.toSeq()
    for colName in columns:
        if colName == reservedColName:
            raise newException(
                    StringDataFrameReservedColNameError,
                    fmt"{reservedColName} is library-reserved name"
                )
    var checker = initTable[seq[string], bool]()
    if columns.len == 0:
        columns = @[df.indexCol]
    #1行ずつ見ていって、同じものがあったらtrue、まだないならfalseを格納
    for i in 0..<df.len:
        let row =
            collect(newSeq):
                for colName in columns:
                    df[colName][i]
        if row in checker:
            result.add(true)
        else:
            result.add(false)
            checker[row] = false

proc dropDuplicates*(df: StringDataFrame, colNames: openArray[ColName] = []): StringDataFrame =
    ## 重複した行を消す.
    ## 重複の評価行をcolNamesで指定する（指定なしの場合はインデックス）.
    ##
    df.drop(df.duplicated(colNames))

proc transpose*(df: StringDataFrame): StringDataFrame =
    result = initStringDataFrame()
    result.indexCol = df.indexCol
    #indexに重複がある場合、エラー
    if df.duplicated().contains(true):
        raise newException(StringDataFrameError, "duplicate indexes are not allowed in transpose action")
    #転置処理
    let colNameTable =
        collect(initTable):
            for i, colName in df.columns.pairs():
                {i: colName}
    for indexValue in df[df.indexCol]:
        result.addColumn(indexValue)
        let dfRow = df.loc(indexValue)
        for i in 0..<df.columns.len:
            if colNameTable[i] == df.indexCol:
                continue
            result[indexValue].add(dfRow[colNameTable[i]][0])
    result[df.indexCol] =
        collect(newSeq):
            for colName in df.columns:
                if colName == df.indexCol:
                    continue
                colName

proc T*(df: StringDataFrame): StringDataFrame =
    df.transpose()
