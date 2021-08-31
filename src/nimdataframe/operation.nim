import sugar
import macros
import strutils
import sequtils
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

proc fillEmpty*[T](df: DataFrame, fill: T): DataFrame =
    result = initDataFrame(df)
    for colName in result.columns:
        result[colName] = fillEmpty(df[colName], fill)

proc dropEmpty*(df: DataFrame): DataFrame =
    result = initDataFrame(df)
    for i in 0..<df.len:
        var skip = false
        for colName in df.columns:
            if df[colName][i] == dfEmpty:
                skip = true
                break
        if skip:
            continue
        for colName in df.columns:
            result.data[colName].add(df[colName][i])


###############################################################
proc dropColumns*(df:DataFrame, colNames: openArray[ColName]): DataFrame =
    ## 指定のDataFrameの列を削除する.
    runnableExamples:
        df.dropColumns(["col1","col2"])
    ##

    result = df
    for colName in colNames:
        result.data.del(colName)

proc renameColumns*(df: DataFrame, renameMap: openArray[(ColName,ColName)]): DataFrame =
    ## DataFrameの列名を変更する.
    ## renameMapには変更前列名と変更後列名のペアを指定する.
    runnableExamples:
        df.renameColumns({"col1":"COL1","col2":"COL2"})
    ##

    result = df
    for renamePair in renameMap:
        if result.data.contains(renamePair[0]):
            result.data[renamePair[1]] = result[renamePair[0]]
            result.data.del(renamePair[0])
            #インデックス列が書き換えられたときはインデックス情報を更新する
            if renamePair[0] == df.indexCol:
                result.indexCol = renamePair[1]


proc resetIndex*[T](df: DataFrame, fn: int -> T): DataFrame =
    result = initDataFrame(df)
    for colName in df.columns:
        if colName == df.indexCol:
            result.data[colName] =
                collect(newSeq):
                    for i in 0..<df.len:
                        fn(i).parseString()
        else:
            result.data[colName] = df[colName]

proc resetIndex*(df: DataFrame): DataFrame =
    let f = proc(i: int): Cell = $i
    result = df.resetIndex(f)

proc setIndex*(df: DataFrame, indexCol: ColName): DataFrame =
    result = df
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
        result.add(fn(fromCell(c)).parseString())

proc map*[T](s: Series, fn: string -> T): Series =
    let f = proc(c: Cell): string = c
    map(s, fn, f)
proc intMap*[T](s: Series, fn: int -> T): Series =
    map(s, fn, parseInt)
proc floatMap*[T](s: Series, fn: float -> T): Series =
    map(s, fn, parseFloat)
proc datetimeMap*[T](s: Series, fn: DateTime -> T, format=defaultDateTimeFormat): Series =
    map(s, fn, genParseDatetime(format))

proc replace*(df: DataFrame, sub: string, by: string): DataFrame =
    result = initDataFrame(df)
    proc f(c: Cell): Cell =
        c.replace(sub, by)
    for colName in df.columns:
        result.data[colName] = df[colName].map(f)

proc replace*(df: DataFrame, sub: Regex, by: string): DataFrame =
    result = initDataFrame(df)
    proc f(c: Cell): Cell =
        c.replacef(sub, by)
    for colName in df.columns:
        result.data[colName] = df[colName].map(f)


###############################################################
proc filter*(df: DataFrame, fltr: Row -> bool): DataFrame =
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
proc sort*[T](df: DataFrame, colName: ColName = "", fromCell: Cell -> T, ascending=true): DataFrame =
    ## DataFrameを指定列でソートする.
    ## 文字列以外のソートの場合はfromCellに文字列から指定型に変換する関数を指定する.
    ##
    result = initDataFrame(df)
    let cn =
        if colName != "":
            colName
        else:
            df.indexCol
    var sortSource = collect(newSeq):
        for rowNumber, row in df.getRows().pairs():
            (rowNumber, fromCell(row[cn]))
    let coef =
        if ascending: 1
        else: -1
    let cmp = proc(x: (int,T), y: (int,T)): int =
        if x[1] < y[1]: -1*coef
        elif x[1] == y[1]: 0
        else: 1*coef
    sortSource.sort(cmp)
    for sorted in sortSource:
        for colName in df.columns:
            result.data[colName].add(df.data[colName][sorted[0]])

proc sort*[T](df: DataFrame, colNames: openArray[ColName], fromCell: Cell -> T, ascending=true): DataFrame =
    result = df.deepCopy()
    for colName in reversed(colNames):
        result = result.sort(colName, fromCell, ascending)

proc sort*(df: DataFrame, colName: ColName = "", ascending=true): DataFrame =
    let f = proc(c: Cell): Cell = c
    sort(df, colName, f, ascending)
proc sort*(df: DataFrame, colNames: openArray[ColName], ascending=true): DataFrame =
    result = df.deepCopy()
    for colName in reversed(colNames):
        result = result.sort(colName, ascending)

proc intSort*(df: DataFrame, colName: ColName = "", ascending=true): DataFrame =
    sort(df, colName, parseInt, ascending)
proc intSort*(df: DataFrame, colNames: openArray[ColName], ascending=true): DataFrame =
    result = df
    for colName in reversed(colNames):
        result = result.intSort(colName, ascending)

proc floatSort*(df: DataFrame, colName: ColName = "", ascending=true): DataFrame =
    sort(df, colName, parseFloat, ascending)
proc floatSort*(df: DataFrame, colNames: openArray[ColName], ascending=true): DataFrame =
    result = df
    for colName in reversed(colNames):
        result = result.floatSort(colName, ascending)

proc datetimeSort*(df: DataFrame, colName: ColName = "", format=defaultDateTimeFormat, ascending=true): DataFrame =
    sort(df, colName, genParseDatetime(format), ascending)
proc datetimeSort*(df: DataFrame, colNames: openArray[ColName], format=defaultDateTimeFormat, ascending=true): DataFrame =
    result = df
    for colName in reversed(colNames):
        result = result.datetimeSort(colName, format, ascending)


###############################################################
proc duplicated*(df: DataFrame, colNames: openArray[ColName] = []): FilterSeries =
    ## 重複した行はtrue、それ以外はfalse.
    ## 重複の評価行をcolNamesで指定する（指定なしの場合はインデックス）.
    ##
    result = initFilterSeries()
    var checker = initTable[seq[string], bool]()
    var columns = colNames.toSeq()
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

proc dropDuplicates*(df: DataFrame, colNames: openArray[ColName] = []): DataFrame =
    ## 重複した行を消す.
    ## 重複の評価行をcolNamesで指定する（指定なしの場合はインデックス）.
    ##
    df.drop(df.duplicated(colNames))

proc transpose*(df: DataFrame): DataFrame =
    result = initDataFrame()
    #indexに重複がある場合、エラー
    if df.duplicated().contains(true):
        raise newException(NimDataFrameError, "duplicate indexes are not allowed in transpose action")
    #転置処理
    let columns = df.getColumns()
    let colNameTable =
        collect(initTable):
            for i, colName in columns.pairs():
                {i: colName}
    for indexValue in df[df.indexCol]:
        result.data[indexValue] = initSeries()
        let dfRow = df.loc(indexValue)
        for i in 0..<columns.len:
            if colNameTable[i] == df.indexCol:
                continue
            result.data[indexValue].add(dfRow[colNameTable[i]][0])
    result.data[result.indexCol] =
        collect(newSeq):
            for colName in columns:
                if colName == df.indexCol:
                    continue
                colName

proc T*(df: DataFrame): DataFrame =
    df.transpose()
