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
proc map*[T, U](s: Series, fn: U -> T, translator: Cell -> U): Series =
    ## Seriesの各セルに対して関数fnを適用する.
    ## 関数fnにはSeriesの各セルが渡され、関数fnは文字列に変換可能な任意の型を返す.
    ## 文字列型以外の操作を関数fn内で行う場合、translator関数にCell型から任意の型に変換する関数を渡す.
    runnableExamples:
        let triple = proc(c: int): int = c * 3
        df["col1"].map(triple, parseInt)
    ##

    result = initSeries()
    for c in s:
        result.add(fn(translator(c)))

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
proc defaultTranslator*(x: Cell): string = x

proc sortImpl*[T](
    df: StringDataFrame,
    colName: ColName,
    ascending: bool,
    translator: proc(c:Cell):T,
    ascFn: proc(x,y:T):int
): StringDataFrame =
    let cn =
        if colName != reservedColName:
            colName
        else:
            df.indexCol
    var sortSource: seq[(int,T)] = @[]
    for rowNumber, cell in df[cn].pairs():
        sortSource.add((rowNumber, translator(cell)))
    if ascending:
        let ascFn2 = proc(x: (int,T), y: (int,T)): int = ascFn(x[1], y[1])
        sortSource.sort(ascFn2)
    else:
        let desFn2 = proc(x: (int,T), y: (int,T)): int = ascFn(x[1], y[1]) * -1
        sortSource.sort(desFn2)
    #
    result = initStringDataFrame(df)
    for sorted in sortSource:
        for colIndex, colName in df.columns.pairs():
            result[colIndex].add(df[colIndex][sorted[0]])

template typeOrSeqType(U: type): type = U or seq[U]

template sort*[T](
    df: StringDataFrame,
    colNames: ColName or seq[ColName] = reservedColName,
    ascendings: bool or seq[bool] = true,
    translators: proc(c:Cell):T or seq[proc(c:Cell):T] = defaultTranslator,
    ascFns: proc(x,y:T):int or seq[proc(x,y:T):int] = cmp,
): StringDataFrame =
    let length =
        max(@[
            when typeof(colNames) is seq: colNames.len else: 1,
            when typeof(ascendings) is seq: ascendings.len else: 1,
            when typeof(translators) is seq: translators.len else: 1,
            when typeof(ascFns) is seq: ascFns.len else: 1,
        ])
    when typeof(colNames) is seq:
        var newColNames = colNames
    else:
        var newColNames: seq[ColName] = @[]
        for i in 0..<length:
            newColNames.add(colNames)
    when typeof(ascendings) is seq:
        var newAscendings = ascendings
    else:
        var newAscendings: seq[bool] = @[]
        for i in 0..<length:
            newAscendings.add(ascendings)
    when typeof(translators) is seq:
        var newTranslators = translators
    else:
        var newTranslators: seq[proc(c:Cell):T] = @[]
        for i in 0..<length:
            newTranslators.add(translators)
    when typeof(ascFns) is seq:
        var newAscFns = ascFns
    else:
        var newAscFns: seq[proc(x,y:T):int] = @[]
        for i in 0..<length:
            newAscFns.add(ascFns)
    if newColNames.len != length or
      newAscendings.len != length or
      newTranslators.len != length or
      newAscFns.len != length:
        raise newException(StringDataFrameError, "all arguments must be the same length(colNames:" & $(newColNames.len) & "translators:" & $(newTranslators.len) & ", ascendings:" & $(newAscendings.len) & ", ascFns:" & $(newAscFns.len))
    #
    var result = df
    for i in countdown(length-1, 0):
        result = result.sortImpl(
            newColNames[i],
            newAscendings[i],
            newTranslators[i],
            newAscFns[i],
        )
    result

proc intSort*(
    df: StringDataFrame,
    colNames: ColName or seq[ColName] = reservedColName,
    ascendings: bool or seq[bool] = true,
): StringDataFrame =
    result = df.sort(colNames, parseInt, ascendings)

proc floatSort*(
    df: StringDataFrame,
    colNames: ColName or seq[ColName] = reservedColName,
    ascendings: bool or seq[bool] = true,
): StringDataFrame =
    result = df.sort(colNames, parseFloat, ascendings)

#[
proc datetimeSort*(
    df: StringDataFrame,
    colNames: ColName|seq[ColName] = reservedColName,
    ascendings: bool|seq[bool] = true,
    formats: string|seq[string] = defaultDatetimeFormat,
): StringDataFrame =
    when typeof(formats) is seq:
        var translators =
            collect(newSeq):
                for f in newFormats:
                    genParseDatetime(f)
    else:
        var translators = genParseDatetime(formats)
    result = sort(df, colNames, translators, ascendings)
]#
proc datetimeSort*(df: StringDataFrame): StringDataFrame = df

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
