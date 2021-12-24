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
type StringDataFrameError* = object of CatchableError
type StringDataFrameReservedColNameError* = object of CatchableError
type UnimplementedError* = object of CatchableError


###############################################################
# 「_」で囲われた文字列を持つ列名は、基本的には禁止にする
const dfEmpty* = ""
const reservedColName* = "_stringdataframereservedcolname_"
const defaultIndexName* = "_idx_"
const mergeIndexName* = "_on_"
const defaultDatetimeFormat* = "yyyy-MM-dd HH:mm:ss"


###############################################################
#parse... : cellに対しての型変換
#to...    : seriesに対しての型変換

template `[]`*(df: StringDataFrame, colName: ColName): untyped =
    ## DataFrameからSeriesを取り出す.
    df.data[df.colTable[colName]]

template `[]`*(df: StringDataFrame, colIndex: int): untyped =
    ## DataFrameからSeriesを取り出す.
    df.data[colIndex]


proc initRow*(): Row =
    result = initTable[string, Cell]()

proc initSeries*(): Series =
    result = @[]

proc initFilterSeries*(): FilterSeries =
    result = @[]


proc `~`*(a: StringDataFrame, b: StringDataFrame): bool =
    ## almost equal
    if a.indexCol != b.indexCol:
        return false
    if toHashSet(a.columns) != toHashSet(b.columns):
        return false
    for colName in a.columns:
        if b[colName] != a[colName]:
            return false
    return true

proc `==`*(a: StringDataFrame, b: StringDataFrame): bool =
    ## equal
    result = (
        a.data == b.data and
        a.columns == b.columns and
        a.indexCol == b.indexCol
    )

proc `!=`*(a: StringDataFrame, b: StringDataFrame): bool =
    ## not equal
    result = not (a == b)

proc `===`*(a: StringDataFrame, b: StringDataFrame): bool =
    ## perfectly equal
    result = (
        a == b and
        a.colTable == b.colTable and
        a.datetimeFormat == b.datetimeFormat
    )

proc `!==`*(a: StringDataFrame, b: StringDataFrame): bool =
    ## perfectly not equal
    result = not (a === b)

proc addColumn*(df: var StringDataFrame, colName: ColName) =
    ## Library外での使用は非推奨
    if colName == reservedColName:
        raise newException(
                StringDataFrameReservedColNameError,
                fmt"{reservedColName} is library-reserved name"
            )
    df.data.add(initSeries())
    df.columns.add(colName)
    df.colTable[colName] = df.columns.len - 1


proc `[]=`*[T](df: var StringDataFrame, colIndex: int, right: openArray[T]) =
    ## DataFrameのSeriesに代入する.
    ## 代入されるarrayの各値はstringにキャストされる.
    
    when typeof(T) is string:
        df.data[colIndex] = right.toSeq()
    else:
        df.data[colIndex] = right.toString()

proc `[]=`*[T](df: var StringDataFrame, colName: ColName, right: openArray[T]) =
    if colName == reservedColName:
        raise newException(
                StringDataFrameReservedColNameError,
                fmt"{reservedColName} is library-reserved name"
            )
    if not df.columns.contains(colName):
        df.addColumn(colName)
    when typeof(T) is string:
        df[df.colTable[colName]] = right.toSeq()
    else:
        df[df.colTable[colName]] = right.toString()

proc len*(df: StringDataFrame): int =
    ## DataFrameの長さを返す
    ## no healthCheck
    result = df[df.indexCol].len

#[
proc addColumn*(df: var StringDataFrame, colName: ColName, s: Series) =
    if colName == reservedColName:
        raise newException(
                StringDataFrameReservedColNameError,
                fmt"{reservedColName} is library-reserved name"
            )
    if s.len != df.len:
        raise newException(StringDataFrameError, "all columns must be the same length")
    df.addColumn(colName)
    df[colName] = s
]#

iterator rows*(df: StringDataFrame): Row =
    let maxRowNumber = min(
        collect(newSeq) do:
            for colIndex, colName in df.columns.pairs():
                df[colIndex].len
    )
    for i in 0..<maxRowNumber:
        var row = initRow()
        for colIndex, colName in df.columns.pairs():
            row[colName] = df[colIndex][i]
        yield row

proc getRows*(df: StringDataFrame): seq[Row] =
    for row in df.rows:
        result.add(row)

proc initRow*(df: StringDataFrame): Row =
    result = initRow()
    for colName in df.columns:
        result[colName] = dfEmpty


proc `$`*(x: DateTime): string =
    x.format(defaultDatetimeFormat)

proc parseString*[T](x: T): Cell =
    when typeof(T) is DateTime:
        result = x.format(defaultDatetimeFormat)
    else:
        result = $(x)

proc parseDatetime*(c: Cell, format=defaultDatetimeFormat): DateTime =
    c.parse(format)

proc genParseDatetime*(format=defaultDatetimeFormat): Cell -> DateTime =
    result =
        proc(c:Cell): DateTime =
            c.parseDatetime(format)

proc add*[T](s: var Series, item: T) =
    s.add(y=item.parseString())

proc to*[T](s: Series, parser: Cell -> T): seq[T] =
    result =
        collect(newSeq):
            for c in s:
                parser(c)

proc toInt*(s: Series): seq[int] =
    to(s, parseInt)

proc toFloat*(s: Series): seq[float] =
    to(s, parseFloat)

proc toDatetime*(s: Series, format=defaultDatetimeFormat): seq[DateTime] =
    to(s, genParseDatetime(format))

proc toString*[T](arr: openArray[T]): Series =
    result = initSeries()
    for a in arr:
        result.add(a)


proc initRow*[T](cells: openArray[(ColName,T)]): Row =
    result = initRow()
    for (colName, cell) in cells:
        result[colName] = cell.parseString()

proc initSeries*[T](s: seq[T]): Series =
    result = s.toString()

proc initStringDataFrame*(): StringDataFrame =
    result.data = @[]
    result.columns = @[]
    result.colTable = initTable[ColName, int]()
    result.indexCol = defaultIndexName
    result.datetimeFormat = defaultDatetimeFormat

proc initStringDataFrame*(df: StringDataFrame, copy=false): StringDataFrame =
    result = initStringDataFrame()
    result.indexCol = df.indexCol
    result.datetimeFormat = df.datetimeFormat
    for colName in df.columns:
        result.addColumn(colName)
        if copy:
            result[colName] = df[colName]

proc initStringDataFrameGroupBy*(df: StringDataFrame): StringDataFrameGroupBy =
    result.df = df
    result.group = @[]
    result.multiIndex = @[]
    result.multiIndexTable = initTable[seq[ColName],int]()
    result.columns = @[]

#[
proc deepCopy*(df: StringDataFrame): StringDataFrame =
    result = initStringDataFrame(df)
    for i in 0..<df.len:
        for colIndex, colName in df.columns.pairs():
            result[colName].add(df[colIndex][i])
]#

proc `[]`*(df: StringDataFrame, colNames: openArray[ColName]): StringDataFrame =
    ## 指定した列だけ返す.
    result = initStringDataFrame()
    let keepColumns = toHashSet(colNames.toSeq() & @[df.indexCol])
    if (keepColumns - toHashSet(df.columns)).len != 0:
        raise newException(StringDataFrameError, fmt"df doesn't have columns {keepColumns - toHashSet(df.columns)}")
    for colName in df.columns:
        if keepColumns.contains(colName):
            result[colName] = df[colName]
    result.indexCol = df.indexCol
    result.datetimeFormat = df.datetimeFormat

proc keep*(df: StringDataFrame, fs: FilterSeries): StringDataFrame =
    ## trueをkeepする（fsがtrueの行だけ返す）.
    ##

    if fs.len != df.len:
        raise newException(StringDataFrameError, fmt"filter series must be the same length of data frame")
    result = initStringDataFrame(df)
    for colIndex, colName in df.columns.pairs():
        for i, b in fs.pairs():
            if b:
                result[colIndex].add(df[colIndex][i])

proc drop*(df: StringDataFrame, fs: FilterSeries): StringDataFrame =
    ## trueをdropする（fsがtrueの行を落として返す）（fsがfalseの行だけ返す）.
    ## 
    
    if fs.len != df.len:
        raise newException(StringDataFrameError, fmt"filter series must be the same length of data frame")
    result = initStringDataFrame(df)
    for colIndex, colName in df.columns.pairs():
        for i, b in fs.pairs():
            if not b:
                result[colIndex].add(df[colIndex][i])

proc `[]`*(df: StringDataFrame, fs: FilterSeries): StringDataFrame =
    ## fsがtrueの行だけ返す.
    df.keep(fs)

proc `[]`*(df: StringDataFrame, slice: HSlice[int, int]): StringDataFrame =
    ## sliceの範囲の行だけ返す.
    result = initStringDataFrame(df)
    let dfLen = df.len
    for i in slice:
        if i < 0 or i >= dfLen:
            continue
        for colIndex, colName in df.columns.pairs():
            result[colIndex].add(df[colIndex][i])

proc `[]`*(df: StringDataFrame, indices: openArray[int]): StringDataFrame =
    ## indicesの行だけ返す.
    result = initStringDataFrame(df)
    let dfLen = df.len
    for i in indices:
        if i < 0 or i >= dfLen:
            continue
        for colIndex, colName in df.columns.pairs():
            result[colIndex].add(df[colIndex][i])

proc iloc*(df: StringDataFrame, i: int): Row =
    ## index番目の行をRow形式で返す.
    result = initRow()
    for colIndex, colName in df.columns.pairs():
        result[colName] = df[colIndex][i]

proc loc*(df: StringDataFrame, c: Cell): StringDataFrame =
    ## indexの行の値がcの値と一致する行を返す.
    result = initStringDataFrame(df)
    for i in 0..<df.len:
        if df[df.indexCol][i] == c:
            for colIndex, colName in df.columns.pairs():
                result[colIndex].add(df[colIndex][i])

proc head*(df: StringDataFrame, num=5): StringDataFrame =
    result = initStringDataFrame(df)
    for i in 0..<min(num,df.len):
        for colIndex, colName in df.columns.pairs():
            result[colIndex].add(df[colIndex][i])

proc tail*(df: StringDataFrame, num=5): StringDataFrame =
    result = initStringDataFrame(df)
    for i in df.len-min(num,df.len)..<df.len:
        for colIndex, colName in df.columns.pairs():
            result[colIndex].add(df[colIndex][i])

proc index*(df: StringDataFrame): Series =
    df[df.indexCol]

proc shape*(df: StringDataFrame): (int,int) =
    let colNumber = df.columns.len
    let rowNumber = df.len
    result = (rowNumber, colNumber)

proc size*(df: StringDataFrame, excludeIndex=false): int =
    result = df.len * (
        if excludeIndex:
            df.columns.len - 1
        else:
            df.columns.len
    )

#[
proc appendRow*(df: StringDataFrame, row: Row, autoIndex=false, fillEmpty=false): StringDataFrame =
    ##
    runnableExamples:
        var df = toDataFrame(
            {
                "col1": @[1],
                "col2": @[10],
            },
            indexCol="col1",
        )
        var r: Row = initRow()
        r["col1"] = "2"
        r["col2"] = "20"
        df = df.appendRow(r)
    ##

    result = df
    var columns: seq[ColName] = @[]
    for colName in row.keys:
        columns.add(colName)
    let columnsHash = toHashSet(columns)
    let dfColumnsHash = toHashSet(df.columns)
    if dfColumnsHash == columnsHash or
        fillEmpty or
        (autoIndex and dfColumnsHash - toHashSet([df.indexCol]) == columnsHash):
        for colName in dfColumnsHash:
            #fillEmptyフラグ無し
            if not fillEmpty:
                #autoIndexフラグ無し
                if not autoIndex:
                    result[colName].add(row[colName])
                #autoIndexフラグあり
                else:
                    if colName == df.indexCol:
                        result[colName].add(df.len)
                    else:
                        result[colName].add(row[colName])
            #fillEmptyフラグあり
            else:
                #autoIndexフラグ無し
                if not autoIndex:
                    if columnsHash.contains(colName):
                        result[colName].add(row[colName])
                    else:
                        result[colName].add(dfEmpty)
                #autoIndexフラグあり
                else:
                    if colName == df.indexCol:
                        result[colName].add(df.len)
                    else:
                        if columnsHash.contains(colName):
                            result[colName].add(row[colName])
                        else:
                            result[colName].add(dfEmpty)
    else:
        raise newException(StringDataFrameError, fmt"not found {dfColumnsHash-columnsHash}")
]#

proc appendRows*[T](df: StringDataFrame, rows: openArray[(ColName, seq[T])], autoIndex=false, fillEmptyRow=false, fillEmptyCol=false): StringDataFrame =
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
        df = df.appendRow(
            rows = {
                "b": @[50,60],
                "c": @[500]
            },
            autoIndex=true,
            fillEmptyRow=true,
            fillEmptyCol=true,
        )
    ##

    result = df
    let rowTable = rows.toTable()
    var columns: seq[ColName] = @[]
    var lengths: seq[int] = @[]
    for (colName, s) in rowTable.pairs():
        columns.add(colName)
        lengths.add(s.len)
    let columnsHash = toHashSet(columns)
    let dfColumnsHash = toHashSet(df.columns)
    let lengthsHash = toHashSet(lengths)
    let length = lengths.max()
    let dfLen = df.len
    if dfColumnsHash == columnsHash or
        (fillEmptyCol and (columnsHash - dfColumnsHash).len == 0) or
        (autoIndex and dfColumnsHash - toHashSet([df.indexCol]) == columnsHash):
        if lengthsHash.len == 1 or fillEmptyRow:
            for colName in dfColumnsHash.items:
                #fillEmptyColフラグあり
                if fillEmptyCol:
                    #fillEmptyRowフラグあり
                    if fillEmptyRow:
                        #autoIndexフラグあり、かつ、colNameがindexCol
                        if autoIndex and colName == df.indexCol:
                            for i in 0..<length:
                                result[colName].add(dfLen+i)
                        else:
                            #列名がない場合
                            if not columnsHash.contains(colName):
                                for i in 0..<length:
                                    result[colName].add(dfEmpty)
                            #列名がある場合
                            else:
                                for c in rowTable[colName]:
                                    result[colName].add(c)
                                for i in rowTable[colName].len..<length:
                                    result[colName].add(dfEmpty)
                    #fillEmptyRowフラグ無し
                    else:
                        #autoIndexフラグあり、かつ、colNameがindexCol
                        if autoIndex and colName == df.indexCol:
                            for i in 0..<length:
                                result[colName].add(dfLen+i)
                        else:
                            #列名がない場合
                            if not columnsHash.contains(colName):
                                for i in 0..<length:
                                    result[colName].add(dfEmpty)
                            #列名がある場合
                            else:
                                for c in rowTable[colName]:
                                    result[colName].add(c)
                #fillEmptyColフラグ無し
                else:
                    #fillEmptyRowフラグあり
                    if fillEmptyRow:
                        #autoIndexフラグあり、かつ、colNameがindexCol
                        if autoIndex and colName == df.indexCol:
                            for i in 0..<length:
                                result[colName].add(dfLen+i)
                        else:
                            for c in rowTable[colName]:
                                result[colName].add(c)
                            for i in rowTable[colName].len..<length:
                                result[colName].add(dfEmpty)
                    #fillEmptyRowフラグ無し
                    else:
                        #autoIndexフラグあり、かつ、colNameがindexCol
                        if autoIndex and colName == df.indexCol:
                            for i in 0..<length:
                                result[colName].add(dfLen+i)
                        else:
                            for c in rowTable[colName]:
                                result[colName].add(c)
        else:
            raise newException(StringDataFrameError, fmt"rows must be same length, but got '{lengthsHash}'")
    else:
        let diff = dfColumnsHash - columnsHash
        raise newException(StringDataFrameError, fmt"not found {diff}")

proc addRows*[T](df: var StringDataFrame, rows: openArray[(ColName, seq[T])], autoIndex=false, fillEmptyRow=false, fillEmptyCol=false) =
    df = df.appendRows(rows, autoIndex, fillEmptyRow, fillEmptyCol)

proc appendRow*[T](df: StringDataFrame, row: openArray[(ColName, T)], autoIndex=false, fillEmptyCol=false): StringDataFrame =
    var newRows: seq[(ColName, seq[T])] =
        collect(newSeq):
            for r in row:
                (r[0], @[r[1]])
    result = df.appendRows(newRows, autoIndex, false, fillEmptyCol)

proc addRow*[T](df: var StringDataFrame, row: openArray[(ColName, T)], autoIndex=false, fillEmptyCol=false) =
    df = df.appendRow(row, autoIndex, fillEmptyCol)

proc appendColumns*[T](df: StringDataFrame, columns: openArray[(ColName, seq[T])], fillEmpty=false, override=false): StringDataFrame =
    result = df
    let columnTable = columns.toTable()
    var lengths: seq[int] = @[]
    var appendedColumns: seq[ColName] = @[]
    for (colName, s) in columnTable.pairs():
        if colName == reservedColName:
            raise newException(
                    StringDataFrameReservedColNameError,
                    fmt"{reservedColName} is library-reserved name"
                )
        lengths.add(s.len)
        appendedColumns.add(colName)
    let intersectionColumns = toHashSet(df.columns).intersection(toHashSet(appendedColumns))
    let lengthHash = toHashSet(lengths)
    let colLength = max(lengths)
    let dfLen = df.len
    #すべての長さが一致していた場合、またはfillEmptyオプションがついている場合
    if ((fillEmpty and colLength <= dfLen) or (not fillEmpty and lengthHash.len == 1 and colLength == dfLen)) and
        (override or (not override and intersectionColumns.len == 0)):
        for colName in columnTable.keys:
            if intersectionColumns.contains(colName):
                continue
            result.addColumn(colName)
        if fillEmpty:
            for colName in columnTable.keys:
                let colLen = columnTable[colName].len
                let isIntersection = intersectionColumns.contains(colName)
                for i in 0..<dfLen:
                    if i < colLen:
                        if isIntersection:
                            result[colName][i] = columnTable[colName][i].parseString()
                        else:
                            result[colName].add(columnTable[colName][i])
                    else:
                        if isIntersection:
                            result[colName][i] = dfEmpty
                        else:
                            result[colName].add(dfEmpty)
        else:
            for colName in columnTable.keys:
                let isIntersection = intersectionColumns.contains(colName)
                for i in 0..<dfLen:
                    if isIntersection:
                        result[colName][i] = columnTable[colName][i].parseString()
                    else:
                        result[colName].add(columnTable[colName][i])
    else:
        if lengthHash.len != 1:
            raise newException(StringDataFrameError, fmt"argument 'columns'({lengthHash}) must be same length({dfLen})")
        elif fillEmpty and colLength > dfLen:
            raise newException(StringDataFrameError, fmt"each argument 'columns'({lengthHash}) must be shorter than length of StringDataFrame({dfLen})")
        elif not fillEmpty and (lengthHash.len != 1 or colLength != dfLen):
            raise newException(StringDataFrameError, fmt"length of argument 'columns'({lengthHash}) must be the same as length of StringDataFrame({dfLen})")
        else:#override
            raise newException(StringDataFrameError, fmt"if override option is not set, argument 'columns' can not specify columns of dataframe({intersectionColumns})")

proc addColumns*[T](df: var StringDataFrame, columns: openArray[(ColName, seq[T])], fillEmpty=false, override=false) =
    df = df.appendColumns(columns, fillEmpty, override)

proc dropColumns*(df: StringDataFrame, colNames: openArray[ColName], newIndexCol=reservedColName, forceDropIndex=false): StringDataFrame =
    ## 指定のDataFrameの列を削除する.
    ## 既定設定では、index列は削除できない（エラー）
    ## forceDropIndexをtrue指定すると、index列を削除できる（非推奨）
    ## newIndexColに削除後のindex列名を指定すると、現在のindex列を削除できる上、指定された列をindex列に設定する
    runnableExamples:
        df.dropColumns(["col1","col2"])
    ##

    result = df
    if (not forceDropIndex and newIndexCol == reservedColName and colNames.contains(df.indexCol)) or
        (newIndexCol != reservedColName and colNames.contains(newIndexCol)):
        raise newException(StringDataFrameError, fmt"can not drop index column({df.indexCol})")
    if (toHashSet(colNames) - toHashSet(df.columns)).len != 0:
        let diff = toHashSet(colNames) - toHashSet(df.columns)
        raise newException(StringDataFrameError, fmt"not found {diff}")
    for colName in colNames:
        let itr = result.colTable[colName]
        result.data.delete(itr)
        result.columns.delete(itr)
        result.colTable.del(colName)
        for cn in result.columns[itr..high(result.columns)]:
            result.colTable[cn] -= 1
    if newIndexCol != reservedColName:
        result.indexCol = newIndexCol

proc dropColumn*(df: StringDataFrame, colName: ColName, newIndexCol=reservedColName, forceDropIndex=false): StringDataFrame =
    df.dropColumns([colName], newIndexCol, forceDropIndex)

proc deleteColumns*(df: var StringDataFrame, colNames: openArray[ColName], newIndexCol=reservedColName, forceDropIndex=false) =
    ## 指定のDataFrameの列を削除する.
    runnableExamples:
        df.deleteColumns(["col1","col2"])
    ##

    df = df.dropColumns(colNames, newIndexCol, forceDropIndex)

proc deleteColumn*(df: var StringDataFrame, colName: ColName, newIndexCol=reservedColName, forceDropIndex=false) =
    df.deleteColumns([colName], newIndexCol, forceDropIndex)

proc keepColumns*(df: StringDataFrame, colNames: openArray[ColName], newIndexCol=reservedColName, forceDropIndex=false): StringDataFrame =
    ## 指定列以外削除する
    ## インデックス列が指定されていない場合、自動で追加される
    ## 
    var dropCols: seq[ColName] = @[]
    for colName in df.columns:
        if not colNames.contains(colName):
            if forceDropIndex or
                newIndexCol != reservedColName or
                ((not forceDropIndex or newIndexCol == reservedColName) and colName != df.indexCol):
                dropCols.add(colName)
    result = df.dropColumns(dropCols, newIndexCol, forceDropIndex)

proc keepColumn*(df: StringDataFrame, colName: ColName, newIndexCol=reservedColName, forceDropIndex=false): StringDataFrame =
    df.keepColumns([colName], newIndexCol, forceDropIndex)

proc surviveColumns*(df: var StringDataFrame, colNames: openArray[ColName], newIndexCol=reservedColName, forceDropIndex=false) =
    df = df.keepColumns(colNames, newIndexCol, forceDropIndex)

proc surviveColumn*(df: var StringDataFrame, colName: ColName, newIndexCol=reservedColName, forceDropIndex=false) =
    df = df.keepColumns([colName], newIndexCol, forceDropIndex)