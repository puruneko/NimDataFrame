import sugar
import macros
import strutils
import strformat
import sequtils
import tables
import times
import stats
import algorithm
import sets
import math
import encodings
import re

###############################################################
type Cell = string
type ColName = string
type Row = Table[string, Cell]
type Series = seq[Cell]
type DataFrameData = Table[ColName, Series]
type DataFrame = object
    data: DataFrameData
    indexCol: ColName
type FilterSeries = seq[bool]
type DataFrameGroupBy = object
    data: Table[seq[ColName], DataFrame]
    indexCol: ColName
    columns: seq[ColName]
type DataFrameResample = object
    data: DataFrame
    window: string
    format: string


###############################################################
type NimDataFrameError = object of CatchableError
type UnimplementedError = object of CatchableError


###############################################################
const dfEmpty = ""
const defaultIndexName = "__index__"
const defaultDateTimeFormat = "yyyy-MM-dd HH:mm:ss"


###############################################################
#parse... : cellに対しての型変換
#to...    : seriesに対しての型変換
proc initRow(): Row =
    result = initTable[string, Cell]()

proc initSeries(): Series =
    result = @[]

proc initFilterSeries(): FilterSeries =
    result = @[]


iterator columns(df: DataFrame): string =
    for key in df.data.keys:
        yield key

iterator rows(df: DataFrame): Row =
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

proc getColumnName(df: DataFrame): seq[string] =
    for column in df.columns:
        result.add(column)

proc getSeries(df: DataFrame): seq[Series] =
    for value in df.data.values:
        result.add(value)

proc getRows(df: DataFrame): seq[Row] =
    for row in df.rows:
        result.add(row)


proc `$`(x: DateTime): string =
    x.format(defaultDateTimeFormat)

proc parseString[T](x: T): Cell =
    $x

proc parseDatetime(c: Cell, format=defaultDateTimeFormat): DateTime =
    c.parse(format)

proc genParseDatetime(format=defaultDateTimeFormat): Cell -> DateTime =
    result =
        proc(c:Cell): DateTime =
            c.parseDatetime(format)


proc to[T](s: Series, parser: Cell -> T): seq[T] =
    result = collect(newSeq):
        for c in s:
            parser(c)

proc toInt(s: Series): seq[int] =
    to(s, parseInt)

proc toFloat(s: Series): seq[float] =
    to(s, parseFloat)

proc toDatetime(s: Series, format=defaultDateTimeFormat): seq[DateTime] =
    to(s, genParseDatetime(format))


proc `[]`(df: DataFrame, colName: ColName): Series =
    ## DataFrameからSeriesを取り出す.
    df.data[colName]

proc `[]=`[T](df: var DataFrame, colName: ColName, right: openArray[T]) {. discardable .} =
    ## DataFrameのSeriesに代入する.
    ## 代入されるarrayの各値はstringにキャストされる.
    var newSeries = collect(newSeq):
        for c in right:
            c.parseString()
    df.data.del(colName)
    df.data.add(colName, newSeries)


proc initDataFrame(): DataFrame =
    result.data = initTable[ColName, Series]()
    result.indexCol = defaultIndexName

proc initDataFrame(df: DataFrame): DataFrame =
    result = initDataFrame()
    for colName in df.columns:
        result[colName] = initSeries()

proc initDataFrameGroupBy(): DataFrameGroupBy =
    result.data = initTable[seq[ColName], DataFrame]()
    result.columns = @[]


proc len(df: DataFrame): int =
    result = max(
        collect(newSeq) do:
            for colName in df.columns:
                df[colName].len
    )

proc deepCopy(df: DataFrame): DataFrame =
    result = initDataFrame(df)
    for i in 0..<df.len:
        for colName in df.columns:
            result.data[colName].add(df[colName][i])

proc `[]`(df: DataFrame, colNames: openArray[ColName]): DataFrame =
    ## 指定した列だけ返す.
    result = initDataFrame()
    let columns = df.getColumnName()
    for colName in colNames:
        if not columns.contains(colName):
            raise newException(NimDataFrameError, fmt"df doesn't have column {colName}")
        result[colName] = df[colName]
    result[df.indexCol] = df[df.indexCol]

proc keep(df: DataFrame, fs: FilterSeries): DataFrame =
    ## trueをkeepする（fsがtrueの行だけ返す）.
    result = initDataFrame(df)
    for colName in df.columns:
        for i, b in fs.pairs():
            if b:
                result.data[colName].add(df[colName][i])
proc drop(df: DataFrame, fs: FilterSeries): DataFrame =
    ## trueをdropする（fsがtrueの行を落として返す）（fsがfalseの行だけ返す）.
    result = initDataFrame(df)
    for colName in df.columns:
        for i, b in fs.pairs():
            if not b:
                result.data[colName].add(df[colName][i])

proc `[]`(df: DataFrame, fs: FilterSeries): DataFrame =
    ## fsがtrueの行だけ返す.
    df.keep(fs)

proc `[]`(df: DataFrame, slice: HSlice[int, int]): DataFrame =
    ## sliceの範囲の行だけ返す.
    result = initDataFrame(df)
    let len = df.len
    for i in slice:
        if i < 0 or i >= len:
            continue
        for colName in df.columns:
            result.data[colName].add(df[colName][i])

proc `[]`(df: DataFrame, indices: openArray[int]): DataFrame =
    ## indicesの行だけ返す.
    result = initDataFrame(df)
    let len = df.len
    for i in indices:
        if i < 0 or i >= len:
            continue
        for colName in df.columns:
            result.data[colName].add(df[colName][i])

proc iloc(df: DataFrame, i: int): Row =
    ## index番目の行をRow形式で返す.
    result = initRow()
    for colName in df.columns:
        result[colName] = df.data[colName][i]

proc loc(df: DataFrame, c: Cell): DataFrame =
    ## indexの行の値がcの値と一致する行を返す.
    result = initDataFrame(df)
    let columns = df.getColumnName()
    for i in 0..<df.len:
        if df[df.indexCol][i] == c:
            for colName in columns:
                result.data[colName].add(df[colName][i])

proc head(df: DataFrame, num: int): DataFrame =
    result = initDataFrame(df)
    for i in 0..<min(num,df.len):
        for colName in result.columns:
            result.data[colName].add(df[colName][i])
proc tail(df: DataFrame, num: int): DataFrame =
    result = initDataFrame(df)
    for i in df.len-min(num,df.len)..<df.len:
        for colName in result.columns:
            result.data[colName].add(df[colName][i])

proc index(df: DataFrame): Series =
    df[df.indexCol]

proc shape(df: DataFrame): (int,int) =
    let colNumber = df.getColumnName.len
    let rowNumber = df.len
    result = (rowNumber, colNumber)


###############################################################
proc isIntSeries(s: Series): bool =
    result = true
    try:
        for c in s:
            discard parseInt(c)
    except:
        result = false

proc isFloatSeries(s: Series): bool =
    result = true
    try:
        for c in s:
            discard parseFloat(c)
    except:
        result = false

proc isDatetimeSeries(s: Series, format=defaultDateTimeFormat): bool =
    result = true
    try:
        for c in s:
            discard parseDatetime(c, format)
    except:
        result = false


###############################################################
proc fillEmpty[T](s: Series, fill: T): Series =
    result =
        collect(newSeq):
            for c in s:
                if c == dfEmpty:
                    fill.parseString()
                else:
                    c

proc fillEmpty[T](df: DataFrame, fill: T): DataFrame =
    result = initDataFrame(df)
    for colName in result.columns:
        result[colName] = fillEmpty(df[colName], fill)

proc dropEmpty(df: DataFrame): DataFrame =
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
proc toDataFrame(
    text: string,
    sep=",",
    headers: openArray[ColName],
    headerRows= 0,
    indexCol="",
    encoding="utf-8"
): DataFrame =
    ## テキストで表現されたデータ構造をDataFrameに変換する.
    runnableExamples:
        var df = toDataFrame(
            text=tsv,
            sep="\t",
            headers=["col1","col2","col3"],
            headerRows=1,
        )
    ##

    #初期化
    result = initDataFrame()
    for colName in headers:
        result[colName] = initSeries()
    #エンコード変換
    let ec = open("utf-8", encoding)
    defer: ec.close()
    let textConverted = ec.convert(text)
    #テキストデータの変換
    let lines = textConverted.strip().split("\n")
    for rowNumber, line in lines.pairs():
        if rowNumber < headerRows:
            continue
        for (cell, colName) in zip(line.split(sep), headers):
            result.data[colName].add(cell.strip())
    #インデックスの設定
    if indexCol != "":
        if result.getColumnName().contains(indexCol):
            result.indexCol = indexCol
        else:
            raise newException(NimDataFrameError, fmt"not found {indexCol}")
    else:
        result[defaultIndexName] =
            collect(newSeq):
                for i in 0..<lines.len-headerRows: $i

proc toDataFrame[T](rows: openArray[seq[T]], colNames: openArray[ColName] = [], indexCol=""): DataFrame =
    ## 配列で表現されたデータ構造をDataFrameに変換する.
    runnableExamples:
        var df = toDataFrame(
            [
                @[1,2,3],
                @[4,5,6],
                @[7,8,],
                @[1,10,11,12]
            ],
            colNames=["col1","col2","col3","col4"],
            indexCol="col1"
        )
    ##

    result = initDataFrame()
    let colCount = max(
        collect(newSeq) do:
            for row in rows:
                row.len
    )
    #列名が指定されている場合
    if colNames.len > 0:
        if colCount <= colNames.len:
            for colName in colNames:
                result[colName] = initSeries()
            for row in rows:
                for colNumber, colName in colNames.pairs():
                    result.data[colName].add(
                        if colNumber < row.len:
                            row[colNumber].parseString()
                        else:
                            dfEmpty
                    )
        else:
            raise newException(NimDataFrameError, "each row.len must be lower than columns.len.")
    #列名が指定されていない場合
    else:
        #列数は各行の長さの最大値
        let colNames2 = collect(newSeq):
            for i in 0..<colCount:
                fmt"col{i}"
        for colName in colNames2:
            result[colName] = initSeries()
        for row in rows:
            for colNumber, colName in colNames2.pairs():
                result.data[colName].add(
                    if colNumber < row.len:
                        row[colNumber].parseString()
                    else:
                        dfEmpty
                )
    #インデックスの設定
    if indexCol != "":
        if result.getColumnName().contains(indexCol):
            result.indexCol = indexCol
        else:
            raise newException(NimDataFrameError, fmt"not found {indexCol}")
    else:
        result[defaultIndexName] =
            collect(newSeq):
                for i in 0..<rows.len: $i


###############################################################
proc toCsv(df: DataFrame): string =
    result = ""
    var line = ""
    for colName in df.columns:
        line &= (colName & ",")
    result &= line[0..<line.len-1] & "\n"
    for i in 0..<df.len:
        line = ""
        for colName in df.columns:
            line &= df[colName][i] & ","
        result &= line[0..<line.len-1] & "\n"

proc toCsv(df: DataFrame, filename: string, encoding="utf-8") =
    var fp: File
    let openOk = fp.open(filename, fmWrite)
    defer: fp.close()
    if not openOk:
        raise newException(NimDataFrameError, fmt"{filename} open error.")
    #
    let ec = open(encoding, "utf-8")
    defer: ec.close()
    fp.write(ec.convert(df.toCsv()))


###############################################################
proc concat(dfs: openArray[DataFrame]): DataFrame =
    ## 単純に下にDataFrameを連結し続ける.
    ## インデックスは最後に指定したDataFrameのインデックスとなる.
    runnableExamples:
        concat([df1, df2, df3])
    ##

    result = initDataFrame()
    #全列名の抽出
    let columns = toHashSet(
        collect(newSeq) do:
            for df in dfs:
                for colName in df.columns:
                    colName
    )
    #DataFrameの連結
    for colName in columns:
        result[colName] = initSeries()
    for df in dfs:
        for colName in columns:
            if df.data.contains(colName):
                for c in df[colName]:
                    result.data[colName].add(c)
            else:
                for i in 0..<df.len:
                    result.data[colName].add(dfEmpty)
    result.indexCol = dfs[^1].indexCol

proc indexOf[T](s: openArray[T], key: T): int =
    result = -1
    for i, x in s.pairs():
        if x == key:
            result = i
            break

proc indicesOf[T](s: openArray[T], key: T): seq[int] =
    result = @[]
    for i, x in s.pairs():
        if x == key:
            result.add(i)

proc merge(df1: DataFrame, df2: DataFrame, onLeft: openArray[ColName], onRight: openArray[ColName] how="inner"): DataFrame =
    ## df1とdf2をマージする
    ## 

    result = initDataFrame()
    if how == "inner":
        #on列が存在する場合
        if toHashSet(df1.getColumnName())*toHashSet(onLeft) == toHashSet(on) and
            toHashSet(df2.getColumnName())*toHashSet(onRight) == toHashSet(on):
            #resultの初期化・重複列の処理
            var colNames = (toHashSet(df1.getColumnName()) + toHashSet(df2.getColumnName())).toSeq()
            let duplicatedCols = (toHashSet(df1.getColumnName()) * toHashSet(df2.getColumnName())) - toHashSet(on)
            var columnsTable1 = 
                collect(initTable()):
                    for colName in df1.columns:
                        {colName: colName}
            var columnsTable2 = 
                collect(initTable()):
                    for colName in df2.columns:
                        {colName: colName}
            let columns1 = toHashSet(df1.getColumnName())
            let columns2 = (toHashSet(df2.getColumnName()) - columns1) + duplicatedCols
            for colName in duplicatedCols:
                colNames.del(colNames.indexOf(colName))
                colNames = concat(colNames, @[fmt"{colName}_1", fmt"{colName}_2"])
                columnsTable1[colName] = fmt"{colName}_1"
                columnsTable2[colName] = fmt"{colName}_2"
            for colName in colNames:
                result[colName] = initSeries()
            #on列の共通部分の計算
            let df1on =
                collect(newSeq):
                    for i in 0..<df1.len:
                        var row: seq[Cell] = @[]
                        for colName in on:
                            row.add(df1[colName][i])
                        row
            let df2on =
                collect(newSeq):
                    for i in 0..<df2.len:
                        var row: seq[Cell] = @[]
                        for colName in on:
                            row.add(df2[colName][i])
                        row
            let adoptedOn = toHashSet(df1on) * toHashSet(df2on)
            #共通部分を含むindexを抜き出し、その行の値を追加していく
            for c in adoptedOn:
                for index1 in df1on.indicesOf(c):
                    for index2 in df2on.indicesOf(c):
                        for colName in columns1:
                            result.data[columnsTable1[colName]].add(df1[colName][index1])
                        for colName in columns2:
                            result.data[columnsTable2[colName]].add(df2[colName][index2])
        else:
            raise newException(NimDataFrameError, fmt"column '{on}' not found")
    elif how == "left":
        #on列が存在する場合
        if toHashSet(df1.getColumnName())*toHashSet(on) == toHashSet(on) and
            toHashSet(df2.getColumnName())*toHashSet(on) == toHashSet(on):
            #resultの初期化・重複列の処理
            var colNames = (toHashSet(df1.getColumnName()) + toHashSet(df2.getColumnName())).toSeq()
            let duplicatedCols = (toHashSet(df1.getColumnName()) * toHashSet(df2.getColumnName())) - toHashSet(on)
            var columnsTable1 = 
                collect(initTable()):
                    for colName in df1.columns:
                        {colName: colName}
            var columnsTable2 = 
                collect(initTable()):
                    for colName in df2.columns:
                        {colName: colName}
            let columns1 = toHashSet(df1.getColumnName())
            let columns2 = (toHashSet(df2.getColumnName()) - columns1) + duplicatedCols
            for colName in duplicatedCols:
                colNames.del(colNames.indexOf(colName))
                colNames = concat(colNames, @[fmt"{colName}_1", fmt"{colName}_2"])
                columnsTable1[colName] = fmt"{colName}_1"
                columnsTable2[colName] = fmt"{colName}_2"
            for colName in colNames:
                result[colName] = initSeries()
            #df1のon列の計算
            let df1on =
                collect(newSeq):
                    for i in 0..<df1.len:
                        var row: seq[Cell] = @[]
                        for colName in on:
                            row.add(df1[colName][i])
                        row
            let df2on =
                collect(newSeq):
                    for i in 0..<df2.len:
                        var row: seq[Cell] = @[]
                        for colName in on:
                            row.add(df2[colName][i])
                        row
            let adoptedOn = toHashSet(df1on)
            #共通部分を含むindexを抜き出し、その行の値を追加していく
            for c in adoptedOn:
                for index1 in df1on.indicesOf(c):
                    let indices2 = df2on.indicesOf(c)
                    if indices2.len != 0:
                        for index2 in indices2:
                            for colName in columns1:
                                result.data[columnsTable1[colName]].add(df1[colName][index1])
                            for colName in columns2:
                                result.data[columnsTable2[colName]].add(df2[colName][index2])
                    else:
                        for colName in columns1:
                            result.data[columnsTable1[colName]].add(df1[colName][index1])
                        for colName in columns2:
                            result.data[columnsTable2[colName]].add(dfEmpty)
        else:
            raise newException(NimDataFrameError, fmt"column '{on}' not found")
    elif how == "right":
        #on列が存在する場合
        if toHashSet(df1.getColumnName())*toHashSet(on) == toHashSet(on) and
            toHashSet(df2.getColumnName())*toHashSet(on) == toHashSet(on):
            #resultの初期化・重複列の処理
            var colNames = (toHashSet(df1.getColumnName()) + toHashSet(df2.getColumnName())).toSeq()
            let duplicatedCols = (toHashSet(df1.getColumnName()) * toHashSet(df2.getColumnName())) - toHashSet(on)
            var columnsTable1 = 
                collect(initTable()):
                    for colName in df1.columns:
                        {colName: colName}
            var columnsTable2 = 
                collect(initTable()):
                    for colName in df2.columns:
                        {colName: colName}
            let columns2 = toHashSet(df2.getColumnName())
            let columns1 = (toHashSet(df1.getColumnName()) - columns2) + duplicatedCols
            for colName in duplicatedCols:
                colNames.del(colNames.indexOf(colName))
                colNames = concat(colNames, @[fmt"{colName}_1", fmt"{colName}_2"])
                columnsTable1[colName] = fmt"{colName}_1"
                columnsTable2[colName] = fmt"{colName}_2"
            for colName in colNames:
                result[colName] = initSeries()
            #df2のon列の計算
            let df1on =
                collect(newSeq):
                    for i in 0..<df1.len:
                        var row: seq[Cell] = @[]
                        for colName in on:
                            row.add(df1[colName][i])
                        row
            let df2on =
                collect(newSeq):
                    for i in 0..<df2.len:
                        var row: seq[Cell] = @[]
                        for colName in on:
                            row.add(df2[colName][i])
                        row
            let adoptedOn = toHashSet(df2on)
            #共通部分を含むindexを抜き出し、その行の値を追加していく
            for c in adoptedOn:
                for index2 in df2on.indicesOf(c):
                    let indices1 = df1on.indicesOf(c)
                    if indices1.len != 0:
                        for index1 in indices1:
                            for colName in columns2:
                                result.data[columnsTable2[colName]].add(df2[colName][index2])
                            for colName in columns1:
                                result.data[columnsTable1[colName]].add(df1[colName][index1])
                    else:
                        for colName in columns2:
                            result.data[columnsTable2[colName]].add(df2[colName][index2])
                        for colName in columns1:
                            result.data[columnsTable1[colName]].add(dfEmpty)
        else:
            raise newException(NimDataFrameError, fmt"column '{on}' not found")
    elif how == "outer":
        #on列が存在する場合
        if toHashSet(df1.getColumnName())*toHashSet(on) == toHashSet(on) and
            toHashSet(df2.getColumnName())*toHashSet(on) == toHashSet(on):
            #resultの初期化・重複列の処理
            var colNames = (toHashSet(df1.getColumnName()) + toHashSet(df2.getColumnName())).toSeq()
            let duplicatedCols = (toHashSet(df1.getColumnName()) * toHashSet(df2.getColumnName())) - toHashSet(on)
            var columnsTable1 = 
                collect(initTable()):
                    for colName in df1.columns:
                        {colName: colName}
            var columnsTable2 = 
                collect(initTable()):
                    for colName in df2.columns:
                        {colName: colName}
            let columns1 = toHashSet(df1.getColumnName())
            let columns2 = (toHashSet(df2.getColumnName()) - columns1) + duplicatedCols
            for colName in duplicatedCols:
                colNames.del(colNames.indexOf(colName))
                colNames = concat(colNames, @[fmt"{colName}_1", fmt"{colName}_2"])
                columnsTable1[colName] = fmt"{colName}_1"
                columnsTable2[colName] = fmt"{colName}_2"
            for colName in colNames:
                result[colName] = initSeries()
            #on列の和集合の計算
            let df1on =
                collect(newSeq):
                    for i in 0..<df1.len:
                        var row: seq[Cell] = @[]
                        for colName in on:
                            row.add(df1[colName][i])
                        row
            let df2on =
                collect(newSeq):
                    for i in 0..<df2.len:
                        var row: seq[Cell] = @[]
                        for colName in on:
                            row.add(df2[colName][i])
                        row
            let adoptedOn = toHashSet(df1on) + toHashsET(df2on)
            #共通部分を含むindexを抜き出し、その行の値を追加していく
            for c in adoptedOn:
                let indices1 = df1on.indicesOf(c)
                if indices1.len != 0:
                    for index1 in indices1:
                        let indices2 = df2on.indicesOf(c)
                        if indices2.len != 0:
                            for index2 in indices2:
                                for colName in columns1:
                                    result.data[columnsTable1[colName]].add(df1[colName][index1])
                                for colName in columns2:
                                    result.data[columnsTable2[colName]].add(df2[colName][index2])
                        else:
                            for colName in columns1:
                                result.data[columnsTable1[colName]].add(df1[colName][index1])
                            for colName in columns2:
                                result.data[columnsTable2[colName]].add(dfEmpty)
                else:
                    let indices2 = df2on.indicesOf(c)
                    if indices2.len != 0:
                        for index2 in indices2:
                            for colName in columns2 + toHashSet(on):
                                result.data[columnsTable2[colName]].add(df2[colName][index2])
                            for colName in columns1 - toHashSet(on):
                                result.data[columnsTable1[colName]].add(dfEmpty)
                    else:
                        raise newException(NimDataFrameError, "unknown error")
        else:
            raise newException(NimDataFrameError, fmt"common column '{on}' not found")
    else:
        raise newException(NimDataFrameError, fmt"invalid method '{how}'")

proc merge2(df1: DataFrame, df2: DataFrame, on: openArray[ColName], how="inner"): DataFrame =
    ## df1とdf2をマージする
    ## 

    result = initDataFrame()
    if how == "inner":
        #on列が存在する場合
        if toHashSet(df1.getColumnName())*toHashSet(on) == toHashSet(on) and
            toHashSet(df2.getColumnName())*toHashSet(on) == toHashSet(on):
            #resultの初期化・重複列の処理
            var colNames = (toHashSet(df1.getColumnName()) + toHashSet(df2.getColumnName())).toSeq()
            let duplicatedCols = (toHashSet(df1.getColumnName()) * toHashSet(df2.getColumnName())) - toHashSet(on)
            var columnsTable1 = 
                collect(initTable()):
                    for colName in df1.columns:
                        {colName: colName}
            var columnsTable2 = 
                collect(initTable()):
                    for colName in df2.columns:
                        {colName: colName}
            let columns1 = toHashSet(df1.getColumnName())
            let columns2 = (toHashSet(df2.getColumnName()) - columns1) + duplicatedCols
            for colName in duplicatedCols:
                colNames.del(colNames.indexOf(colName))
                colNames = concat(colNames, @[fmt"{colName}_1", fmt"{colName}_2"])
                columnsTable1[colName] = fmt"{colName}_1"
                columnsTable2[colName] = fmt"{colName}_2"
            for colName in colNames:
                result[colName] = initSeries()
            #on列の共通部分の計算
            let df1on =
                collect(newSeq):
                    for i in 0..<df1.len:
                        var row: seq[Cell] = @[]
                        for colName in on:
                            row.add(df1[colName][i])
                        row
            let df2on =
                collect(newSeq):
                    for i in 0..<df2.len:
                        var row: seq[Cell] = @[]
                        for colName in on:
                            row.add(df2[colName][i])
                        row
            let adoptedOn = toHashSet(df1on) * toHashSet(df2on)
            #共通部分を含むindexを抜き出し、その行の値を追加していく
            for c in adoptedOn:
                for index1 in df1on.indicesOf(c):
                    for index2 in df2on.indicesOf(c):
                        for colName in columns1:
                            result.data[columnsTable1[colName]].add(df1[colName][index1])
                        for colName in columns2:
                            result.data[columnsTable2[colName]].add(df2[colName][index2])
        else:
            raise newException(NimDataFrameError, fmt"column '{on}' not found")
    elif how == "left":
        #on列が存在する場合
        if toHashSet(df1.getColumnName())*toHashSet(on) == toHashSet(on) and
            toHashSet(df2.getColumnName())*toHashSet(on) == toHashSet(on):
            #resultの初期化・重複列の処理
            var colNames = (toHashSet(df1.getColumnName()) + toHashSet(df2.getColumnName())).toSeq()
            let duplicatedCols = (toHashSet(df1.getColumnName()) * toHashSet(df2.getColumnName())) - toHashSet(on)
            var columnsTable1 = 
                collect(initTable()):
                    for colName in df1.columns:
                        {colName: colName}
            var columnsTable2 = 
                collect(initTable()):
                    for colName in df2.columns:
                        {colName: colName}
            let columns1 = toHashSet(df1.getColumnName())
            let columns2 = (toHashSet(df2.getColumnName()) - columns1) + duplicatedCols
            for colName in duplicatedCols:
                colNames.del(colNames.indexOf(colName))
                colNames = concat(colNames, @[fmt"{colName}_1", fmt"{colName}_2"])
                columnsTable1[colName] = fmt"{colName}_1"
                columnsTable2[colName] = fmt"{colName}_2"
            for colName in colNames:
                result[colName] = initSeries()
            #df1のon列の計算
            let df1on =
                collect(newSeq):
                    for i in 0..<df1.len:
                        var row: seq[Cell] = @[]
                        for colName in on:
                            row.add(df1[colName][i])
                        row
            let df2on =
                collect(newSeq):
                    for i in 0..<df2.len:
                        var row: seq[Cell] = @[]
                        for colName in on:
                            row.add(df2[colName][i])
                        row
            let adoptedOn = toHashSet(df1on)
            #共通部分を含むindexを抜き出し、その行の値を追加していく
            for c in adoptedOn:
                for index1 in df1on.indicesOf(c):
                    let indices2 = df2on.indicesOf(c)
                    if indices2.len != 0:
                        for index2 in indices2:
                            for colName in columns1:
                                result.data[columnsTable1[colName]].add(df1[colName][index1])
                            for colName in columns2:
                                result.data[columnsTable2[colName]].add(df2[colName][index2])
                    else:
                        for colName in columns1:
                            result.data[columnsTable1[colName]].add(df1[colName][index1])
                        for colName in columns2:
                            result.data[columnsTable2[colName]].add(dfEmpty)
        else:
            raise newException(NimDataFrameError, fmt"column '{on}' not found")
    elif how == "right":
        #on列が存在する場合
        if toHashSet(df1.getColumnName())*toHashSet(on) == toHashSet(on) and
            toHashSet(df2.getColumnName())*toHashSet(on) == toHashSet(on):
            #resultの初期化・重複列の処理
            var colNames = (toHashSet(df1.getColumnName()) + toHashSet(df2.getColumnName())).toSeq()
            let duplicatedCols = (toHashSet(df1.getColumnName()) * toHashSet(df2.getColumnName())) - toHashSet(on)
            var columnsTable1 = 
                collect(initTable()):
                    for colName in df1.columns:
                        {colName: colName}
            var columnsTable2 = 
                collect(initTable()):
                    for colName in df2.columns:
                        {colName: colName}
            let columns2 = toHashSet(df2.getColumnName())
            let columns1 = (toHashSet(df1.getColumnName()) - columns2) + duplicatedCols
            for colName in duplicatedCols:
                colNames.del(colNames.indexOf(colName))
                colNames = concat(colNames, @[fmt"{colName}_1", fmt"{colName}_2"])
                columnsTable1[colName] = fmt"{colName}_1"
                columnsTable2[colName] = fmt"{colName}_2"
            for colName in colNames:
                result[colName] = initSeries()
            #df2のon列の計算
            let df1on =
                collect(newSeq):
                    for i in 0..<df1.len:
                        var row: seq[Cell] = @[]
                        for colName in on:
                            row.add(df1[colName][i])
                        row
            let df2on =
                collect(newSeq):
                    for i in 0..<df2.len:
                        var row: seq[Cell] = @[]
                        for colName in on:
                            row.add(df2[colName][i])
                        row
            let adoptedOn = toHashSet(df2on)
            #共通部分を含むindexを抜き出し、その行の値を追加していく
            for c in adoptedOn:
                for index2 in df2on.indicesOf(c):
                    let indices1 = df1on.indicesOf(c)
                    if indices1.len != 0:
                        for index1 in indices1:
                            for colName in columns2:
                                result.data[columnsTable2[colName]].add(df2[colName][index2])
                            for colName in columns1:
                                result.data[columnsTable1[colName]].add(df1[colName][index1])
                    else:
                        for colName in columns2:
                            result.data[columnsTable2[colName]].add(df2[colName][index2])
                        for colName in columns1:
                            result.data[columnsTable1[colName]].add(dfEmpty)
        else:
            raise newException(NimDataFrameError, fmt"column '{on}' not found")
    elif how == "outer":
        #on列が存在する場合
        if toHashSet(df1.getColumnName())*toHashSet(on) == toHashSet(on) and
            toHashSet(df2.getColumnName())*toHashSet(on) == toHashSet(on):
            #resultの初期化・重複列の処理
            var colNames = (toHashSet(df1.getColumnName()) + toHashSet(df2.getColumnName())).toSeq()
            let duplicatedCols = (toHashSet(df1.getColumnName()) * toHashSet(df2.getColumnName())) - toHashSet(on)
            var columnsTable1 = 
                collect(initTable()):
                    for colName in df1.columns:
                        {colName: colName}
            var columnsTable2 = 
                collect(initTable()):
                    for colName in df2.columns:
                        {colName: colName}
            let columns1 = toHashSet(df1.getColumnName())
            let columns2 = (toHashSet(df2.getColumnName()) - columns1) + duplicatedCols
            for colName in duplicatedCols:
                colNames.del(colNames.indexOf(colName))
                colNames = concat(colNames, @[fmt"{colName}_1", fmt"{colName}_2"])
                columnsTable1[colName] = fmt"{colName}_1"
                columnsTable2[colName] = fmt"{colName}_2"
            for colName in colNames:
                result[colName] = initSeries()
            #on列の和集合の計算
            let df1on =
                collect(newSeq):
                    for i in 0..<df1.len:
                        var row: seq[Cell] = @[]
                        for colName in on:
                            row.add(df1[colName][i])
                        row
            let df2on =
                collect(newSeq):
                    for i in 0..<df2.len:
                        var row: seq[Cell] = @[]
                        for colName in on:
                            row.add(df2[colName][i])
                        row
            let adoptedOn = toHashSet(df1on) + toHashsET(df2on)
            #共通部分を含むindexを抜き出し、その行の値を追加していく
            for c in adoptedOn:
                let indices1 = df1on.indicesOf(c)
                if indices1.len != 0:
                    for index1 in indices1:
                        let indices2 = df2on.indicesOf(c)
                        if indices2.len != 0:
                            for index2 in indices2:
                                for colName in columns1:
                                    result.data[columnsTable1[colName]].add(df1[colName][index1])
                                for colName in columns2:
                                    result.data[columnsTable2[colName]].add(df2[colName][index2])
                        else:
                            for colName in columns1:
                                result.data[columnsTable1[colName]].add(df1[colName][index1])
                            for colName in columns2:
                                result.data[columnsTable2[colName]].add(dfEmpty)
                else:
                    let indices2 = df2on.indicesOf(c)
                    if indices2.len != 0:
                        for index2 in indices2:
                            for colName in columns2 + toHashSet(on):
                                result.data[columnsTable2[colName]].add(df2[colName][index2])
                            for colName in columns1 - toHashSet(on):
                                result.data[columnsTable1[colName]].add(dfEmpty)
                    else:
                        raise newException(NimDataFrameError, "unknown error")
        else:
            raise newException(NimDataFrameError, fmt"common column '{on}' not found")
    else:
        raise newException(NimDataFrameError, fmt"invalid method '{how}'")

proc merge(df1: DataFrame, df2: DataFrame, on: ColName, how="inner"): DataFrame =
    merge(df1, df2, [on], how)

proc join(dfSource: DataFrame, dfArray: openArray[DataFrame], how="left"): DataFrame =
    ## dfSourceとdfsをマージする
    ## 

    result = initDataFrame()
    result.indexCol = dfSource.indexCol
    let dfs = concat(@[dfSource], dfArray.toSeq())
    if how == "inner":
        #resultの初期化・重複列の処理
        var colNames = toHashSet(dfs[0].getColumnName())
        for df in dfs[1..^1]:
            colNames = colNames + toHashSet(df.getColumnName())
        var colNamesSeq = colNames.toSeq()
        var columnsTable: seq[Table[ColName,ColName]] = @[]
        for df in dfs:
            columnsTable.add(
                collect(initTable()) do:
                    for colName in df.columns:
                        {colName: colName}
            )
        for i in 0..<dfs.len:
            for j in i+1..<dfs.len:
                let dup = (toHashSet(dfs[i].getColumnName()) * toHashSet(dfs[j].getColumnName())) - toHashSet([dfs[i].indexCol, dfs[j].indexCol])
                for colName in dup:
                    colNamesSeq.del(colNamesSeq.indexOf(colName))
                    colNamesSeq = concat(colNamesSeq, @[fmt"{colName}_{i}", fmt"{colName}_{j}"])
                    columnsTable[i][colName] = fmt"{colName}_{i}"
                    columnsTable[j][colName] = fmt"{colName}_{j}"
        colNames = toHashSet(colNamesSeq)
        for colName in colNames:
            result[colName] = initSeries()
        #on列の共通部分の計算
        var dfIndex: seq[seq[Cell]] = @[]
        for df in dfs:
            dfIndex.add(
                collect(newSeq) do:
                    for i in 0..<df.len:
                        df[df.indexCol][i]
            )
        var adoptedIndex = toHashSet(dfIndex[0])
        for i in 1..<dfs.len:
            adoptedIndex = adoptedIndex * toHashSet(dfIndex[i])
        #共通部分を含むindexを抜き出し、その行の値を追加していく
        proc addValue(res: var DataFrame, c: ColName, dfs: seq[DataFrame], dfIndex: seq[seq[Cell]], columnsTable: seq[Table[ColName,ColName]], indices: seq[int] = @[], i = 0) =
            if i < dfs.len:
                let localIndices = dfIndex[i].indicesOf(c)
                if localIndices.len != 0:
                    for index in localIndices:
                        addValue(res, c, dfs, dfIndex, columnsTable, concat(indices, @[index]), i+1)
                else:
                    addValue(res, c, dfs, dfIndex, columnsTable, concat(indices, @[-1]), i+1)
            else:
                res.data[res.indexCol].add(dfs[0][dfs[0].indexCol][indices[0]])
                for j, df in dfs.pairs():
                    for colName in df.getColumnName():
                        if colName == df.indexCol:
                            continue
                        if indices[j] != -1:
                            res.data[columnsTable[j][colName]].add(df[colName][indices[j]])
                        else:
                            res.data[columnsTable[j][colName]].add(dfEmpty)

        for c in adoptedIndex:
            addValue(result, c, dfs, dfIndex, columnsTable)
    elif how == "left":
        #resultの初期化・重複列の処理
        var colNames = toHashSet(dfs[0].getColumnName())
        for df in dfs[1..^1]:
            colNames = colNames + toHashSet(df.getColumnName())
        var colNamesSeq = colNames.toSeq()
        var columnsTable: seq[Table[ColName,ColName]] = @[]
        for df in dfs:
            columnsTable.add(
                collect(initTable()) do:
                    for colName in df.columns:
                        {colName: colName}
            )
        for i in 0..<dfs.len:
            for j in i+1..<dfs.len:
                let dup = (toHashSet(dfs[i].getColumnName()) * toHashSet(dfs[j].getColumnName())) - toHashSet([dfs[i].indexCol, dfs[j].indexCol])
                for colName in dup:
                    colNamesSeq.del(colNamesSeq.indexOf(colName))
                    colNamesSeq = concat(colNamesSeq, @[fmt"{colName}_{i}", fmt"{colName}_{j}"])
                    columnsTable[i][colName] = fmt"{colName}_{i}"
                    columnsTable[j][colName] = fmt"{colName}_{j}"
        colNames = toHashSet(colNamesSeq)
        for colName in colNames:
            result[colName] = initSeries()
        #on列の共通部分の計算
        var dfIndex: seq[seq[Cell]] = @[]
        for df in dfs:
            dfIndex.add(
                collect(newSeq) do:
                    for i in 0..<df.len:
                        df[df.indexCol][i]
            )
        var adoptedIndex = toHashSet(dfIndex[0])
        #[
        for i in 1..<dfs.len:
            adoptedIndex = adoptedIndex * toHashSet(dfIndex[i])
        ]#
        #共通部分を含むindexを抜き出し、その行の値を追加していく
        proc addValue(res: var DataFrame, c: ColName, dfs: seq[DataFrame], dfIndex: seq[seq[Cell]], columnsTable: seq[Table[ColName,ColName]], indices: seq[int] = @[], i = 0) =
            if i < dfs.len:
                let localIndices = dfIndex[i].indicesOf(c)
                if localIndices.len != 0:
                    for index in localIndices:
                        addValue(res, c, dfs, dfIndex, columnsTable, concat(indices, @[index]), i+1)
                else:
                    addValue(res, c, dfs, dfIndex, columnsTable, concat(indices, @[-1]), i+1)
            else:
                res.data[res.indexCol].add(dfs[0][dfs[0].indexCol][indices[0]])
                for j, df in dfs.pairs():
                    for colName in df.getColumnName():
                        if colName == df.indexCol:
                            continue
                        if indices[j] != -1:
                            res.data[columnsTable[j][colName]].add(df[colName][indices[j]])
                        else:
                            res.data[columnsTable[j][colName]].add(dfEmpty)

        for c in adoptedIndex:
            addValue(result, c, dfs, dfIndex, columnsTable)
    else:
        raise newException(NimDataFrameError, fmt"invalid method '{how}'")


###############################################################
proc `+`(a: Cell, b: float): float =
    ## 左辺Cell型、右辺float型の加算を計算する.
    parseFloat(a) + b
proc `-`(a: Cell, b: float): float =
    parseFloat(a) - b
proc `*`(a: Cell, b: float): float =
    parseFloat(a) * b
proc `/`(a: Cell, b: float): float =
    parseFloat(a) / b
proc `+`(a: float, b: Cell): float =
    ## 左辺float型、右辺Cell型の加算を計算する.
    a + parseFloat(b)
proc `-`(a: float, b: Cell): float =
    a - parseFloat(b)
proc `*`(a: float, b: Cell): float =
    a * parseFloat(b)
proc `/`(a: float, b: Cell): float =
    a / parseFloat(b)

#TODO: int版も作る
proc `==`(a: Cell, b: float): bool =
    ## 左辺Cell型、右辺float型を等価比較する.
    result = a.parseFloat() == b
proc `!=`(a: Cell, b: float): bool =
    result = a.parseFloat() != b
proc `>`(a: Cell, b: float): bool =
    result = a.parseFloat() > b
proc `<`(a: Cell, b: float): bool =
    result = a.parseFloat() < b
proc `>=`(a: Cell, b: float): bool =
    result = a.parseFloat() >= b
proc `<=`(a: Cell, b: float): bool =
    result = a.parseFloat() <= b
proc `==`(a: float, b: Cell): bool =
    ## 左辺float型、右辺Cell型を等価比較する.
    result = a == b.parseFloat()
proc `!=`(a: float, b: Cell): bool =
    result = a != b.parseFloat()
proc `>`(a: float, b: Cell): bool =
    result = a > b.parseFloat()
proc `<`(a: float, b: Cell): bool =
    result = a < b.parseFloat()
proc `>=`(a: float, b: Cell): bool =
    result = a >= b.parseFloat()
proc `<=`(a: float, b: Cell): bool =
    result = a <= b.parseFloat()

proc agg[T](s: Series, aggFn: Series -> T): Cell =
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

proc aggMath(s: Series, aggFn: openArray[float] -> float): Cell =
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

proc count(s: Series): Cell =
    let cnt = proc(s: openArray[float]): float =
        float(s.len)
    s.aggMath(cnt)
proc sum(s: Series): Cell =
    s.aggMath(sum)
proc mean(s: Series): Cell =
    s.aggMath(stats.mean)
proc std(s: Series): Cell =
    s.aggMath(stats.standardDeviation)
proc max(s: Series): Cell =
    s.aggMath(max)
proc min(s: Series): Cell =
    s.aggMath(min)
proc v(s: Series): Cell =
    s.aggMath(stats.variance)

proc agg[T](df: DataFrame, aggFn: Series -> T): DataFrame =
    ## DataFrameの各列に対して統計量を計算する.
    ## aggFnにはSeriesの統計量を計算する関数を指定する.
    runnableExamples:
        df.agg(mean)
    ##

    result = initDataFrame()
    for (colName, s) in df.data.pairs():
        let c = aggFn(s)
        result[colName] = @[c.parseString()]

proc count(df: DataFrame): DataFrame =
    df.agg(count)
proc sum(df: DataFrame): DataFrame =
    df.agg(sum)
proc mean(df: DataFrame): DataFrame =
    df.agg(mean)
proc std(df: DataFrame): DataFrame =
    df.agg(std)
proc max(df: DataFrame): DataFrame =
    df.agg(max)
proc min(df: DataFrame): DataFrame =
    df.agg(min)
proc v(df: DataFrame): DataFrame =
    df.agg(v)


###############################################################
proc dropColumns(df:DataFrame, colNames: openArray[ColName]): DataFrame =
    ## 指定のDataFrameの列を削除する.
    runnableExamples:
        df.dropColumns(["col1","col2"])
    ##

    result = df
    for colName in colNames:
        result.data.del(colName)

proc renameColumns(df: DataFrame, renameMap: openArray[(ColName,ColName)]): DataFrame =
    ## DataFrameの列名を変更する.
    ## renameMapには変更前列名と変更後列名のペアを指定する.
    runnableExamples:
        df.renameColumns({"col1":"COL1","col2":"COL2"})
    ##

    result = df
    for renamePair in renameMap:
        if result.data.contains(renamePair[0]):
            result[renamePair[1]] = result[renamePair[0]]
            result.data.del(renamePair[0])


proc resetIndex(df: DataFrame): DataFrame =
    result = initDataFrame(df)
    for colName in df.columns:
        if colName == df.indexCol:
            result[colName] =
                collect(newSeq):
                    for i in 0..<df.len: $i
        else:
            result[colName] = df[colName]

proc setIndex(df: DataFrame, indexCol: ColName): DataFrame =
    result = df
    result.indexCol = indexCol


###############################################################
proc map[T, U](s: Series, fn: U -> T, fromCell: Cell -> U): Series =
    ## Seriesの各セルに対して関数fnを適用する.
    ## 関数fnにはSeriesの各セルが渡され、関数fnは文字列に変換可能な任意の型を返す.
    ## 文字列型以外の操作を関数fn内で行う場合、fromCell関数にCell型から任意の型に変換する関数を渡す.
    runnableExamples:
        let triple = proc(c: int): int = c * 3
        df["col1"].map(triple, parseInt)
    ##

    for c in s:
        result.add(fn(fromCell(c)).parseString())

proc intMap[T](s: Series, fn: int -> T): Series =
    map(s, fn, parseInt)
proc floatMap[T](s: Series, fn: float -> T): Series =
    map(s, fn, parseFloat)
proc datetimeMap[T](s: Series, fn: DateTime -> T, format=defaultDateTimeFormat): Series =
    map(s, fn, genParseDatetime(format))


###############################################################
proc filter(df: DataFrame, fltr: Row -> bool): DataFrame =
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
proc sort[T](df: DataFrame, colName: ColName, fromCell: Cell -> T, ascending=true): DataFrame =
    ## DataFrameを指定列でソートする.
    ## 文字列以外のソートの場合はfromCellに文字列から指定型に変換する関数を指定する.
    ##
    result = initDataFrame(df)
    var sortSource = collect(newSeq):
        for rowNumber, row in df.getRows().pairs():
            (rowNumber, fromCell(row[colName]))
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

proc sort[T](df: DataFrame, colNames: openArray[ColName], fromCell: Cell -> T, ascending=true): DataFrame =
    result = df.deepCopy()
    for colName in reversed(colNames):
        result = result.sort(colName, fromCell, ascending)

proc sort(df: DataFrame, colName: ColName, ascending=true): DataFrame =
    let f = proc(c: Cell): Cell = c
    sort(df, colName, f, ascending)
proc sort(df: DataFrame, colNames: openArray[ColName], ascending=true): DataFrame =
    result = df.deepCopy()
    for colName in reversed(colNames):
        result = result.sort(colName, ascending)

proc intSort(df: DataFrame, colName: ColName, ascending=true): DataFrame =
    sort(df, colName, parseInt, ascending)
proc intSort(df: DataFrame, colNames: openArray[ColName], ascending=true): DataFrame =
    result = df
    for colName in reversed(colNames):
        result = result.intSort(colName, ascending)

proc floatSort(df: DataFrame, colName: ColName, ascending=true): DataFrame =
    sort(df, colName, parseFloat, ascending)
proc floatSort(df: DataFrame, colNames: openArray[ColName], ascending=true): DataFrame =
    result = df
    for colName in reversed(colNames):
        result = result.floatSort(colName, ascending)

proc datetimeSort(df: DataFrame, colName: ColName, format=defaultDateTimeFormat, ascending=true): DataFrame =
    sort(df, colName, genParseDatetime(format), ascending)
proc datetimeSort(df: DataFrame, colNames: openArray[ColName], format=defaultDateTimeFormat, ascending=true): DataFrame =
    result = df
    for colName in reversed(colNames):
        result = result.datetimeSort(colName, format, ascending)


###############################################################
proc duplicated(df: DataFrame, colNames: openArray[ColName] = []): FilterSeries =
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

proc dropDuplicates(df: DataFrame, colNames: openArray[ColName] = []): DataFrame =
    ## 重複した行を消す.
    ## 重複の評価行をcolNamesで指定する（指定なしの場合はインデックス）.
    ##
    df.drop(df.duplicated(colNames))


###############################################################
proc groupby(df: DataFrame, colNames: openArray[ColName]): DataFrameGroupBy =
    ## DataFrameを指定の列の値でグループ化する（戻り値はDataFrameGroupBy型）.
    ## 
    result = initDataFrameGroupBy()
    #マルチインデックスの作成
    let multiIndex = toHashSet(
        collect(newSeq) do:
            for i in 0..<df.len:
                var index: seq[Cell] = @[]
                for colName in colNames:
                    index.add(df[colName][i])
                index
    )
    #データのグループ化
    result.indexCol = df.indexCol
    result.columns = colNames.toSeq()
    for mi in multiIndex:
        result.data[mi] = initDataFrame(df)
    for i in 0..<df.len:
        let mi =
            collect(newSeq):
                for specifiedColName in colNames:
                    df[specifiedColName][i]
        for colName in df.columns:
            result.data[mi].data[colName].add(df[colName][i])

proc agg[T](dfg: DataFrameGroupBy, aggFn: openArray[(string,Series -> T)]): DataFrame =
    ## groupbyしたDataFrameの指定列に対して関数を実行する.
    ## 指定する関数に{.closure.}オプションをつけないとエラーになる.
    runnableExamples:
        proc f(series: Series): float{.closure.} =
            series.toFloat().mean()/100
        df.groupby(["col1","col2"]).agg({"col3",f})
    ##

    result = initDataFrame()
    result.indexCol = dfg.indexCol
    var dfs: seq[DataFrame] = @[]
    for mi in dfg.data.keys:
        #関数の計算
        var df = initDataFrame()
        for (colName, fn) in aggFn:
            let c = fn(dfg.data[mi][colName])
            df[colName] = @[c.parseString()]
        #マルチインデックス値の上書き
        for (colName, colValue) in zip(dfg.columns, mi):
            df[colName] = @[colValue]
        dfs.add(df)
    result = concat(dfs = dfs)

proc agg(dfg: DataFrameGroupBy, aggFn: DataFrame -> DataFrame): DataFrame =
    ## groupbyしたDataFrameに対して統計量を計算する.
    ## aggFnにはDataFrameの統計量を計算する関数を指定する.
    runnableExamples:
        df.groupby(["col1","col2"]).agg(sum)
    ##

    result = initDataFrame()
    var dfs: seq[DataFrame] = @[]
    for mi in dfg.data.keys:
        #統計値の計算
        var df = aggFn(dfg.data[mi])
        #マルチインデックス値の上書き
        for (colName, colValue) in zip(dfg.columns, mi):
            df[colName] = @[colValue]
        dfs.add(df)
    result = concat(dfs = dfs)

proc count(dfg: DataFrameGroupBy): DataFrame =
    dfg.agg(count)
proc sum(dfg: DataFrameGroupBy): DataFrame =
    dfg.agg(sum)
proc mean(dfg: DataFrameGroupBy): DataFrame =
    dfg.agg(mean)
proc std(dfg: DataFrameGroupBy): DataFrame =
    dfg.agg(std)
proc max(dfg: DataFrameGroupBy): DataFrame =
    dfg.agg(max)
proc min(dfg: DataFrameGroupBy): DataFrame =
    dfg.agg(min)
proc v(dfg: DataFrameGroupBy): DataFrame =
    dfg.agg(v)

proc apply[T](dfg: DataFrameGroupBy, applyFn: DataFrame -> Table[ColName,T]): DataFrame =
    ## groupby下DataFrameの各groupに対して関数を実行する.
    ## applyFn関数はTableを返すことに注意.
    runnableExamples:
        proc f(df: DataFrame): Table[ColName,Cell] =
            var cell: Cell
            if df["col2"][0] == "abc":
                cell = df["col3"].intMap(c => c/10).mean()
            else:
                cell = df["col3"].intMap(c => c*10).mean()
            result = {
                "col3_changed": cell
            }.toTable()
        df.groupby(["col1","col2"]).apply(f)
    ##

    result = initDataFrame()
    result.indexCol = dfg.indexCol
    var dfs: seq[DataFrame] = @[]
    for mi in dfg.data.keys:
        #関数の計算
        var df = initDataFrame()
        var applyTable = applyFn(dfg.data[mi])
        for (colName, c) in applyTable.pairs():
            df[colName] = @[c.parseString()]
        #マルチインデックス値の上書き
        for (colName, colValue) in zip(dfg.columns, mi):
            df[colName] = @[colValue]
        dfs.add(df)
    result = concat(dfs = dfs)


###############################################################
proc resample(df: DataFrame, window: int, format=defaultDateTimeFormat): DataFrameResample =
    ## DataFrameを指定の行数でリサンプルする（戻り値はDataFrameResample型）.
    ##

    result.data = df
    result.window = $window
    result.format = format

proc resample(df: DataFrame, window: string, format=defaultDateTimeFormat): DataFrameResample =
    result.data = df
    result.window = window
    result.format = format

proc genGetInterval(datetimeId: string): int -> TimeInterval =
        case datetimeId:
        of "Y": result = proc(interval:int):TimeInterval=interval.years
        of "m": result = proc(interval:int):TimeInterval=interval.months
        of "d": result = proc(interval:int):TimeInterval=interval.days
        of "H": result = proc(interval:int):TimeInterval=interval.hours
        of "M": result = proc(interval:int):TimeInterval=interval.minutes
        of "S": result = proc(interval:int):TimeInterval=interval.seconds

proc flattenDatetime(dt: DateTime, datetimeId: string): DateTime =
    result = dt
    case datetimeId:
    of "Y":
        result -= result.month.ord.months
        result -= result.monthday.ord.days
        result -= result.hour.ord.hours
        result -= result.minute.ord.minutes
        result -= result.second.ord.seconds
    of "m":
        result -= result.monthday.ord.days
        result -= result.hour.ord.hours
        result -= result.minute.ord.minutes
        result -= result.second.ord.seconds
    of "d":
        result -= result.hour.ord.hours
        result -= result.minute.ord.minutes
        result -= result.second.ord.seconds
    of "H":
        result -= result.minute.ord.minutes
        result -= result.second.ord.seconds
    of "M":
        result -= result.second.ord.seconds
    of "S":
        result = result

template resampleAggTemplate(body: untyped): untyped{.dirty.} =
    result = initDataFrame()
    #数字指定かdatetime指定か判断する
    var matches: array[2, string]
    if match(dfre.window, re"(\d+)([a-zA-Z]+)?", matches):
        #数字のみ（行数指定）の場合
        if matches[1] == "" and matches[0] != "":
            let w = matches[0].parseInt()
            #各行をwindow飛ばしで処理する
            when typeof(fn) is (openArray[(ColName, Series -> T)]):#agg1用
                for (colName, _) in fn:
                    result[colName] = initSeries()
            else:
                for colName in dfre.data.columns:
                    result[colName] = initSeries()
            var index: seq[Cell] = @[]
            when typeof(fn) is (DataFrame -> Table[ColName,T]):#apply用
                var dfs: seq[DataFrame] = @[]
            for i in countup(0, dfre.data.len-1, w):
                var slice = i..<i+w
                if slice.b >= dfre.data.len:
                    slice.b = dfre.data.len-1
                    
                body

                index.add(dfre.data[dfre.data.indexCol][i])
            result.data[dfre.data.indexCol] = index
        #datetime範囲指定の場合
        elif matches[1] != "" and matches[0] != "":
            let datetimeId = matches[1]
            let w = matches[0].parseInt()
            #インデックスがdatetimeフォーマットに準拠している場合
            if isDatetimeSeries(dfre.data[dfre.data.indexCol]):
                let datetimes = dfre.data[dfre.data.indexCol].toDatetime()
                let getInterval = genGetInterval(datetimeId)
                let startDatetime = flattenDatetime(datetimes[0], datetimeId)
                var startIndex = 0
                var interval = w
                var index: seq[DateTime] = @[]
                when typeof(fn) is (DataFrame -> Table[ColName,T]):#apply用
                    var dfs: seq[DataFrame] = @[]
                #DateTime型に変換したindexを上から順にみていく
                result.indexCol = dfre.data.indexCol
                when typeof(fn) is (openArray[(ColName, Series -> T)]):#agg1用
                    for (colName, _) in fn:
                        result[colName] = initSeries()
                else:
                    for colName in dfre.data.columns:
                        result[colName] = initSeries()
                for i, dt in datetimes.pairs():
                    #範囲外になった場合、集計
                    if startDatetime + getInterval(interval) <= dt:
                        var slice = startIndex..<i
                        if slice.b >= dfre.data.len:
                            slice.b = dfre.data.len-1
                            
                        body

                        index.add(startDatetime + getInterval(interval-w))
                        startIndex = i
                        interval += w
                #window刻みの余り分の処理
                if startIndex < dfre.data.len-1:
                    var slice = startIndex..<dfre.data.len
                    
                    body

                    index.add(startDatetime + getInterval(interval-w))
                when typeof(fn) is (DataFrame -> Table[ColName,T]):#apply用
                    result = concat(dfs = dfs)
                    result.indexCol = dfre.data.indexCol
                result[dfre.data.indexCol] = index
            #インデックスがdatetimeフォーマットでない場合
            else:
                raise newException(NimDataFrameError, "index column isn't datetime format")
        #指定フォーマットでない場合
        else:
            raise newException(NimDataFrameError, "invalid datetime format")
    #指定フォーマットにひっからなかった場合（エラー）
    else:
        raise newException(NimDataFrameError, "invalid datetime format")

proc agg[T](dfre: DataFrameResample, fn: openArray[(ColName, Series -> T)]): DataFrame =
    ## リサンプルされたDataFrameの各グループの指定列に対して関数fnを適用する
    ## 指定する関数に{.closure.}オプションをつけないとエラーになる.
    runnableExamples:
        proc f(s: Series): float{.closure.} =
            sum(s)*100
        df.resample("30M").agg({"sales": f})
    ##

    resampleAggTemplate:
        for (colName, f) in fn:
            result.data[colName].add(f(dfre.data[colName][slice]).parseString())

proc agg[T](dfre: DataFrameResample, fn: Series -> T): DataFrame =
    ## リサンプルされたDataFrameの各グループの全列に対して関数fnを適用する
    runnableExamples:
        df.resample("30M").agg(mean)
    ##

    resampleAggTemplate:
        for colName in result.columns:
            result.data[colName].add(fn(dfre.data[colName][slice]).parseString())

proc count(dfre: DataFrameResample): DataFrame =
    dfre.agg(count)
proc sum(dfre: DataFrameResample): DataFrame =
    dfre.agg(sum)
proc mean(dfre: DataFrameResample): DataFrame =
    dfre.agg(mean)
proc std(dfre: DataFrameResample): DataFrame =
    dfre.agg(std)
proc max(dfre: DataFrameResample): DataFrame =
    dfre.agg(max)
proc min(dfre: DataFrameResample): DataFrame =
    dfre.agg(min)
proc v(dfre: DataFrameResample): DataFrame =
    dfre.agg(v)

proc apply[T](dfre: DataFrameResample, fn: DataFrame -> Table[ColName,T]): DataFrame =
    ## リサンプルされたDataFrameの各グループのDataFrameに対して関数fnを適用する
    ## 関数fnはTableを返すことに注意.
    runnableExamples:
        proc f(df: DataFrame): Table[ColName,Cell] =
            var cell: Cell
            if df["col2"][0] == "abc":
                cell = df["col3"].intMap(c => c/10).mean()
            else:
                cell = df["col3"].intMap(c => c*10).mean()
            result = {
                "col3_changed": cell
            }.toTable()
        df.resample("30M").apply(f)
    ##

    resampleAggTemplate:
        #applyFnに渡すDataFrame作成
        var df1 = initDataFrame(dfre.data)
        if slice.b >= dfre.data.len:
            slice.b = dfre.data.len-1
        for colName in result.columns:
            df1[colName] = dfre.data[colName][slice]
        #applyFn適用
        var applyTable = fn(df1)
        var df2 = initDataFrame()
        for (colName, c) in applyTable.pairs():
            df2[colName] = @[c.parseString()]
        dfs.add(df2)


###############################################################
###############################################################
###############################################################
proc toBe() =
    const filename = "sample.csv"
    var fp: File
    let openOk = fp.open(filename, fmRead)
    defer: fp.close()
    if not openOk:
        quit(fmt"{filename} open failed.")
    let csv = fp.readAll()
    #
    echo "df--------------------------------"
    var df = toDataFrame(
        text=csv,
        headers=["time","name","sales","日本語"],
        headerRows=1,
    )
    echo df
    #df.toCsv("test.csv")
    #
    echo "dropEmpty--------------------------------"
    echo df.dropEmpty()
    #
    echo "fillEmpty--------------------------------"
    df["sales"] = df["sales"].fillEmpty(0)
    echo df
    #
    echo "df1--------------------------------"
    var df1 = toDataFrame(
        [
            @[1,2,3],
            @[4,5,6],
            @[7,8,],
            @[1,10,11,12]
        ],
        colNames=["col1","col2","col3","col10"],
        indexCol="col1"
    )
    echo df1
    #
    echo "drop--------------------------------"
    echo df.dropColumns(["time","name"])
    #
    echo "rename--------------------------------"
    echo df.renameColumns({"time":"TIME","name":"NAME","sales":"SALES"})
    #
    echo "stats--------------------------------"
    echo df.mean()
    echo df.max()
    #
    echo "map--------------------------------"
    echo df["sales"].intMap(c => c*2)
    echo df["time"].datetimeMap(c => c+initDuration(hours=1))
    let triple = proc(c: int): int =
        c * 3
    echo df["sales"].map(triple, parseInt)
    #
    echo "filter--------------------------------"
    echo df.filter(row => row["sales"] >= 2000)
    echo df.filter(row => row["sales"] > 1000 and 3000 > row["sales"])
    #
    echo "loc,iloc--------------------------------"
    echo df1.loc("1")
    echo df.iloc(0)
    #
    echo "getRows--------------------------------"
    echo df.getRows()
    echo df.getColumnName()
    #
    echo "sort--------------------------------"
    echo df.sort("name", ascending=false)
    echo df.sort("sales", parseInt, ascending=true)
    echo df.sort("sales", parseInt, ascending=false)
    echo df.datetimeSort("time", ascending=false)
    #
    echo "resetIndex--------------------------------"
    echo df.intSort("sales").resetIndex()
    #
    echo "index,shape--------------------------------"
    echo df.index
    echo df.shape
    #
    echo "[]--------------------------------"
    echo df[["time","sales"]]
    echo df[0..4]
    echo df[[2,4,6]]
    #
    echo "head,tail--------------------------------"
    echo df.head(5)
    echo df.tail(5)
    echo df.head(999999999)
    echo df.tail(999999999)
    #
    echo "duplicated--------------------------------"
    echo df.duplicated(["sales"])
    echo df.dropDuplicates(["sales"])
    echo df.dropDuplicates()
    echo df.dropDuplicates(["time","sales"])
    #
    echo "groupby--------------------------------"
    echo df.groupby(["time","name"])
    #
    echo "groupby mean,max--------------------------------"
    echo df.groupby(["time","name"]).mean()
    echo df.groupby(["time","name"]).max()
    #
    echo "groupby agg--------------------------------"
    proc aggFnG(s: Series): float {.closure.} =
        result = s.toFloat().mean()/100
    echo df.groupby(["time","name"]).agg({"sales": aggFnG})
    #
    echo "groupby apply--------------------------------"
    proc applyFnG(df: DataFrame): Table[ColName,Cell] =
        var c: Cell
        if df["name"][0] == "abc":
            c = df["sales"].intMap(c => c/10).mean()
        else:
            c = df["sales"].intMap(c => c*10).mean()
        result = {
            "sales_changed": c
        }.toTable()
    echo df.groupby(["time","name"]).apply(applyFnG)
    #
    echo "resaple 5 mean--------------------------------"
    echo df.resample(5).sum()
    #
    echo "resaple 1H agg1--------------------------------"
    echo df.setIndex("time").resample("1H").mean()
    #
    echo "resaple 30M agg1--------------------------------"
    echo df.setIndex("time").resample("30M").mean()
    #
    echo "resaple 30M agg2--------------------------------"
    proc aggFnRe(s: Series): float{.closure.} =
        sum(s)*100
    echo df.setIndex("time").resample("30M").agg({"sales":aggFnRe})
    #
    echo "resaple 30M apply--------------------------------"
    echo df.setIndex("time").resample("30M").apply(applyFnG)
    #
    echo "merge inner(1)--------------------------------"
    var df_ab = toDataFrame(
        rows = [
            @["a_1", "b_1"],
            @["a_1", "b_2"],
            @["a_2", "b_2"],
            @["a_3", "b_3"],
        ],
        colNames = ["a","b"]
    )
    var df_ac = toDataFrame(
        rows = [
            @["a_1", "c_10"],
            @["a_1", "c_20"],
            @["a_1", "c_30"],
            @["a_2", "c_2"],
            @["a_4", "c_4"],
        ],
        colNames = ["a","c"]
    )
    echo merge(df_ab, df_ac, on="a").sort(["a","b"])
    #
    echo "merge inner(2)--------------------------------"
    var df_ac2 = toDataFrame(
        rows = [
            @["a_1", "b_10", "c_10"],
            @["a_1", "b_20", "c_20"],
            @["a_1", "b_30", "c_30"],
            @["a_2", "b_2", "c_2"],
            @["a_4", "b_4", "c_4"],
        ],
        colNames = ["a","b","c"]
    )
    echo merge(df_ab, df_ac2, on=["a","b"]).sort(["a","b"])
    #
    echo "merge left(1)--------------------------------"
    echo merge(df_ab, df_ac, on="a", how="left").sort(["a","b"])
    #
    echo "merge left(2)--------------------------------"
    echo merge(df_ab, df_ac2, on=["a","b"], how="left").sort(["a","b"])
    #
    echo "merge right(1)--------------------------------"
    echo merge(df_ab, df_ac, on="a", how="right").sort(["a","b"])
    #
    echo "merge right(2)--------------------------------"
    echo merge(df_ab, df_ac2, on=["a","b"], how="right").sort(["a","b"])
    #
    echo "merge outer(1)--------------------------------"
    echo merge(df_ab, df_ac, on="a", how="outer").sort(["a","b"])
    #
    echo "merge outer(2)--------------------------------"
    echo merge(df_ab, df_ac2, on=["a","b"], how="outer").sort(["a","b"])
    #
    echo "join inner(1)--------------------------------"
    let df_j1 = toDataFrame(
        rows = [
            @[1,3,7],
            @[2,6,14],
            @[3,9,21],
        ],
        colNames = ["a","b","c"],
        indexCol = "a",
    )
    let df_j2 = toDataFrame(
        rows = [
            @[1,3,8],
            @[2,6,16],
            @[4,9,24],
        ],
        colNames = ["a","b","d"],
        indexCol = "a",
    )
    let df_j3 = toDataFrame(
        rows = [
            @[1,3,9],
            @[2,6,18],
            @[5,9,27],
        ],
        colNames = ["a","d","e"],
        indexCol = "a",
    )
    echo df_j1.join([df_j2], how="inner").sort("a")
    #
    echo "join inner(2)--------------------------------"
    echo df_j1.join([df_j2, df_j3], how="inner").sort("a")
    #
    echo "join left(1)--------------------------------"
    echo df_j1.join([df_j2], how="left").sort("a")
    #
    echo "join left(2)--------------------------------"
    echo df_j2.join([df_j3], how="left").sort("a")
    #
    echo "join right(1)--------------------------------"
    echo df_j2.join([df_j3], how="right").sort("a")
    #[
    ]#

if isMainModule:
    toBe()