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
const defaultIndexName = "_idx_"
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

proc toString[T](arr: openArray[T]): Series =
    result = initSeries()
    for a in arr:
        result.add(a.parseString())


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
    result.indexCol = df.indexCol
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
        result.indexCol = defaultIndexName

proc toDataFrame[T](columns: openArray[(ColName, seq[T])], indexCol="" ): DataFrame =
    result = initDataFrame()
    var c: seq[ColName] = @[]
    var l: seq[int] = @[]
    #代入
    for (colName, s) in columns:
        result[colName] = initSeries()
        for c in s.toString():
            result.data[colName].add(c)
        c.add(colName)
        l.add(s.len)
    #長さチェック
    if toHashSet(l).len != 1:
        raise newException(NimDataFrameError, "arrays must all be same length")
    #インデックスの設定
    if c.contains(indexCol):
        result.indexCol = indexCol
    else:
        if indexCol == "":
            result[defaultIndexName] =
                collect(newSeq):
                    for i in 0..<l[0]: $i
            result.indexCol = defaultIndexName
        else:
            raise newException(NimDataFrameError, fmt"not found {indexCol}")

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

proc toFigure(df: DataFrame, indexColSign=false): string =
    result = ""
    #空のデータフレームの場合
    if df[df.indexCol].len == 0:
        result &= "+-------+\n"
        result &= "| empty |\n"
        result &= "+-------+"
    #空ではない場合
    else:
        var columns = df.getColumnName()
        for i, colName in columns.pairs():
            if colName == df.indexCol:
                columns.del(i)
                columns = concat(@[df.indexCol], columns)
                break
        var width: Table[string,int]
        var fullWidth = df.getColumnName().len
        for colName in columns:
            let dataWidth = max(
                collect(newSeq) do:
                    for i in 0..<df[colName].len:
                        df[colName][i].len
            )
            width[colName] = max(colName.len, dataWidth) + indexColSign.ord
            fullWidth += width[colName] + 2
        #
        result &= "+" & "-".repeat(fullWidth-1) & "+"
        result &= "\n"
        result &= "|"
        for colName in columns:
            let name =
                if indexColSign and colName == df.indexCol:
                    colName & "*"
                else:
                    colName
            result &= " ".repeat(width[colName]-name.len+1) & name & " |"
        result &= "\n"
        result &= "|" & "-".repeat(fullWidth-1) & "|"
        result &= "\n"
        #
        for i in 0..<df.len:
            result &= "|"
            for colName in columns:
                result &= " ".repeat(width[colName]-df[colName][i].len+1) & df[colName][i] & " |"
            result &= "\n"
        result &= "+" & "-".repeat(fullWidth-1) & "+"

proc show(df: DataFrame, indexColSign=false) =
    echo df.toFigure(indexColSign)

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

proc merge(left: DataFrame, right: DataFrame, leftOn: openArray[ColName], rightOn: openArray[ColName], how="inner"): DataFrame =
    ## df1とdf2をマージする
    ## indexColはleftの値が使用される（how="right"の場合はrightの値）
    ## indexColの名前はleftのindexCol名がrightにもある場合は「_0」が後ろにつく
    ## 

    result = initDataFrame()
    #
    if ["inner","left","outer"].contains(how):
        #on列が存在する場合
        if toHashSet(left.getColumnName())*toHashSet(leftOn) == toHashSet(leftOn) and
            toHashSet(right.getColumnName())*toHashSet(rightOn) == toHashSet(rightOn):
            #resultの初期化・重複列の処理
            var colNames = (toHashSet(left.getColumnName()) + toHashSet(right.getColumnName())).toSeq()
            let on =
                if leftOn == rightOn:
                    leftOn.toSeq()
                else:
                    @[]
            let dupCols = (toHashSet(left.getColumnName()) * toHashSet(right.getColumnName())) - toHashSet(on)
            var columnsTableL = 
                collect(initTable()):
                    for colName in left.columns:
                        {colName: colName}
            var columnsTableR = 
                collect(initTable()):
                    for colName in right.columns:
                        {colName: colName}
            for colName in dupCols:
                colNames.del(colNames.indexOf(colName))
                colNames = concat(colNames, @[fmt"{colName}_0", fmt"{colName}_1"])
                columnsTableL[colName] = fmt"{colName}_0"
                columnsTableR[colName] = fmt"{colName}_1"
            for colName in colNames:
                result[colName] = initSeries()
            result.indexCol = columnsTableL[leftOn[0]]
            let columnsL = toHashSet(left.getColumnName())
            let columnsR = (toHashSet(right.getColumnName()) - columnsL) + dupCols
            #on列の共通部分の計算
            let leftOnSeries =
                collect(newSeq):
                    for i in 0..<left.len:
                        var row: seq[Cell] = @[]
                        for colName in leftOn:
                            row.add(left[colName][i])
                        row
            let rightOnSeries =
                collect(newSeq):
                    for i in 0..<right.len:
                        var row: seq[Cell] = @[]
                        for colName in rightOn:
                            row.add(right[colName][i])
                        row
            let adoptedOn = 
                if how == "inner":
                    toHashSet(leftOnSeries) * toHashSet(rightOnSeries)
                elif how == "left":
                    toHashSet(leftOnSeries)
                else:
                    toHashSet(leftOnSeries) + toHashSet(rightOnSeries)
            #共通部分を含むindexを抜き出し、その行の値を追加していくく
            for c in adoptedOn:
                let indicesL = leftOnSeries.indicesOf(c)
                if indicesL.len != 0:
                    for indexL in indicesL:
                        let indicesR = rightOnSeries.indicesOf(c)
                        if indicesR.len != 0:
                            for indexR in indicesR:
                                for colName in columnsL:
                                    result.data[columnsTableL[colName]].add(left[colName][indexL])
                                for colName in columnsR:
                                    result.data[columnsTableR[colName]].add(right[colName][indexR])
                        else:
                            for colName in columnsL:
                                result.data[columnsTableL[colName]].add(left[colName][indexL])
                            for colName in columnsR:
                                result.data[columnsTableR[colName]].add(dfEmpty)
                else:
                    let indicesR = rightOnSeries.indicesOf(c)
                    if indicesR.len != 0:
                        for indexR in indicesR:
                            for colName in columnsR + toHashSet(on):
                                result.data[columnsTableR[colName]].add(right[colName][indexR])
                            for colName in columnsL - toHashSet(on):
                                result.data[columnsTableL[colName]].add(dfEmpty)
                    else:
                        raise newException(NimDataFrameError, "unknown error")
        else:
            var msg = ""
            if toHashSet(left.getColumnName())*toHashSet(leftOn) == toHashSet(leftOn):
                msg &= fmt"left column '{leftOn}' not found. "
            else:
                msg &= fmt"right column '{leftOn}' not found. "
            raise newException(NimDataFrameError, msg)
    elif how == "right":
        result = merge(right, left, rightOn, leftOn, "left")
    else:
        raise newException(NimDataFrameError, fmt"invalid method '{how}'")

proc merge(left: DataFrame, right: DataFrame, leftOn: ColName, rightOn: ColName, how="inner"): DataFrame =
    merge(left, right, @[leftOn], @[rightOn], how)

proc merge(left: DataFrame, right: DataFrame, on: openArray[ColName], how="inner"): DataFrame =
    merge(left, right, on, on, how)

proc merge(left: DataFrame, right: DataFrame, on: ColName, how="inner"): DataFrame =
    merge(left, right, @[on], @[on], how)

proc join(dfSource: DataFrame, dfs: openArray[DataFrame], how="left"): DataFrame =
    result = dfSource.deepCopy()
    for i in 0..<dfs.len:
        result = merge(result, dfs[i], result.indexCol, dfs[i].indexCol, how)

proc join(dfSource: DataFrame, df: DataFrame, how="left"): DataFrame =
    join(dfSource, @[df], how)

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
    result.indexCol = dfg.columns[0]

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
    result.indexCol = dfg.columns[0]

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
    result.indexCol = dfg.columns[0]


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
    echo "df################################"
    var df = toDataFrame(
        text=csv,
        headers=["time","name","sales","日本語"],
        headerRows=1,
    )
    df.show(true)
    #df.toCsv("test.csv")
    #
    echo "dropEmpty################################"
    df.dropEmpty().show(true)
    #
    echo "fillEmpty################################"
    df["sales"] = df["sales"].fillEmpty(0)
    df.show(true)
    #
    echo "df1################################"
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
    echo "drop################################"
    df.dropColumns(["time","name"]).show(true)
    #
    echo "rename################################"
    df.renameColumns({"time":"TIME","name":"NAME","sales":"SALES"}).show(true)
    #
    echo "stats################################"
    df.mean().show(true)
    df.max().show(true)
    #
    echo "map################################"
    echo df["sales"].intMap(c => c*2)
    echo df["time"].datetimeMap(c => c+initDuration(hours=1))
    let triple = proc(c: int): int =
        c * 3
    echo df["sales"].map(triple, parseInt)
    #
    echo "filter################################"
    df.filter(row => row["sales"] >= 2000).show(true)
    df.filter(row => row["sales"] > 1000 and 3000 > row["sales"]).show(true)
    #
    echo "loc,iloc################################"
    echo df1.loc("1")
    echo df.iloc(0)
    #
    echo "getRows################################"
    echo df.getRows()
    echo df.getColumnName()
    #
    echo "sort################################"
    df.sort("name", ascending=false).show(true)
    df.sort("sales", parseInt, ascending=true).show(true)
    df.sort("sales", parseInt, ascending=false).show(true)
    df.datetimeSort("time", ascending=false).show(true)
    #
    echo "resetIndex################################"
    df.intSort("sales").resetIndex().show(true)
    #
    echo "index,shape################################"
    echo df.index
    echo df.shape
    #
    echo "[]################################"
    df[["time","sales"]].show(true)
    df[0..4].show(true)
    df[[2,4,6]].show(true)
    #
    echo "head,tail################################"
    df.head(5).show(true)
    df.tail(5).show(true)
    df.head(999999999).show(true)
    df.tail(999999999).show(true)
    #
    echo "duplicated################################"
    echo df.duplicated(["sales"])
    df.dropDuplicates(["sales"]).show(true)
    df.dropDuplicates().show(true)
    df.dropDuplicates(["time","sales"]).show(true)
    #
    echo "groupby################################"
    echo df.groupby(["time","name"])
    #
    echo "groupby mean,max################################"
    df.groupby(["time","name"]).mean().show(true)
    df.groupby(["time","name"]).max().show(true)
    #
    echo "groupby agg################################"
    proc aggFnG(s: Series): float {.closure.} =
        result = s.toFloat().mean()/100
    echo df.groupby(["time","name"]).agg({"sales": aggFnG})
    #
    echo "groupby apply################################"
    proc applyFnG(df: DataFrame): Table[ColName,Cell] =
        var c: Cell
        if df["name"][0] == "abc":
            c = df["sales"].intMap(c => c/10).mean()
        else:
            c = df["sales"].intMap(c => c*10).mean()
        result = {
            "sales_changed": c
        }.toTable()
    df.groupby(["time","name"]).apply(applyFnG).show(true)
    #
    echo "resaple 5 mean################################"
    df.resample(5).sum().show(true)
    #
    echo "resaple 1H agg1################################"
    df.setIndex("time").resample("1H").mean().show(true)
    #
    echo "resaple 30M agg1################################"
    df.setIndex("time").resample("30M").mean().show(true)
    #
    echo "resaple 30M agg2################################"
    proc aggFnRe(s: Series): float{.closure.} =
        sum(s)*100
    df.setIndex("time").resample("30M").agg({"sales":aggFnRe}).show(true)
    #
    echo "resaple 30M apply################################"
    df.setIndex("time").resample("30M").apply(applyFnG).show(true)
    #
    echo "merge inner(1)################################"
    var df_ab = toDataFrame(
        columns = {
            "a": @["A_1", "A_1", "A_2", "A_3"],
            "b": @["B_1", "B_2", "B_2", "B_3"],
        }
    )
    echo "df_ab"
    df_ab.show(true)
    var df_ac = toDataFrame(
        columns = {
            "a": @["A_1", "A_1", "A_1", "A_2", "A_4"],
            "c": @["C_10", "C_20", "C_30", "C_2", "C_4"]
        }
    )
    echo "df_ac"
    df_ac.show(true)
    var df_ac2 = toDataFrame(
        columns = {
            "a": @["A_1", "A_1", "A_1", "A_2", "A_4"],
            "b": @["B_10", "B_20", "B_30", "B_2", "B_4"],
            "c": @["C_10", "C_20", "C_30", "C_2", "C_4"]
        }
    )
    echo "df_ac2"
    df_ac2.show(true)

    merge(df_ab, df_ac, left_on=["a"], right_on=["a"], how="inner").sort(["a","b"]).show(true)
    #
    echo "merge2 inner(2)################################"
    #[
    var df_ab = toDataFrame(
        rows = [
            @["A_1", "B_1"],
            @["A_1", "B_2"],
            @["A_2", "B_2"],
            @["A_3", "B_3"],
        ],
        colNames = ["a","b"]
    )
    ]#
    var df_ac3 = toDataFrame(
        rows = [
            @["A_1", "A_1", "C_10"],
            @["A_1", "A_2", "C_20"],
            @["A_1", "A_3", "C_30"],
            @["A_2", "A_4",  "C_2"],
            @["A_4", "A_5",  "C_4"],
        ],
        colNames = ["a","a_","c"]
    )
    merge(df_ab, df_ac3, left_on=["a"], right_on=["a_"], how="inner").sort(["a_","b"]).show(true)
    #
    echo "merge left(1)################################"
    merge(df_ab, df_ac, left_on=["a"], right_on=["a"], how="left").sort(["a","b"]).show(true)
    #
    echo "merge left(2)################################"
    merge(df_ac3, df_ab, left_on=["a_"], right_on=["a"], how="left").sort(["a_","b"]).show(true)
    #
    echo "merge left(3)################################"
    merge(df_ab, df_ac3, left_on=["a"], right_on=["a_"], how="left").sort(["a_","b"]).show(true)
    #
    echo "merge right(1)################################"
    merge(df_ab, df_ac, left_on=["a"], right_on=["a"], how="right").sort(["a","b"]).show(true)
    #
    echo "merge right(2)################################"
    merge(df_ac3, df_ab, left_on=["a_"], right_on=["a"], how="right").sort(["a_","b"]).show(true)
    #
    echo "merge outer(1)################################"
    merge(df_ab, df_ac, left_on=["a"], right_on=["a"], how="outer").sort(["a","b"]).show(true)
    #
    echo "merge outer(2)################################"
    merge(df_ac3, df_ab, left_on=["a_"], right_on=["a"], how="outer").sort(["a_","b"]).show(true)
    #
    echo "join left(1)################################"
    var df_j1 = toDataFrame(
        columns = {
            "a": @[1,2,3,4,5],
            "b": @[10,20,30,40,50],
            "c": @[100,200,300,400,500],
        },
        indexCol="a"
    )
    var df_j2 = toDataFrame(
        columns = {
            "a": @[1,2,3],
            "d": @[1000,2000,3000],
        },
        indexCol="a"
    )
    var df_j3 = toDataFrame(
        columns = {
            "a": @[1,2],
            "e": @[10000,20000],
        },
        indexCol="a"
    )
    var df_j4 = toDataFrame(
        columns = {
            "a": @[1,6,7],
            "c": @[600,700,800],
        },
        indexCol="a"
    )
    join(df_j1, [df_j2, df_j3]).sort("a").show(true)
    #
    echo "join inner(1)################################"
    join(df_j1, [df_j2, df_j3], how="inner").sort("a").show(true)
    #
    echo "join outer(1)################################"
    join(df_j1, [df_j2, df_j3], how="outer").sort("a").show(true)
    #
    echo "join right(1)################################"
    join(df_j1, [df_j2, df_j3], how="right").sort("a").show(true)
    #
    echo "join left(2)################################"
    join(df_j1, [df_j2, df_j4], how="left").sort("a").show(true)
    #
    echo "join inner(2)################################"
    join(df_j1, [df_j2, df_j4], how="inner").sort("a").show(true)
    #
    echo "join outer(2)################################"
    join(df_j1, [df_j2, df_j4], how="outer").sort("a").show(true)
    #
    echo "join right(2)################################"
    join(df_j1, [df_j2, df_j4], how="right").sort("a").show(true)
    #[
    ]#

if isMainModule:
    toBe()