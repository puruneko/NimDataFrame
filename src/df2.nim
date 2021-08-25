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
    indexCol: string
type FilterSeries = seq[bool]
type DataFrameGroupBy = object
    data: Table[seq[ColName], DataFrame]
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
    ## DataFrameからSeriesを取り出す。
    df.data[colName]

proc `[]=`[T](df: var DataFrame, colName: ColName, right: openArray[T]) {. discardable .} =
    ## DataFrameのSeriesに代入する。
    ## 代入されるarrayの各値はstringにキャストされる。
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

proc `[]`(df: DataFrame, colNames: openArray[ColName]): DataFrame =
    ## 指定した列だけ返す。
    result = initDataFrame()
    let columns = df.getColumnName()
    for colName in colNames:
        if not columns.contains(colName):
            raise newException(NimDataFrameError, fmt"df doesn't have column {colName}")
        result[colName] = df[colName]
    result[df.indexCol] = df[df.indexCol]

proc keep(df: DataFrame, fs: FilterSeries): DataFrame =
    ## trueをkeepする（fsがtrueの行だけ返す）。
    result = initDataFrame(df)
    for colName in df.columns:
        for i, b in fs.pairs():
            if b:
                result.data[colName].add(df[colName][i])
proc drop(df: DataFrame, fs: FilterSeries): DataFrame =
    ## trueをdropする（fsがtrueの行を落として返す）（fsがfalseの行だけ返す）。
    result = initDataFrame(df)
    for colName in df.columns:
        for i, b in fs.pairs():
            if not b:
                result.data[colName].add(df[colName][i])

proc `[]`(df: DataFrame, fs: FilterSeries): DataFrame =
    ## fsがtrueの行だけ返す。
    df.keep(fs)

proc `[]`(df: DataFrame, slice: HSlice[int, int]): DataFrame =
    ## sliceの範囲の行だけ返す。
    result = initDataFrame(df)
    let len = df.len
    for i in slice:
        if i < 0 or i >= len:
            continue
        for colName in df.columns:
            result.data[colName].add(df[colName][i])

proc `[]`(df: DataFrame, indices: openArray[int]): DataFrame =
    ## indicesの行だけ返す。
    result = initDataFrame(df)
    let len = df.len
    for i in indices:
        if i < 0 or i >= len:
            continue
        for colName in df.columns:
            result.data[colName].add(df[colName][i])

proc iloc(df: DataFrame, i: int): Row =
    ## index番目の行をRow形式で返す。
    result = initRow()
    for colName in df.columns:
        result[colName] = df.data[colName][i]

proc loc(df: DataFrame, c: Cell): DataFrame =
    ## indexの行の値がcの値と一致する行を返す。
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
    ## テキストで表現されたデータ構造をDataFrameに変換する。
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

proc toDataFrame[T](rows: openArray[seq[T]], columns: openArray[ColName] = [], indexCol=""): DataFrame =
    ## 配列で表現されたデータ構造をDataFrameに変換する。
    runnableExamples:
        var df = toDataFrame(
            [
                @[1,2,3],
                @[4,5,6],
                @[7,8,],
                @[1,10,11,12]
            ],
            columns=["col1","col2","col3","col4"],
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
    if columns.len > 0:
        if colCount <= columns.len:
            for colName in columns:
                result[colName] = initSeries()
            for row in rows:
                for colNumber, colName in columns.pairs():
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
        let colNames = collect(newSeq):
            for i in 0..<colCount:
                fmt"col{i}"
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
proc toCsv(df: DataFrame, filename: string, encoding="utf-8") =
    var fp: File
    let openOk = fp.open(filename, fmWrite)
    defer: fp.close()
    if not openOk:
        raise newException(NimDataFrameError, fmt"{filename} open error.")
    #
    let ec = open(encoding, "utf-8")
    defer: ec.close()
    var csv = ""
    var line = ""
    for colName in df.columns:
        line &= (colName & ",")
    csv &= ec.convert(line[0..<line.len-1] & "\n")
    for i in 0..<df.len:
        line = ""
        for colName in df.columns:
            line &= df[colName][i] & ","
        csv &= ec.convert(line[0..<line.len-1] & "\n")
    fp.write(csv)

###############################################################
proc concat(dfs: openArray[DataFrame]): DataFrame =
    ## 単純に下にDataFrameを連結し続ける。
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
    ).toSeq()
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

###############################################################
proc `+`(a: Cell, b: float): float =
    ## 左辺Cell型、右辺float型の加算を計算する。
    parseFloat(a) + b
proc `-`(a: Cell, b: float): float =
    parseFloat(a) - b
proc `*`(a: Cell, b: float): float =
    parseFloat(a) * b
proc `/`(a: Cell, b: float): float =
    parseFloat(a) / b
proc `+`(a: float, b: Cell): float =
    ## 左辺float型、右辺Cell型の加算を計算する。
    a + parseFloat(b)
proc `-`(a: float, b: Cell): float =
    a - parseFloat(b)
proc `*`(a: float, b: Cell): float =
    a * parseFloat(b)
proc `/`(a: float, b: Cell): float =
    a / parseFloat(b)

#TODO: int版も作る
proc `==`(a: Cell, b: float): bool =
    ## 左辺Cell型、右辺float型を等価比較する。
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
    ## 左辺float型、右辺Cell型を等価比較する。
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
    
proc stat(s: Series, statFn: openArray[float] -> float): Cell =
    ## Seriesの統計量を計算する。
    ## statFnにはSeriesをfloat変換した配列の統計量を計算する関数を指定する。
    runnableExamples:
        df["col1"].stat(stats.mean)
    ##

    try:
        let f = s.toFloat()
        result = statFn(f).parseString()
    except:
        result = dfEmpty
proc count(s: Series): Cell =
    let cnt = proc(s: openArray[float]): float =
        float(s.len)
    s.stat(cnt)
proc sum(s: Series): Cell =
    s.stat(sum)
proc mean(s: Series): Cell =
    s.stat(stats.mean)
proc std(s: Series): Cell =
    s.stat(stats.standardDeviation)
proc max(s: Series): Cell =
    s.stat(max)
proc min(s: Series): Cell =
    s.stat(min)
proc v(s: Series): Cell =
    s.stat(stats.variance)

proc stat(df: DataFrame, statFn: Series -> Cell): DataFrame =
    ## DataFrameの各列に対して統計量を計算する。
    ## statFnにはSeriesの統計量を計算する関数を指定する。
    runnableExamples:
        df.stat(mean)
    ##

    result = initDataFrame()
    for (colName, s) in df.data.pairs():
        let c = statFn(s)
        result[colName] = @[c]
proc count(df: DataFrame): DataFrame =
    df.stat(count)
proc sum(df: DataFrame): DataFrame =
    df.stat(sum)
proc mean(df: DataFrame): DataFrame =
    df.stat(mean)
proc std(df: DataFrame): DataFrame =
    df.stat(std)
proc max(df: DataFrame): DataFrame =
    df.stat(max)
proc min(df: DataFrame): DataFrame =
    df.stat(min)
proc v(df: DataFrame): DataFrame =
    df.stat(v)

proc stat(dfg: DataFrameGroupBy, statFn: DataFrame -> DataFrame): DataFrame =
    ## groupbyしたDataFrameに対して統計量を計算する。
    ## statFnにはDataFrameの統計量を計算する関数を指定する。
    runnableExamples:
        df.groupby(["col1","col2"]).stat(sum)
    ##

    result = initDataFrame()
    var dfs: seq[DataFrame] = @[]
    for mi in dfg.data.keys:
        #統計値の計算
        var df = statFn(dfg.data[mi])
        #マルチインデックス値の上書き
        for (colName, colValue) in zip(dfg.columns, mi):
            df[colName] = @[colValue]
        dfs.add(df)
    result = concat(dfs = dfs)
proc count(dfg: DataFrameGroupBy): DataFrame =
    dfg.stat(count)
proc sum(dfg: DataFrameGroupBy): DataFrame =
    dfg.stat(sum)
proc mean(dfg: DataFrameGroupBy): DataFrame =
    dfg.stat(mean)
proc std(dfg: DataFrameGroupBy): DataFrame =
    dfg.stat(std)
proc max(dfg: DataFrameGroupBy): DataFrame =
    dfg.stat(max)
proc min(dfg: DataFrameGroupBy): DataFrame =
    dfg.stat(min)
proc v(dfg: DataFrameGroupBy): DataFrame =
    dfg.stat(v)

###############################################################
proc dropColumns(df:DataFrame, colNames: openArray[ColName]): DataFrame =
    ## 指定のDataFrameの列を削除する。
    runnableExamples:
        df.dropColumns(["col1","col2"])
    ##

    result = df
    for colName in colNames:
        result.data.del(colName)
proc renameColumns(df: DataFrame, renameMap: openArray[(ColName,ColName)]): DataFrame =
    ## DataFrameの列名を変更する。
    ## renameMapには変更前列名と変更後列名のペアを指定する。
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
    ## Seriesの各セルに対して関数fnを適用する。
    ## 関数fnにはSeriesの各セルが渡され、関数fnは文字列に変換可能な任意の型を返す。
    ## 文字列型以外の操作を関数fn内で行う場合、fromCell関数にCell型から任意の型に変換する関数を渡す。
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
    ## fltr関数に従ってDataFrameにフィルタをかける。
    ## fltr関数にはDataFrameの各列が渡され、fltr関数は論理値を返す。
    runnableExamples:
        df.filter(row => row["col1"] > 1000 and 3000 > row["col2"])
    ##

    var fs: FilterSeries = initFilterSeries()
    for row in df.rows:
        fs.add(fltr(row))
    result = df[fs]

###############################################################
proc sort[T](df: DataFrame, colName: ColName, fromCell: Cell -> T, ascending=true): DataFrame =
    ## DataFrameを指定列でソートする。
    ## 文字列以外のソートの場合はfromCellに文字列から指定型に変換する関数を指定する。
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
proc sort(df: DataFrame, colName: ColName, ascending=true): DataFrame =
    let f = proc(c: Cell): Cell = c
    sort(df, colName, f, ascending)
proc intSort(df: DataFrame, colName: ColName, ascending=true): DataFrame =
    sort(df, colName, parseInt, ascending)
proc floatSort(df: DataFrame, colName: ColName, ascending=true): DataFrame =
    sort(df, colName, parseFloat, ascending)
proc datetimeSort(df: DataFrame, colName: ColName, format=defaultDateTimeFormat, ascending=true): DataFrame =
    sort(df, colName, genParseDatetime(format), ascending)

###############################################################
proc duplicated(df: DataFrame, colNames: openArray[ColName] = []): FilterSeries =
    ## 重複した行はtrue、それ以外はfalse。
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
    ## 重複した行を消す。
    ##
    df.drop(df.duplicated(colNames))

###############################################################
proc groupby(df: DataFrame, colNames: openArray[ColName]): DataFrameGroupBy =
    ## DataFrameを指定の列の値でグループ化する（戻り値はDataFrameGroupBy型）。
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
    ## groupbyしたDataFrameの各列に対して関数を実行する。
    ## 指定する関数に{.closure.}オプションをつけないとエラーになる。
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
proc apply[T](dfg: DataFrameGroupBy, applyFn: DataFrame -> Table[ColName,T]): DataFrame =
    ## groupby下DataFrameの各groupに対して関数を実行する。
    ## 関数はTableを返すことに注意。
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

###############################################################
proc resample(df: DataFrame, window: int, format=defaultDateTimeFormat): DataFrameResample =
    result.data = df
    result.window = $window
    result.format = format
proc resample(df: DataFrame, window: string, format=defaultDateTimeFormat): DataFrameResample =
    result.data = df
    result.window = window
    result.format = format
proc stat(dfre: DataFrameResample, statFn: Series -> Cell): DataFrame =
    result = initDataFrame()
    #数字指定かdatetime指定か判断する
    var matches: array[2, string]
    if match(dfre.window, re"(\d+)([a-zA-Z]+)?", matches):
        #数字のみ（行数指定）の場合
        if matches[1] == "" and matches[0] != "":
            let w = matches[0].parseInt()
            #各行をwindow飛ばしで処理する
            for colName in dfre.data.columns:
                result[colName] = initSeries()
            var index: seq[Cell] = @[]
            for i in countup(0, dfre.data.len-1, w):
                index.add(dfre.data[dfre.data.indexCol][i])
                var slice = i..<i+w
                if slice.b >= dfre.data.len:
                    slice.b = dfre.data.len-1
                for colName in result.columns:
                    result.data[colName].add(statFn(dfre.data[colName][slice]))
            result.data[dfre.data.indexCol] = index
        #datetime範囲指定の場合
        elif matches[1] != "" and matches[0] != "":
            let datetimeId = matches[1]
            let w = matches[0].parseInt()
            #インデックスがdatetimeフォーマットに準拠している場合
            if isDatetimeSeries(dfre.data[dfre.data.indexCol]):
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
                let datetimes = dfre.data[dfre.data.indexCol].toDatetime()
                let getInterval = genGetInterval(datetimeId)
                let startDatetime = flattenDatetime(datetimes[0], datetimeId)
                var startIndex = 0
                var interval = w
                var index: seq[DateTime] = @[]
                #DateTime型に変換したindexを上から順にみていく
                for colName in dfre.data.columns:
                    result[colName] = initSeries()
                for i, dt in datetimes.pairs():
                    #範囲外になった場合、集計
                    if startDatetime + getInterval(interval) <= dt:
                        var slice = startIndex..<i
                        if slice.b >= dfre.data.len:
                            slice.b = dfre.data.len-1
                        for colName in result.columns:
                            result.data[colName].add(statFn(dfre.data[colName][slice]))
                        index.add(startDatetime + getInterval(interval-w))
                        startIndex = i
                        interval += w
                result[dfre.data.indexCol] = index
                if startIndex < dfre.data.len-1:
                    var slice = startIndex..<dfre.data.len
                    for colName in result.columns:
                        result.data[colName].add(statFn(dfre.data[colName][slice]))
                    result.data[dfre.data.indexCol].add((startDatetime + getInterval(interval-w)).parseString())
            #インデックスがdatetimeフォーマットでない場合
            else:
                raise newException(NimDataFrameError, "invalid datetime format")
        #指定フォーマットでない場合
        else:
            raise newException(NimDataFrameError, "invalid datetime format")
    #指定フォーマットにひっからなかった場合（エラー）
    else:
        raise newException(NimDataFrameError, "invalid datetime format")
proc count(dfre: DataFrameResample): DataFrame =
    dfre.stat(count)
proc sum(dfre: DataFrameResample): DataFrame =
    dfre.stat(sum)
proc mean(dfre: DataFrameResample): DataFrame =
    dfre.stat(mean)
proc std(dfre: DataFrameResample): DataFrame =
    dfre.stat(std)
proc max(dfre: DataFrameResample): DataFrame =
    dfre.stat(max)
proc min(dfre: DataFrameResample): DataFrame =
    dfre.stat(min)
proc v(dfre: DataFrameResample): DataFrame =
    dfre.stat(v)

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
        columns=["col1","col2","col3","col10"],
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
    proc aggFn(s: Series): float {.closure.} =
        result = s.toFloat().mean()/100
    echo df.groupby(["time","name"]).agg({"sales": aggFn})
    #
    echo "groupby apply--------------------------------"
    proc applyFn(df: DataFrame): Table[ColName,Cell] =
        var c: Cell
        if df["name"][0] == "abc":
            c = df["sales"].intMap(c => c/10).mean()
        else:
            c = df["sales"].intMap(c => c*10).mean()
        result = {
            "sales_changed": c
        }.toTable()
    echo df.groupby(["time","name"]).apply(applyFn)
    #
    echo "resaple mean--------------------------------"
    echo df.resample(5).mean()
    #
    echo "resaple 1H--------------------------------"
    echo df.setIndex("time").resample("1H").mean()
    #
    echo "resaple 30M--------------------------------"
    echo df.setIndex("time").resample("30M").mean()
    #[
    ]#

if isMainModule:
    toBe()