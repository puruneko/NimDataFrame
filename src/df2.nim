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

const dfEmpty = ""
const defaultIndexName = "__index__"
const defaultTimeFormat = "yyyy/MM/dd HH:mm:ss"

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
    x.format(defaultTimeFormat)
proc parseString[T](x: T): Cell =
    $x
proc parseTime(c: Cell, format=defaultTimeFormat): DateTime =
    c.parse(format)
proc genParseTime(format=defaultTimeFormat): Cell -> DateTime =
    result =
        proc(c:Cell): DateTime =
            c.parseTime(format)

proc `[]`(df: DataFrame, colName: ColName): Series =
    ## DataFrameからSeriesを取り出す
    df.data[colName]

proc `[]=`[T](df: var DataFrame, colName: ColName, right: openArray[T]) {. discardable .} =
    ## DataFrameのSeriesに代入する
    ## 代入されるarrayの各値はstringにキャストされる
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
    ## 指定した列だけ返す
    result = initDataFrame()
    let columns = df.getColumnName()
    for colName in colNames:
        if not columns.contains(colName):
            raise newException(ValueError, fmt"df doesn't have column {colName}")
        result[colName] = df[colName]
    result[df.indexCol] = df[df.indexCol]

proc keep(df: DataFrame, fs: FilterSeries): DataFrame =
    ## trueをkeepする（fsがtrueの行だけ返す）
    result = initDataFrame(df)
    for colName in df.columns:
        for i, b in fs.pairs():
            if b:
                result.data[colName].add(df[colName][i])
proc drop(df: DataFrame, fs: FilterSeries): DataFrame =
    ## trueをdropする（fsがtrueの行を落として返す）（fsがfalseの行だけ返す）
    result = initDataFrame(df)
    for colName in df.columns:
        for i, b in fs.pairs():
            if not b:
                result.data[colName].add(df[colName][i])

proc `[]`(df: DataFrame, fs: FilterSeries): DataFrame =
    ## fsがtrueの行だけ返す
    df.keep(fs)

proc `[]`(df: DataFrame, slice: HSlice[int, int]): DataFrame =
    ## sliceの範囲の行だけ返す
    result = initDataFrame(df)
    let len = df.len
    for i in slice:
        if i < 0 or i >= len:
            continue
        for colName in df.columns:
            result.data[colName].add(df[colName][i])

proc `[]`(df: DataFrame, indices: openArray[int]): DataFrame =
    ## indicesの行だけ返す
    result = initDataFrame(df)
    let len = df.len
    for i in indices:
        if i < 0 or i >= len:
            continue
        for colName in df.columns:
            result.data[colName].add(df[colName][i])

proc iloc(df: DataFrame, i: int): Row =
    ## index番目の行をRow形式で返す
    result = initRow()
    for colName in df.columns:
        result[colName] = df.data[colName][i]

proc loc(df: DataFrame, c: Cell): DataFrame =
    ## indexの行の値がcの値と一致する行を返す
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

proc to[T](s: Series, parser: Cell -> T): seq[T] =
    result = collect(newSeq):
        for c in s:
            parser(c)
proc toInt(s: Series): seq[int] =
    to(s, parseInt)
proc toFloat(s: Series): seq[float] =
    to(s, parseFloat)
proc toTime(s: Series, format=defaultTimeFormat): seq[DateTime] =
    to(s, genParseTime(format))

proc `+`(a: Cell, b: float): float =
    parseFloat(a) + b
proc `-`(a: Cell, b: float): float =
    parseFloat(a) - b
proc `*`(a: Cell, b: float): float =
    parseFloat(a) * b
proc `/`(a: Cell, b: float): float =
    parseFloat(a) / b
proc `+`(a: float, b: Cell): float =
    a + parseFloat(b)
proc `-`(a: float, b: Cell): float =
    a - parseFloat(b)
proc `*`(a: float, b: Cell): float =
    a * parseFloat(b)
proc `/`(a: float, b: Cell): float =
    a / parseFloat(b)

#TODO: int版も作る
proc `==`(a: Cell, b: float): bool =
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
    
###############################################################
proc toDataFrame(
    text: string,
    sep=",",
    headers: openArray[ColName],
    headerRows= 0,
    indexCol="",
): DataFrame =
    #初期化
    result = initDataFrame()
    for colName in headers:
        result[colName] = initSeries()
    #テキストデータの変換
    let lines = text.strip().split("\n")
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
            raise newException(ValueError, fmt"not found {indexCol}")
    else:
        result[defaultIndexName] =
            collect(newSeq):
                for i in 0..<lines.len-headerRows: $i

proc toDataFrame[T](rows: openArray[seq[T]], columns: openArray[ColName] = [], indexCol=""): DataFrame =
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
            raise newException(ValueError, "each row.len must be lower than columns.len.")
    #列名が指定されていない場合
    else:
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
            raise newException(ValueError, fmt"not found {indexCol}")
    else:
        result[defaultIndexName] =
            collect(newSeq):
                for i in 0..<rows.len: $i


###############################################################
proc stat(df: DataFrame, statFn: openArray[float] -> float): DataFrame =
    result = initDataFrame()
    for (colName, s) in df.data.pairs():
        try:
            let f = s.toFloat()
            result[colName] = @[f.statFn()]
        except:
            result[colName] = @[dfEmpty]
    result[df.indexCol] = @["0"]
proc mean(df: DataFrame): DataFrame =
    df.stat(stats.mean)
proc std(df: DataFrame): DataFrame =
    df.stat(stats.standardDeviation)
proc max(df: DataFrame): DataFrame =
    df.stat(max)
proc min(df: DataFrame): DataFrame =
    df.stat(min)
proc v(df: DataFrame): DataFrame =
    df.stat(stats.variance)

###############################################################
proc dropColumns(df:DataFrame, colNames: openArray[ColName]): DataFrame =
    result = df
    for colName in colNames:
        result.data.del(colName)
proc renameColumns(df: DataFrame, renameMap: openArray[(ColName,ColName)]): DataFrame =
    result = df
    for renamePair in renameMap:
        if result.data.contains(renamePair[0]):
            result[renamePair[1]] = result[renamePair[0]]
            result.data.del(renamePair[0])

proc map[T, U](s: Series, fn: U -> T, fromCell: Cell -> U): Series =
    for c in s:
        result.add(fn(fromCell(c)).parseString())
proc intMap[T](s: Series, fn: int -> T): Series =
    map(s, fn, parseInt)
proc floatMap[T](s: Series, fn: float -> T): Series =
    map(s, fn, parseFloat)
proc timeMap[T](s: Series, fn: DateTime -> T, format=defaultTimeFormat): Series =
    map(s, fn, genParseTime(format))

proc filter(df: DataFrame, fltr: Row -> bool): DataFrame =
    var fs: FilterSeries = initFilterSeries()
    for row in df.rows:
        fs.add(fltr(row))
    result = df[fs]

proc sort[T](df: DataFrame, colName: ColName, fromCell: Cell -> T, ascending=true): DataFrame =
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
proc intSort(df: DataFrame, colName: ColName, ascending=true): DataFrame =
    sort(df, colName, parseInt, ascending)
proc floatSort(df: DataFrame, colName: ColName, ascending=true): DataFrame =
    sort(df, colName, parseFloat, ascending)
proc timeSort(df: DataFrame, colName: ColName, format=defaultTimeFormat, ascending=true): DataFrame =
    sort(df, colName, genParseTime(format), ascending)

proc duplicated(df: DataFrame, colNames: openArray[ColName] = []): FilterSeries =
    ## 重複した行はtrue、それ以外はfalse
    result = initFilterSeries()
    var checker = initTable[seq[string], bool]()
    var columns = colNames.toSeq()
    if columns.len == 0:
        columns = @[df.indexCol]
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
    df.drop(df.duplicated(colNames))

proc concat(dfs: openArray[DataFrame]): DataFrame =
    ## 単純に下にDataFrameを連結し続ける
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


proc groupby(df: DataFrame, colNames: openArray[ColName]): DataFrameGroupBy =
    ## DataFrameを指定の列の値でグループ化する（戻り値はDataFrameGroupBy型）
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
proc stat(dfg: DataFrameGroupBy, statFn: DataFrame -> DataFrame): DataFrame =
    result = initDataFrame()
    var dfs: seq[DataFrame] = @[]
    for mi in dfg.data.keys:
        var df = statFn(dfg.data[mi])
        for (colName, colValue) in zip(dfg.columns, mi):
            df[colName] = @[colValue]
        dfs.add(df)
    result = concat(dfs = dfs)
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
        headers=["time","name","sales"],
        headerRows=1,
    )
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
    echo "stats--------------------------------"
    echo df.mean()
    echo df.max()
    #
    echo "map--------------------------------"
    echo df["sales"].intMap(c => c*2)
    echo df["time"].timeMap(c => c+initDuration(hours=1))
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
    echo "sort--------------------------------"
    echo df.sort("sales", parseInt, ascending=true)
    echo df.sort("sales", parseInt, ascending=false)
    echo df.timeSort("time", ascending=false)
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
    #[
    ]#

if isMainModule:
    toBe()