import sugar
import macros
import strutils
import strformat
import sequtils
import tables
import times
import stats
import algorithm

type Cell = string
type Row = Table[string, Cell]
type Series = seq[Cell]
type DataFrameData = Table[string, Series]
type DataFrame = object
    data: DataFrameData
    indexCol: string

const defaultTimeFormat = "yyyy/MM/dd HH:mm:ss"

###############################################################
#parse... : cellに対しての型変換
#to...    : seriesに対しての型変換
proc initDataFrame(): DataFrame =
    result.data = initTable[string, Series]()
    result.indexCol = ""
proc initSeries(): Series =
    result = @[]
proc initRow(): Row =
    result = initTable[string, Cell]()

proc `[]`(df: DataFrame, colName: string): Series =
    df.data[colName]

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

proc `[]=`[T](df: var DataFrame, colName: string, right: openArray[T]) {. discardable .} =
    var val = collect(newSeq):
        for c in right:
            c.parseString()
    df.data.add(colName, val)

proc `[]`(df: DataFrame, boolean: seq[bool]): DataFrame =
    result = initDataFrame()
    for colName in df.columns:
        result[colName] = initSeries()
    for colName in df.columns:
        for i, b in boolean.pairs():
            if b:
                result.data[colName].add(df.data[colName][i])

proc iloc(df: DataFrame, index: int): Row =
    result = initRow()
    for colName in df.columns:
        result[colName] = df.data[colName][index]

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
    headers: openArray[string],
    headerRows= -1
    indexCol=""
): DataFrame =
    result = initDataFrame()
    for colName in headers:
        result[colName] = initSeries()
    for rowNumber, line in text.split("\n").pairs():
        if rowNumber < headerRows:
            continue
        for (cell, colName) in zip(line.split(sep), headers):
            result.data[colName].add(cell.strip())

proc toDataFrame[T](rows: openArray[seq[T]], columns: openArray[string] = []): DataFrame =
    result = initDataFrame()
    let colCount = max(
        collect(newSeq) do:
            for row in rows:
                row.len
    )
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
                            ""
                    )
        else:
            raise newException(ValueError, "each row.len must be lower than columns.len.")
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
                        ""
                )


###############################################################
proc dropColumns(df:DataFrame, colNames: openArray[string]): DataFrame =
    result = df
    for colName in colNames:
        result.data.del(colName)
proc renameColumns(df: DataFrame, renameMap: openArray[(string,string)]): DataFrame =
    result = df
    for renamePair in renameMap:
        if result.data.contains(renamePair[0]):
            result.data[renamePair[1]] = result.data[renamePair[0]]
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
    var boolean: seq[bool] = @[]
    for row in df.rows:
        boolean.add(fltr(row))
    result = df[boolean]

proc sort[T](df: DataFrame, colName: string, fromCell: Cell -> T, ascending=true): DataFrame =
    result = initDataFrame()
    for colName in df.columns:
        result[colName] = initSeries()
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
proc intSort(df: DataFrame, colName: string, ascending=true): DataFrame =
    sort(df, colName, parseInt, ascending)
proc floatSort(df: DataFrame, colName: string, ascending=true): DataFrame =
    sort(df, colName, parseFloat, ascending)
proc timeSort(df: DataFrame, colName: string, format=defaultTimeFormat, ascending=true): DataFrame =
    sort(df, colName, genParseTime(format), ascending)


###############################################################
proc mean(df: DataFrame): DataFrame =
    result = initDataFrame()
    for (colName, s) in df.data.pairs():
        try:
            let f = s.toFloat()
            result[colName] = @[f.mean()]
        except:
            result[colName] = ""
proc std(df: DataFrame): DataFrame =
    discard
proc max(df: DataFrame): DataFrame =
    discard
proc min(df: DataFrame): DataFrame =
    discard
proc v(df: DataFrame): DataFrame =
    discard

###############################################################
proc toBe() =
    const filename = "sample.csv"
    var fp: File
    let openOk = fp.open(filename, fmRead)
    defer: fp.close()
    if not openOk:
        quit(fmt"{filename} open failed.")
    let csv = fp.readAll()
    echo "--------------------------------"
    #
    var df = toDataFrame(
        text=csv,
        headers=["time","name","sales"],
        headerRows=1,
    )
    echo df
    echo "--------------------------------"
    #
    var df1 = toDataFrame(
        [
            @[1,2,3],
            @[4,5,6],
            @[7,8,],
        ],
        columns=["col1","col2","col3","col10"],
    )
    echo df1
    echo "--------------------------------"
    #
    echo df.mean()
    echo "--------------------------------"
    #
    echo df["sales"].intMap(c => c*2)
    echo df["time"].timeMap(c => c+initDuration(hours=1))
    var triple = proc(c: int): int =
        c * 3
    echo df["sales"].map(triple, parseInt)
    echo "--------------------------------"
    #
    echo df.filter(row => row["sales"] >= 1000)
    echo df.filter(row => row["sales"] > 100 and 1000 > row["sales"])
    echo "--------------------------------"
    #
    echo df.iloc(0)
    echo "--------------------------------"
    #
    echo df.sort("sales", parseInt, ascending=true)
    echo df.sort("sales", parseInt, ascending=false)
    echo df.timeSort("time", ascending=false)
    echo "--------------------------------"
    #[
    #
    
    ]#

if isMainModule:
    toBe()