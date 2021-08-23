import sugar
import macros
import strutils
import strformat
import sequtils
import tables
import times
import stats

type Cell = string
type Series = seq[Cell]
type DataFrame = Table[string, Series]

const defaultTimeFormat = "yyyy/MM/dd HH:mm:ss"

###############################################################
proc `[]=`[T](df: var DataFrame, colName: string, right: openArray[T]) {. discardable .} =
    var val = collect(newSeq):
        for c in right:
            c.parseString()
    df.add(colName, val)

iterator columns(df: DataFrame): string =
    for key in df.keys:
        yield key

proc getColumnName(df: DataFrame): seq[string] =
    for column in df.columns:
        result.add(column)
proc getSeries(df: DataFrame): seq[Series] =
    for value in df.values:
        result.add(value)

###############################################################
#parse... : cellに対しての型変換
#to...    : seriesに対しての型変換
proc `$`(x: DateTime): string =
    x.format(defaultTimeFormat)
proc parseString[T](x: T): Cell =
    $x
proc parseTime(c: Cell, format=defaultTimeFormat): DateTime =
    c.parse(format)

proc toSome[T](s: Series, parser: Cell -> T): seq[T] =
    result = collect(newSeq):
        for c in s:
            parser(c)
proc toInt(s: Series): seq[int] =
    toSome(s, parseInt)
proc toFloat(s: Series): seq[float] =
    toSome(s, parseFloat)
proc toTime(s: Series, format=defaultTimeFormat): seq[DateTime] =
    result = collect(newSeq):
        for c in s:
            parseTime(c, format)

proc initDataFrame(): DataFrame =
    result = initTable[string, Series]()
proc initSeries(): Series =
    result = @[]

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
    
###############################################################
proc toDataFrame(text: string, sep=",", headers: openArray[string], headerRows= -1): DataFrame =
    result = initDataFrame()
    for colName in headers:
        result[colName] = initSeries()
    for rowNumber, line in text.split("\n").pairs():
        if rowNumber < headerRows:
            continue
        for (cell, colName) in zip(line.split(sep), headers):
            result[colName].add(cell.strip())

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
                    result[colName].add(
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
                result[colName].add(
                    if colNumber < row.len:
                        row[colNumber].parseString()
                    else:
                        ""
                )


###############################################################
proc dropColumns(df:DataFrame, colNames: openArray[string]): DataFrame =
    result = df
    for colName in colNames:
        result.del(colName)
proc renameColumns(df: DataFrame, renameMap: openArray[(string,string)]): DataFrame =
    result = df
    for renamePair in renameMap:
        if result.contains(renamePair[0]):
            result[renamePair[1]] = result[renamePair[0]]
            result.del(renamePair[0])

proc someMap[T, U](s: Series, fn: U -> T, fromCell: Cell -> U): Series =
    for c in s:
        result.add(fn(fromCell(c)).parseString())
proc intMap[T](s: Series, fn: int -> T): Series =
    someMap(s, parseInt)
proc floatMap[T](s: Series, fn: float -> T): Series =
    someMap(s, parseFloat)
proc timeMap[T](s: Series, fn: Time -> T, format=defaultTimeFormat): Series =
    for c in s:
        result.add(fn(parseTime(c, format)).parseString())

###############################################################
proc mean(df: DataFrame): DataFrame =
    result = initDataFrame()
    for (colName, s) in df.pairs():
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
proc var(df: DataFrame): DataFrame =
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
    #
    var df = toDataFrame(
        text=csv,
        headers=["time","name","sales"],
        headerRows=1,
    )
    echo df
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
    #
    echo df.mean()
    #[
    #
    
    ]#

if isMainModule:
    toBe()