import sugar
import macros
import strutils
import strformat
import sequtils
import tables
import times
import stats
import sets
import encodings
import re
import streams

import nimdataframe/typedef as typedef
export typedef

import nimdataframe/core as core
export core

import nimdataframe/operation as operation
export operation

import nimdataframe/calculation as calculation
export calculation

import nimdataframe/aggregation as aggregation
export aggregation

import nimdataframe/checker as checker
export checker


###############################################################
proc toDataFrame*(
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
        result.data[colName] = initSeries()
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
        if result.getColumns().contains(indexCol):
            result.indexCol = indexCol
        else:
            raise newException(NimDataFrameError, fmt"not found {indexCol}")
    else:
        result.data[defaultIndexName] =
            collect(newSeq):
                for i in 0..<lines.len-headerRows: $i

proc toDataFrame*[T](rows: openArray[seq[T]], colNames: openArray[ColName] = [], indexCol=""): DataFrame =
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
        if result.getColumns().contains(indexCol):
            result.indexCol = indexCol
        else:
            raise newException(NimDataFrameError, fmt"not found {indexCol}")
    else:
        result[defaultIndexName] =
            collect(newSeq):
                for i in 0..<rows.len: $i
        result.indexCol = defaultIndexName

proc toDataFrame*[T](columns: openArray[(ColName, seq[T])], indexCol="" ): DataFrame =
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
        raise newException(NimDataFrameError, "series must all be same length")
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
proc toCsv*(df: DataFrame): string =
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

proc toCsv*(df: DataFrame, filename: string, encoding="utf-8") =
    var fp: File
    let openOk = fp.open(filename, fmWrite)
    defer: fp.close()
    if not openOk:
        raise newException(NimDataFrameError, fmt"{filename} open error.")
    #
    let ec = open(encoding, "utf-8")
    defer: ec.close()
    fp.write(ec.convert(df.toCsv()))

proc toCsv*(df: DataFrame, writableStrem: Stream, encoding="utf-8") =
    let ec = open(encoding, "utf-8")
    defer: ec.close()
    writableStrem.write(ec.convert(df.toCsv()))

proc toFigure*(df: DataFrame, indexColSign=false): string =
    result = ""
    #空のデータフレームの場合
    if df[df.indexCol].len == 0:
        result &= "+-------+\n"
        result &= "| empty |\n"
        result &= "+-------+"
    #空ではない場合
    else:
        var columns = df.getColumns()
        for i, colName in columns.pairs():
            if colName == df.indexCol:
                columns.del(i)
                columns = concat(@[df.indexCol], columns)
                break
        var width: Table[string,int]
        var fullWidth = df.getColumns().len
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

proc show*(df: DataFrame, indexColSign=false, writableStrem: Stream = nil) =
    var stream: Stream
    if writableStrem.isNil:
        stream = newFileStream(stdout)
    else:
        stream = writableStrem
    stream.writeLine( df.toFigure(indexColSign) )

