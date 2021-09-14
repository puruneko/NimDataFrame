import sugar
import macros
import strutils
import strformat
import sequtils
import tables
import times
import stats
import sets
import math
import re

import typedef
import core
import operation
import calculation
import checker

###############################################################
proc concat*(dfs: openArray[DataFrame]): DataFrame =
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
        result.addColumn(colName)
    for df in dfs:
        for colName in columns:
            if df.columns.contains(colName):
                for c in df[colName]:
                    result[colName].add(c)
            else:
                for i in 0..<df.len:
                    result[colName].add(dfEmpty)
    result.indexCol = dfs[^1].indexCol
    result.healthCheck(raiseException=true)

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

proc merge*(left: DataFrame, right: DataFrame, leftOn: openArray[ColName], rightOn: openArray[ColName], how="inner"): DataFrame =
    ## leftとrightをマージする
    ## indexColはleftの値が使用される（how="right"の場合はrightの値）
    ## indexColの名前はleftのindexCol名がrightにもある場合は「_0」が後ろにつく
    runnableExamples:
        var df1 = toDataFrame(
            columns = {
                "a": @["A_1", "A_1", "A_2", "A_3"],
                "b": @["B_1", "B_2", "B_2", "B_3"],
            }
        )
        var df2 = toDataFrame(
            columns = {
                "a": @["A_1", "A_1", "A_1", "A_2", "A_4"],
                "a'": @["A_1", "A_2", "A_3", "A_4", "A_5"],
                "c": @["C_10", "C_20", "C_30", "C_2", "C_4"],
            }
        )
        var df3 = merge(df1, df2, ["a"], ["a'"], "left")
    ## 

    result = initDataFrame()
    #
    if ["inner","left","outer"].contains(how):
        #on列が存在する場合
        if toHashSet(left.columns)*toHashSet(leftOn) == toHashSet(leftOn) and
            toHashSet(right.columns)*toHashSet(rightOn) == toHashSet(rightOn):
            #resultの初期化・重複列の処理
            var colNames = (toHashSet(left.columns) + toHashSet(right.columns)).toSeq()
            let on =
                if leftOn == rightOn:
                    leftOn.toSeq()
                else:
                    @[]
            let dupCols = (toHashSet(left.columns) * toHashSet(right.columns)) - toHashSet(on)
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
            let pickingColumnsL = toHashSet(left.columns)
            let pickingColumnsR = (toHashSet(right.columns) - pickingColumnsL) + dupCols
            for colName in colNames:
                result.addColumn(colName)
            result.indexCol = columnsTableL[leftOn[0]]
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
            #共通部分を含むindexを抜き出し、その行の値を追加していく
            var onColumn: seq[ColName] = @[]
            for c in adoptedOn:
                let indicesL = leftOnSeries.indicesOf(c)
                if indicesL.len != 0:
                    for indexL in indicesL:
                        let indicesR = rightOnSeries.indicesOf(c)
                        if indicesR.len != 0:
                            for indexR in indicesR:
                                onColumn.add(left[leftOn[0]][indexL])
                                for colName in pickingColumnsL:
                                    result[columnsTableL[colName]].add(left[colName][indexL])
                                for colName in pickingColumnsR:
                                    result[columnsTableR[colName]].add(right[colName][indexR])
                        else:
                            onColumn.add(left[leftOn[0]][indexL])
                            for colName in pickingColumnsL:
                                result[columnsTableL[colName]].add(left[colName][indexL])
                            for colName in pickingColumnsR:
                                result[columnsTableR[colName]].add(dfEmpty)
                else:
                    let indicesR = rightOnSeries.indicesOf(c)
                    if indicesR.len != 0:
                        for indexR in indicesR:
                            onColumn.add(right[rightOn[0]][indexR])
                            for colName in pickingColumnsR + toHashSet(on):
                                result[columnsTableR[colName]].add(right[colName][indexR])
                            for colName in pickingColumnsL - toHashSet(on):
                                result[columnsTableL[colName]].add(dfEmpty)
                    else:
                        raise newException(NimDataFrameError, "unknown error")
            #インデックスの設定
            result.indexCol = mergeIndexName
            result[mergeIndexName] = onColumn
        else:
            var msg = ""
            if toHashSet(left.columns)*toHashSet(leftOn) == toHashSet(leftOn):
                msg &= fmt"left column '{leftOn}' not found. "
            else:
                msg &= fmt"right column '{leftOn}' not found. "
            raise newException(NimDataFrameError, msg)
    elif how == "right":
        result = merge(right, left, rightOn, leftOn, "left")
    else:
        raise newException(NimDataFrameError, fmt"invalid method '{how}'")
    result.healthCheck(raiseException=true)

proc merge*(left: DataFrame, right: DataFrame, leftOn: ColName, rightOn: ColName, how="inner"): DataFrame =
    merge(left, right, [leftOn], [rightOn], how)

proc merge*(left: DataFrame, right: DataFrame, on: openArray[ColName], how="inner"): DataFrame =
    merge(left, right, on, on, how)

proc merge*(left: DataFrame, right: DataFrame, on: ColName, how="inner"): DataFrame =
    merge(left, right, [on], [on], how)

proc join*(dfSource: DataFrame, dfArray: openArray[DataFrame], how="left"): DataFrame =
    let dfs = concat(@[dfSource], dfArray.toSeq())
    #重複列の名前とその変更後の名前を求めておく
    var dupColsSeq: seq[ColName] = @[]
    for i in 0..<dfs.len:
        for j in i+1..<dfs.len:
            for dup in toHashSet(dfs[i].columns) * toHashSet(dfs[j].columns):
                dupColsSeq.add(dup)
    let dupCols = toHashSet(dupColsSeq)
    var renameList: seq[seq[(ColName,ColName)]] = @[]
    for i in 0..<dfs.len:
        var renames: seq[(ColName, ColName)] = @[]
        for colName in dfs[i].columns:
            if dupCols.contains(colName):
                renames.add((colName, fmt"{colName}_{i}"))
        renameList.add(renames)
    #１つずつマージ
    result = dfs[0].renameColumns(renameList[0])
    for i in 1..<dfs.len:
        let df = dfs[i].renameColumns(renameList[i])
        result = merge(result, df, result.indexCol, df.indexCol, how)
    result.healthCheck(raiseException=true)

proc join*(dfSource: DataFrame, df: DataFrame, how="left"): DataFrame =
    join(dfSource, [df], how)

###############################################################
proc groupby*(df: DataFrame, colNames: openArray[ColName]): DataFrameGroupBy =
    ## DataFrameを指定の列の値でグループ化する（戻り値はDataFrameGroupBy型）.
    ## 
    
    let tStart = cpuTime()
    result = initDataFrameGroupBy(df)
    #マルチインデックスの作成
    let multiIndex =
        collect(newSeq):
            for i in 0..<df.len:
                var index: seq[Cell] = @[]
                for colName in colNames:
                    index.add(df[colName][i])
                index
    let multiIndexSet = toHashSet(multiIndex).toSeq()
    echo cpuTime() - tStart
    #データのグループ化
    result.indexCol = df.indexCol
    result.columns = colNames.toSeq()
    var i = 0
    for mi in multiIndexSet:
        result.group.add(@[])
        result.multiIndexTable[mi] = i
        i.inc()
    echo cpuTime() - tStart
    for i, mi in multiIndex.pairs():
        for colIndex, colName in df.columns.pairs():
            result.group[result.multiIndexTable[mi]].add(i)
    result.multiIndex = multiIndexSet

proc agg*[T](dfg: DataFrameGroupBy, aggFn: openArray[(ColName,Series -> T)]): DataFrame =
    ## groupbyしたDataFrameの指定列に対して関数を実行する.
    ## 指定する関数に{.closure.}オプションをつけないとエラーになる.
    runnableExamples:
        proc f(series: Series): float{.closure.} =
            series.toFloat().mean()/100
        df.groupby(["col1","col2"]).agg({"col3",f})
    ##

    result = initDataFrame()
    for (colName, _) in aggFn:
        result.addColumn(colName)
    #関数の適用
    for miIndex, mi in dfg.multiIndex.pairs():
        #関数の計算
        for (colName, fn) in aggFn:
            var s = initSeries()
            for j in dfg.group[miIndex]:
                s.add(dfg.df[colName][j])
            result[colName].add(fn(s))
        #マルチインデックス値の上書き
        for (colName, colValue) in zip(dfg.columns, mi):
            if not result.columns.contains(colName):
                result.addColumn(colName)
            if result[colName].len - 1 == miIndex:
                result[colName][miIndex] = colValue
            else:
                result[colName].add(colValue)
    result.indexCol = dfg.columns[0]
    result.healthCheck(raiseException=true)

proc agg*(dfg: DataFrameGroupBy, aggFn: Series -> Cell): DataFrame =
    ## groupbyしたDataFrameに対して統計量を計算する.
    ## aggFnにはDataFrameの統計量を計算する関数を指定する.
    runnableExamples:
        df.groupby(["col1","col2"]).agg(sum)
    ##

    result = initDataFrame(dfg.df)
    for colIndex, colName in result.columns.pairs():
        result[colIndex] = newSeq[Cell](dfg.multiIndex.len)
    #関数の適用
    var t = cpuTime()
    var a = 0.0
    var b = 0.0
    for i, mi in dfg.multiIndex.pairs():
        #統計値の計算
        for colIndex, colName in dfg.df.columns.pairs():
            var s =
                collect(newSeq):
                    for j in dfg.group[i]:
                        dfg.df[colIndex][j]
            result[colIndex][i] = aggFn(s)
        a += cpuTime() - t
        t = cpuTime()
        #マルチインデックス値の上書き
        for (colName, colValue) in zip(dfg.columns, mi):
            result[colName][i] = colValue
        b += cpuTime() - t
        t = cpuTime()
    result.indexCol = dfg.columns[0]
    result.healthCheck(raiseException=true)
    echo a, b

proc apply*[T](dfg: DataFrameGroupBy, applyFn: DataFrame -> Table[ColName,T]): DataFrame =
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
    for mi in dfg.multiIndex:
        #関数の計算
        var applyTable = applyFn(dfg.df[dfg.group[dfg.multiIndexTable[mi]]])
        for (colName, c) in applyTable.pairs():
            if not result.columns.contains(colName):
                result.addColumn(colName)
            result[colName].add(c)
        #マルチインデックス値の上書き
        for (colName, colValue) in zip(dfg.columns, mi):
            if not result.columns.contains(colName):
                result.addColumn(colName)
            if applyTable.contains(colName):
                result[colName][^1] = colValue
            else:
                result[colName].add(colValue)
    result.indexCol = dfg.columns[0]
    result.healthCheck(raiseException=true)

proc aggMath*(dfg: DataFrameGroupBy, aggMathFn: openArray[float] -> float): DataFrame =
    ## groupbyしたDataFrameに対して統計量を計算する.
    ## aggFnにはDataFrameの統計量を計算する関数を指定する.
    runnableExamples:
        df.groupby(["col1","col2"]).agg(sum)
    ##

    result = initDataFrame(dfg.df)
    var fSeriesSeq: seq[seq[float]] = @[]
    var validColumns: seq[int] = @[]
    for colIndex, colName in result.columns.pairs():
        result[colIndex] = newSeq[Cell](dfg.multiIndex.len)
        try:
            fSeriesSeq.add(dfg.df[colIndex].toFloat())
            validColumns.add(colIndex)
        except:
            fSeriesSeq.add(@[0.0])
    #関数の適用
    var t = cpuTime()
    var a = 0.0
    var b = 0.0
    for i, mi in dfg.multiIndex.pairs():
        #統計値の計算
        for colIndex, colName in dfg.df.columns.pairs():
            if validColumns.contains(colIndex):
                var s =
                    collect(newSeq):
                        for j in dfg.group[i]:
                            fSeriesSeq[colIndex][j]
                result[colIndex][i] = aggMathFn(s).parseString()
            else:
                result[colIndex][i] = dfEmpty
        a += cpuTime() - t
        t = cpuTime()
        #マルチインデックス値の上書き
        for (colName, colValue) in zip(dfg.columns, mi):
            result[colName][i] = colValue
        b += cpuTime() - t
        t = cpuTime()
    result.indexCol = dfg.columns[0]
    result.healthCheck(raiseException=true)
    echo a, b

proc count*(dfg: DataFrameGroupBy): DataFrame =
    proc count(s: openArray[float]): float =
        float(s.len)
    dfg.aggMath(count)
proc sum*(dfg: DataFrameGroupBy): DataFrame =
    dfg.aggMath(sum)
proc mean*(dfg: DataFrameGroupBy): DataFrame =
    dfg.aggMath(mean)
proc std*(dfg: DataFrameGroupBy): DataFrame =
    dfg.aggMath(stats.standardDeviation)
proc max*(dfg: DataFrameGroupBy): DataFrame =
    dfg.aggMath(max)
proc min*(dfg: DataFrameGroupBy): DataFrame =
    dfg.aggMath(min)
proc v*(dfg: DataFrameGroupBy): DataFrame =
    dfg.aggMath(stats.variance)

###############################################################
proc resample*(df: DataFrame, window: int, format=defaultDatetimeFormat): DataFrameResample =
    ## DataFrameを指定の行数でリサンプルする（戻り値はDataFrameResample型）.
    ##

    result.data = df
    result.window = $window
    result.format = format

proc resample*(df: DataFrame, window: string, format=defaultDatetimeFormat): DataFrameResample =
    result.data = df
    result.window = window
    result.format = format

proc genGetInterval*(datetimeId: string): int -> TimeInterval =
        case datetimeId:
        of "Y": result = proc(interval:int):TimeInterval=interval.years
        of "m": result = proc(interval:int):TimeInterval=interval.months
        of "d": result = proc(interval:int):TimeInterval=interval.days
        of "H": result = proc(interval:int):TimeInterval=interval.hours
        of "M": result = proc(interval:int):TimeInterval=interval.minutes
        of "S": result = proc(interval:int):TimeInterval=interval.seconds

proc flattenDatetime*(dt: DateTime, datetimeId: string): DateTime =
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
    let tStart = cpuTime()
    result = initDataFrame()
    #数字指定かdatetime指定か判断する
    var matches: array[2, string]
    let matchOk = match(dfre.window, re"(\d+)([YmdHMS])?", matches)
    let m0: string = matches[0]
    let m1: string = matches[1]
    if matchOk:
        let dataLen = dfre.data.len
        #数字のみ（行数指定）の場合
        if m1 == "" and m0 != "":
            #結果を格納する変数を用意しておく
            let w = m0.parseInt()
            let resampleLen = int(ceil(dataLen/w))
            when typeof(fn) is (openArray[(ColName, Series -> T)]):#agg1用
                for (colName, _) in fn:
                    result.addColumn(colName)
            else:
                for colName in dfre.data.columns:
                    result.addColumn(colName)
            #各行をwindow飛ばしで処理する
            var indexSeries: seq[Cell] = @[]
            var index = 0
            for i in countup(0, dataLen-1, w):
                var slice = i..<i+w
                if slice.b >= dataLen:
                    slice.b = dataLen-1
                
                body

                indexSeries.add(dfre.data[dfre.data.indexCol][i])
                index.inc()
            result.indexCol = dfre.data.indexCol
            result[dfre.data.indexCol] = indexSeries
        #datetime範囲指定の場合
        elif m1 != "" and m0 != "":
            try:
                let datetimeId = m1
                let w = m0.parseInt()
                #インデックスがdatetimeフォーマットに準拠している場合
                let datetimes = dfre.data[dfre.data.indexCol].toDatetime()
                echo cpuTime() - tStart
                let getInterval = genGetInterval(datetimeId)
                let startDatetime = flattenDatetime(datetimes[0], datetimeId)
                when typeof(fn) is (openArray[(ColName, Series -> T)]):#agg1用
                    for (colName, _) in fn:
                        result.addColumn(colName)
                when typeof(fn) is (Series -> T):#agg2用
                    for colName in dfre.data.columns:
                        result.addColumn(colName)
                #DateTime型に変換したindexを上から順にみていく
                var indexSeries: seq[DateTime] = @[]
                var index = 0
                var startIndex = 0
                var interval = getInterval(w)
                var nowLimit = startDatetime + interval
                for i, dt in datetimes.pairs():
                    #範囲外になった場合、集計
                    if nowLimit <= dt:
                        var slice = startIndex..<i
                        if slice.b >= dataLen:
                            slice.b = dataLen-1
                        
                        body
                        
                        indexSeries.add(nowLimit - interval)
                        startIndex = i
                        nowLimit = nowLimit + interval
                        index.inc()
                #window刻みの余り分の処理
                if startIndex < dataLen-1:
                    var slice = startIndex..<dataLen
                    
                    body

                    indexSeries.add(nowLimit - interval)
                result.indexCol = dfre.data.indexCol
                result[dfre.data.indexCol] = indexSeries.toString()
            except:
                #インデックスがdatetimeフォーマットでない場合、エラー
                if not isDatetimeSeries(dfre.data[dfre.data.indexCol]):
                    raise newException(NimDataFrameError, "index column isn't datetime format")
                else:
                    raise
        #指定フォーマットでない場合
        else:
            raise newException(NimDataFrameError, "invalid datetime format")
    #指定フォーマットにひっからなかった場合（エラー）
    else:
        raise newException(NimDataFrameError, "invalid datetime format")
    result.healthCheck(raiseException=true)

proc agg*[T](dfre: DataFrameResample, fn: openArray[(ColName, Series -> T)]): DataFrame =
    ## リサンプルされたDataFrameの各グループの指定列に対して関数fnを適用する
    ## 指定する関数に{.closure.}オプションをつけないとエラーになる.
    runnableExamples:
        proc f(s: Series): float{.closure.} =
            sum(s)*100
        df.resample("30M").agg({"sales": f})
    ##

    resampleAggTemplate:
        for (colName, f) in fn:
            #result.data[colName].add(f(seriesSeq[colIndices[k]][slice]).parseString())
            #temporarySeries[k][index] = f(seriesSeq[colIndices[k]][slice]).parseString()
            result[colName].add(f(dfre.data[colName][slice]))

proc agg*[T](dfre: DataFrameResample, fn: Series -> T): DataFrame =
    ## リサンプルされたDataFrameの各グループの全列に対して関数fnを適用する
    runnableExamples:
        df.resample("30M").agg(mean)
    ##

    resampleAggTemplate:
        for colIndex, colName in dfre.data.columns.pairs():
            #result.data[colName].add(fn(seriesSeq[colIndex][slice]).parseString())
            #temporarySeries[colIndex][index] = fn(seriesSeq[colIndex][slice]).parseString()
            result[colIndex].add(fn(dfre.data[colIndex][slice]))

proc apply*[T](dfre: DataFrameResample, fn: DataFrame -> Table[ColName,T]): DataFrame =
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
        var dfTemp = initDataFrame(dfre.data)
        if slice.b >= dataLen:
            slice.b = dataLen-1
        for colIndex, colName in dfre.data.columns.pairs():
            dfTemp[colIndex] = dfre.data[colIndex][slice]
        #applyFn適用
        var applyTable = fn(dfTemp)
        for (colName, c) in applyTable.pairs():
            if not result.columns.contains(colName):
                result.addColumn(colName)
            result[colName].add(c)

proc aggMath*(dfre: DataFrameResample, fn: openArray[float] -> float): DataFrame =
    let tStart = cpuTime()
    result = initDataFrame()
    var fSeriesSeq: seq[seq[float]] = @[]
    var validColumns: seq[int] = @[]
    for colIndex, colName in dfre.data.columns:
        try:
            fSeriesSeq.add(dfre.data[colIndex].toFloat())
            validColumns.add(colIndex)
        except:
            fSeriesSeq.add(@[0.0])
    #数字指定かdatetime指定か判断する
    var matches: array[2, string]
    let matchOk = match(dfre.window, re"(\d+)([YmdHMS])?", matches)
    let m0: string = matches[0]
    let m1: string = matches[1]
    if matchOk:
        let dataLen = dfre.data.len
        #数字のみ（行数指定）の場合
        if m1 == "" and m0 != "":
            #結果を格納する変数を用意しておく
            let w = m0.parseInt()
            for colName in dfre.data.columns:
                result.addColumn(colName)
            #各行をwindow飛ばしで処理する
            var indexSeries: seq[Cell] = @[]
            var index = 0
            for i in countup(0, dataLen-1, w):
                var slice = i..<i+w
                if slice.b >= dataLen:
                    slice.b = dataLen-1
                for colIndex, colName in dfre.data.columns.pairs():
                    if validColumns.contains(colIndex):
                        result[colIndex].add(fn(fSeriesSeq[colIndex][slice]))
                    else:
                        result[colIndex].add(dfEmpty)
                indexSeries.add(dfre.data[dfre.data.indexCol][i])
                index.inc()
            result.indexCol = dfre.data.indexCol
            result[dfre.data.indexCol] = indexSeries
        #datetime範囲指定の場合
        elif m1 != "" and m0 != "":
            try:
                let datetimeId = m1
                let w = m0.parseInt()
                #インデックスがdatetimeフォーマットに準拠している場合
                let datetimes = dfre.data[dfre.data.indexCol].toDatetime()
                echo cpuTime() - tStart
                let getInterval = genGetInterval(datetimeId)
                let startDatetime = flattenDatetime(datetimes[0], datetimeId)
                for colName in dfre.data.columns:
                    result.addColumn(colName)
                #DateTime型に変換したindexを上から順にみていく
                var indexSeries: seq[DateTime] = @[]
                var index = 0
                var startIndex = 0
                var interval = getInterval(w)
                var nowLimit = startDatetime + interval
                for i, dt in datetimes.pairs():
                    #範囲外になった場合、集計
                    if nowLimit <= dt:
                        var slice = startIndex..<i
                        if slice.b >= dataLen:
                            slice.b = dataLen-1
                        for colIndex, colName in dfre.data.columns.pairs():
                            if validColumns.contains(colIndex):
                                result[colIndex].add(fn(fSeriesSeq[colIndex][slice]))
                            else:
                                result[colIndex].add(dfEmpty)
                        indexSeries.add(nowLimit - interval)
                        startIndex = i
                        nowLimit = nowLimit + interval
                        index.inc()
                #window刻みの余り分の処理
                if startIndex < dataLen-1:
                    var slice = startIndex..<dataLen
                    for colIndex, colName in dfre.data.columns.pairs():
                        if validColumns.contains(colIndex):
                            result[colIndex].add(fn(fSeriesSeq[colIndex][slice]))
                        else:
                            result[colIndex].add(dfEmpty)
                    indexSeries.add(nowLimit - interval)
                result.indexCol = dfre.data.indexCol
                result[dfre.data.indexCol] = indexSeries.toString()
            except:
                #インデックスがdatetimeフォーマットでない場合、エラー
                if not isDatetimeSeries(dfre.data[dfre.data.indexCol]):
                    raise newException(NimDataFrameError, "index column isn't datetime format")
                else:
                    raise
        #指定フォーマットでない場合
        else:
            raise newException(NimDataFrameError, "invalid datetime format")
    #指定フォーマットにひっからなかった場合（エラー）
    else:
        raise newException(NimDataFrameError, "invalid datetime format")
    result.healthCheck(raiseException=true)

proc count*(dfre: DataFrameResample): DataFrame =
    proc count(s: openArray[float]): float =
        float(s.len)
    dfre.aggMath(count)
proc sum*(dfre: DataFrameResample): DataFrame =
    dfre.aggMath(sum)
proc mean*(dfre: DataFrameResample): DataFrame =
    dfre.aggMath(mean)
proc std*(dfre: DataFrameResample): DataFrame =
    dfre.aggMath(stats.standardDeviation)
proc max*(dfre: DataFrameResample): DataFrame =
    dfre.aggMath(max)
proc min*(dfre: DataFrameResample): DataFrame =
    dfre.aggMath(min)
proc v*(dfre: DataFrameResample): DataFrame =
    dfre.aggMath(stats.variance)


###############################################################
proc rolling*(df: DataFrame, window: int, format=defaultDatetimeFormat): DataFrameRolling =
    ## DataFrameを指定の行数でリサンプルする（戻り値はDataFrameRolling型）.
    ##

    result.data = df
    result.window = $window
    result.format = format

proc rolling*(df: DataFrame, window: string, format=defaultDatetimeFormat): DataFrameRolling =
    result.data = df
    result.window = window
    result.format = format

template rollingAggTemplate(body: untyped): untyped{.dirty.} =
    result = initDataFrame()
    #数字指定かdatetime指定か判断する
    var matches: array[2, string]
    let matchOk = match(dfro.window, re"(\d+)([YmdHMS])?", matches)
    let m0: string = matches[0]
    let m1: string = matches[1]
    if matchOk:
        let dataLen = dfro.data.len
        #数字のみ（行数指定）の場合
        if m1 == "" and m0 != "":
            let w = m0.parseInt()
            #resultの初期化
            when typeof(fn) is (openArray[(ColName, Series -> T)]):#agg1用
                for (colName, _) in fn:
                    result.addColumn(colName)
            when typeof(fn) is (Series -> T):#agg2用
                for colName in dfro.data.columns:
                    result.addColumn(colName)
            when typeof(fn) is (DataFrame -> Table[ColName,T]):#apply用
                var dfTmp = initDataFrame(dfro.data)
                var sliceTmp = 0..1
                if sliceTmp.b >= dataLen:
                    sliceTmp.b = dataLen-1
                for colName in dfro.data.columns:
                    dfTmp[colName] = dfro.data[colName][sliceTmp]
                var at = fn(dfTmp)
                for (colName, _) in at.pairs():
                    result.addColumn(colName)
            #先頭行のパッディング
            for colName in result.columns:
                for i in 0..<w-1:
                    result[colName].add(dfEmpty)
            #indexの初期化
            var index: seq[Cell] = @[]
            for i in 0..<w-1:
                index.add(dfro.data[dfro.data.indexCol][i])
            #各行をwindow毎に処理する
            for i in 0..<dataLen-w:
                var slice = i..<i+w
                if slice.b >= dataLen:
                    slice.b = dataLen-1
                    
                body

                index.add(dfro.data[dfro.data.indexCol][i])
            result.indexCol = dfro.data.indexCol
            if not result.columns.contains(dfro.data.indexCol):
                result.addColumn(dfro.data.indexCol)
            result[dfro.data.indexCol] = index
        #datetime範囲指定の場合
        elif m1 != "" and m0 != "":
            try:
                let datetimeId = m1
                let w = m0.parseInt()
                let datetimes = dfro.data[dfro.data.indexCol].toDatetime()
                let getInterval = genGetInterval(datetimeId)
                var index: seq[DateTime] = @[]
                #resultの初期化
                when typeof(fn) is (openArray[(ColName, Series -> T)]):#agg1用
                    for (colName, _) in fn:
                        result.addColumn(colName)
                when typeof(fn) is (Series -> T):#agg2用
                    for colName in dfro.data.columns:
                        result.addColumn(colName)
                when typeof(fn) is (DataFrame -> Table[ColName,T]):#apply用
                    var dfTmp = initDataFrame(dfro.data)
                    var sliceTmp = 0..1
                    if sliceTmp.b >= dataLen:
                        sliceTmp.b = dataLen-1
                    for colName in dfro.data.columns:
                        dfTmp[colName] = dfro.data[colName][sliceTmp]
                    var at = fn(dfTmp)
                    for (colName, _) in at.pairs():
                        result.addColumn(colName)
                #DateTime型に変換したindexを上から順にみていく
                let timeInterval = getInterval(w)
                var underIndex = 0
                for i, dt in datetimes.pairs():
                    #範囲内を集計
                    var slice = 0..i
                    let underLimit = dt - timeInterval
                    for j in underIndex..i:
                        if datetimes[j] <= underLimit:
                            slice.a = j + 1
                            underIndex = j
                            
                    body

                    index.add(dt)
                result.indexCol = dfro.data.indexCol
                if not result.columns.contains(dfro.data.indexCol):
                    result.addColumn(dfro.data.indexCol)
                result[dfro.data.indexCol] = index.toString()
            except:
                #インデックスがdatetimeフォーマットでない場合、エラー
                if not isDatetimeSeries(dfro.data[dfro.data.indexCol]):
                    raise newException(NimDataFrameError, "index column isn't datetime format")
                else:
                    raise
        #指定フォーマットでない場合
        else:
            raise newException(NimDataFrameError, "invalid datetime format")
    #指定フォーマットにひっからなかった場合（エラー）
    else:
        raise newException(NimDataFrameError, "invalid datetime format")
    result.healthCheck(raiseException=true)

proc agg*[T](dfro: DataFrameRolling, fn: openArray[(ColName, Series -> T)]): DataFrame =
    ## rollingされたDataFrameの各グループの指定列に対して関数fnを適用する
    ## 指定する関数に{.closure.}オプションをつけないとエラーになる.
    runnableExamples:
        proc f(s: Series): float{.closure.} =
            sum(s)*100
        df.rolling("30M").agg({"sales": f})
    ##

    rollingAggTemplate:
        for (colName, f) in fn:
            #result.data[colName].add(f(seriesSeq[colTable[colName]][slice]).parseString())
            result[colName].add(f(dfro.data[colName][slice]))

proc agg*[T](dfro: DataFrameRolling, fn: Series -> T): DataFrame =
    ## rollingされたDataFrameの各グループの全列に対して関数fnを適用する
    runnableExamples:
        df.resample("30M").agg(mean)
    ##

    rollingAggTemplate:
        for colIndex, colName in dfro.data.columns.pairs():
            #result.data[colName].add(fn(seriesSeq[colIndex][slice]).parseString())
            result[colName].add(fn(dfro.data[colIndex][slice]))

proc apply*[T](dfro: DataFrameRolling, fn: DataFrame -> Table[ColName,T]): DataFrame =
    ## rollingされたDataFrameの各グループのDataFrameに対して関数fnを適用する
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

    rollingAggTemplate:
        #applyFnに渡すDataFrame作成
        var dfTemp = initDataFrame(dfro.data)
        if slice.b >= dataLen:
            slice.b = dataLen-1
        for colName in dfro.data.columns:
            dfTemp[colName] = dfro.data[colName][slice]
        #applyFn適用
        var applyTable = fn(dfTemp)
        for (colName, c) in applyTable.pairs():
            result[colName].add(c)

proc aggMath*(dfro: DataFrameRolling, fn: openArray[float] -> float): DataFrame =
    result = initDataFrame()
    var fSeriesSeq: seq[seq[float]] = @[]
    var validColumns: seq[int] = @[]
    for colIndex, colName in dfro.data.columns:
        try:
            fSeriesSeq.add(dfro.data[colIndex].toFloat())
            validColumns.add(colIndex)
        except:
            fSeriesSeq.add(@[0.0])
    #数字指定かdatetime指定か判断する
    var matches: array[2, string]
    let matchOk = match(dfro.window, re"(\d+)([YmdHMS])?", matches)
    let m0: string = matches[0]
    let m1: string = matches[1]
    if matchOk:
        let dataLen = dfro.data.len
        #数字のみ（行数指定）の場合
        if m1 == "" and m0 != "":
            let w = m0.parseInt()
            var index: seq[Cell] = @[]
            for colName in dfro.data.columns:
                result.addColumn(colName)
                for i in 0..<w-1:
                    result[colName].add(dfEmpty)
            for i in 0..<w-1:
                index.add(dfro.data[dfro.data.indexCol][i])
            #各行をwindow毎に処理する
            for i in 0..<dataLen-w:
                var slice = i..<i+w
                if slice.b >= dataLen:
                    slice.b = dataLen-1
                for colIndex, colName in dfro.data.columns.pairs():
                    if validColumns.contains(colIndex):
                        result[colIndex].add(fn(fSeriesSeq[colIndex][slice]))
                    else:
                        result[colIndex].add(dfEmpty)
                index.add(dfro.data[dfro.data.indexCol][i])
            result.indexCol = dfro.data.indexCol
            result[dfro.data.indexCol] = index
        #datetime範囲指定の場合
        elif m1 != "" and m0 != "":
            try:
                let datetimeId = m1
                let w = m0.parseInt()
                let datetimes = dfro.data[dfro.data.indexCol].toDatetime()
                let getInterval = genGetInterval(datetimeId)
                var index: seq[DateTime] = @[]
                for colName in dfro.data.columns:
                    result.addColumn(colName)
                #DateTime型に変換したindexを上から順にみていく
                let timeInterval = getInterval(w)
                var underIndex = 0
                for i, dt in datetimes.pairs():
                    #範囲内を集計
                    var slice = 0..i
                    let underLimit = dt - timeInterval
                    for j in underIndex..i:
                        if datetimes[j] <= underLimit:
                            slice.a = j + 1
                            underIndex = j
                    for colIndex, colName in dfro.data.columns.pairs():
                        if validColumns.contains(colIndex):
                            result[colIndex].add(fn(fSeriesSeq[colIndex][slice]))
                        else:
                            result[colIndex].add(dfEmpty)
                    index.add(dt)
                result.indexCol = dfro.data.indexCol
                result[dfro.data.indexCol] = index.toString()
            except:
                #インデックスがdatetimeフォーマットでない場合、エラー
                if not isDatetimeSeries(dfro.data[dfro.data.indexCol]):
                    raise newException(NimDataFrameError, "index column isn't datetime format")
                else:
                    raise
        #指定フォーマットでない場合
        else:
            raise newException(NimDataFrameError, "invalid datetime format")
    #指定フォーマットにひっからなかった場合（エラー）
    else:
        raise newException(NimDataFrameError, "invalid datetime format")
    result.healthCheck(raiseException=true)

proc count*(dfro: DataFrameRolling): DataFrame =
    proc count(s: openArray[float]): float =
        float(s.len)
    dfro.aggMath(count)
proc sum*(dfro: DataFrameRolling): DataFrame =
    dfro.aggMath(sum)
proc mean*(dfro: DataFrameRolling): DataFrame =
    dfro.aggMath(mean)
proc std*(dfro: DataFrameRolling): DataFrame =
    dfro.aggMath(stats.standardDeviation)
proc max*(dfro: DataFrameRolling): DataFrame =
    dfro.aggMath(max)
proc min*(dfro: DataFrameRolling): DataFrame =
    dfro.aggMath(min)
proc v*(dfro: DataFrameRolling): DataFrame =
    dfro.aggMath(stats.variance)
