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
        result.data[colName] = initSeries()
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
        if toHashSet(left.getColumns())*toHashSet(leftOn) == toHashSet(leftOn) and
            toHashSet(right.getColumns())*toHashSet(rightOn) == toHashSet(rightOn):
            #resultの初期化・重複列の処理
            var colNames = (toHashSet(left.getColumns()) + toHashSet(right.getColumns())).toSeq()
            let on =
                if leftOn == rightOn:
                    leftOn.toSeq()
                else:
                    @[]
            let dupCols = (toHashSet(left.getColumns()) * toHashSet(right.getColumns())) - toHashSet(on)
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
            let columnsL = toHashSet(left.getColumns())
            let columnsR = (toHashSet(right.getColumns()) - columnsL) + dupCols
            for colName in colNames:
                result.data[colName] = initSeries()
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
                                for colName in columnsL:
                                    result.data[columnsTableL[colName]].add(left[colName][indexL])
                                for colName in columnsR:
                                    result.data[columnsTableR[colName]].add(right[colName][indexR])
                        else:
                            onColumn.add(left[leftOn[0]][indexL])
                            for colName in columnsL:
                                result.data[columnsTableL[colName]].add(left[colName][indexL])
                            for colName in columnsR:
                                result.data[columnsTableR[colName]].add(dfEmpty)
                else:
                    let indicesR = rightOnSeries.indicesOf(c)
                    if indicesR.len != 0:
                        for indexR in indicesR:
                            onColumn.add(right[rightOn[0]][indexR])
                            for colName in columnsR + toHashSet(on):
                                result.data[columnsTableR[colName]].add(right[colName][indexR])
                            for colName in columnsL - toHashSet(on):
                                result.data[columnsTableL[colName]].add(dfEmpty)
                    else:
                        raise newException(NimDataFrameError, "unknown error")
            #インデックスの設定
            result.indexCol = mergeIndexName
            result.data[mergeIndexName] = onColumn
        else:
            var msg = ""
            if toHashSet(left.getColumns())*toHashSet(leftOn) == toHashSet(leftOn):
                msg &= fmt"left column '{leftOn}' not found. "
            else:
                msg &= fmt"right column '{leftOn}' not found. "
            raise newException(NimDataFrameError, msg)
    elif how == "right":
        result = merge(right, left, rightOn, leftOn, "left")
    else:
        raise newException(NimDataFrameError, fmt"invalid method '{how}'")

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
            for dup in toHashSet(dfs[i].getColumns()) * toHashSet(dfs[j].getColumns()):
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

proc join*(dfSource: DataFrame, df: DataFrame, how="left"): DataFrame =
    join(dfSource, [df], how)

###############################################################
proc groupby*(df: DataFrame, colNames: openArray[ColName]): DataFrameGroupBy =
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

proc agg*[T](dfg: DataFrameGroupBy, aggFn: openArray[(string,Series -> T)]): DataFrame =
    ## groupbyしたDataFrameの指定列に対して関数を実行する.
    ## 指定する関数に{.closure.}オプションをつけないとエラーになる.
    runnableExamples:
        proc f(series: Series): float{.closure.} =
            series.toFloat().mean()/100
        df.groupby(["col1","col2"]).agg({"col3",f})
    ##

    result = initDataFrame()
    #関数の適用
    var rows: seq[Row] = @[]
    for mi in dfg.data.keys:
        #関数の計算
        var row = initRow()
        for (colName, fn) in aggFn:
            row[colName] = fn(dfg.data[mi][colName]).parseString()
        #マルチインデックス値の上書き
        for (colName, colValue) in zip(dfg.columns, mi):
            row[colName] = colValue
        rows.add(row)
    #結合
    for colName in rows[0].keys:
        result[colName] = initSeries()
    for row in rows:
        result.addRow(row)
    result.indexCol = dfg.columns[0]

proc agg*(dfg: DataFrameGroupBy, aggFn: DataFrame -> Row): DataFrame =
    ## groupbyしたDataFrameに対して統計量を計算する.
    ## aggFnにはDataFrameの統計量を計算する関数を指定する.
    runnableExamples:
        df.groupby(["col1","col2"]).agg(sum)
    ##

    result = initDataFrame()
    #関数の適用
    var rows: seq[Row] = @[]
    for mi in dfg.data.keys:
        #統計値の計算
        var row = aggFn(dfg.data[mi])
        #マルチインデックス値の上書き
        for (colName, colValue) in zip(dfg.columns, mi):
            row[colName] = colValue
        rows.add(row)
    #結合
    for colName in rows[0].keys:
        result.data[colName] = initSeries()
    for row in rows:
        result.addRow(row)
    result.indexCol = dfg.columns[0]

proc count*(dfg: DataFrameGroupBy): DataFrame =
    dfg.agg(count)
proc sum*(dfg: DataFrameGroupBy): DataFrame =
    dfg.agg(sum)
proc mean*(dfg: DataFrameGroupBy): DataFrame =
    dfg.agg(mean)
proc std*(dfg: DataFrameGroupBy): DataFrame =
    dfg.agg(std)
proc max*(dfg: DataFrameGroupBy): DataFrame =
    dfg.agg(max)
proc min*(dfg: DataFrameGroupBy): DataFrame =
    dfg.agg(min)
proc v*(dfg: DataFrameGroupBy): DataFrame =
    dfg.agg(v)

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
proc resample*(df: DataFrame, window: int, format=defaultDateTimeFormat): DataFrameResample =
    ## DataFrameを指定の行数でリサンプルする（戻り値はDataFrameResample型）.
    ##

    result.data = df
    result.window = $window
    result.format = format

proc resample*(df: DataFrame, window: string, format=defaultDateTimeFormat): DataFrameResample =
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
    result = initDataFrame()
    #数字指定かdatetime指定か判断する
    var matches: array[2, string]
    let matchOk = match(dfre.window, re"(\d+)([a-zA-Z]+)?", matches)
    let m0: string = matches[0]
    let m1: string = matches[1]
    if matchOk:
        #数字のみ（行数指定）の場合
        if m1 == "" and m0 != "":
            let w = m0.parseInt()
            #各行をwindow飛ばしで処理する
            when typeof(fn) is (openArray[(ColName, Series -> T)]):#agg1用
                for (colName, _) in fn:
                    result.data[colName] = initSeries()
            else:
                for colName in dfre.data.columns:
                    result.data[colName] = initSeries()
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
        elif m1 != "" and m0 != "":
            let datetimeId = m1
            let w = m0.parseInt()
            #datetimeIdが不正な場合、エラー
            if not ["Y","m","d","H","M","S"].contains(datetimeId):
                raise newException(NimDataFrameError, fmt"invalid datetime ID '{datetimeId}'")
            #インデックスがdatetimeフォーマットでない場合、エラー
            if not isDatetimeSeries(dfre.data[dfre.data.indexCol]):
                raise newException(NimDataFrameError, "index column isn't datetime format")
            #インデックスがdatetimeフォーマットに準拠している場合
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
                    result.data[colName] = initSeries()
            else:
                for colName in dfre.data.columns:
                    result.data[colName] = initSeries()
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
            result.data[dfre.data.indexCol] = index.toString()
        #指定フォーマットでない場合
        else:
            raise newException(NimDataFrameError, "invalid datetime format")
    #指定フォーマットにひっからなかった場合（エラー）
    else:
        raise newException(NimDataFrameError, "invalid datetime format")

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
            result.data[colName].add(f(dfre.data[colName][slice]).parseString())

proc agg*[T](dfre: DataFrameResample, fn: Series -> T): DataFrame =
    ## リサンプルされたDataFrameの各グループの全列に対して関数fnを適用する
    runnableExamples:
        df.resample("30M").agg(mean)
    ##

    resampleAggTemplate:
        for colName in result.columns:
            result.data[colName].add(fn(dfre.data[colName][slice]).parseString())

proc count*(dfre: DataFrameResample): DataFrame =
    dfre.agg(count)
proc sum*(dfre: DataFrameResample): DataFrame =
    dfre.agg(sum)
proc mean*(dfre: DataFrameResample): DataFrame =
    dfre.agg(mean)
proc std*(dfre: DataFrameResample): DataFrame =
    dfre.agg(std)
proc max*(dfre: DataFrameResample): DataFrame =
    dfre.agg(max)
proc min*(dfre: DataFrameResample): DataFrame =
    dfre.agg(min)
proc v*(dfre: DataFrameResample): DataFrame =
    dfre.agg(v)

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
proc rolling*(df: DataFrame, window: int, format=defaultDateTimeFormat): DataFrameRolling =
    ## DataFrameを指定の行数でリサンプルする（戻り値はDataFrameRolling型）.
    ##

    result.data = df
    result.window = $window
    result.format = format

proc rolling*(df: DataFrame, window: string, format=defaultDateTimeFormat): DataFrameRolling =
    result.data = df
    result.window = window
    result.format = format

template rollingAggTemplate(body: untyped): untyped{.dirty.} =
    result = initDataFrame()
    #数字指定かdatetime指定か判断する
    var matches: array[2, string]
    let matchOk = match(dfro.window, re"(\d+)([a-zA-Z]+)?", matches)
    let m0: string = matches[0]
    let m1: string = matches[1]
    if matchOk:
        #数字のみ（行数指定）の場合
        if m1 == "" and m0 != "":
            let w = m0.parseInt()
            #各行をwindowブロック毎に処理する
            when typeof(fn) is (openArray[(ColName, Series -> T)]):#agg1用
                for (colName, _) in fn:
                    result.data[colName] = initSeries()
            else:
                for colName in dfro.data.columns:
                    result.data[colName] = initSeries()
            var index: seq[Cell] = @[]
            when typeof(fn) is (DataFrame -> Table[ColName,T]):#apply用
                var dfs: seq[DataFrame] = @[]
            for colName in result.columns:
                for i in 0..<w-1:
                    result.data[colName].add(dfEmpty)
            for i in 0..<w-1:
                index.add(dfro.data[dfro.data.indexCol][i])
            for i in 0..<dfro.data.len-w:
                var slice = i..<i+w
                if slice.b >= dfro.data.len:
                    slice.b = dfro.data.len-1
                    
                body

                index.add(dfro.data[dfro.data.indexCol][i])
            result.indexCol = dfro.data.indexCol
            result.data[dfro.data.indexCol] = index
        #datetime範囲指定の場合
        elif m1 != "" and m0 != "":
            let datetimeId = m1
            let w = m0.parseInt()
            #datetimeIdが不正な場合、エラー
            if not ["Y","m","d","H","M","S"].contains(datetimeId):
                raise newException(NimDataFrameError, fmt"invalid datetime ID '{datetimeId}'")
            #インデックスがdatetimeフォーマットでない場合、エラー
            if not isDatetimeSeries(dfro.data[dfro.data.indexCol]):
                raise newException(NimDataFrameError, "index column isn't datetime format")
            let datetimes = dfro.data[dfro.data.indexCol].toDatetime()
            let getInterval = genGetInterval(datetimeId)
            var index: seq[DateTime] = @[]
            when typeof(fn) is (DataFrame -> Table[ColName,T]):#apply用
                var dfs: seq[DataFrame] = @[]
            #DateTime型に変換したindexを上から順にみていく
            when typeof(fn) is (openArray[(ColName, Series -> T)]):#agg1用
                for (colName, _) in fn:
                    result.data[colName] = initSeries()
            else:
                for colName in dfro.data.columns:
                    result.data[colName] = initSeries()
            let timeInterval = getInterval(w)
            for i, dt in datetimes.pairs():
                #範囲内を集計
                var slice = 0..i
                for j, dt2 in datetimes.pairs():
                    if dt2 <= dt - timeInterval:
                        slice.a = j + 1
                    if j > i:
                        break
                echo slice
                        
                body

                index.add(dt)
            when typeof(fn) is (DataFrame -> Table[ColName,T]):#apply用
                result = concat(dfs = dfs)
                result.indexCol = dfro.data.indexCol
            result.indexCol = dfro.data.indexCol
            result.data[dfro.data.indexCol] = index.toString()
        #指定フォーマットでない場合
        else:
            raise newException(NimDataFrameError, "invalid datetime format")
    #指定フォーマットにひっからなかった場合（エラー）
    else:
        raise newException(NimDataFrameError, "invalid datetime format")

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
            result.data[colName].add(f(dfro.data[colName][slice]).parseString())

proc agg*[T](dfro: DataFrameRolling, fn: Series -> T): DataFrame =
    ## rollingされたDataFrameの各グループの全列に対して関数fnを適用する
    runnableExamples:
        df.resample("30M").agg(mean)
    ##

    rollingAggTemplate:
        for colName in result.columns:
            result.data[colName].add(fn(dfro.data[colName][slice]).parseString())

proc count*(dfro: DataFrameRolling): DataFrame =
    dfro.agg(count)
proc sum*(dfro: DataFrameRolling): DataFrame =
    dfro.agg(sum)
proc mean*(dfro: DataFrameRolling): DataFrame =
    dfro.agg(mean)
proc max*(dfro: DataFrameRolling): DataFrame =
    dfro.agg(max)
proc min*(dfro: DataFrameRolling): DataFrame =
    dfro.agg(min)
proc v*(dfro: DataFrameRolling): DataFrame =
    dfro.agg(v)

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
        var df1 = initDataFrame(dfro.data)
        if slice.b >= dfro.data.len:
            slice.b = dfro.data.len-1
        for colName in result.columns:
            df1[colName] = dfro.data[colName][slice]
        #applyFn適用
        var applyTable = fn(df1)
        var df2 = initDataFrame()
        for (colName, c) in applyTable.pairs():
            df2[colName] = @[c.parseString()]
        dfs.add(df2)
