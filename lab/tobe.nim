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

import ../src/nimdataframe

proc toBe*() =
    const filename = "./test/sample.csv"
    var fp: File
    let openOk = fp.open(filename, fmRead)
    defer: fp.close()
    if not openOk:
        quit(fmt"{filename} open failed.")
    let csv = fp.readAll()
    #
    echo "df (1)################################"
    var df = toDataFrame(
        text=csv,
        headers=["time","name","sales","日本語","dummy"],
        headerRows=1,
    )
    df.show(true)
    #df.toCsv("test.csv")
    #
    echo "df (2)################################"
    df = toDataFrame(
        text=csv,
        headerLineNumber=1,
    )
    df.show(true)
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
    echo df.mean()
    echo df.max()
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
    echo df.getColumns()
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
    df.groupby(["time","name"]).agg({"sales": aggFnG}).show(true)
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
    echo "merge inner(2)################################"
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
        colNames = ["a","a'","c"]
    )
    merge(df_ab, df_ac3, left_on=["a"], right_on=["a'"], how="inner").sort(["a'","b"]).show(true)
    #
    echo "merge left(1)################################"
    merge(df_ab, df_ac, left_on=["a"], right_on=["a"], how="left").sort(["a","b"]).show(true)
    #
    echo "merge left(2)################################"
    merge(df_ac3, df_ab, left_on=["a'"], right_on=["a"], how="left").sort(["a'","b"]).show(true)
    #
    echo "merge left(3)################################"
    merge(df_ab, df_ac3, left_on=["a"], right_on=["a'"], how="left").sort(["a'","b"]).show(true)
    #
    echo "merge right(1)################################"
    merge(df_ab, df_ac, left_on=["a"], right_on=["a"], how="right").sort(["a","b"]).show(true)
    #
    echo "merge right(2)################################"
    merge(df_ac3, df_ab, left_on=["a'"], right_on=["a"], how="right").sort(["a'","b"]).show(true)
    #
    echo "merge outer(1)################################"
    merge(df_ab, df_ac, left_on=["a"], right_on=["a"], how="outer").sort(["a","b"]).show(true)
    #
    echo "merge outer(2)################################"
    merge(df_ac3, df_ab, left_on=["a'"], right_on=["a"], how="outer").sort(["a'","b"]).show(true)
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
    join(df_j1, [df_j2, df_j3]).sort().show(true)
    #
    echo "join inner(1)################################"
    join(df_j1, [df_j2, df_j3], how="inner").sort().show(true)
    #
    echo "join outer(1)################################"
    join(df_j1, [df_j2, df_j3], how="outer").sort().show(true)
    #
    echo "join right(1)################################"
    join(df_j1, [df_j2, df_j3], how="right").sort().show(true)
    #
    echo "join left(2)################################"
    join(df_j1, [df_j2, df_j4], how="left").sort().show(true)
    #
    echo "join inner(2)################################"
    join(df_j1, [df_j2, df_j4], how="inner").sort().show(true)
    #
    echo "join outer(2)################################"
    join(df_j1, [df_j2, df_j4], how="outer").sort().show(true)
    #
    echo "join right(1)################################"
    join(df_j1, [df_j2, df_j4], how="right").sort().show(true)

    #
    echo "healthCheck################################"
    var df_h = initDataFrame()
    df_h["a"] = @[1]
    df_h["b"] = @[1,2,3,4,5]
    df_h.indexCol = "a"
    echo healthCheck(df_h)
    df_h.indexCol = "b"
    echo healthCheck(df_h)
    df_h["b"] = @[2]
    echo healthCheck(df_h)
    df_h.indexCol = "c"
    echo healthCheck(df_h)
    #
    echo "rolling agg(1)################################"
    echo df.setIndex("time").rolling(5).count()
    df.setIndex("time").rolling(5).sum().show(true)
    #
    echo "rolling agg(2)################################"
    df.setIndex("time").rolling("1H").count().show(true)
    df.setIndex("time").rolling("1H").sum().show(true)
    #
    echo "resaple 30M apply################################"
    df.setIndex("time").rolling("30M").apply(applyFnG).show(true)
    #
    echo "transpose################################"
    df_j1.show(true)
    df_j1.transpose().show(true)
    #
    echo "replace(1)################################"
    df.replace("abc", "ABC").show(true)
    #
    echo "replace(2)################################"
    df.replace(re"(\d\d)", "@$1@").show(true)
    #
    echo "addRow################################"
    var df2 = toDataFrame(
        columns = {
            "a": @[1,2],
            "b": @[3,4],
            "c": @[10,20],
            "d": @[100,200]
        },
        indexCol = "a",
    )
    df2.addRow({"a": 3, "b": 5, "c": 30, "d": 300})
    df2.show(true)
    df2.addRow({"a": 4}, fillEmpty=true)
    df2.show(true)
    df2.addRow({"b": 7, "c": 50, "d": 500}, autoIndex=true)
    df2.show(true)
    df2.addRow({"c": 60}, autoIndex=true, fillEmpty=true)
    df2.show(true)
    #
    echo "addRows################################"
    df2.addRows(
        items = {
            "b": @[9,10],
            "c": @[70]
        },
        autoIndex=true,
        fillEmptyRow=true,
        fillEmptyCol=true,
    )
    df2.show(true)
    #
    echo "addColumns(1)################################"
    df2.addColumns(
        columns = {
            "b": @[0,1,2,3,4,5,6,7],
            "e": @[0,1,2,3,4,5,6,7],
        }
    )
    df2.show(true)
    #
    echo "addColumns(2)################################"
    df2.addColumns(
        columns = {
            "b": @[0,0,0,0,0,0,0],
            "f": @[0,0,0,0,0,0,0],
        },
        fillEmpty = true
    )
    df2.show(true)
    #
    echo "size################################"
    echo df2.size()
    echo df2.size(true)
    #[
    ]#

if isMainModule:
    toBe()