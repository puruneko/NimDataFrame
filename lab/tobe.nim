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

template timeAttack(name: static[string], body: untyped): untyped =
    echo name & "####################"
    let tStart = cpuTime()

    body

    echo "t: " & $(cpuTime() - tStart) & "[s]"


proc toBe*() =
    proc echo3[T](a: varargs[T]) =
        discard
    proc show(a: DataFrame, b: bool) =
        discard
    const filename = "./test/sample.csv"
    var fp: File
    let openOk = fp.open(filename, fmRead)
    defer: fp.close()
    if not openOk:
        quit(fmt"{filename} open failed.")
    let csv = fp.readAll()
    #
    timeAttack("df (1)################################"):
        var df = toDataFrame(
            text=csv,
            headers=["time","name","sales","日本語","dummy"],
            headerRows=1,
        )
        df.show(true)
    #
    timeAttack("df (2)################################"):
        df = toDataFrame(
            text=csv,
            headerLineNumber=1,
        )
        df.show(true)
    #
    timeAttack("df huge"):
        const filename_huge = "./test/sample_data.csv"
        var fp_huge: File
        let openOk_huge = fp.open(filename_huge, fmRead)
        defer: fp_huge.close()
        if not openOk_huge:
            quit(fmt"{filename_huge} open failed.")
        let csv_huge = fp.readAll()
        var df_huge = toDataFrame(
            text=csv_huge,
            headerLineNumber=1,
        )
        echo3 ""
        echo3 df_huge.toCsv()

        df = df_huge
    #
    timeAttack("dropEmpty"):
        df.dropEmpty().show(true)
    #
    timeAttack("fillEmpty"):
        df["sales"] = df["sales"].fillEmpty(0)
        df.show(true)
    #
    timeAttack("df1"):
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
        echo3 df1
    #
    timeAttack("drop"):
        df.dropColumns(["time","name"]).show(true)
    #
    timeAttack("rename"):
        df.renameColumns({"time":"TIME","name":"NAME","sales":"SALES"}).show(true)
    #
    timeAttack("stats"):
        echo3 df.mean()
        echo3 df.max()
    #
    timeAttack("map"):
        echo3 df["sales"].intMap(c => c*2)
        echo3 df["time"].datetimeMap(c => c+initDuration(hours=1))
        let triple = proc(c: int): int =
            c * 3
        echo3 df["sales"].map(triple, parseInt)
    #
    timeAttack("filter"):
        df.filter(row => row["sales"] >= 2000).show(true)
        df.filter(row => row["sales"] > 1000 and 3000 > row["sales"]).show(true)
    #
    timeAttack("loc,iloc"):
        echo3  df1.loc("1")
        echo3 df.iloc(0)
    #
    timeAttack("getRows"):
        echo3 df.getRows()
        echo3 df.getColumns()
    #
    timeAttack("sort name"):
        df.sort("name", ascending=false).show(true)
    timeAttack("sort sales"):
        df.sort("sales", parseInt, ascending=true).show(true)
    timeAttack("sort sales (dec)"):
        df.sort("sales", parseInt, ascending=false).show(true)
    timeAttack("sort (datetime)"):
        df.datetimeSort("time", ascending=false).show(true)
    #
    timeAttack("resetIndex"):
        df.intSort("sales").resetIndex().show(true)
    #
    timeAttack("index,shape"):
        echo3 df.index
        echo3 df.shape
    #
    timeAttack("[]"):
        df[["time","sales"]].show(true)
        df[0..4].show(true)
        df[[2,4,6]].show(true)
    #
    timeAttack("head,tail"):
        df.head(5).show(true)
        df.tail(5).show(true)
        df.head(999999999).show(true)
        df.tail(999999999).show(true)
    #
    timeAttack("duplicated"):
        echo3 df.duplicated(["sales"])
    timeAttack("dropDuplicates sales"):
        df.dropDuplicates(["sales"]).show(true)
    timeAttack("dropDuplicates all"):
        df.dropDuplicates().show(true)
    timeAttack("dropDuplicates [time, sales]"):
        df.dropDuplicates(["time","sales"]).show(true)
    #
    timeAttack("groupby"):
        echo3 df.groupby(["time","name"])
    #
    timeAttack("groupby mean"):
        df.groupby(["time","name"]).mean().show(true)
    timeAttack("groupby max"):
        df.groupby(["time","name"]).max().show(true)
    #
    proc aggFnG(s: Series): float {.closure.} =
        result = s.toFloat().mean()/100
    timeAttack("groupby agg"):
        df.groupby(["time","name"]).agg({"sales": aggFnG}).show(true)
    #
    proc applyFnG(df: DataFrame): Table[ColName,Cell] =
        var c: Cell
        if df["name"][0] == "abc":
            c = df["sales"].intMap(c => c/10).mean()
        else:
            c = df["sales"].intMap(c => c*10).mean()
        result = {
            "sales_changed": c
        }.toTable()
    timeAttack("groupby apply"):
        df.groupby(["time","name"]).apply(applyFnG).show(true)
    #
    timeAttack("resaple 5 mean"):
        df.resample(5).sum().show(true)
    #
    timeAttack("resaple 1H agg1"):
        df.setIndex("time").resample("1H").mean().show(true)
    #
    timeAttack("resaple 30M agg1"):
        df.setIndex("time").resample("30M").mean().show(true)
    #
    proc aggFnRe(s: Series): float{.closure.} =
        sum(s)*100
    timeAttack("resaple 30M agg2"):
        df.setIndex("time").resample("30M").agg({"sales":aggFnRe}).show(true)
    #
    timeAttack("resaple 30M apply"):
        df.setIndex("time").resample("30M").apply(applyFnG).show(true)
    #
    var df_ab = toDataFrame(
        columns = {
            "a": @["A_1", "A_1", "A_2", "A_3"],
            "b": @["B_1", "B_2", "B_2", "B_3"],
        }
    )
    echo3 "df_ab"
    df_ab.show(true)
    var df_ac = toDataFrame(
        columns = {
            "a": @["A_1", "A_1", "A_1", "A_2", "A_4"],
            "c": @["C_10", "C_20", "C_30", "C_2", "C_4"]
        }
    )
    echo3 "df_ac"
    df_ac.show(true)
    var df_ac2 = toDataFrame(
        columns = {
            "a": @["A_1", "A_1", "A_1", "A_2", "A_4"],
            "b": @["B_10", "B_20", "B_30", "B_2", "B_4"],
            "c": @["C_10", "C_20", "C_30", "C_2", "C_4"]
        }
    )
    echo3 "df_ac2"
    df_ac2.show(true)
    timeAttack("merge inner(1)"):
        merge(df_ab, df_ac, left_on=["a"], right_on=["a"], how="inner").sort(["a","b"]).show(true)
    #
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
    timeAttack("merge inner(2)"):
        merge(df_ab, df_ac3, left_on=["a"], right_on=["a'"], how="inner").sort(["a'","b"]).show(true)
    #
    timeAttack("merge left(1)"):
        merge(df_ab, df_ac, left_on=["a"], right_on=["a"], how="left").sort(["a","b"]).show(true)
    #
    timeAttack("merge left(2)"):
        merge(df_ac3, df_ab, left_on=["a'"], right_on=["a"], how="left").sort(["a'","b"]).show(true)
    #
    timeAttack("merge left(3)"):
        merge(df_ab, df_ac3, left_on=["a"], right_on=["a'"], how="left").sort(["a'","b"]).show(true)
    #
    timeAttack("merge right(1)"):
        merge(df_ab, df_ac, left_on=["a"], right_on=["a"], how="right").sort(["a","b"]).show(true)
    #
    timeAttack("merge right(2)"):
        merge(df_ac3, df_ab, left_on=["a'"], right_on=["a"], how="right").sort(["a'","b"]).show(true)
    #
    timeAttack("merge outer(1)"):
        merge(df_ab, df_ac, left_on=["a"], right_on=["a"], how="outer").sort(["a","b"]).show(true)
    #
    timeAttack("merge outer(2)"):
        merge(df_ac3, df_ab, left_on=["a'"], right_on=["a"], how="outer").sort(["a'","b"]).show(true)
    #
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
    timeAttack("join left(1)"):
        join(df_j1, [df_j2, df_j3]).sort().show(true)
    #
    timeAttack("join inner(1)"):
        join(df_j1, [df_j2, df_j3], how="inner").sort().show(true)
    #
    timeAttack("join outer(1)"):
        join(df_j1, [df_j2, df_j3], how="outer").sort().show(true)
    #
    timeAttack("join right(1)"):
        join(df_j1, [df_j2, df_j3], how="right").sort().show(true)
    #
    timeAttack("join left(2)"):
        join(df_j1, [df_j2, df_j4], how="left").sort().show(true)
    #
    timeAttack("join inner(2)"):
        join(df_j1, [df_j2, df_j4], how="inner").sort().show(true)
    #
    timeAttack("join outer(2)"):
        join(df_j1, [df_j2, df_j4], how="outer").sort().show(true)
    #
    timeAttack("join right(1)"):
        join(df_j1, [df_j2, df_j4], how="right").sort().show(true)

    #
    timeAttack("healthCheck"):
        var df_h = initDataFrame()
        df_h["a"] = @[1]
        df_h["b"] = @[1,2,3,4,5]
        df_h.indexCol = "a"
        echo3 healthCheck(df_h)
        df_h.indexCol = "b"
        echo3 healthCheck(df_h)
        df_h["b"] = @[2]
        echo3 healthCheck(df_h)
        df_h.indexCol = "c"
        echo3 healthCheck(df_h)
    #
    timeAttack("rolling 5 count"):
        echo3 df.setIndex("time").rolling(5).count()
    timeAttack("rolling 5 sum"):
        df.setIndex("time").rolling(5).sum().show(true)
    timeAttack("rolling 5 apply"):
        df.setIndex("time").rolling(5).apply(applyFnG).show(true)
    #
    timeAttack("rolling 1H count"):
        df.setIndex("time").rolling("1H").count().show(true)
    timeAttack("rolling 1H sum"):
        df.setIndex("time").rolling("1H").sum().show(true)
    #
    timeAttack("rolling 30M apply"):
        df.setIndex("time").rolling("30M").apply(applyFnG).show(true)
    #
    timeAttack("transpose"):
        df_j1.show(true)
        df_j1.transpose().show(true)
    #
    timeAttack("replace(1)"):
        df.replace("abc", "ABC").show(true)
    #
    timeAttack("replace(2)"):
        df.replace(re"(\d\d)", "@$1@").show(true)
    #
    var df2 = toDataFrame(
        columns = {
            "a": @[1,2],
            "b": @[3,4],
            "c": @[10,20],
            "d": @[100,200]
        },
        indexCol = "a",
    )
    timeAttack("addRow"):
        df2.addRow({"a": 3, "b": 5, "c": 30, "d": 300})
        df2.show(true)
        df2.addRow({"a": 4}, fillEmpty=true)
        df2.show(true)
        df2.addRow({"b": 7, "c": 50, "d": 500}, autoIndex=true)
        df2.show(true)
        df2.addRow({"c": 60}, autoIndex=true, fillEmpty=true)
        df2.show(true)
    #
    timeAttack("addRows"):
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
    timeAttack("addColumns(1)"):
        df2.addColumns(
            columns = {
                "b": @[0,1,2,3,4,5,6,7],
                "e": @[0,1,2,3,4,5,6,7],
            }
        )
        df2.show(true)
    #
    timeAttack("addColumns(2)"):
        df2.addColumns(
            columns = {
                "b": @[0,0,0,0,0,0,0],
                "f": @[0,0,0,0,0,0,0],
            },
            fillEmpty = true
        )
        df2.show(true)
    #
    timeAttack("size"):
        echo3 df2.size()
        echo3 df2.size(true)
    #[
    ]#

if isMainModule:
    toBe()