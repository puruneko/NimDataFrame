import unittest
import sugar
import tables
import sequtils
import strutils
import strformat
import times
import re
import complex

import StringDataFrame

proc translatorToComplex(c: Cell): Complex[float] =
    var matches: array[2, string]
    let matchOk = match(c, re"\((\d+(?:\.\d)?),(\d+(?:\.\d)?)\)", matches)
    if matchOk:
        result = complex(
            parseFloat(matches[0]),
            parseFloat(matches[1]),
        )
    else:
        result = complex(0.0,0.0)

echo "\n----------test_operation----------"

suite "fillEmpty(series)":
    
    test "(happyPath)基本機能":
        #setup
        let s = @["1","2",dfEmpty,"4",dfEmpty,"6"]
        #do
        let newS = s.fillEmpty("@")
        #check
        check newS == @["1","2","@","4","@","6"]

suite "fillEmpty(dataframe)":
    
    test "(happyPath)基本機能":
        #setup
        var df = toDataFrame(
            {
                "col1": @["1",dfEmpty],
                "col2": @[dfEmpty,"20"],
            },
            indexCol="col1",
        )
        #do
        let newDf = df.fillEmpty(99)
        #check
        check newDf == toDataFrame(
            {
                "col1": @[1,99],
                "col2": @[99,20],
            },
            indexCol="col1",
        )

suite "dropEmpty(series)":
    
    test "(happyPath)基本機能":
        #setup
        let s = @["1","2",dfEmpty,"4",dfEmpty,"6"]
        #do
        let newS = s.dropEmpty()
        #check
        check newS == @["1","2","4","6"]

suite "dropEmpty(dataframe)":
    
    test "(happyPath)基本機能":
        #setup
        var df = toDataFrame(
            {
                "col1": @["1",dfEmpty],
                "col2": @[dfEmpty,"20"],
            },
            indexCol="col1",
        )
        #do
        let newDf = df.dropEmpty()
        #check
        check newDf == toDataFrame(
            ["col1", "col2"],
            indexCol="col1",
        )

suite "renameColumns":
    
    test "(happyPath)基本機能":
        #setup
        var df = toDataFrame(
            {
                "col1": @[1],
                "col2": @[10],
            },
            indexCol="col1",
        )
        #do
        let newDf = df.renameColumns({"col2":"COL2"})
        #check
        check newDf ~ toDataFrame(
            {
                "col1": @[1],
                "COL2": @[10],
            },
            indexCol="col1",
        )
    
    test "(happyPath)index列の書き換え":
        #setup
        var df = toDataFrame(
            {
                "col1": @[1],
                "col2": @[10],
            },
            indexCol="col1",
        )
        #do
        let newDf = df.renameColumns({"col1":"COL1"})
        #check
        check newDf ~ toDataFrame(
            {
                "COL1": @[1],
                "col2": @[10],
            },
            indexCol="COL1",
        )
        
    test "(exceptionPath)存在しない列の指定":
        #setup
        var df = toDataFrame(
            {
                "col1": @[1],
                "col2": @[10],
            },
            indexCol="col1",
        )
        #do
        expect StringDataFrameError:
            discard df.renameColumns({"col3":"COL3"})

suite "resetIndex(proc)":
    
    test "(happyPath)基本機能":
        #setup
        var df = toDataFrame(
            {
                "col1": @[1,2,3,4]
            },
            indexCol="col1",
        )
        #do
        proc indexProc(i: int): DateTime =
            now() + initDuration(days=i)
        let newDf = df.resetIndex(indexProc)
        #check
        check newDf == toDataFrame(
            {
                "col1": (
                    collect(newSeq) do:
                        for i in 0..<df.len:
                            now() + initDuration(days=i)
                )
            },
            indexCol="col1"
        )

suite "ressetIndex":

    test "(happyPath)基本機能":
        #setup
        var df = toDataFrame(
            {
                "col1": @[1,2,3,4]
            },
            indexCol="col1",
        )
        #do
        let newDf = df.resetIndex()
        #check
        check newDf == toDataFrame(
            {
                "col1": @[0,1,2,3],
            },
            indexCol="col1",
        )

suite "setIndex":
    
    test "(happyPath)基本機能":
        #setup
        var df = toDataFrame(
            {
                "col1": @[1,2,3],
                "col2": @[10,20,30],
            },
            indexCol="col1",
        )
        #do
        let newDf = df.setIndex("col2")
        #check
        check newDf == toDataFrame(
            {
                "col1": @[1,2,3],
                "col2": @[10,20,30],
            },
            indexCol="col2",
        )
    
    test "(happyPath)deletePredecessor":
        #setup
        var df = toDataFrame(
            {
                "col1": @[1,2,3],
                "col2": @[10,20,30],
            },
            indexCol="col1",
        )
        #do
        let newDf = df.setIndex("col2", delete=true)
        #check
        check newDf == toDataFrame(
            {
                "col2": @[10,20,30],
            },
            indexCol="col2",
        )

    test "(exceptionPath)存在しない列の指定":
        #setup
        var df = toDataFrame(
            {
                "col1": @[1,2,3],
                "col2": @[10,20,30],
            }
        )
        #do
        expect StringDataFrameError:
            discard df.setIndex("col3")

suite "map":
    
    test "(happyPath)基本機能":
        #setup
        var df = toDataFrame(
            {
                "col1": @["(1,1)","(2,2)","(3,3)"],
            },
            indexCol="col1"
        )
        #do
        proc fnComplex(z: Complex[float]): float =
            z.abs
        df["col1"] = df["col1"].map(
            fnComplex,
            translatorToComplex,
        )
        #check
        check df == toDataFrame(
            {
                "col1": @[
                    "1.414213562373095",
                    "2.82842712474619",
                    "4.242640687119285"
                ]
            },
            indexCol="col1",
        )

suite "stringMap":
    
    test "(happyPath)基本機能":
        #setup
        var df = toDataFrame(
            {
                "col1": @["a","b","c"],
            },
            indexCol="col1",
        )
        #do
        df["col1"] = df["col1"].stringMap(x => x.toUpper())
        #check
        check df == toDataFrame(
            {
                "col1": @["A","B","C"],
            },
            indexCol="col1",
        )
    
suite "intMap":
    
    test "(happyPath)基本機能":
        #setup
        var df = toDataFrame(
            {
                "col1": @["10","20","30"],
            },
            indexCol="col1",
        )
        #do
        df["col1"] = df["col1"].intMap(x => x*2)
        #check
        check df == toDataFrame(
            {
                "col1": @["20","40","60"],
            },
            indexCol="col1",
        )
    
suite "floatMap":
    
    test "(happyPath)基本機能":
        #setup
        var df = toDataFrame(
            {
                "col1": @["1","2","3"],
            },
            indexCol="col1",
        )
        #do
        df["col1"] = df["col1"].floatMap(x => x*0.5)
        #check
        check df == toDataFrame(
            {
                "col1": @["0.5","1.0","1.5"],
            },
            indexCol="col1",
        )
    
suite "datetimeMap":
    
    test "(happyPath)基本機能":
        #setup
        var df = toDataFrame(
            {
                "col1": @[
                    "2000-01-01 00:00:00",
                    "2000-02-01 00:00:00",
                    "2000-03-01 00:00:00",
                ]
            },
            indexCol="col1",
        )
        #do
        df["col1"] = df["col1"].datetimeMap(
            date => date + initDuration(hours=1)
        )
        #check
        check df == toDataFrame(
            {
                "col1": @[
                    "2000-01-01 01:00:00",
                    "2000-02-01 01:00:00",
                    "2000-03-01 01:00:00",
                ],
            },
            indexCol="col1",
        )
        
    test "(happyPath)formatの設定":
        #setup
        var df = toDataFrame(
            {
                "col1": @[
                    "2000/01/01 00:00:00",
                    "2000/02/01 00:00:00",
                    "2000/03/01 00:00:00",
                ]
            },
            indexCol="col1",
        )
        #do
        df["col1"] = df["col1"].datetimeMap(
            date => date + initDuration(hours=1),
            "yyyy/MM/dd HH:mm:ss"
        )
        #check
        check df == toDataFrame(
            {
                "col1": @[
                    "2000-01-01 01:00:00",
                    "2000-02-01 01:00:00",
                    "2000-03-01 01:00:00",
                ],
            },
            indexCol="col1",
        )
    
suite "replace":
    
    test "(happyPath)基本機能":
        #setup
        var df = toDataFrame(
            {
                "col1": @["a","b","c"],
                "col2": @["c","b","a"],
            },
            indexCol="col1",
        )
        #do
        let newDf = df.replace("a", "A")
        #check
        check newDf == toDataFrame(
            {
                "col1": @["A","b","c"],
                "col2": @["c","b","A"],
            },
            indexCol="col1",
        )

suite "replace(regex)":
    
    test "(happyPath)基本機能":
        #setup
        var df = toDataFrame(
            {
                "col1": @["var x","let y"," const z"],
                "col2": @["const a","let b","var c"],
            },
            indexCol="col1",
        )
        #do
        let newDf = df.replace(re"var (\w+)", "var mut_$1")
        #check
        check newDf == toDataFrame(
            {
                "col1": @["var mut_x","let y"," const z"],
                "col2": @["const a","let b","var mut_c"],
            },
            indexCol="col1",
        )

suite "filter":
    
    test "(happyPath)基本機能":
        #setup
        var df = toDataFrame(
            {
                "col1": @[1,2,3,4,5],
                "col2": @[10,20,30,40,50],
            },
            indexCol="col1",
        )
        #do
        let newDf = df.filter(row => row["col1"] > 2 and 40 >= row["col2"])
        #check
        check newDf == toDataFrame(
            {
                "col1": @[3,4],
                "col2": @[30,40],
            },
            indexCol="col1",
        )

suite "sort":

    setup:
        #setup
        var df = toDataFrame(
            {
                "col1": @["6","5","4","3","2","1"],
                "col2": @["(2.0,3.0)","(1.0,1.0)","(4.0,3.0)","(2.0,3.0)","(1.0,1.0)","(4.0,3.0)"],
            },
            indexCol="col1"
        )
        proc translatorToComplex(c: Cell): Complex[float] {.closure.} =
            var matches: array[2, string]
            let matchOk = match(c, re"\((\d+(?:\.\d)?),(\d+(?:\.\d)?)\)", matches)
            if matchOk:
                result = complex(
                    parseFloat(matches[0]),
                    parseFloat(matches[1]),
                )
            else:
                result = complex(0.0,0.0)
        let asc = proc(a: Complex[float], b: Complex[float]): int {.closure.} =
            if a.abs < b.abs: -1
            elif a.abs == b.abs: 0
            else: 1
        
    test "(happyPath)基本機能":
        discard
        #[
        #do
        let newDf = df.sort()
        #check
        check newDf == toDataFrame(
            {
                "col1": @["1","2","3","4","5","6"],
                "col2": @["(4.0,3.0)","(1.0,1.0)","(2.0,3.0)","(4.0,3.0)","(1.0,1.0)","(2.0,3.0)"],
            },
            indexCol="col1"
        )
        ]#
    
    test "(happyPath)引数全指定":
        #do
        let newDf = df.sort("col2", true, translatorToComplex, asc)
        #check
        check newDf == toDataFrame(
            {
                "col1": @["5","2","6","3","4","1"],
                "col2": @["(1.0,1.0)","(1.0,1.0)","(2.0,3.0)","(2.0,3.0)","(4.0,3.0)","(4.0,3.0)"],
            },
            indexCol="col1"
        )
    
    test "(happyPath)引数全指定(dec)":
        #do
        let newDf = df.sort("col2", false, translatorToComplex, asc)
        #check
        check newDf == toDataFrame(
            {
                "col1": @["1","3","2"],
                "col2": @["(4.0,3.0)","(2.0,3.0)","(1.0,1.0)"],
            },
            indexCol="col1"
        )
    
    test "(happyPath)colName省略":
        #setup
        let ascInt = proc(x: int, y: int): int =
            if x < y: -1
            elif x == y: 0
            else: 1
        #do
        let newDf = df.sort(ascendings=false, translators=parseInt, ascFns = ascInt)
        #check
        check newDf == toDataFrame(
            {
                "col1": @["1","2","3"],
                "col2": @["(4.0,3.0)","(1.0,1.0)","(2.0,3.0)"],
            },
            indexCol="col1"
        )
    
    test "(happyPath)translator省略":
        #do
        let newDf = df.sort(colNames="col1", ascendings=false, ascFns = cmp)
        #check
        check newDf == toDataFrame(
            {
                "col1": @["1","2","3"],
                "col2": @["(4.0,3.0)","(1.0,1.0)","(2.0,3.0)"],
            },
            indexCol="col1"
        )

    test "(happyPath)ascending省略":
        discard
    test "(happyPath)ascFn省略":
        discard
    test "(happyPath)複数指定:引数全指定":
        discard
    test "(happyPath)複数指定:引数全指定(dec)":
        discard
    test "(happyPath)複数指定:colName省略":
        discard
    test "(happyPath)複数指定:translator省略":
        discard
    test "(happyPath)複数指定:ascending省略":
        discard
    test "(happyPath)複数指定:ascFn省略":
        discard
    test "(exceptionPath)引数の長さが異なる":
        discard

suite "intSort":
    discard
suite "floatSort":
    discard
suite "datetimeSort":
    discard
suite "duplicated":
    
    test "(happyPath)基本機能":
        #setup
        var df = toDataFrame(
            {
                "col1": @["1","1","2","2"],
                "col2": @["10","20","20","20"],
            },
            indexCol="col1",
        )
        #do
        let d = df.duplicated(["col2"])
        #check
        check d == @[false, false, true, true]

    test "(happyPath)カラム指定無し":
        #setup
        var df = toDataFrame(
            {
                "col1": @["1","1","2","2"],
                "col2": @["10","20","20","20"],
            },
            indexCol="col1",
        )
        #do
        let d = df.duplicated()
        #check
        check d == @[false, true, false, true]

suite "dropDuplicates":
    
    test "(happyPath)基本機能":
        #setup
        var df = toDataFrame(
            {
                "col1": @["1","1","2","2"],
                "col2": @["10","20","20","20"],
            },
            indexCol="col1",
        )
        #do
        let newDf = df.dropDuplicates(["col2"])
        #check
        check newDf == toDataFrame(
            {
                "col1": @["1","1"],
                "col2": @["10","20"],
            },
            indexCol="col1",
        )

suite "transpose":
    
    test "(happyPath)基本機能":
        #setup
        var df = toDataFrame(
            {
                "col1": @["1","2","3"],
                "col2": @["10","20","30"],
            },
            indexCol="col1",
        )
        #do
        let newDf = df.transpose()
        #check
        check newDf == toDataFrame(
            {
                "1": @["10"],
                "2": @["20"],
                "3": @["30"],
                "col1": @["col2"],
            },
            indexCol="col1"
        )