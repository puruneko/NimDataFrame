import unittest
import tables
import sequtils
import strutils
import strformat
import times

import StringDataFrame

echo "\n----------test_core----------"

suite "df~df":

    test "(happyPath)基本機能":
        #setup
        let a = toDataFrame(
            {
                "col1": @[1,2,3,4],
                "col2": @[10,20,30,40],
                "col3": @[100,200,300,400],
            },
            indexCol="col1",
        )
        let b = toDataFrame(
            {
                "col1": @[1,2,3,4],
                "col2": @[10,20,30,40],
                "col3": @[100,200,300,400],
            },
            indexCol="col1",
        )
        #check
        check a ~ b

    test "(happyPath)列の順番が異なる":
        #setup
        let a = toDataFrame(
            {
                "col1": @[1,2,3,4],
                "col2": @[10,20,30,40],
                "col3": @[100,200,300,400],
            },
            indexCol="col1",
        )
        let b = toDataFrame(
            {
                "col2": @[10,20,30,40],
                "col3": @[100,200,300,400],
                "col1": @[1,2,3,4],
            },
            indexCol="col1",
        )
        #check
        check a ~ b

suite "df==df, df!=df, df===df, df!==df":

    test "(happyPath)df==df:基本機能":
        #setup
        let a = toDataFrame(
            {
                "col1": @[1],
                "col2": @[10],
            }
        )
        let b = toDataFrame(
            {
                "col1": @[1],
                "col2": @[10],
            },
        )
        #check
        check a == b
    
    test "(happyPath)df==df:datetimeFormatが異なる":
        #setup
        let a = toDataFrame(
            {
                "col1": @[1],
                "col2": @[10],
            },
            indexCol="col1",
            datetimeFormat="yyyy/mm/dd HH:MM:SS",
        )
        let b = toDataFrame(
            {
                "col1": @[1],
                "col2": @[10],
            },
            indexCol="col1"
        )
        #check
        check a == b
    
    test "(happyPath)df!=df:基本機能":
        #setup
        let a = toDataFrame(
            {
                "col1": @[1],
                "col2": @[10],
            },
            indexCol="col1",
            datetimeFormat="yyyy/mm/dd HH:MM:SS",
        )
        let b = toDataFrame(
            {
                "col1": @[1],
                "col2": @[10],
            },
            indexCol="col2"
        )
        #check
        check a != b
        
    test "(happyPath)df===df:基本機能":
        #setup
        let a = toDataFrame(
            {
                "col1": @[1],
                "col2": @[10],
            },
            indexCol="col1",
            datetimeFormat="yyyy/mm/dd HH:MM:SS",
        )
        let b = toDataFrame(
            {
                "col1": @[1],
                "col2": @[10],
            },
            indexCol="col1",
            datetimeFormat="yyyy/mm/dd HH:MM:SS",
        )
        #check
        check a === b
        
    test "(happyPath)df!==df:基本機能":
        #setup
        let a = toDataFrame(
            {
                "col1": @[1],
                "col2": @[10],
            },
            indexCol="col1",
            datetimeFormat="yyyy/mm/dd HH:MM:SS",
        )
        let b = toDataFrame(
            {
                "col1": @[1],
                "col2": @[10],
            },
            indexCol="col1",
        )
        #check
        check a !== b

suite "[](colName指定)":
    
    test "(happyPath)基本機能":
        #setup
        let df = toDataFrame(
            {
                "col1": @[1,2],
                "col2": @[3,4],
            },
            indexCol="col1",
        )
        #check
        check df["col1"] == @["1","2"]

suite "[](colIndex指定)":
    
    test "(happyPath)基本機能":
        #setup
        let df = toDataFrame(
            {
                "col1": @[1,2],
                "col2": @[3,4],
            },
            indexCol="col1",
        )
        #check
        check df[0] == @["1","2"]
        
suite "addColumn":
    
    test "(happyPath)基本機能":
        #setup
        var df = toDataFrame(
            {
                "col1": @[1,2],
                "col2": @[3,4],
            },
            indexCol="col1",
        )
        #do
        df.addColumn("col3")
        #check
        check df.data == @[
            @["1","2"],
            @["3","4"],
            @[],
        ]
        check df.columns == @["col1","col2","col3"]
        check df.colTable == {"col1":0,"col2":1,"col3":2}.toTable()
        check df.indexCol == "col1"
        check df.datetimeFormat == defaultDatetimeFormat

suite "[]=(colIndex指定)":
    
    test "(happyPath)基本機能":
        #setup
        var df = toDataFrame(
            {
                "col1": @[1,2],
                "col2": @[3,4],
            },
            indexCol="col1",
        )
        #do
        df[0] = @["5","6"]
        #check
        check df.data == @[
            @["5","6"],
            @["3","4"]
        ]
    
    test "(happyPath)string変換":
        #setup
        var df = toDataFrame(
            {
                "col1": @[1,2],
                "col2": @[3,4],
            },
            indexCol="col1",
        )
        #do
        df[0] = @[5,6]
        #check
        check df.data == @[
            @["5","6"],
            @["3","4"]
        ]

suite "[]=(colName指定)":

    test "(happyPath)基本機能":
        #setup
        var df = toDataFrame(
            {
                "col1": @[1,2],
                "col2": @[3,4],
            },
            indexCol="col1",
        )
        #do
        df["col1"] = @["5","6"]
        #check
        check df.data == @[
            @["5","6"],
            @["3","4"]
        ]
    
    test "(happyPath)string変換":
        #setup
        var df = toDataFrame(
            {
                "col1": @[1,2],
                "col2": @[3,4],
            },
            indexCol="col1",
        )
        #do
        df["col1"] = @[5,6]
        #check
        check df.data == @[
            @["5","6"],
            @["3","4"]
        ]
        
    test "(happyPath)新規列作成":
        #setup
        var df = toDataFrame(
            {
                "col1": @[1,2],
                "col2": @[3,4],
            },
            indexCol="col1",
        )
        #do
        df["col3"] = @[5,6]
        #check
        check df.data == @[
            @["1","2"],
            @["3","4"],
            @["5","6"],
        ]
        check df.columns == @["col1","col2","col3"]
        check df.colTable == {"col1":0,"col2":1,"col3":2}.toTable()

suite "len":
    
    test "(happyPath)基本機能":
        #setup
        var df = toDataFrame(
            {
                "col1": @[1,2],
                "col2": @[3,4],
            },
            indexCol="col1",
        )
        #check
        check df.len == 2
        
suite "rows":
    
    test "(happyPath)基本機能":
        #setup
        var df = toDataFrame(
            {
                "col1": @[1,2],
                "col2": @[3,4],
            },
            indexCol="col1",
        )
        #check
        var expectation: seq[Table[string,string]] = @[]
        expectation.add({"col1":"1","col2":"3"}.toTable())
        expectation.add({"col1":"2","col2":"4"}.toTable())
        var i = 0
        for row in df.rows:
            check row == expectation[i]
            i.inc()

suite "parseString":
    
    test "(happyPath)int->string":
        check 1.parseString() == "1"

    test "(happyPath)float->string":
        check (1.0).parseString() == "1.0"

    test "(happyPath)string->string":
        check "1".parseString() == "1"

    test "(happyPath)datetime->string":
        check "2021/09/01".parse("yyyy/MM/dd").parseString() == "2021-09-01 00:00:00"

suite "add":
    
    test "(happyPath)基本機能":
        #setup
        var df = toDataFrame(
            {
                "col1": @[1,2],
                "col2": @[3,4],
            },
            indexCol="col1",
        )
        #do
        df["col1"].add(10)
        #check
        check df.data == @[
            @["1","2","10"],
            @["3","4"],
        ]

suite "to":
    
    test "(happyPath)基本機能":
        #setup
        var s = @["1","2","3"]
        proc f(c: Cell): Duration =
            result = initDuration(days=c.parseInt)
        #check
        check s.to(f) == @[initDuration(days=1),initDuration(days=2),initDuration(days=3)]
    
    test "(happyPath)toInt":
        check @["1","2","3"].toInt() == @[1,2,3]
    
    test "(happyPath)toFloat":
        check @["1","2","3"].toFloat() == @[1.0,2.0,3.0]
    
    test "(happyPath)toString":
        check @[1,2,3].toString() == @["1","2","3"]
    
    test "(happyPath)toDatetime":
        check @["2021/09/01","2021/09/02","2021/09/03"].toDatetime("yyyy/MM/dd") == @[initDateTime(1, mSep, 2021, 00, 00, 00, 00),initDateTime(2, mSep, 2021, 00, 00, 00, 00),initDateTime(3, mSep, 2021, 00, 00, 00, 00)]

suite "initRow(cells)":

    test "(happyPath)基本機能":
        #do
        let row = initRow(
            {
                "col1": "1",
                "col2": "2",
            }
        )
        #check
        check row == {"col1":"1","col2":"2"}.toTable()

suite "initStringDataFrame(空)":
    
    test "(happyPath)基本機能":
        #do
        var df = initStringDataFrame()
        #check
        check df.data == newSeq[Series](0)
        check df.columns == newSeq[ColName](0)
        check df.colTable == initTable[ColName,int]()
        check df.indexCol == defaultIndexName
        check df.datetimeFormat == defaultDatetimeFormat

suite "initStringDataFrame(from StringDataFrame)":
    
    test "(happyPath)基本機能":
        #setup
        var src = toDataFrame(
            {
                "col1": @[1,2],
                "col2": @[3,4],
            },
            indexCol="col1",
        )
        #do
        var df = initStringDataFrame(src)
        #check
        check df.data == newSeq[Series](2)
        check df.columns == @["col1","col2"]
        check df.colTable == {"col1":0,"col2":1}.toTable()
        check df.indexCol == "col1"
        check df.datetimeFormat == defaultDatetimeFormat
    
    test "(happyPath)copy機能":
        #setup
        var src = toDataFrame(
            {
                "col1": @[1,2],
                "col2": @[3,4],
            },
            indexCol="col1",
        )
        #do
        var df = initStringDataFrame(src, copy=true)
        #check
        check df.data == @[
            @["1","2"],
            @["3","4"],
        ]
        check df.columns == @["col1","col2"]
        check df.colTable == {"col1":0,"col2":1}.toTable()
        check df.indexCol == "col1"
        check df.datetimeFormat == defaultDatetimeFormat

suite "initStringDataFrameGroupBy":

    test "(happyPath)基本機能":
        #setup
        var df = toDataFrame(
            {
                "col1": @[1,2],
                "col2": @[3,4],
            },
            indexCol="col1",
        )
        #do
        var dfg = initStringDataFrameGroupBy(df)
        #check
        check dfg.df.data == @[
            @["1","2"],
            @["3","4"],
        ]
        check dfg.df.columns == @["col1","col2"]
        check dfg.df.colTable == {"col1":0,"col2":1}.toTable()
        check dfg.df.indexCol == "col1"
        check dfg.df.datetimeFormat == defaultDatetimeFormat
        check dfg.group == newSeq[seq[int]](0)
        check dfg.multiIndex == newSeq[seq[ColName]](0)
        check dfg.multiIndexTable == initTable[seq[ColName],int]()
        check dfg.columns == newSeq[ColName](0)
        
suite "[](複数colName指定)":
    
    test "(happyPath)基本機能":
        #setup
        var df = toDataFrame(
            {
                "col1": @[1,2],
                "col2": @[3,4],
                "col3": @[5,6],
            },
            indexCol="col1",
        )
        #check
        check df[["col1","col2"]] == toDataFrame(
            {
                "col1": @[1,2],
                "col2": @[3,4],
            },
            indexCol="col1",
        )
        
    test "(happyPath)index列のコピー":
        #setup
        var df = toDataFrame(
            {
                "col1": @[1,2],
                "col2": @[3,4],
                "col3": @[5,6],
            },
            indexCol="col1",
        )
        #check
        check df[["col2","col3"]] == toDataFrame(
            {
                "col1": @[1,2],
                "col2": @[3,4],
                "col3": @[5,6],
            },
            indexCol="col1",
        )

    test "(exceptionPath)存在しない列の指定":
        #setup
        var df = toDataFrame(
            {
                "col1": @[1,2],
                "col2": @[3,4],
                "col3": @[5,6],
            },
            indexCol="col1",
        )
        #check
        expect StringDataFrameError:
            discard df[["col3","col4"]]

suite "keep":
    
    test "(happyPath)基本機能":
        #setup
        var df = toDataFrame(
            {
                "col1": @[1,2,3,4,5],
                "col2": @[10,20,30,40,50],
                "col3": @[100,200,300,400,500],
            },
            indexCol="col1",
        )
        #check
        check df.keep(@[true,false,true,false,true]) == toDataFrame(
            {
                "col1": @[1,3,5],
                "col2": @[10,30,50],
                "col3": @[100,300,500],
            },
            indexCol="col1",
        )
    
    test "(exceptionPath)不正な長さのfilterSeries":
        #setup
        var df = toDataFrame(
            {
                "col1": @[1,2,3,4,5],
                "col2": @[10,20,30,40,50],
                "col3": @[100,200,300,400,500],
            },
            indexCol="col1",
        )
        #check
        expect StringDataFrameError:
            discard df.keep(@[true])

suite "drop":
    
    test "(happyPath)基本機能":
        #setup
        var df = toDataFrame(
            {
                "col1": @[1,2,3,4,5],
                "col2": @[10,20,30,40,50],
                "col3": @[100,200,300,400,500],
            },
            indexCol="col1",
        )
        #check
        check df.drop(@[true,false,true,false,true]) == toDataFrame(
            {
                "col1": @[2,4],
                "col2": @[20,40],
                "col3": @[200,400],
            },
            indexCol="col1",
        )
    
    test "(exceptionPath)不正な長さのfilterSeries":
        #setup
        var df = toDataFrame(
            {
                "col1": @[1,2,3,4,5],
                "col2": @[10,20,30,40,50],
                "col3": @[100,200,300,400,500],
            },
            indexCol="col1",
        )
        #check
        expect StringDataFrameError:
            discard df.drop(@[true])

suite "[](slice)":
    
    test "(happyPath)基本機能":
        #setup
        var df = toDataFrame(
            {
                "col1": @[1,2,3,4,5],
                "col2": @[10,20,30,40,50],
                "col3": @[100,200,300,400,500],
            },
            indexCol="col1",
        )
        #check
        check df[0..2] == toDataFrame(
            {
                "col1": @[1,2,3],
                "col2": @[10,20,30],
                "col3": @[100,200,300],
            },
            indexCol="col1",
        )
    
    test "(happyPath)レンジ外指定":
        #setup
        var df = toDataFrame(
            {
                "col1": @[1,2,3,4,5],
                "col2": @[10,20,30,40,50],
                "col3": @[100,200,300,400,500],
            },
            indexCol="col1",
        )
        #check
        check df[-100..100] == toDataFrame(
            {
                "col1": @[1,2,3,4,5],
                "col2": @[10,20,30,40,50],
                "col3": @[100,200,300,400,500],
            },
            indexCol="col1",
        )

suite "iloc":
    
    test "(happyPath)基本機能":
        #setup
        var df = toDataFrame(
            {
                "col1": @[1,2,3,4,5],
                "col2": @[10,20,30,40,50],
                "col3": @[100,200,300,400,500],
            },
            indexCol="col1",
        )
        #check
        check df.iloc(1) == {"col1":"2","col2":"20","col3":"200"}.toTable()

suite "loc":
    
    test "(happyPath)基本機能":
        #setup
        var df = toDataFrame(
            {
                "col1": @[1,1,1,4,5],
                "col2": @[10,20,30,40,50],
                "col3": @[100,200,300,400,500],
            },
            indexCol="col1",
        )
        #check
        check df.loc("1") == toDataFrame(
            {
                "col1": @[1,1,1],
                "col2": @[10,20,30],
                "col3": @[100,200,300],
            },
            indexCol="col1",
        )

suite "head":
    
    test "(happyPath)基本機能":
        #setup
        var df = toDataFrame(
            {
                "col1": @[1,2,3,4,5],
                "col2": @[10,20,30,40,50],
                "col3": @[100,200,300,400,500],
            },
            indexCol="col1",
        )
        #check
        check df.head(2) == toDataFrame(
            {
                "col1": @[1,2],
                "col2": @[10,20],
                "col3": @[100,200],
            },
            indexCol="col1",
        )

suite "tail":
    
    test "(happyPath)基本機能":
        #setup
        var df = toDataFrame(
            {
                "col1": @[1,2,3,4,5],
                "col2": @[10,20,30,40,50],
                "col3": @[100,200,300,400,500],
            },
            indexCol="col1",
        )
        #check
        check df.tail(2) == toDataFrame(
            {
                "col1": @[4,5],
                "col2": @[40,50],
                "col3": @[400,500],
            },
            indexCol="col1",
        )

suite "shape":
    
    test "(happyPath)基本機能":
        #setup
        var df = toDataFrame(
            {
                "col1": @[1,2,3,4,5],
                "col2": @[10,20,30,40,50],
                "col3": @[100,200,300,400,500],
            },
            indexCol="col1",
        )
        #check
        check df.shape == (5,3)

suite "size":
    
    test "(happyPath)基本機能":
        #setup
        var df = toDataFrame(
            {
                "col1": @[1,2,3,4,5],
                "col2": @[10,20,30,40,50],
                "col3": @[100,200,300,400,500],
            },
            indexCol="col1",
        )
        #check
        check df.size == 15
        
    test "(happyPath)excludeIndexの設定":
        #setup
        var df = toDataFrame(
            {
                "col1": @[1,2,3,4,5],
                "col2": @[10,20,30,40,50],
                "col3": @[100,200,300,400,500],
            },
            indexCol="col1",
        )
        #check
        check df.size(excludeIndex=true) == 10

suite "appendRows":
    
    test "(happyPath)基本機能":
        #setup
        var df = toDataFrame(
            {
                "col1": @[1,2],
                "col2": @[10,20],
            },
            indexCol="col1",
        )
        #do
        let newDf = df.appendRows(
            {
                "col1": @[3,4],
                "col2": @[30,40],
            }
        )
        #check
        check newDf == toDataFrame(
            {
                "col1": @[1,2,3,4],
                "col2": @[10,20,30,40],
            },
            indexCol="col1",
        )
    
    test "(happyPath)autoIndexの設定":
        #setup
        var df = toDataFrame(
            {
                "col1": @[1,2],
                "col2": @[10,20],
            },
        )
        #do
        let newDf = df.appendRows(
            {
                "col1": @[3,4],
                "col2": @[30,40],
            },
            autoIndex=true
        )
        #check
        check newDf == toDataFrame(
            {
                "col1": @[1,2,3,4],
                "col2": @[10,20,30,40],
            },
        )
    
    test "(happyPath)fillEmptyRowの設定":
        #setup
        var df = toDataFrame(
            {
                "col1": @[1,2],
                "col2": @[10,20],
            },
            indexCol="col1",
        )
        #do
        let newDf = df.appendRows(
            {
                "col1": @[3,4],
                "col2": @[30],
            },
            fillEmptyRow=true
        )
        #check
        check newDf == toDataFrame(
            {
                "col1": @["1","2","3","4"],
                "col2": @["10","20","30",dfEmpty],
            },
            indexCol="col1",
        )

    test "(happyPath)fillEmptyColの設定":
        #setup
        var df = toDataFrame(
            {
                "col1": @[1,2],
                "col2": @[10,20],
            },
            indexCol="col1",
        )
        #do
        let newDf = df.appendRows(
            {
                "col1": @[3,4],
            },
            fillEmptyCol=true
        )
        #check
        check newDf == toDataFrame(
            {
                "col1": @["1","2","3","4"],
                "col2": @["10","20",dfEmpty,dfEmpty],
            },
            indexCol="col1",
        )
    
    test "(exceptionPath)列の長さの不一致":
        #setup
        var df = toDataFrame(
            {
                "col1": @[1,2],
                "col2": @[10,20],
            },
            indexCol="col1",
        )
        #do
        expect StringDataFrameError:
            discard df.appendRows(
                {
                    "col1": @[3,4],
                    "col2": @[30]
                },
            )

    test "(exceptionPath)存在しない列の指定":
        #setup
        var df = toDataFrame(
            {
                "col1": @[1,2],
                "col2": @[10,20],
            },
            indexCol="col1",
        )
        #do
        expect StringDataFrameError:
            discard df.appendRows(
                {
                    "col3": @[100,200],
                },
            )
        expect StringDataFrameError:
            discard df.appendRows(
                {
                    "col3": @[100,200],
                },
                fillEmptyCol=true,
            )

suite "addRows":
    
    test "(happyPath)基本機能":
        #setup
        var df = toDataFrame(
            {
                "col1": @[1,2],
                "col2": @[10,20],
            },
            indexCol="col1",
        )
        #do
        df.addRows(
            {
                "col1": @[3,4],
                "col2": @[30,40],
            }
        )
        #check
        check df == toDataFrame(
            {
                "col1": @[1,2,3,4],
                "col2": @[10,20,30,40],
            },
            indexCol="col1",
        )

suite "appendRow":
    
    test "(happyPath)基本機能":
        #setup
        var df = toDataFrame(
            {
                "col1": @[1,2],
                "col2": @[10,20],
            },
            indexCol="col1",
        )
        #do
        let newDf = df.appendRow(
            {
                "col1": 3,
                "col2": 30,
            }
        )
        #check
        check newDf == toDataFrame(
            {
                "col1": @[1,2,3],
                "col2": @[10,20,30],
            },
            indexCol="col1",
        )

suite "addRow":
    
    test "(happyPath)基本機能":
        #setup
        var df = toDataFrame(
            {
                "col1": @[1,2],
                "col2": @[10,20],
            },
            indexCol="col1",
        )
        #do
        df.addRow(
            {
                "col1": 3,
                "col2": 30,
            }
        )
        #check
        check df == toDataFrame(
            {
                "col1": @[1,2,3],
                "col2": @[10,20,30],
            },
            indexCol="col1",
        )

suite "appendColumns":
    
    test "(happyPath)基本機能":
        #setup
        var df = toDataFrame(
            {
                "col1": @[1,2],
                "col2": @[10,20],
            },
            indexCol="col1",
        )
        #do
        let newDf = df.appendColumns(
            {
                "col3": @[100,200],
            }
        )
        #check
        check newDf == toDataFrame(
            {
                "col1": @[1,2],
                "col2": @[10,20],
                "col3": @[100,200]
            },
            indexCol="col1",
        )

    test "(happyPath)fillEmpty":
        #setup
        var df = toDataFrame(
            {
                "col1": @[1,2],
                "col2": @[10,20],
            },
            indexCol="col1",
        )
        #do
        let newDf = df.appendColumns(
            {
                "col3": @[100],
            },
            fillEmpty=true,
        )
        #check
        check newDf == toDataFrame(
            {
                "col1": @["1","2"],
                "col2": @["10","20"],
                "col3": @["100",dfEmpty]
            },
            indexCol="col1",
        )

    test "(happyPath)override":
        #setup
        var df = toDataFrame(
            {
                "col1": @[1,2],
                "col2": @[10,20],
            },
            indexCol="col1",
        )
        #do
        let newDf = df.appendColumns(
            {
                "col2": @[30,40],
            },
            override=true,
        )
        #check
        check newDf == toDataFrame(
            {
                "col1": @["1","2"],
                "col2": @["30","40"],
            },
            indexCol="col1",
        )

    test "(happyPath)fillEmpty & override":
        #setup
        var df = toDataFrame(
            {
                "col1": @[1,2],
                "col2": @[10,20],
            },
            indexCol="col1",
        )
        #do
        let newDf = df.appendColumns(
            {
                "col2": @[30,40],
                "col3": @[100,200]
            },
            fillEmpty=true,
            override=true,
        )
        #check
        check newDf == toDataFrame(
            {
                "col1": @["1","2"],
                "col2": @["30","40"],
                "col3": @["100","200"],
            },
            indexCol="col1",
        )

    test "(exceptionPath)列の長さの不一致":
        #setup
        var df = toDataFrame(
            {
                "col1": @[1,2],
                "col2": @[10,20],
            },
            indexCol="col1",
        )
        #check
        expect StringDataFrameError:
            discard df.appendColumns(
                {
                    "col3": @[100],
                }
            )
        
    test "(exceptionPath)fillEmpty指定で列の長さがdf以上":
        #setup
        var df = toDataFrame(
            {
                "col1": @[1,2],
                "col2": @[10,20],
            },
            indexCol="col1",
        )
        #check
        expect StringDataFrameError:
            discard df.appendColumns(
                {
                    "col3": @[100,200,300],
                }
            )
    
    test "(exceptionPath)override指定なしで存在する列を指定":
        #setup
        var df = toDataFrame(
            {
                "col1": @[1,2],
                "col2": @[10,20],
            },
            indexCol="col1",
        )
        #check
        expect StringDataFrameError:
            discard df.appendColumns(
                {
                    "col2": @[10,20],
                }
            )

suite "addColumns":
    
    test "(happyPath)基本機能":
        #setup
        var df = toDataFrame(
            {
                "col1": @[1,2],
                "col2": @[10,20],
            },
            indexCol="col1",
        )
        #do
        df.addColumns(
            {
                "col3": @[100,200],
            }
        )
        #check
        check df == toDataFrame(
            {
                "col1": @[1,2],
                "col2": @[10,20],
                "col3": @[100,200],
            },
            indexCol="col1"
        )

suite "dropColumns":
    
    test "(happyPath)基本機能":
        #setup
        var df = toDataFrame(
            {
                "col1": @[1,2],
                "col2": @[10,20],
                "col3": @[100,200],
            },
            indexCol="col1",
        )
        #do
        let newDf = df.dropColumns(
            ["col2","col3"],
        )
        #check
        check newDf == toDataFrame(
            {
                "col1": @[1,2],
            },
            indexCol="col1",
        )
    
    test "(happyPath)newIndexColの指定":
        #setup
        var df = toDataFrame(
            {
                "col1": @[1,2],
                "col2": @[10,20],
                "col3": @[100,200],
            },
            indexCol="col1",
        )
        #do
        let newDf = df.dropColumns(
            ["col1","col2"],
            newIndexCol="col3",
        )
        #check
        check newDf == toDataFrame(
            {
                "col3": @[100,200],
            },
            indexCol="col3",
        )
    
    test "(happyPath)forceDropIndexの指定":
        #setup
        var df = toDataFrame(
            {
                "col1": @[1,2],
                "col2": @[10,20],
                "col3": @[100,200],
            },
            indexCol="col1",
        )
        #do
        let newDf = df.dropColumns(
            ["col1","col2"],
            forceDropIndex=true,
        )
        #check
        check newDf.data == @[@["100","200"]]
        check newDf.columns == @["col3"]
        check newDf.colTable == {"col3":0}.toTable()
        check newDf.indexCol == df.indexCol
        check newDf.datetimeFormat == df.datetimeFormat
    
    test "(exceptionPath)indexCol列を削除しようとする":
        #setup
        var df = toDataFrame(
            {
                "col1": @[1,2],
                "col2": @[10,20],
                "col3": @[100,200],
            },
            indexCol="col1",
        )
        #check
        expect StringDataFrameError:
            discard df.dropColumns(
                ["col1"],
            )
        
    test "(exceptionPath)存在しない列を指定する":
        #setup
        var df = toDataFrame(
            {
                "col1": @[1,2],
                "col2": @[10,20],
                "col3": @[100,200],
            },
            indexCol="col1",
        )
        #check
        expect StringDataFrameError:
            discard df.dropColumns(
                ["col4"],
            )

suite "dropColumn":
    
    test "(happyPath)基本機能":
        #setup
        var df = toDataFrame(
            {
                "col1": @[1,2],
                "col2": @[10,20],
                "col3": @[100,200],
            },
            indexCol="col1",
        )
        #do
        let newDf = df.dropColumn("col3")
        #check
        check newDf == toDataFrame(
            {
                "col1": @[1,2],
                "col2": @[10,20],
            },
            indexCol="col1",
        )

suite "deleteColumns":
    
    test "(happyPath)基本機能":
        #setup
        var df = toDataFrame(
            {
                "col1": @[1,2],
                "col2": @[10,20],
                "col3": @[100,200],
            },
            indexCol="col1",
        )
        #do
        df.deleteColumns(["col2","col3"])
        #check
        check df == toDataFrame(
            {
                "col1": @[1,2],
            },
            indexCol="col1",
        )

suite "deleteColumn":
    
    test "(happyPath)基本機能":
        #setup
        var df = toDataFrame(
            {
                "col1": @[1,2],
                "col2": @[10,20],
                "col3": @[100,200],
            },
            indexCol="col1",
        )
        #do
        df.deleteColumn("col3")
        #check
        check df == toDataFrame(
            {
                "col1": @[1,2],
                "col2": @[10,20],
            },
            indexCol="col1",
        )

suite "keepColumns":
    
    test "(happyPath)基本機能":
        #setup
        var df = toDataFrame(
            {
                "col1": @[1,2],
                "col2": @[10,20],
                "col3": @[100,200],
            },
            indexCol="col1",
        )
        #do
        let newDf = df.keepColumns(
            ["col1","col2"]
        )
        #check
        check newDf == toDataFrame(
            {
                "col1": @[1,2],
                "col2": @[10,20],
            },
            indexCol="col1",
        )
    
    test "(happyPath)newIndexCol":
        #setup
        var df = toDataFrame(
            {
                "col1": @[1,2],
                "col2": @[10,20],
                "col3": @[100,200],
            },
            indexCol="col1",
        )
        #do
        let newDf = df.keepColumns(
            ["col2","col3"],
            newIndexCol="col2"
        )
        #check
        check newDf == toDataFrame(
            {
                "col2": @[10,20],
                "col3": @[100,200]
            },
            indexCol="col2",
        )
    
    test "(happyPath)forceDropIndex":
        #setup
        var df = toDataFrame(
            {
                "col1": @[1,2],
                "col2": @[10,20],
                "col3": @[100,200],
            },
            indexCol="col1",
        )
        #do
        let newDf = df.keepColumns(
            ["col2","col3"],
            forceDropIndex=true,
        )
        #check
        check newDf.data == @[
            @["10","20"],
            @["100","200"],
        ]
        check newDf.columns == @["col2","col3"]
        check newDf.colTable == {"col2":0,"col3":1}.toTable()
        check newDf.indexCol == df.indexCol
        check newDf.datetimeFormat == df.datetimeFormat

suite "keepColumn":
    
    test "(happyPath)基本機能":
        #setup
        var df = toDataFrame(
            {
                "col1": @[1,2],
                "col2": @[10,20],
                "col3": @[100,200],
            },
            indexCol="col1",
        )
        #do
        let newDf = df.keepColumn("col1")
        #check
        check newDf == toDataFrame(
            {
                "col1": @[1,2],
            },
            indexCol="col1",
        )
    
suite "serviveColumns":
    
    test "(happyPath)基本機能":
        #setup
        var df = toDataFrame(
            {
                "col1": @[1,2],
                "col2": @[10,20],
                "col3": @[100,200],
            },
            indexCol="col1",
        )
        #do
        df.surviveColumns(
            ["col1","col2"]
        )
        #check
        check df == toDataFrame(
            {
                "col1": @[1,2],
                "col2": @[10,20]
            },
            indexCol="col1",
        )

suite "serviveColumn":
    
    test "(happyPath)基本機能":
        #setup
        var df = toDataFrame(
            {
                "col1": @[1,2],
                "col2": @[10,20],
                "col3": @[100,200],
            },
            indexCol="col1",
        )
        #do
        df.surviveColumn("col1")
        #check
        check df == toDataFrame(
            {
                "col1": @[1,2],
            },
            indexCol="col1",
        )