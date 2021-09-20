import unittest
import sugar
import tables
import encodings

import stringdataframe

echo "\n----------test----------"

const csv = """h1,h2,h3
1,10,100
2,20,200
3,30,300
"""

suite("toDataFrame(text指定、header指定)"):

    test("基本機能"):
        #do
        let df = toDataFrame(
            text=csv,
            headers=["H1","H2","H3"],
            headerRows=1,
        )
        #check
        check df.data == @[
            @["1","2","3"],
            @["10","20","30"],
            @["100","200","300"],
            @["0","1","2"],
        ]
        check df.columns == @["H1","H2","H3",defaultIndexName]
        check df.colTable == {"H1":0, "H2":1, "H3":2, defaultIndexName:3}.toTable()
        check df.indexCol == defaultIndexName
        check df.columns == @["H1","H2","H3",defaultIndexName]
        check df.datetimeFormat == defaultDatetimeFormat

    test("sepの指定(\\t)"):
        #setup
        var tsv = "h1\th2\th3\n"
        tsv &= "1\t10\t100\n"
        tsv &= "2\t20\t200\n"
        tsv &= "3\t30\t300\n"
        #do
        let df = toDataFrame(
            text=tsv,
            headers=["H1","H2","H3"],
            headerRows=1,
            sep='\t',
        )
        #check
        check df.data == @[
            @["1","2","3"],
            @["10","20","30"],
            @["100","200","300"],
            @["0","1","2"],
        ]
        check df.columns == @["H1","H2","H3",defaultIndexName]
        check df.colTable == {"H1":0, "H2":1, "H3":2, defaultIndexName:3}.toTable()
        check df.indexCol == defaultIndexName
        check df.columns == @["H1","H2","H3",defaultIndexName]
        check df.datetimeFormat == defaultDatetimeFormat

    test("indexColの指定"):
        #setup
        let indexCol = "H1"
        #do
        let df = toDataFrame(
            text=csv,
            headers=["H1","H2","H3"],
            headerRows=1,
            indexCol=indexCol,
        )
        #check
        check df.data == @[
            @["1","2","3"],
            @["10","20","30"],
            @["100","200","300"],
        ]
        check df.columns == @["H1","H2","H3"]
        check df.colTable == {"H1":0, "H2":1, "H3":2}.toTable()
        check df.indexCol == indexCol
        check df.columns == @["H1","H2","H3"]
        check df.datetimeFormat == defaultDatetimeFormat
    
    test("encodingの指定"):
        #setup
        let encoding = "shift-jis"
        let ec = open(encoding, "utf-8")
        defer: ec.close()
        let csv2 = ec.convert(csv)
        #do
        let df = toDataFrame(
            text=csv2,
            headers=["H1","H2","H3"],
            headerRows=1,
            encoding=encoding,
        )
        #check
        check df.data == @[
            @["1","2","3"],
            @["10","20","30"],
            @["100","200","300"],
            @["0","1","2"],
        ]
        check df.columns == @["H1","H2","H3",defaultIndexName]
        check df.colTable == {"H1":0, "H2":1, "H3":2, defaultIndexName:3}.toTable()
        check df.indexCol == defaultIndexName
        check df.columns == @["H1","H2","H3",defaultIndexName]
        check df.datetimeFormat == defaultDatetimeFormat
    
    test("header不一致"):
        #do
        expect StringDataFrameError:
            discard toDataFrame(
                text=csv,
                headers=["H1","H2"],
                headerRows=1,
            )
    
    test("indexColに存在しない列を指定"):
        #do
        expect StringDataFrameError:
            discard toDataFrame(
                text=csv,
                headers=["H1","H2","H3"],
                headerRows=1,
                indexCol="H4"
            )

suite("toDataFrame(text指定、header検知)"):

    test("基本機能"):
        #do
        let df = toDataFrame(
            text=csv,
        )
        #check
        check df.data == @[
            @["1","2","3"],
            @["10","20","30"],
            @["100","200","300"],
            @["0","1","2"],
        ]
        check df.columns == @["h1","h2","h3",defaultIndexName]
        check df.colTable == {"h1":0, "h2":1, "h3":2, defaultIndexName:3}.toTable()
        check df.indexCol == defaultIndexName
        check df.columns == @["h1","h2","h3",defaultIndexName]
        check df.datetimeFormat == defaultDatetimeFormat
    
    test("headerLineNumberの指定"):
        #do
        let df = toDataFrame(
            text=csv,
            headerLineNumber=2,
        )
        #check
        check df.data == @[
            @["2","3"],
            @["20","30"],
            @["200","300"],
            @["0","1"],
        ]
        check df.columns == @["1","10","100",defaultIndexName]
        check df.colTable == {"1":0, "10":1, "100":2, defaultIndexName:3}.toTable()
        check df.indexCol == defaultIndexName
        check df.columns == @["1","10","100",defaultIndexName]
        check df.datetimeFormat == defaultDatetimeFormat
    
    test("duplicatedHeaderの指定"):
        #setup
        var csv2 = "dup,dup2,dup,dup2\n"
        csv2 &= "1,2,3,4\n"
        csv2 &= "5,6,7,8\n"
        #do
        let df = toDataFrame(
            text=csv2,
            duplicatedHeader=true,
        )
        #check
        check df.data == @[
            @["1","5"],
            @["2","6"],
            @["3","7"],
            @["4","8"],
            @["0","1"],
        ]
        check df.columns == @["dup_0","dup2_0","dup_1","dup2_1",defaultIndexName]
        check df.colTable == {"dup_0":0,"dup2_0":1,"dup_1":2,"dup2_1":3,defaultIndexName:4}.toTable()
        check df.indexCol == defaultIndexName
        check df.columns == @["dup_0","dup2_0","dup_1","dup2_1",defaultIndexName]
        check df.datetimeFormat == defaultDatetimeFormat
    
    test("indexColの指定"):
        #setup
        let indexCol = "h1"
        #do
        let df = toDataFrame(
            text=csv,
            indexCol=indexCol,
        )
        #check
        check df.data == @[
            @["1","2","3"],
            @["10","20","30"],
            @["100","200","300"],
        ]
        check df.columns == @["h1","h2","h3"]
        check df.colTable == {"h1":0, "h2":1, "h3":2}.toTable()
        check df.indexCol == indexCol
        check df.columns == @["h1","h2","h3"]
        check df.datetimeFormat == defaultDatetimeFormat
    
    test("duplicatedHeaderをtrueにしない状態で重複した列名を指定"):
        #setup
        var csv2 = "dup,dup2,dup,dup2\n"
        csv2 &= "1,2,3,4\n"
        csv2 &= "5,6,7,8\n"
        #do
        expect StringDataFrameError:
            discard toDataFrame(
                text=csv2,
            )

suite("toDataFrame(array指定)"):

    test("基本機能"):
        discard
    test("")