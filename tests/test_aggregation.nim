import unittest
import sugar
import tables
import sequtils
import strutils
import strformat
import times
import re
import complex
import algorithm

import StringDataFrame

template pass() = discard

suite "concat":
    test "(happyPath)":
        #setup
        let df1 = toDataFrame(
            {
                "col1": @["1"],
            },
            indexCol="col1"
        )
        let df2 = toDataFrame(
            {
                "col2": @["1","2"],
                "col3": @["1","2","3"],
            },
            indexCol="col2"
        )
        let df3 = toDataFrame(
            {
                "col1": @["1"],
                "col2": @["1","2"],
                "col3": @["1","2","3"],
            },
            indexCol="col3"
        )
        #do
        let newDf = concat([df1, df2, df3])
        #check
        check newDf == toDataFrame(
            {
                "col1": @["1","","","1","",""],
                "col2": @["","1","2","1","2",""],
                "col3": @["","","","1","2","3"],
            },
            indexCol="col1",
        )

    test "(exceptionPath)": pass
suite "merge":
    test "(happyPath)": pass
suite "join":
    test "(happyPath)": pass
suite "groupby":
    test "(happyPath)": pass
suite "agg(groupby)(複数列)":
    test "(happyPath)": pass
suite "agg(groupby)":
    test "(happyPath)": pass
suite "apply(groupby)":
    test "(happyPath)": pass
suite "aggMath(groupby)":
    test "(happyPath)": pass
suite "count(groupby)":
    test "(happyPath)": pass
suite "sum(groupby)":
    test "(happyPath)": pass
suite "mean(groupby)":
    test "(happyPath)": pass
suite "std(groupby)":
    test "(happyPath)": pass
suite "max(groupby)":
    test "(happyPath)": pass
suite "min(groupby)":
    test "(happyPath)": pass
suite "v(groupby)":
    test "(happyPath)": pass
suite "resample":
    test "(happyPath)": pass
suite "agg(resample)(複数列)":
    test "(happyPath)": pass
suite "agg(resample)":
    test "(happyPath)": pass
suite "apply(resample)":
    test "(happyPath)": pass
suite "aggMath(resample)":
    test "(happyPath)": pass
suite "count(resample)":
    test "(happyPath)": pass
suite "sum(resample)":
    test "(happyPath)": pass
suite "mean(resample)":
    test "(happyPath)": pass
suite "std(resample)":
    test "(happyPath)": pass
suite "max(resample)":
    test "(happyPath)": pass
suite "min(resample)":
    test "(happyPath)": pass
suite "v(resample)":
    test "(happyPath)": pass
suite "rolling":
    test "(happyPath)": pass
suite "agg(rolling)(複数列)":
    test "(happyPath)": pass
suite "agg(rolling)":
    test "(happyPath)": pass
suite "apply(rolling)":
    test "(happyPath)": pass
suite "aggMath(rolling)":
    test "(happyPath)": pass
suite "count(rolling)":
    test "(happyPath)": pass
suite "sum(rolling)":
    test "(happyPath)": pass
suite "mean(rolling)":
    test "(happyPath)": pass
suite "std(rolling)":
    test "(happyPath)": pass
suite "max(rolling)":
    test "(happyPath)": pass
suite "min(rolling)":
    test "(happyPath)": pass
suite "v(rolling)":
    test "(happyPath)": pass
