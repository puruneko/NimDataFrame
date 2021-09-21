import strutils
import strformat

import typedef
import core

proc healthCheck*(df: StringDataFrame, raiseException=false): bool{.discardable.} =
    #indexColチェック
    if not df.columns.contains(df.indexCol):
        if raiseException:
            raise newException(StringDataFrameError, fmt"not found index column '{df.indexCol}' in StringDataFrame")
        return false
    #Seriesの長さチェックと予約列名チェック
    let length = df.len
    for colName in df.columns:
        if colName == reservedColName:
            if raiseException:
                raise newException(
                        StringDataFrameReservedColNameError,
                        fmt"{reservedColName} is library-reserved name"
                    )
            return false
        if df[colName].len != length:
            if raiseException:
                raise newException(StringDataFrameError, fmt"all series must be same length {length} (but '{colName}' is {df[colName].len})")
            return false
    return true

proc isEmpty*(c: Cell): bool =
    c == dfEmpty

proc isIntSeries*(s: Series): bool =
    result = true
    try:
        for c in s:
            discard parseInt(c)
    except:
        result = false

proc isFloatSeries*(s: Series): bool =
    result = true
    try:
        for c in s:
            discard parseFloat(c)
    except:
        result = false

proc isDatetimeSeries*(s: Series, format=defaultDatetimeFormat): bool =
    result = true
    try:
        for c in s:
            discard parseDatetime(c, format)
    except:
        result = false
