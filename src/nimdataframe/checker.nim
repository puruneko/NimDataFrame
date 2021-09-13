import strutils
import strformat

import typedef
import core

proc healthCheck*(df: DataFrame, raiseException=false): bool{.discardable.} =
    #indexColチェック
    if not df.columns.contains(df.indexCol):
        if raiseException:
            raise newException(NimDataFrameError, fmt"not found index column '{df.indexCol}' in DataFrame")
        return false
    #Seriesの長さチェック
    let length = df.len
    for colName in df.columns:
        if df[colName].len != length:
            if raiseException:
                raise newException(NimDataFrameError, fmt"series must all be same length {length} (but {colName} is {df[colName].len})")
            return false
    return true

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

proc isDatetimeSeries*(s: Series, format=defaultDateTimeFormat): bool =
    result = true
    try:
        for c in s:
            discard parseDatetime(c, format)
    except:
        result = false
