import tables

type Cell* = string
type ColName* = string
type Row* = Table[string, Cell]
type Series* = seq[Cell]
type StringDataFrame* = object
    data*: seq[Series]
    columns*: seq[ColName]
    colTable*: Table[ColName,int]
    indexCol*: ColName
    datetimeFormat*: string
type FilterSeries* = seq[bool]
type StringDataFrameGroupBy* = object
    df*: StringDataFrame
    group*: seq[seq[int]]
    multiIndex*: seq[seq[ColName]]
    multiIndexTable*: Table[seq[ColName], int]
    indexCol*: ColName
    columns*: seq[ColName]
type StringDataFrameResample* = object
    data*: StringDataFrame
    window*: string
    format*: string
type StringDataFrameRollilng* = object
    data*: StringDataFrame
    window*: string
    format*: string
