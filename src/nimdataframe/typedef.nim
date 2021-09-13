import tables

type Cell* = string
type ColName* = string
type Row* = Table[string, Cell]
type Series* = seq[Cell]
type DataFrame* = object
    data*: seq[Series]
    columns*: seq[ColName]
    colTable*: Table[ColName,int]
    indexCol*: ColName
    datetimeFormat*: string
type FilterSeries* = seq[bool]
type DataFrameGroupBy* = object
    df*: DataFrame
    group*: seq[seq[int]]
    multiIndex*: seq[seq[ColName]]
    multiIndexTable*: Table[seq[ColName], int]
    indexCol*: ColName
    columns*: seq[ColName]
type DataFrameResample* = object
    data*: DataFrame
    window*: string
    format*: string
type DataFrameRolling* = object
    data*: DataFrame
    window*: string
    format*: string
