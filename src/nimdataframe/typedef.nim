import tables

type Cell* = string
type ColName* = string
type Row* = Table[string, Cell]
type Series* = seq[Cell]
type DataFrameData* = Table[ColName, Series]
type DataFrame* = object
    data*: DataFrameData
    indexCol*: ColName
type FilterSeries* = seq[bool]
type DataFrameGroupBy* = object
    data*: Table[seq[ColName], DataFrame]
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
