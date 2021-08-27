import datetime
import pandas as pd

print('----------------------------------')
df1 = pd.DataFrame({
    "a": [1,2,3],
    "b": [3,6,9],
    "c": [7,14,21],
}).set_index("a")
print(df1)

print('----------------------------------')
df1_1 = pd.DataFrame({
    "a": [1,2,3],
    "b": [-1,2,4],
    "c": [7,14,21],
}).set_index("a")
print(df1)

print('----------------------------------')
df2 = pd.DataFrame({
    "a": [1,2,4],
    "b": [3,6,8],
    "d": [8,16,24],
}).set_index("a")
print(df2)

print('----------------------------------')
df3 = pd.DataFrame({
    "a": [1,2,5],
    "b": [3,6,9],
    "e": [9,18,27],
}).set_index("a")
print(df3)

print('----------------------------------')
df4 = pd.DataFrame({
    "a": [1,2,5],
    "f": [10,11,12],
    "g": [13,14,15],
}).set_index("a")
print(df3)


print('merge inner(1)----------------------------------')
print(df1.merge(df2, how="inner", on="b"))
print('merge inner(2)----------------------------------')
print(df1.merge(df2, how="inner", on="b"))
print('merge inner(3)----------------------------------')
print(df1.merge(df1_1, how="inner", left_on="b", right_on="a"))

print('join inner----------------------------------')
print(df1.join(df2, how="inner", lsuffix='_0',rsuffix='_1'))
print('join left(1)----------------------------------')
print(df1.join(df2, how="left", lsuffix='_0',rsuffix='_1'))
print('join left(2)----------------------------------')
print(df2.join(df3, how="left", lsuffix='_0',rsuffix='_1'))
print('join right(1)----------------------------------')
print(df2.join(df3, how="right", lsuffix='_0',rsuffix='_1'))
print('join right(2)----------------------------------')
#print(df2.join([df3,df4], how="right", lsuffix='_0',rsuffix='_1'))