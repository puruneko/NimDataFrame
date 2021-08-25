import datetime
import pandas as pd

df = pd.DataFrame({
    "time": [datetime.datetime.strptime(f'2021-08-{d} {10 if d%2==0 else 23}:00:00', '%Y-%m-%d %H:%M:%S') for d in range(4,30)],
    "value": [x for x in range(4,30)]
}).set_index("time")
print(df)

print(df.resample('5D').sum())