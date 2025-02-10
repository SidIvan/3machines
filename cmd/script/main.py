import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa

df = pd.read_parquet("~/aboba.parquet")
print(min(df['timestampMs']), max(df['timestampMs']))
df.to_csv("axaxa.csv")
