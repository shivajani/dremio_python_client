import pandas as pd
import pyarrow.parquet as pq
# Read the Parquet dataset
table = pq.read_table("./data/local/incident.parquet")
# Convert to pandas DataFrame
df = table.to_pandas()
# Show sample data
print(df.sample(5))
