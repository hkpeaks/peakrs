import polars as pl
import sys
import time
from pathlib import Path

s = time.time()

if len(sys.argv) == 1: 
    file_path = "10-MillionRows.csv" 
elif len(sys.argv) == 2: 
    file_path = sys.argv[1]  

df = pl.scan_csv(file_path)
df = df.filter((pl.col('Shop') >= "S20") & (pl.col('Shop') <= "S50"))
df = df.filter((pl.col('Product') >= 500) & (pl.col('Product') <= 800))

df = df.group_by(["Shop", "Product"]).agg([
        pl.count().alias('Count'),
        pl.sum('Quantity').alias('Quantity(Sum)'),        
        pl.sum('Base_Amount').alias('Base_Amount(Sum)'),        
    ])

output_file_path = f"PolarsGroupByResult-{Path(file_path).name}"
df.sink_csv(output_file_path)

e = time.time()
print("Polars Filter GroupBy SinkCSV Time = {}".format(round(e-s,3)))
