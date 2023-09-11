import polars as pl
from time import time
from datetime import datetime

start_time = datetime.now()

master = pl.scan_csv("Inbox/Master.csv")

fact_table = pl.scan_csv("Inbox/10000M-Fact.csv")

result = fact_table.join(master, on=["Product","Style"], 
                         how="inner").with_columns((
    pl.col("Quantity") * pl.col("Unit_Price")).alias("Amount"))

result.sink_csv("Outbox/PolarsJoinResult.csv")

elapsed = datetime.now() - start_time
print(f"\nPolars InnerJoin Duration (in second): {
    elapsed.total_seconds():.3f}")