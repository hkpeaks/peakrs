from time import time
import sys
##import os
from datetime import datetime
from pathlib import Path
import peakrs as pr

def filter(file_path: str, ref_df: pr.Dataframe) -> pr.Dataframe:
    
    pr.create_log(ref_df)

    if ref_df.partition_size_mb == 0:
        ref_df.partition_size_mb = 20

    if ref_df.thread == 0:
        ref_df.thread = 20    

    ref_df = pr.get_csv_partition_address(ref_df, file_path)

    print()  
    print("Partition Count: ", ref_df.partition_count)   

    processed_partition = 0
    streaming_batch = 0

    output_file_path = f"PeakrsResult-{Path(file_path).name}"
   
    try:
        file = open(output_file_path, "w")
    except:
        return "Fail to create file"

    partition_batch = ref_df.thread

    partition_count = ref_df.partition_count
    thread = ref_df.thread    

    while processed_partition < partition_count:
        if partition_count - processed_partition < thread:
            partition_batch = partition_count - processed_partition
       
        ref_df.processed_partition = processed_partition
        ref_df.streaming_batch = streaming_batch      

        df = pr.read_csv(ref_df, partition_batch, file_path)
        df = pr.filter(df,"Shop(S20..S50)")                      
        df = pr.filter(df,"Product(500..800)")                      

        pr.append_csv(df, output_file_path)

        processed_partition += partition_batch
        print(f"{processed_partition} ", end="")
        sys.stdout.flush()

        streaming_batch += 1       


if __name__ == "__main__":
   
    start_time = datetime.now()

df = pr.Dataframe()

df.log_file_name = "Log-" + datetime.now().strftime("%y%m%d-%H%M%S") + ".csv"

if len(sys.argv) == 1: ## Input 0 para after Python run.py
    file_path = "10-MillionRows.csv" ## default value
elif len(sys.argv) == 2: ## Input 1 para "file_name.csv" after Python Preview_file.py
    file_path = sys.argv[1]  

## pr.view_sample(file_path)

df.partition_size_mb = 10
df.thread = 100

df = filter(file_path, df)

elapsed = datetime.now() - start_time
print()
if elapsed.total_seconds() <= 1.0:
    print(f"Peakrs Filter-GroupBy Duration: {elapsed.total_seconds():.3f} second")
else:
    print(f"Peakrs Filter-GroupBy Duration: {elapsed.total_seconds():.3f} seconds")