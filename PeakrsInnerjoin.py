"""
Peakrs has implemented an alternative approach to handling large datasets with 
limited memory, which differs from the approach used by Polars. Peakrs provides 
a lower-level API for Python users, allowing them to operate on file partitions 
using a Python "while loop". This model is similar to the one used by Pytorch, 
where a while loop is used to iterate over tensors to find the optimal loss. By
providing this level of control, Peakrs enables users to efficiently process 
large datasets with limited memory resources. In addition, if your dataframe app
have to work with Pytorch for machine learning, your coding will be more 
consistence from dataframe to tensor.

When using the app, the presence of a `group_by` or `distinct` operation will
affect how datasets are merged and written to a file. There are two ways to 
write the final output: using the `write_csv` method or the `append_csv` method.

If your operation includes a `group_by` or `distinct`, you should use the 
`write_csv` method in the main program to write the final output. If your
 operation does not include a `group_by` or `distinct`, you can use the 
 `append_csv` method within a while loop to write the final output.

It's important to choose the appropriate method for writing the final output 
based on whether your operation includes a `group_by` or `distinct`, as this 
will ensure that your data is processed and written correctly. """

from time import time
import sys
import os
from datetime import datetime
from pathlib import Path
import peakrs as pr

def join_table(ref_df: pr.Dataframe, source_file_path: str, result_file_path: str):
    
    ref_df = pr.get_csv_partition_address(ref_df, "Inbox/Master.csv")  
    master_df = pr.read_csv(ref_df, "Inbox/Master.csv")
    master_df = pr.filter(master_df,"Product(200..220)")                      
    master_df = pr.build_key_value(master_df, "Product, Style => Table(KeyValue)")
    
    ref_df = pr.get_csv_partition_address(ref_df, source_file_path)
    print("\nPartition Count: ", ref_df.partition_count)
    ref_df.processed_partition = 0
    ref_df.streaming_batch = 0   

    while ref_df.processed_partition < ref_df.partition_count:        
       
        df = pr.read_csv(ref_df, source_file_path)
        df = pr.filter(df,"Shop(S11..S89)Product(105..899)")                      
        df = pr.join_key_value(df, master_df, "Product, Style => Inner(KeyValue)")    
        df = pr.add_column(df, "Quantity, Unit_Price => Multiply(Amount)")

        pr.append_csv(df, result_file_path)
        
        ref_df.processed_partition += df.thread
        ref_df.streaming_batch += 1
        print(f"{ref_df.processed_partition} ", end="")
        sys.stdout.flush()       

   
start_time = datetime.now()
df = pr.Dataframe()
df.log_file_name = "Outbox/Log-" + datetime.now().strftime("%y%m%d-%H%M%S") + ".csv"
pr.create_log(df)

if len(sys.argv) == 1:
    source_file_path = os.path.join("Inbox", "10-MillionRows.csv") ## default value
elif len(sys.argv) == 2:
    if os.path.exists(sys.argv[1]):
        source_file_path = sys.argv[1] ## input file name in CLI
    elif os.path.exists(os.path.join("Inbox", sys.argv[1])):
        source_file_path = os.path.join("Inbox", sys.argv[1])
    else:
        print(f"File {sys.argv[1]} not found in current directory or Inbox.")

pr.view_sample(source_file_path)
df.partition_size_mb = 10
df.thread = 100

result_file = f"ResultJointable-{os.path.basename(source_file_path)}"
result_file_path = "Outbox/" + result_file

try:
    file = open(result_file_path, "w")
except:
    print("Fail to create file")

join_table(df, source_file_path, result_file_path)

pr.view_sample(result_file_path)

elapsed = datetime.now() - start_time
print(f"\nPeakrs InnerJoin Duration (in second): {elapsed.total_seconds():.3f}")