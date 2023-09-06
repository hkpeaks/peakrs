# Peakrs Dataframe

Peakrs Dataframe is a library and framework facilitates the extraction, transformation, and loading (ETL) of data. Its first application:-

``import peakrs as pr``
  
``df = pr.get_csv_sample(file_path, 1000)``

1,000 represents number of sample row you want to get. The file can be split into 1,000 or more partitions to extract and validate the first row of each partition. In many cases, the entire process of this application runs instantly, regardless of whether the file size exceeds 10GB or contains billions of rows.

It can verify whether a file is a comma-separated values (CSV) file and determine its delimiter other than comma. If the file passes validation, it can instantly preview a billion-row file. 
 
``pr.view_csv(df)``

And you can output all validated rows to a disk file

``df = pr.write_csv(df)``

You can print the meta information.

``print("File Size: " + format(df.file_size) + " bytes", end =" ")``

``print("  Total Column: ", format(df.total_column))``

``print("Validated Row: ", format(df.validate_row), end =" ")``

``print("  Estimated Row: ",format(df.estimate_row))``

``print("Delimiter: " + format(df.delimiter) + " [" + chr(df.delimiter) + "]")``

``print("Is Line Br 10/13 Exist: ", df.is_line_br_10_exist, "/", df.is_line_br_13_exist)``

Like the Peaks Consolidation project https://github.com/hkpeaks/peaks-consolidation, you can easily configure complex and high-performance operations using a new ETL framework for data transformation. The streaming engine takes care of allocating and distributing file partitions to the query engine, preventing your machine from running out of memory. This makes it simple to set up ETL processes and enjoy their benefits. In addtion, the design of the streaming engine can avoid generating many temp files which make your disk run out of disk space.

Peaks Consolidation is written in Go, while Peakrs is written in Rust with Python bingings.

Peaks Consolidation is purely an ETL framework, now Peakrs extend to cover many Python and Rust APIs you run its as library.

Peakrs will also be extended to cover realtime Web by Websocket.

With the power of Python bindings, Peakrs can offer effective mean to support your machine learning exerciese interacting with Pytorch and Tensorflow.

## The Folder "py-peakrs" is a Rust app with Python bindings

This app is written in Rust with Python binding using Pyo3. 

Please refer to the instructions in the ‘run.py’ file. This file allows you to preview CSV files and their metadata instantly, even if the file size exceeds 10GB. Demo video: https://youtu.be/71GHzDnEYno

## Command List

   #### Double quote represents the syntax of the data transformation framework.
   #### df represents dataframe, you can use alternative name

   df = pr.add_column(df, "column, column => math(new_col_name)") 
   
        where math includes add, subtract, multiply and divide
    
   df = pr.build_keyvalue(df, "column, column => keyvalue_tablename")
   
   df = pr.distinct(df, "column, column")
 
   df = pr.filter(df, "column(compare_operator value) column(compare_operator value)")
 
   df = pr.filter_unmatch(df, "column(compare_operator value) column(compare_operator value)")

        where compare_operator includes >,<,>=,<=,=,!= & Range e.g. 100..200
              compare integer or float e.g. Float > number, float100..200
   
   df = pr.groupby(df, "column, column => count() sum(column) max(column) min(column)")
   
   df = pr.join_keyvalue(df, "column, column => join_type(keyvalue_table_name)")
   
   df = pr.jointable(df, "column, column => join_type(keyvalue_table_name)")

        where join_type includes all_match & inner
   
   df = pr.orderby(df,"primary_col(sorting order) secondary_col(sorting order)")       
  
   df = pr.orderby{df, "secondaryCol(sorting order) => create_folder_lake(primary_col,folder_name or file_name.csv)")

        where sorting order represents by A or D, to sort real numbers, use either floatA or floatD
 
   df = pr.read_csv(file_path or file_name.csv)
   
   df = pr.select(df, "column, column")
   
   df = pr.select_unmatch(df, "column, column")  
   
   df = pr.split_file(file_path or file_name.csv, number_of_split)
   
   df = pr.create_folder_lake(df, "column, column => split_folder_name")
   
   pr.view(df)

   df = pr.write_csv(df, file_name.csv or %expand_by_100_time.csv) 


