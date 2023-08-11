# Peakrs Dataframe
Peakrs Dataframe is a library and framework facilitates the extraction, transformation, and loading (ETL) of data. Its first application:-

``import peakrs as pr``
  
``pr.csv_vector, csv_meta = pr.get_csv_sample(file_path, 1000)``

1,000 represents number of sample row you want to get. The file can be split into 1,000 or more partitions to extract and validate the first row of each partition. In many cases, the entire process of this application runs instantly, regardless of whether the file size exceeds 10GB or contains billions of rows.

It can verify whether a file is a comma-separated values (CSV) file and determine its delimiter other than comma. If the file passes validation, it can instantly preview a billion-row file. 
 
``pr.view_csv(csv_vector, csv_meta)``

And you can output all validated rows to a disk file

``pr.write_csv(csv_vector, csv_meta)``

You can print the meta information.

``print("File Size: " + format(csv_meta.file_size) + " bytes", end =" ")``

``print("  Total Column: ", format(csv_meta.total_column))``

``print("Validated Row: ", format(csv_meta.validate_row), end =" ")``

``print("  Estimated Row: ",format(csv_meta.estimate_row))``

``print("Delimiter: " + format(csv_meta.delimiter) + " [" + chr(csv_meta.delimiter) + "]")``

``print("Is Line Br 10/13 Exist: ", csv_meta.is_line_br_10_exist, "/", csv_meta.is_line_br_13_exist)``

Like the Peaks Consolidation project https://github.com/hkpeaks/peaks-consolidation, you can easily configure complex and high-performance operations using a new ETL framework for data transformation. The streaming engine takes care of allocating and distributing file partitions to the query engine, preventing your machine from running out of memory. This makes it simple to set up ETL processes and enjoy their benefits. In addtion, the design of the streaming engine can avoid generating many temp files which make your disk run out of disk space.

Peaks Consolidation is written in Go, while Peakrs is written in Rust with Python bingings.

Peaks Consolidation is purely an ETL framework, now Peakrs extend to cover many Python and Rust APIs you run its as library.

Peakrs will also be extended to cover realtime Web by Websocket.

With the power of Python bindings, Peakrs can offer effective mean to support your machine learning exerciese interacting with Pytorch and Tensorflow.

## The Folder "py-peakrs" is a Rust app with Python bindings

This app is written in Rust with Python binding using Pyo3. 

Please refer to the instructions in the ‘run.py’ file. This file allows you to preview CSV files and their metadata instantly, even if the file size exceeds 10GB. Demo video: https://youtu.be/71GHzDnEYno

## Command List

   vector, meta_info = add_column(vector, meta_info, "column, column => math(new_col_name)") 
   
        where math includes add, subtract, multiply & divide
    
   vector, meta_info = build_keyvalue(vector, meta_info, "column, column ~ keyvalue_tablename")
   
   vector, meta_info = distinct(vector, meta_info, "column, column")
 
   vector, meta_info = filter(vector, meta_info, "column(compare_operator value) column(compare_operator value)")
 
   vector, meta_info = filter_unmatch(vector, meta_info, "column(compare_operator value) column(compare_operator value)")

        where compare operator includes >,<,>=,<=,=,!= & Range e.g. 100..200
              compare integer or float e.g. Float > number, float100..200
   
   vector, meta_info = groupby(vector, meta_info, "column, column => count() sum(Column) max(Column) min(Column)")
   
   vector, meta_info = join_keyvalue(vector, meta_info, "column, column => join_type(keyvalue_table_name)")
   
   vector, meta_info = jointable(vector, meta_info, "column, Column => join_type(KeyValueTableName)")

        where join_type includes all_match & inner
   
   vector, meta_info = orderBy(vector, meta_info,"primary_col(Sorting Order) secondary_col(sorting order)")       
  
   meta_info = orderBy{vector, meta_info, "secondaryCol(Sorting Order) => create_folder_lake(primary_col) ~ folder_name or file_name.csv"}

        where sorting order represents by A or D, to sort real numbers, use either floatA or floatD
 
   vector, meta_info = read_csv(file_path/file_name.csv)
   
   vector, meta_info = select(vector, meta_info, "column, column")
   
   vector, meta_info = select_unmatch(vector, meta_info, "column, column")  
   
   meta_info = split_file{file_path/file_name.csv, number_of_split)
   
   meta_info = create_folder_lake(vector, meta_info, "column, column => split_folder_name")
   
   meta_info = view(vector, meta_info)

   meta_info = write_csv(vector, meta_info, file_name.csv or %expand_by_100_time.csv} 


