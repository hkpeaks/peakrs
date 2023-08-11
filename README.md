# Peakrs Dataframe
Peakrs Dataframe is a library and framework facilitates the extraction, transformation, and loading (ETL) of data. Its first application, “Preview File” can verify whether a file is a comma-separated values (CSV) file and determine its delimiter other than comma. If the file passes validation, it can instantly preview a billion-row file. The file can be split into 1,000 or more partitions to extract and validate the first row of each partition. In many cases, the entire process of this application runs instantly, regardless of whether the file size exceeds 10GB or contains billions of rows.

Like the Peaks Consolidation project https://github.com/hkpeaks/peaks-consolidation, you can easily configure complex and high-performance operations using a new ETL framework. The streaming engine takes care of allocating and distributing file partitions to the query engine, preventing your machine from running out of memory. This makes it simple to set up ETL processes and enjoy their benefits. In addtion, the design of the streaming engine can avoid generating many temp files which make your disk run out of disk space.

Peaks Consolidation is written in Go, while Peakrs is written in Rust with Python bingings.

Peaks Consolidation is purely an ETL framework, now Peakrs extend to cover many Python and Rust APIs you run its as library.

Peakrs will also be extended to cover realtime Web by Websocket.

With the power of Python bindings, Peakrs can offer effective mean to support your machine learning exerciese interacting with Pytorch and Tensorflow.

## The Folder "py-peakrs" is Python Running Rust Version of the Peaks

This app is written in Rust with Python binding using Pyo3. See preview-file.py for instruction.

The following Peaks.py does not has rust code, however it use a C++ file seek function, it can run very fast.

Sample App extracted from run.py of the above folder "py-peakrs":-

``

** peakrs will be arranged for registering in Pypi **

import peakrs as pr

** 1000 means validating first row of 1000 partitions as given by the file_path **

csv_vector, csv_meta = pr.get_csv_sample(file_path, 1000) 

if len(csv_meta.error_message) > 0:
    print(csv_meta.error_message)
else: 
    ** Print first 20 sample rows to screen **
    pr.view_csv(csv_vector, csv_meta)

    ** Print all validated rows to a disk file "%Sample.csv **
    pr.write_csv(csv_vector, csv_meta)

    **  Print validation summary to screen **
    print("File Size: " + format(csv_meta.file_size) + " bytes", end =" ")
    print("  Total Column: ", format(csv_meta.total_column))
    print("Validated Row: ", format(csv_meta.validate_row), end =" ")
    print("  Estimated Row: ",format(csv_meta.estimate_row))

    print("Column Name: ", end =" ")

    for i in range(len(csv_meta.column_name)):
        if i < len(csv_meta.column_name) - 1:
            print(csv_meta.column_name[i], end=",")
        else:
            print(csv_meta.column_name[i])

    if csv_meta.delimiter == 0:
        print("Delimiter: ")
    else:              
        print("Delimiter: " + format(csv_meta.delimiter) + " [" + chr(csv_meta.delimiter) + "]")

    print("Is Line Br 10/13 Exist: ", csv_meta.is_line_br_10_exist, "/", csv_meta.is_line_br_13_exist)

``

## New ETL Framework for File, In-memory Table and Network Stream

First line is to define data extraction and data load. Below are 3 possible scenarios:-

UserDefineFunctionName = from Extraction to Load

Or 

UserDefineFunctionName = from Extraction, Extraction, Extraction to Load

Or

UserDefineFunctionName = from Extraction to Load, Load, Load

You can define query/ data transformation function from second line and after.

Examples:

#### ExpandFile = from Fact.csv to 1BillionRows.csv

.ExpandFactor: 123

#### JoinTable = from 1BillionRows.csv to Test1Results.csv

.Filter: Saleman(Mary,Peter,John)

.JoinTable: Product, Category => InnerJoin(Master.csv)

.AddColumn: Quantity, Unit_Price => Multiply(Amount)

.Filter: Amount(Float20000..29999)

.GroupBy: Saleman, Shop, Product => Count() Sum(Quantity) Sum(Amount)

.OrderBy: Saleman(A) Product(A) Date(D)

#### SplitFile = from Test1Results.csv to FolderLake

.CreateFolderLake: Shop

#### FilterFolder = from Outbox/FolderLake/S15/*.csv to Result-FilterFolderLake.csv

.Filter: Product(222..888) Style(=F)

#### ReadSample2View = from Outbox/Result-FilterFolderLake.csv to SampleTable

.ReadSample: StartPosition%(0) ByteLength(100000)

.View
## Command List

   AddColumn{Column, Column => Math(NewColName)} 
   
        where Math includes Add, Subtract, Multiply & Divide
    
   BuildKeyValue{Column, Column ~ KeyValueTableName}
   
   CurrentSetting{StreamMB(Number) Thread(Number)}
  
   Distinct{Column, Column}
 
   Filter{Column(CompareOperator Value) Column(CompareOperator Value)}
 
   FilterUnmatch{Column(CompareOperator Value) Column(CompareOperator Value)}

        where Compare operator includes >,<,>=,<=,=,!= & Range e.g. 100..200
              Compare integer or float e.g. Float > Number, Float100..200
   
   GroupBy{Column, Column => Count() Sum(Column) Max(Column) Min(Column)}
   
   JoinKeyValue{Column, Column => JoinType(KeyValueTableName)} 
        
        where JoinType includes AllMatch, Filter & FilterUnmatch
   
   JoinTable{Column, Column => JoinType(KeyValueTableName)}

        where JoinType includes AllMatch & InnerJoin
   
   OrderBy{PrimaryCol(Sorting Order) SecondaryCol(Sorting Order)}       
  
   OrderBy{SecondaryCol(Sorting Order) => CreateFolderLake(PrimaryCol) ~ FolderName or FileName.csv}

        where Sorting Order represents by A or D, to sort real numbers, use either FloatA or FloatD
   
   ReadSample{StartPosition%(Number) ByteLength(Number)}
   
   ReadSample{Repeat(Number) ByteLength(Number)}   
   
   Select{Column, Column}
   
   SelectUnmatch{Column, Column}
   
   SplitFile{FileName.csv ~ NumberOfSplit}
   
   CreateFolderLake{Column, Column ~ SplitFolderName}
   
   View{TableName}
