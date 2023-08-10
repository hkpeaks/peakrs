# PyPeaks Dataframe
PyPeaks Dataframes is a library and framework facilitates the extraction, transformation, and loading (ETL) of data. Its first application, “Preview File” can verify whether a file is a comma-separated values (CSV) file and determine its delimiter other than comma. If the file passes validation, it can instantly preview a billion-row file. The file can be split into 1,000 or more partitions to extract and validate the first row of each partition. In many cases, the entire process of this application runs instantly, regardless of whether the file size exceeds 10GB or contains billions of rows.

Like the Peaks Consolidation project https://github.com/hkpeaks/peaks-consolidation, you can easily configure complex and high-performance operations using a new ETL framework. The streaming engine takes care of allocating and distributing file partitions to the query engine, preventing your machine from running out of memory. This makes it simple to set up ETL processes and enjoy their benefits. In addtion, the design of the streaming engine can avoid generating many temp files which make your disk run out of disk space.

Peaks Consolidation is written in Go, while PyPeaks is written in Rust with Python bingings.

Peaks Consolidation is purely an ETL framework, now PyPeaks extend to cover many Python and Rust APIs you run its as library.

PyPeaks will also be extended to cover realtime Web by Websocket.

With the power of Python bindings, PyPeaks can offer effective mean to support your machine learning exerciese interacting with Pytorch and Tensorflow.

## The Folder "preview-file" is Python Call Rust 

This app is written in Rust with Python binding using Pyo3. See preview-file.py for instruction.

The following Peaks.py does not has rust code, however it use a C++ file seek function, it can run very fast.

## The File "preview.py" is Python without Call Rust
This app is uploaded in this repository and used to validate and preview CSV files. For every 1% position of a CSV file, it will extract one row for validation and preview. On the screen, it will display 20 rows but will output all validated rows to a disk file. If you have any issues with this app, please leave your message at    https://github.com/hkpeaks/pypeaks/issues. 

The app will gradually expand to become an ETL Framework for Polars (and/or Pandas, DuckDB, NumPy) with the implementation of the newly designed SQL statements. The ETL script will be compatible with the Peaks Consolidation https://github.com/hkpeaks/peaks-consolidation, meaning you can run the script using Python with Polars or purely using the Peaks runtime without Python.
    
    How to use this app:

    Python preview-file.py File or File Path

    e.g. python preview-file data.csv

         python preview-file "d:\your folder\data.csv"

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
