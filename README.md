# Peakrs Dataframe
Peakrs Dataframe is a library and framework facilitates the extraction, transformation, and loading (ETL) of data. Its first application:-

  import peakrs as pr
  pr.csv_vector, csv_meta = pr.get_csv_sample(file_path, 1000)

It can verify whether a file is a comma-separated values (CSV) file and determine its delimiter other than comma. If the file passes validation, it can instantly preview a billion-row file. 
 
  pr.view_csv(csv_vector, csv_meta)

And you can output to disk file

  pr.write_csv(csv_vector, csv_meta)

The file can be split into 1,000 or more partitions to extract and validate the first row of each partition. In many cases, the entire process of this application runs instantly, regardless of whether the file size exceeds 10GB or contains billions of rows.

Like the Peaks Consolidation project https://github.com/hkpeaks/peaks-consolidation, you can easily configure complex and high-performance operations using a new ETL framework. The streaming engine takes care of allocating and distributing file partitions to the query engine, preventing your machine from running out of memory. This makes it simple to set up ETL processes and enjoy their benefits. In addtion, the design of the streaming engine can avoid generating many temp files which make your disk run out of disk space.

Peaks Consolidation is written in Go, while Peakrs is written in Rust with Python bingings.

Peaks Consolidation is purely an ETL framework, now Peakrs extend to cover many Python and Rust APIs you run its as library.

Peakrs will also be extended to cover realtime Web by Websocket.

With the power of Python bindings, Peakrs can offer effective mean to support your machine learning exerciese interacting with Pytorch and Tensorflow.

## The Folder "py-peakrs" is a Rust app with Python bindings

This app is written in Rust with Python binding using Pyo3. 

Please refer to the instructions in the ‘run.py’ file. This file allows you to preview CSV files and their metadata instantly, even if the file size exceeds 10GB. Demo video: https://youtu.be/71GHzDnEYno
