""" On August 9, 2023, this is my first published App written in Rust with Python binding.

    Thanks for sample code offers by "Sarmad Gulzar" https://github.com/sarmadgulzar/calculate-pi-python-vs-rust

    And the Pyo3 User guide https://pyo3.rs/v0.19.2/
    
    How to build the app:-
    
    cargo build --release

    For Linux platform, Copy the built .so file from release folder to same directory of the run.py.
    For Windows platform, rename the built .dll from release folder and copy it to same directory of the run.py.
    You can create and customize your run.py and many run.py in alternative file name.

    How to use the app:-

    Python run.py your_csv_file.csv

    Video demo is based on a python code version (with c++ api for file seek function): https://youtu.be/71GHzDnEYno
    So the python bindings of this app, it is not only replace a lot of python code by Rust, but also to replace the c++ api. 

    Next function to be built is to read all data from a file partition (rather than single row).

"""

from time import time
import sys
import peakrs as pr

## This simple function is not suitable migrating to Rust
def format(num: float) -> str:
    
    num_str = f'{num:.16g}'
    parts = num_str.split('.')
    num_of_digits = len(parts[0])
    num_of_commas = (num_of_digits - 1) // 3

    for i in range(1, num_of_commas + 1):
        comma_index = num_of_digits - i * 3
        parts[0] = parts[0][:comma_index] + ',' + parts[0][comma_index:]

    return '.'.join(parts)

if __name__ == "__main__":
   start = time()

if len(sys.argv) == 1: ## Input 0 para after Python run.py
    file_path = "Mushroom.csv" ## default value
elif len(sys.argv) == 2: ## Input 1 para "file_name.csv" after Python Preview_file.py
    file_path = sys.argv[1]    

## 1000 means validating first row of 1000 partitions as given by the file_path
csv_vector, csv_meta = pr.get_csv_sample(file_path, 1000) 

if len(csv_meta.error_message) > 0:
    print(csv_meta.error_message)
else: 
    ## Print first 20 sample rows to screen
    pr.view_csv(csv_vector, csv_meta)

    ## Print all validated rows to a disk file "%Sample.csv"
    pr.write_csv(csv_vector, csv_meta)

    ## Print validation summary to screen
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

end = time()

print()
print(f"Time elapsed: {round((end - start), 2)} second!")