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
import peakrs as ps

## This simple function is not suitable migrating to Rust
def number_display_format(num: float) -> str:
    
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

if len(sys.argv) == 1: ## Input 0 para after Python Preview_file.py
    file_path = "Data.csv" ## default value
elif len(sys.argv) == 2: ## Input 1 para "file_name.csv" after Python Preview_file.py
    file_path = sys.argv[1]

## 1000 means validating of first row of 1000 partitions as given by the file_path
csv_info, validate_byte, error_message = ps.get_csv_info(file_path, 1000) 

if len(error_message) > 0:
    print(error_message)
else:

    ## Print first 20 sample rows to screen
    ps.view(bytes(validate_byte), csv_info)

    ## Print all validated rows to a disk file "%Sample.csv"
    ps.write_csv_sample_file(bytes(validate_byte), csv_info)

    ## Print validation summary to screen
    print("File Size: " + number_display_format(float(csv_info.file_size)) + " bytes", end =" ")
    print("  Total Column: ", number_display_format(float(csv_info.total_column)))
    print("Validated Row: ", number_display_format(float(csv_info.validate_row)), end =" ")
    print("  Estimated Row: ", number_display_format(float(csv_info.estimate_row)))

    print("Column Name: ", end =" ")

    for i in range(len(csv_info.column_name)):
        if i < len(csv_info.column_name) - 1:
            print(csv_info.column_name[i], end=",")
        else:
            print(csv_info.column_name[i])

    if csv_info.delimiter == 0:
        print("Delimiter: ")
    else:              
        print("Delimiter: " + number_display_format(float(csv_info.delimiter)) + " [" + chr(csv_info.delimiter) + "]")

    print("Is Line Br 10/13 Exist: ", csv_info.is_line_br_10_exist, "/", csv_info.is_line_br_13_exist)

end = time()

print()
print(f"Time elapsed: {round((end - start), 2)} second!")