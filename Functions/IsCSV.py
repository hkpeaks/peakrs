import os
import collections
import time

## This function is under construction. Currently it may not support very small csv file.

s = time.time()

def get_byte_array_frequency_distribution(byte_array):
    frequency_distribution = collections.Counter(byte_array)
    return frequency_distribution

def is_csv(filepath):
    try:
        file = open(filepath, "rb")
        file_size = os.path.getsize(filepath)
        frequency_distribution = None
        start_byte = 0
        n = 0
        while n < 99:
            frequency_distribution = get_current_row_frequency_distribution(filepath, file, start_byte)
            for key, value in frequency_distribution.items():
                print(n, start_byte, key, value)
            start_byte = file_size * n // 100
            n += 1
        file.close()
    except FileNotFoundError:
        print(f"** File \"{filepath}\" not found **")

def get_current_row_frequency_distribution(filepath, file, start_byte):
    frequency_distribution = None
    is_valid_row_exist = False
    sample_size = 0
    while not is_valid_row_exist:
        sample_size += 100
        byte_array = file.read(sample_size)
        file.seek(start_byte)
        n = 0
        current_row = bytearray()
        is_first_line_break_exist = False
        is_second_line_break_exist = False
        while n < sample_size and not is_second_line_break_exist:
            if not is_first_line_break_exist and byte_array[n] == 10:
                is_first_line_break_exist = True
            elif is_first_line_break_exist and byte_array[n] == 10:
                is_second_line_break_exist = True
            if is_first_line_break_exist:
                current_row.append(byte_array[n])
            n += 1
        if is_second_line_break_exist and len(current_row) > 0:
            frequency_distribution = get_byte_array_frequency_distribution(current_row)
            is_valid_row_exist = True
    return frequency_distribution


is_csv("Fact.csv")

e = time.time()

print("Processing Time = {}".format(round(e-s,3)) + "s")