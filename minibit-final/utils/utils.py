# utils/utils.py

import os

def partition_file(source_path, destination_folder, segment_size):
    """Split file into fixed-size blocks."""
    os.makedirs(destination_folder, exist_ok=True)
    with open(source_path, 'rb') as source_file:
        block_index = 0
        while data_chunk := source_file.read(segment_size):
            block_path = f"{destination_folder}/{os.path.basename(source_path)}_block_{block_index}"
            with open(block_path, 'wb') as block_file:
                block_file.write(data_chunk)
            block_index += 1
    return block_index  # total number of blocks

def assemble_file(block_numbers, source_folder, output_path, file_name):
    """Rebuild original file from blocks."""
    with open(output_path, 'wb') as result_file:
        for index in sorted(block_numbers):
            block_path = f"{source_folder}/{file_name}_block_{index}"
            with open(block_path, 'rb') as block_file:
                result_file.write(block_file.read())
