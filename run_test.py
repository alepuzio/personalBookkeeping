#!/usr/bin/bash
# execute the read data operation
# (E of ETL process)


input_path="./tests/data/input/csv/2024_october.csv"
output_path="./tests/data/output/report.txt"
 
file_type='csv'
python3 refactor_read_data_csv.py nome_job $input_path $file_type $output_path

