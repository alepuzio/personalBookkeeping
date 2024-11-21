#!/usr/bin/bash
input_path="./tests/data/input/csv/2024_october.csv"
output_path="report.txt"

file_type="csv"
python3 refactor_read_data_csv.py nome_job $input_path $file_type $output_path
