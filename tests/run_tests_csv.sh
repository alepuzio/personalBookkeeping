#!/usr/bin/bash
# run the test of E phase in ETL 

input="./tests/data/input/csv/spese.csv"
if test -f $input; then
	echo "input $input does exist."
else
	echo "input $input does'nt exist."
fi

output="'./data/output/report.txt"
IFS='/'
arrIN=($output)
unset IFS
#TODO link the arrIN as string of the direcotry path

path_src="./src/read_data_csv.py"

python3 $path_src "job_name_to_define" $input $output
