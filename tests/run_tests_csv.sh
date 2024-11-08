#!/usr/bin/bash
echo "ciao"

input="./tests/data/input/csv/spese.csv"
if test -f $input; then
	echo "input $input does exist."
else
	echo "input $input does'nt exist."
fi

output="'./data/output/report.txt"
path_src="./src/read_data_csv.py"

python3 $path_src "job_name_to_define" $input $output
