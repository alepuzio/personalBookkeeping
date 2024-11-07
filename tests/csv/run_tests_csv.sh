#!/usr/bin/bash
echo "ciao"

#declare -a 
##TODO trasform in a specific script
## to find the exisitn part of a path passed
list_dirs=("./tests/data/input/example.xls" "./tests/data/input/example.xls" "./tests/data/input/" "./tests/data/" "./tests/" "./" )


## now loop through the above array
for i in "${!list_dirs[@]}"
do
	input="${list_dirs[$i]}"
	if [ -d "$input" ]; then
  		echo "input $input does exist."
	else
    		echo "input $input does'nt exist."
		echo "$PWD"
	fi
done


input="../data/input/csv/spese.csv"
if test -f $input; then
	echo "input $input does exist."
else
	echo "input $input does'nt exist."
fi

output="'./tests/data/output/report.txt"
path_src="../../src/read_data_csv.py"

python3 $path_src "job_name_to_define" $input $output
