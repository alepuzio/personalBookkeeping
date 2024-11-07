#!/usr/bin/bash
echo "ciao"

#declare -a 
list_dirs=("../tests/data/input/example.xls" "../tests/data/input/example.xls" "../tests/data/input/" "../tests/data/" "../tests/" )


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

output="../../tests/data/output/report.txt"
if [ -d "$output" ]; then
  echo "output $output does exist."
else
    echo "output $output does'nt exist."
fi

python3 read_data.py nome $input $output
