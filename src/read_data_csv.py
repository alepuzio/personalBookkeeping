
import logging

import pandas as pd
from pyspark.sql import SparkSession

import sys

logger = logging.getLogger(__name__)
data_file_delimiter=','

def pre_spark_operations(dataset_input_path):
    print("-"+dataset_input_path+"--")#TODO_20241106 use logging
    return calculate_column_max_length(dataset_input_path)


def run_pyspark(job_name, dataset_input_path, dataset_output_path):
    column_names = pre_spark_operations(dataset_input_path)

    #run pyspark
    spark = SparkSession.builder.appName( job_name ).getOrCreate()
    #run transformation
    
    #input_dataframe = pd.read_csv(dataset_input_path, delimiter=',', names=list(range(10)).dropna(axis='columns', how='all'))
    input_dataframe = pd.read_csv(dataset_input_path, delimiter=data_file_delimiter, names=column_names)
    print(input_dataframe)
    # cleaned_columns = input_dataframe.columns
    # print(*cleaned_columns, sep = ", ") 
    return spark

def logging(job_name):
    #print(logging.__file__)
    #logger.basicConfig(filename=job_name+'.log', level=logging.INFO)
    #logger.info('Started')
    pass

def stop(spark):
    spark.stop()

def main():
    arguments = sys.argv #TODO_20241106 refactor in a function
    if( len (arguments ) < 3) :
        raise ValueError("The parameters have to be 3: name of the job (now useless), input path and output path")
    job_name = arguments[1]
    dataset_input_path = arguments[2]
    dataset_output_path = arguments[3]
    #logging(job_name)
    spark=run_pyspark(job_name, dataset_input_path, dataset_output_path)
    stop(spark)

def calculate_column_max_length(data_file):
    # The max column count a line in the file could have
    largest_column_count = 0

    # Loop the data lines
    with open(data_file, 'r') as temp_f:
        # Read the lines
        lines = temp_f.readlines()

        for l in lines:
            # Count the column count for the current line
            column_count = len(l.split(data_file_delimiter)) + 1

            # Set the new most column count
            largest_column_count = column_count if largest_column_count < column_count else largest_column_count

    # Generate column names (will be 0, 1, 2, ..., largest_column_count - 1)
    column_names = [i for i in range(0, largest_column_count)]

    return column_names


if __name__ == '__main__':
    main()

#def levare_tab_spazi
#def sostituire_NaN
