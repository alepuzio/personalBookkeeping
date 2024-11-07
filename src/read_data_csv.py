
import logging

import pandas as pd
from pyspark.sql import SparkSession

import sys

logger = logging.getLogger(__name__)

def run_pyspark(job_name, dataset_input_path, dataset_output_path):
    #run pyspark
    spark = SparkSession.builder.appName( job_name ).getOrCreate()
    #run transformation
    print("-"+dataset_input_path+"--")#TODO_20241106 use logging
    
    #input_dataframe = pd.read_csv(dataset_input_path, delimiter=',', names=list(range(10)).dropna(axis='columns', how='all'))
    input_dataframe = pd.read_csv(dataset_input_path, delimiter=',', names=list(range(10)))
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

if __name__ == '__main__':
    main()



