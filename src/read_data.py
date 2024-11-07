
import sys

import pandas
from pyspark.sql import SparkSession



if __name__ == '__main__':
    arguments = sys.argv #TODO_20241106 refactor in a function
    if( len (arguments ) < 3) :
        raise ValueError("The parameters have to be 3: name of the job (now useless), input path and output path")
    job_name = arguments[1]
    dataset_input_path = arguments[2]
    dataset_output_path = arguments[3]

    #run pyspark
    spark = SparkSession.builder.appName( job_name ).getOrCreate()
    #run transformation
    print("-"+dataset_input_path+"--")#TODO_20241106 use logging
    """
    input_dataframe = spark.read.format("com.crealytics.spark.excel") \
                        .option("header", "true") \
                        .option("inferSchema", "true") \
                        .load(dataset_input_path)
    """
    input_dataframe = pandas.read_excel(dataset_input_path, sheet_name=0)
    f = spark.createDataFrame(input_dataframe)
   # cleaned_columns = input_dataframe.columns
   # print(*cleaned_columns, sep = ", ") 
    f.show()
    spark.stop()


