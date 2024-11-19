import sys

import pandas
from pyspark.sql import SparkSession
import csv.read_data_csv

if __name__ == "__main__":
    arguments = sys.argv  # TODO_20241106 refactor in a function
    if len(arguments) < 4:
        raise ValueError(
            "The parameters have to be 4: name of the job (now useless), input path, type of input file and output path"
        )
    job_name = arguments[1]
    dataset_input_path = arguments[2]
    dataset_output_path = arguments[4]
    type_file = arguments[3]

    if type_file == "csv":
        print("CSV TODO create object")
        call_csv(job_name, dataset_input_path, dataset_output_path)
    else:
        # run pyspark
        print("no CSV but {0}".format(type_file))
        spark = SparkSession.builder.appName(job_name).getOrCreate()
        # run transformation
        print("-" + dataset_input_path + "--")  # TODO_20241106 use logging
        """
        input_dataframe = spark.read.format("com.crealytics.spark.excel") \
                        .option("header", "true") \
                        .option("inferSchema", "true") \
                        .load(dataset_input_path)
        """
        # TODO decide the type of run using the extensione of the input file
