import logging

from pyspark.sql import SparkSession
import os
import sys
from refactor_read_data_quality import calculate_column_max_length
from refactor_read_data_quality_pyspark import print_initial_situation

import pytest

logger = logging.getLogger(__name__)
data_file_delimiter = ","


def update_spark_log_level(spark, log_level="info"):
    """Declare the log level.
    Args:
        spark: initialized Spark session
        log_level: level of the log, defined with the usual values
    Returns:
        the logger object
    """
    spark.sparkContext.setLogLevel(log_level)
    log4j = spark._jvm.org.apache.log4j
    logger = log4j.LogManager.getLogger("my custom Log Level")
    return logger


def pre_spark_operations(dataset_input_path):
    """Executes some operations beafore reading the input file
    Args:
        dataset_input_path: complete path of the dataset file
    Returns:
        the max number of the columnsi n the dataset file
    """
    print("-" + dataset_input_path + "--")  # TODO_20241106 use logging
    return calculate_column_max_length(dataset_input_path)


def run_pyspark(job_name, dataset_input_path, dataset_output_path, type_file):
    """Run Spark reading the input file
    Args:
        job_name: name of the ETL job
        dataset_input_path: existing complete path of the dataset file
        dataset_output_path: existing and modifiable complete path of the output file
    Returns:
        spark: spark object
        input_dataframe:  dataset of the passed CSV file
    """
    column_names = pre_spark_operations(dataset_input_path)

    # run pyspark
    spark = SparkSession.builder.appName(job_name).getOrCreate()
    input_dataframe = None
    # read csv
    if 'csv' == type_file:
        input_dataframe = call_csv(dataset_input_path)
    else:
        print ("Unsupported file type [{0}]".format(type_file))
    return spark, input_dataframe


def logging(job_name):
    """Activate the logging
    Args:
        job_name: Name of the Spark job.

    Returns:
        nothing
    """
    # print(logging.__file__)
    # logger.basicConfig(filename=job_name+'.log', level=logging.INFO)
    # logger.info('Started')
    pass


def stop(spark):
    """Stop Spark
    Args:
        spark: Spark object
    """
    spark.stop()
    print("Stopped execution")


def main():
    arguments = sys.argv  # TODO_20241106 refactor in a function
    # TODO using a library to parse the input params
    if len(arguments) < 5:
        raise ValueError(
            "The parameters have to be 4: name of the job (now useless), input path , output path and type input file"
        )
    job_name = arguments[1]
    dataset_input_path = arguments[2]
    dataset_output_path = arguments[4]
    type_file = arguments[3]

    logging(job_name)
    spark, input_dataframe = run_pyspark(
        job_name, dataset_input_path, dataset_output_path, type_file
    )
    #print_initial_situation(input_dataframe)

    stop(spark)

def call_csv(dataset_input_path):
    logging(job_name)
    return spark.read.csv(
        dataset_input_path, sep=data_file_delimiter, header=True
    )

if __name__ == "__main__":
    main()

# TODO def levare_tab_spazi
# TODO def sostituire_NaN
"""

Area Test
"""


# class TestInputData: @pytest.fixture(scope="class", autouse=True)
def test_calculate_max_length_columns():
    actual = calculate_column_max_length(my_data())
    expected = 3
    assert actual == expected


# @pytest.fixture
def my_data():
    lines = []
    for i in range(3):
        line = "row {0}, row {1}\n".format(str(i), str(i + 1))
        lines.append(line)
    return lines

