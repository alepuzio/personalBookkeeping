
import logging

from pyspark.sql import SparkSession

import sys

logger = logging.getLogger(__name__)
data_file_delimiter=','

def update_spark_log_level(spark, log_level='info'):
    """Declare the log level

    """
    spark.sparkContext.setLogLevel(log_level)
    log4j = spark._jvm.org.apache.log4j
    logger = log4j.LogManager.getLogger("my custom Log Level")
    return logger;

def pre_spark_operations(dataset_input_path):
    """Executes some operations beafore reading the input file
    """
    print("-"+dataset_input_path+"--")#TODO_20241106 use logging
    return calculate_column_max_length(dataset_input_path)


def run_pyspark(job_name, dataset_input_path, dataset_output_path):
    """Run Spark reading the input file

    """
    column_names = pre_spark_operations(dataset_input_path)

    #run pyspark
    spark = SparkSession.builder.appName( job_name ).getOrCreate()

    #run transformation
    
    #input_dataframe = pd.read_csv(dataset_input_path, delimiter=',', names=list(range(10)).dropna(axis='columns', how='all'))
    input_dataframe = spark.read.csv(dataset_input_path, sep=data_file_delimiter, header=True);# names=column_names)
    return spark, input_dataframe

def print_initial_situation(input_dataframe): 
    """Prints the situation of the dataframe before the data cleaning
    Args:
        input_dataframe: read dataframe by Spark

    Returns:
        nothing
    """
    print("****************************\ninput_dataframe")
    print( input_dataframe)
    print("***************************\ndataframe.show()")
    print( input_dataframe.show())
    print("***************************\n Row number : dataframe.count()")
    print( input_dataframe.count() )
    print("***************************\nDistinct row number: dataframe.distinct.count()")
    print( input_dataframe.distinct().count() )
    print("***************************\nColumns number: len(dataframe.columns)")
    print( len(input_dataframe.columns) )
    print("***************************\nTypes number: len(dataframe.dtypes)")
    print( len(input_dataframe.dtypes) )
    print("***************************\nNull number: len(dataframe.dtypes)")
    print( len(input_dataframe.distinct().dtypes) )

    """TODO
        To count null values in columns, you can use functions like count(when(isnan(column) | col(column).isNull(), column)) for each column to find the number of null, None, or NaN values.
        For counting values in a column, use pyspark.sql.functions.count(column) to count non-null values in a specific column
    """
def logging(job_name):
    """Activate the logging
    Args:
        job_name: Name of the Spark job.

    Returns:
        nothing
    """
    #print(logging.__file__)
    #logger.basicConfig(filename=job_name+'.log', level=logging.INFO)
    #logger.info('Started')
    pass

def stop(spark):
    """Stop Spark
    """
    spark.stop()
    print("Stopped execution")

def main():
    arguments = sys.argv #TODO_20241106 refactor in a function
    if( len (arguments ) < 3) :
        raise ValueError("The parameters have to be 3: name of the job (now useless), input path and output path")
    job_name = arguments[1]
    dataset_input_path = arguments[2]
    dataset_output_path = arguments[3]
    logging(job_name)
    spark,input_dataframe = run_pyspark(job_name, dataset_input_path, dataset_output_path)
    print_initial_situation(input_dataframe)
    stop(spark)

def calculate_column_max_length(data_file):
    """Calculate the expected max length of the columns,
    reading the rows of the file and not only the headers
    """
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
