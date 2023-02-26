
import sys

from pyspark.sql import SparkSession



if __name__ == '__main__':
    arguments = sys.argv
    if( len (arguments ) < 3) :
        raise ValueError("The parameters have to be 3: name of the job, input path and output path")
    job_name = arguments[1]
    dataset_input_path = arguments[2]
    dataset_output_path = arguments[3]
    #run pyspark
    spark = SparkSession.builder.appName( job_name ).getOrCreate()
    #run transformation
    print("-"+dataset_input_path+"--")
    input_dataframe = spark.read.csv(dataset_input_path, header=True)    
    cleaned_columns = input_dataframe.columns
    print(*cleaned_columns, sep = ", ")   
    spark.stop()



def clean_names( names ):
    return names

