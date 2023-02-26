
import sys

from pyspark.sql import SparkSession

import clean_data

class Main:

    if __name__ == '__main__':
        arguments = sys.argv
        if(arguments < 3) :
            raise ValueError("The parameters have to be 3: name of the job, input path and output path")
        job_name = arguments[1]
        dataset_input_path = arguments[2]
        dataset_output_path = arguments[3]
        #run pyspark
        spark = SparkSession.builder.appName(self.name).getOrCreate()
        #run transformation
        
        print("-"+dataset_input_path+"--")
        input_dataframe = spark.read.csv(dataset_input_path, header=True)    
        cleaned_columns = clean_names(input_dataframe.columns)
        print("-"+cleaned_columns+"--")
        spark.stop()




