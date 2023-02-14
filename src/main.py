
import sys

from pyspark.sql import SparkSession

from read import  InputParameters
from clean import CSV



class Main:

    if __name__ == '__main__':
        arguments = sys.argv
        if(len(arguments) < 3) :
            raise ValueError("The parameters have to be 3: name of the job, input path and output path")
        args = InputParameters(arguments)
        print(args.job())
        #run pyspark
        spark = SparkSession.builder.appName( args.job() ).getOrCreate()
        #run transformation
        csv = CSV(spark, args.input(), args.output() )
        csv.run()
        spark.stop()




class SparkFactory:
    
    def __init__ (self, newName):
        self.name = newName
 
    def instance(self):
        return  SparkSession.builder.appName(self.name).getOrCreate()

