"""This module manages is about the loading the data 
to create the dataset

"""
import findspark
findspark.init()
import pyspark 

from pyspark.sql import SparkSession

class Read:
    def __init__(self, newpath):
        self.path = newpath

    def data(self):
        spark = SparkSession.builder.master("local").appName("personalBookkeeping").getOrCreate()
        df = spark.read.csv(self.path)

    ##print schema
    def show(self, df):
        df.printSchema()


if __name__ == '__main__' :
    print("leggo")
    readata = Read("/tmp/resources/zipcodes.csv")
    readata.data()
    print("read the data")
