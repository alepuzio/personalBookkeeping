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
        df = spark.read.csv(self.path, sep =';', header='true')
        return df

    ##print schema
    def show(self, df):
        df.printSchema()

    def csv(self, df):
        df.show(10)

if __name__ == '__main__' :
    print("leggo")
    read_data = Read("../data/expenses.csv")
    dataCSV = read_data.data()
    print("read the data")
    read_data.csv(dataCSV)
    print("end")
