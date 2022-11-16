"""This module manages is about the loading the data 
to create the dataset

"""

import os

class DataFromFilesystem:


    def __init__(self, new_year, new_path):
        """
        Parameters
        ----------
        year: year of bookkeeping I'm interest to
        path: abolute path  of the root of the CSV files
        """
        self.year = new_year
        self.path = new_path


    def load(self):
        """load the csv files in the passed year
        """
        spark = SparkSession.builder().master("local")
          .appName("personalBookkeeping")
          .getOrCreate()

        df = sc.wholeTextFiles(os.path.join(self.path, self.year) + "/*.csv")


          df.printSchema()
