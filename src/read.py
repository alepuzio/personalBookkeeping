
import sys

from pyspark.sql import SparkSession


class SparkFactory:
 
    def __init__ (self, newName):
        self.name = newName

    def instance(self):
        return  SparkSession.builder.appName(self.name).getOrCreate()



class InputParameters:

    """[Summary]
    :param sys.argv: console input
    :type sys.argv [ParamType]
    :raises Error: the number of arguments is less than 3
    :return: constructor
    :rtype: InputParameters
    """
    def __init__(self, arguments):
        # arguments = sys.argv
        if ( len ( arguments )  < 3) :
            raise ValueError("The parameters have to be 3: name of the job, input path and output path")
        self.name = arguments[1]
        arguments[2] = "~/repository_personal_software/git_repo/python/personalBookkeeping/data/"
        self.dataset_input_path = arguments[2]
        self.dataset_output_path = arguments[3]

    """[Summary]
    :param none:
    :raises none: 
    :return: [the path of the input]
    :rtype: [String]
    """
    def input(self):
        return self.dataset_input_path


    def output(self):
        return self.dataset_output_path

    def job(self):
        return self.name
