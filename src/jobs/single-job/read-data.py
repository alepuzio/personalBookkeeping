
import sys

from pyspark.sql import SparkSession

from transformations import clean_data


if __name__ == '__main__':


    args = InputParameters()
    #run pyspark
    spark = SparkFactory.instance( args.job() )
    
    #run transformation
    clean_data.run(spark, args.input(), args.output() )

    spark.stop()


class SparkFactory:

    def __init__ (newName):
        self.name = newName

    def instance(self):
        return  SparkSession.builder.appName(self.name).getOrCreate()


class InputParameters:
   

"""[Summary]

:param sys.argv: console input
:type sys.argv [ParamType]
...
:raises Error: the number of arguments is less than 3
...
:return: constructor
:rtype: InputParameters
"""
  def __init__():
        arguments = sys.argv
        if(arguments < 3) :
            throw Error("The parameters have to be 3: name of the job, input path and output path")
        elif

        self.name = arguments[1]
        self.dataset_input_path = arguments[2]
        self.dataset_output_path = arguments[3]

"""[Summary]

:param none:
...
:raises none: 
...
:return: [the path of the input]
:rtype: [String]
"""
    def input(self):
        return self.dataset_input_path


"""[Summary]

:param [ParamName]: [ParamDescription], defaults to [DefaultParamVal]
:type [ParamName]: [ParamType](, optional)
...
:raises [ErrorType]: [ErrorDescription]
...
:return: [ReturnDescription]
:rtype: [ReturnType]
"""
    def output(self):
        return self.dataset_output_path

"""[Summary]

:param [ParamName]: [ParamDescription], defaults to [DefaultParamVal]
:type [ParamName]: [ParamType](, optional)
...
:raises [ErrorType]: [ErrorDescription]
...
:return: [ReturnDescription]
:rtype: [ReturnType]
"""
    def job(self):
        return self.name
