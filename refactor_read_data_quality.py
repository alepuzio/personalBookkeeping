import logging
import pytest
import numpy as np

logger = logging.getLogger(__name__)
data_file_delimiter = ","


# TODO def levare_tab_spazi
# TODO def sostituire_NaN
"""
def print_initial_situation(input_dataframe):
   ""Prints the situation of the dataframe before the data cleaning
    Args:
        input_dataframe: read dataframe by Spark

    Returns:
        nothing
   ""
    values_dataframe = values(input_dataframe)
    print("****************************\ninput_dataframe")
    print(input_dataframe)
    print("***************************\ndataframe.show()")
    print(input_dataframe.show())
    print("***************************\n Row number : dataframe.count()")
    print(values(input_dataframe))
    print(".show()")
    print(input_dataframe.summary().show())
    print(".printschema()")
    print(input_dataframe.printSchema())

   TODO
        To count null values in columns, you can use functions like count(when(isnan(column) | col(column).isNull(), column)) for each column to find the number of null, None, or NaN values.
        For counting values in a column, use pyspark.sql.functions.count(column) to count non-null values in a specific column
    """
# print(describe_columns(input_dataframe))


def print_partial_sub_dirs(data_file):
    sub_dirs = data_file.split(os.sep)
    sub = ""
    i = 0
    while i < len(sub_dirs) - 1:
        sub = "{0}{1}{2}".format(sub, os.sep, sub_dirs[i])
        if os.path.isdir(sub):
            print("{0} exists".format(sub))
            i = i + 1
        else:
            print("{0} doesnt exists".format(sub))
            break


def calculate_column_max_length(lines):
    # TODO move in quality_data
    largest_column_count = 0
    for l in lines:
     #   print(l)
        # Count the column count for the current line
        column_count = len(l.split(data_file_delimiter)) + 1
    #    print("column count {0}".format(column_count))
        # Set the new most column count
        largest_column_count = (
            column_count
            if largest_column_count < column_count
            else largest_column_count
        )
    # Generate column names (will be 0, 1, 2, ..., largest_column_count - 1)
    return largest_column_count-1


"""
Test Area

"""


def test_values():
    pass


def test_calculate_max_length_columns():
    actual = calculate_column_max_length(mock_my_data())
    expected = 6 
    assert actual == expected

def test_calculate_length_column_alone():
    lines = []
    lines.append("Date")
    actual = calculate_column_max_length(lines)
    expected = 1
    assert actual == expected


def mock_my_data():
    lines = []
    lines.append("Date,Expense, Reason,Earn, Cash withdrawal")
    lines.append("10   ,2000   ,cause 1,    ,  ")
    lines.append("11   ,2000   ,cause 1,    ,  ")
    lines.append("23   ,       ,cause 1,2000,  ")
    lines.append("10   ,0      ,cash,  ,150  ")
    lines.append("10   ,0      ,cash,  ,150  ,sixth column that is unkown")
    return lines
