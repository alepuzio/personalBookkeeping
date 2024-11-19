import logging
import pytest
import pyspark as spark
from pyspark.sql import SparkSession



logger = logging.getLogger(__name__)
data_file_delimiter = ","


# TODO def levare_tab_spazi
# TODO def sostituire_NaN


def print_initial_situation(input_dataframe):
    """Prints the situation of the dataframe before the data cleaning
    Args:
        input_dataframe: read dataframe by Spark

    Returns:
        nothing
    """
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

    """TODO
        To count null values in columns, you can use functions like count(when(isnan(column) | col(column).isNull(), column)) for each column to find the number of null, None, or NaN values.
        For counting values in a column, use pyspark.sql.functions.count(column) to count non-null values in a specific column
    """
    print(describe_columns(input_dataframe))


def values(input_dataframe):
    values = {}
    values["row_count"] = input_dataframe.count()
    values["column_number"] = len(input_dataframe.columns)
    values["dtypes"] = input_dataframe.dtypes
    values["len_dtypes"] = len(input_dataframe.dtypes)
    return values


def string_to_int(df):
    """
    # Print sum of all Revenue column
    sales['Revenue'].sum()

    '23153$1457$36865$32474$472$27510$16158$5694$6876$40487$807$6893$9153$6895$4216..

    # Remove $ from Revenue column
    sales['Revenue'] = sales['Revenue'].str.strip('$')
    sales['Revenue'] = sales['Revenue'].astype('int')

    # Verify that Revenue is now an integer
    assert sales['Revenue'].dtype == 'int'
    """
    pass


def delete_useless_column(df):
    pass


def describe_columns(df):
    print("describe columns")
    """
    for column in df.dtypes:
        print("column [{0}]: dtype {1}".format( column[0] , column[1])) 
        print("count values:\n", df.groupBy(column[0]).orderBy(column[0]).count().show())

        #print("uniques values:\n", df.groupBy(column[0]).unique().show())
    """
    """
    - are categorial or numeric?
    - if numerical print
        - count
        - unique
        - top
        - freq
    - range data
      - if day > max_day_month  or < 0 =>set maximum

"""


def delete_duplicate_values(df):
    # find duplicates row
    # delete it
    pass


"""
Test Area\

"""


def test_describe_columns():
    spark = SparkSession.builder.appName("Testing PySpark Example").getOrCreate()
    df = mock_my_dataframe(spark)
    describe_columns(df)
    spark.stop()
    assert 1 == 1


def test_values():
    pass


def mock_my_dataframe(spark):
    return spark.createDataFrame(
        [
            (1, 200, "cause1", "", ""),
            (2, "", "salary", 2000, ""),
            (3, "", "cash by bamcomat", "", 150),
            (4, 10, "cause2", "", ""),
            (5, 34, "cause1", "", ""),
        ],
        ["Date, Expense, Reason, Earns, Cash withdrawal"],
    )
