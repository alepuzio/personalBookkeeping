# Notes


## reading the excel

With Spark, the architecture is a bit different than traditional applications. You may need to have the Jar at different location: in your application, at the master level, and/or worker level. Ingestion (what you’re doing) is done by the worker, so make sure they have this Jar in their classpath. (by https://stackoverflow.com/questions/70468254/reading-excel-file-using-pyspark-failed-to-find-data-source-com-crealytics-spa)

importError: Missing optional dependency 'openpyxl'.  Use pip or conda to install openpyxl.
installat

## aggiornamento versione

pyspark.errors.exceptions.base.PySparkTypeError: [CANNOT_MERGE_TYPE] Can not merge type `DoubleType` and `LongType`.

## file di input in csv

AttributeError: module 'pandas' has no attribute 'read'

correzione read.csv in read_csv


#errori in campi
pandas.errors.ParserError: Error tokenizing data. C error: Expected 10 fields in line 5, saw 16

semplificato gile

## dataframe.show
AttributeError: 'DataFrame' object has no attribute 'show'
