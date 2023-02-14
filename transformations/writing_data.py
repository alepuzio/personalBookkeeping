#clean data

#read the CSV dir


def run( input_dataframe, dataset_output_path):
    cleaned_columns = clean_names(input_dataframe.columns)

    output_dataframe = input_dataframe.toDF(*cleaned_columns)
    output_dataframe.show()
    output_dataframe.write.parquet(dataset_output_path)
