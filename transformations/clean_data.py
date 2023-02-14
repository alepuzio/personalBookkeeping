#clean data

#read the CSV dir


def run(spark, dataset_input_path, dataset_output_path):
    input_dataframe = spark.read.csv(dataset_input_path, header=True)    
    
    cleaned_columns = clean_names(input_dataframe.columns)

