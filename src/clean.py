#clean data

#read the CSV dir

class CSV:

    def __init__(self, newspark, dataset_input_path):
        self.spark = newspark
        self.dataset_input = dataset_input_path


    def run(self):
        print("-"+self.dataset_input+"--")
        input_dataframe = self.spark.read.csv(self.dataset_input, header=True)    
        cleaned_columns = clean_names(input_dataframe.columns)

