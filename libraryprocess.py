import boto3
import os
from pyspark.sql import SparkSession
from pyspark.sql import Row

class ProcessNotebooks(object):
    """
    Class handles methods related to library processing.
    """

    def __init__(self):

        self.spark = SparkSession \
            .builder \
            .getOrCreate()

    def ProcessEachNotebook(self, notebook_url_df_row):

        file_path = notebook_url_df_row.url
        file_name = os.path.basename(file_path)
        notebook_id = os.path.splitext(file_name)[0]

        print("FILE PATH .............................................")
        print(file_path)

        lines = self.spark.read.text(file_path).rdd.map(lambda r: r[0])
        ls = lines.map(lambda x: x) \
        .filter(lambda x: 'import' in x) \
        .map(lambda x: x.split(' ')) \
        .map(lambda x: [x[i+1] for i in range(len(x)) if x[i]=='"import' or x[i]=='"from']) \
        .map(lambda x: x[0].split('.')).map(lambda x: x[0].split('\\')) \
        .map(lambda x: x[0]) \
        .map(lambda x: (x,1)) \
        .reduceByKey(lambda n,m: n+m) \
        .map(lambda x: x[0])

        lib_count = ls.count()

        #self.spark.stop()

        return (notebook_id,lib_count)
