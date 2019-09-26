from __future__ import print_function
import postgres
import sys
from pyspark.sql import SparkSession
from pyspark.sql import Row
import os
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
import pyspark
from pyspark.sql.types import StringType
import boto3
#import libraryprocess


class ProcessNotebookData(object):

    def __init__(self, notebooks_folder):
        self.spark = SparkSession \
            .builder \
            .appName("LibraryInsights") \
            .getOrCreate()

        self.bucket = "gauravdatabeamdata"
        self.folder = notebooks_folder


    def getNotebookFileLocations(self):

        bucket_name = self.bucket
        prefix = self.folder
        s3_conn = boto3.client('s3')
        s3_result = s3_conn.list_objects_v2(Bucket=bucket_name, Prefix=prefix, Delimiter = "/")

        if 'Contents' not in s3_result:
            return []

        file_list = []
        for key in s3_result['Contents']:
            file_list.append("s3a://" + bucket_name + "/" + key['Key'])
        print("List count = " + str(len(file_list)))

        while s3_result['IsTruncated']:
            continuation_key = s3_result['NextContinuationToken']
            s3_result = s3_conn.list_objects_v2(Bucket=bucket_name, Delimiter="/", ContinuationToken=continuation_key)
            for key in s3_result['Contents']:
                file_list.append(key['Key'])
            print("List count = " + str(len(file_list)))
        return file_list[1:]


    def NotebookUrlListToDF(self, file_list):
        url_list_schema = StructType([StructField("url", StringType(), True)])
        url_list_rdd = self.spark.sparkContext.parallelize(file_list).map(lambda x: Row(x))
        files_urls_df = self.spark.createDataFrame(url_list_rdd, url_list_schema)
        return files_urls_df

    def ProcessEachFile(file_path):

        file_name = os.path.basename(file_path)
        notebook_id = os.path.splitext(file_name)[0]

        lines = self.spark.read.text(file_path).rdd.map(lambda r: r[0])
        ls = lines.map(lambda x: x) \
        .filter(lambda x: 'import' in x) \
        .map(lambda x: x.split(' ')) \
        .map(lambda x: [x[i+1] for i in range(len(x)) if x[i]=='"import' or x[i]=='"from']) \
        .map(lambda x: x[0].split('.')).map(lambda x: x[0].split('\\')) \
        .map(lambda x: x[0]) \
        .map(lambda x: (x,1)) \
        .reduceByKey(lambda n,m: n+m) \
        .map(lambda x: ('lib',1)) \
        .reduceByKey(lambda n,m: n+m) \
        .map(lambda x : (notebook_id,lib_count))


        #lib_count = ls.count()

        #self.spark.stop()

        return ls


    def NotebookMapper(self, file_list):

        #files_urls_df = self.NotebookUrlListToDF(file_list)
        # Farm out audio files to Spark workers with a map
        print('got file list ..................................')

        #process_notebooks = libraryprocess.ProcessNotebooks()
        #processed_rdd = files_urls_df \
        #                .rdd \
        #                .map(lambda x: process_notebooks.ProcessEachNotebook(x))

        for file in file_list:
            processed_rdd = ProcessEachFile(file)


        thing = processed_rdd.collect()
        print(thing)
        print('got processed rdd ..................................')



        processed_schema = StructType([StructField("notebook_id", StringType(), True),
                                             StructField("lib_counts", StringType(), False  )])

        processed_df = processed_rdd \
                      .map(lambda x: Row(x)) \
                      .toDF(processed_schema) \
                      .select("notebook_id", "lib_counts") \
                      #.toDF(["notebook_id", "lib_counts"])

        print('got processed dataframe ..................................')

        return processed_df


    def write_to_postgres(self, processed_df, table_name):
        mode = "append"
        connector = postgres.PostgresConnector()
        connector.write(processed_df, table_name, mode)


    def run(self, notebooks_folder):

        print("batch_run_folder: ", notebooks_folder)
        file_list = self.getNotebookFileLocations()

        print("Sending files to process...")
        processed_df = self.NotebookMapper(file_list)

        print("Saving counts table into Postgres...")
        self.write_to_postgres(processed_df, "lib_counts")

        self.spark.stop()


def main():
    notebooks_folder = "sample_data/data/test_notebooks/"
    proc = ProcessNotebookData(notebooks_folder)
    proc.run(notebooks_folder)

main()
