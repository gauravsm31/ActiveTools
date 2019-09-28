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

# sys.path.append(os.path.join(os.path.dirname(), '..'))
# from libcountprocess import ProcessNotebooks


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
            print(key['Key'])
            file_list.append("s3a://" + bucket_name + "/" + key['Key'])
        print("List count = " + str(len(file_list)))

        while s3_result['IsTruncated']:
            continuation_key = s3_result['NextContinuationToken']
            s3_result = s3_conn.list_objects_v2(Bucket=bucket_name, Delimiter="/", ContinuationToken=continuation_key)
            if 'Contents' not in s3_result:
                break
            else:
                for key in s3_result['Contents']:
                    file_list.append(key['Key'])
                print("List count = " + str(len(file_list)))

        return file_list[1:]


    def NotebookUrlListToDF(self, file_list):
        url_list_schema = StructType([StructField("url", StringType(), True)])
        url_list_rdd = self.spark.sparkContext.parallelize(file_list).map(lambda x: Row(x))
        files_urls_df = self.spark.createDataFrame(url_list_rdd, url_list_schema)
        return files_urls_df

    def ProcessEachFile(self, file_path):
        return file_path


    def NotebookMapper(self, file_list):

        #process_obj = ProcessNotebooks()
        files_urls_df = self.NotebookUrlListToDF(file_list)
        # Farm out audio files to Spark workers with a map
        files_urls_df.show()
        print('got file df ..................................')

        NewRDD = files_urls_df.rdd.map(list)

        test = NewRDD.collect()

        for item in test:
            print(item)

        processed_rdd = NewRDD.map(lambda x: self.ProcessEachFile(x))

        processed_schema = StructType([StructField("notebook_id", StringType(), False),
                                         StructField("lib_counts", StringType(), False )])

        print('got processed rdd ..................................')

        # processed_df = (
        #     processed_rdd \
        #     .map(lambda x: [x[0],x[1]]) \
        #     .toDF(processed_schema) \
        #     .select("notebook_id", "lib_counts")
        #     #.toDF(["notebook_id", "lib_counts"])
        # )

        processed_rdd.collect()

        # self.write_to_postgres(processed_df, "lib_counts")
        #
        # print('wrote to postgres ..................................')

        return


    def write_to_postgres(self, processed_df, table_name):
        print('Writing in Postgres Func ..................................')

        table = table_name
        mode = "append"
        connector = postgres.PostgresConnector()
        connector.write(processed_df, table, mode)


    def run(self, notebooks_folder):

        print("batch_run_folder: ", notebooks_folder)
        file_list = self.getNotebookFileLocations()

        print("Sending files to process...")
        self.NotebookMapper(file_list)

        #print("Saving counts table into Postgres...")
        #self.write_to_postgres(processed_df, "lib_counts")

        print("Saved To Postgres .......................................")

        #self.spark.stop()


def main():
    notebooks_folder = "sample_data/data/test_notebooks/"
    proc = ProcessNotebookData(notebooks_folder)
    proc.run(notebooks_folder)

main()
