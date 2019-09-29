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
from pyspark.sql.functions import udf, expr, concat, col
# import pyspark.implicits._
# import pyspark.sql.functions._

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

    def AttachRepoID(self, files_urls_df):
        repo_df = self.spark.read.csv("s3a://gauravdatabeamdata/sample_data/data/csv/notebooks_sample.csv", header=True, multiLine=True, escape='"')
        len_path = 6 + len(self.bucket) + 1 + len(self.folder)
        files_urls_df = files_urls_df.withColumn("nb_id", expr("substring(url, " + str(len_path+4) + ", length(url)-" + str(len_path) + "-9)"))
        files_urls_df = files_urls_df.join(repo_df,"nb_id")
        files_urls_df = files_urls_df.select([c for c in files_urls_df.columns if c in {'nb_id','url','repo_id'}])
        return files_urls_df


    def AttachTimestamp(self, nbURL_ndID_repoID_df):
        nbURL_nbID_timestamp_df = self.spark.read.json("s3a://gauravdatabeamdata/sample_data/data/repository_metadata/*")
        return nbURL_nbID_timestamp_df

    def NotebookMapper(self, files_urls_df):

        print('got file df ..................................')
        # Farm out audio files to Spark workers with a map
        processed_rdd = files_urls_df.rdd.map(ProcessEachFile)

        processed_schema = StructType([StructField("notebook_id", StringType(), False),
                                         StructField("lib_counts", StringType(), False )])

        print('got processed rdd ..................................')

        # test code
        fpaths = processed_rdd.collect()
        for (path,num) in fpaths:
            print("notebook/path exists: %s %s ..........................................................................." %(path,num))

        processed_df = (
            processed_rdd \
            .map(lambda x: [x[0],x[1]]) \
            .toDF(processed_schema) \
            .select("notebook_id", "lib_counts")
        )

        return processed_df

    def write_to_postgres(self, processed_df, table_name):
        print('Writing in Postgres Func ..................................')

        table = table_name
        mode = "append"
        connector = postgres.PostgresConnector()
        connector.write(processed_df, table, mode)


    def run(self, notebooks_folder):

        print("batch_run_folder: ", notebooks_folder)
        file_list = self.getNotebookFileLocations()

        # Get a dataframe with urls of filenames
        print("Converting file urls list to file urls dataframe .................................")
        files_urls_df = self.NotebookUrlListToDF(file_list)

        print("Getting notebook id - repo id information ................................")
        nbURL_ndID_repoID_df = self.AttachRepoID(files_urls_df)

        print("Getting Timestamp for each notebook .........................................")
        nbURL_nbID_timestamp_df = self.AttachTimestamp(nbURL_ndID_repoID_df)

        nbURL_nbID_timestamp_df.show(10)

        # Process each file
        print("Sending files to process..................................")
        processed_df = self.NotebookMapper(nbURL_ndID_repoID_df)

        print("Saving counts table into Postgres...")
        self.write_to_postgres(processed_df, "lib_counts")

        print("Saved To Postgres .......................................")



def ProcessEachFile(file_path):

    file_path = file_path.url
    file_path = file_path.encode("utf-8")

    # strip off the starting s3a:// from the bucket
    current_bucket = os.path.dirname(str(file_path))[6:24]
    key = str(file_path)[25:]
    file_name = os.path.basename(str(file_path))
    notebook_id = os.path.splitext(file_name)[0][3:]

    s3_res = boto3.resource('s3')
    s3_res.Bucket(current_bucket).download_file(key,file_name)

    with open(file_name) as f:
        if 'import' in f.read():
            return (notebook_id,str(1))
        else:
            return (notebook_id,str(0))


def main():
    notebooks_folder = "sample_data/data/test_notebooks/"
    proc = ProcessNotebookData(notebooks_folder)
    proc.run(notebooks_folder)

main()
