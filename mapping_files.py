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
import pandas as pd
import datetime
import json
from process_file import FileProcessor


class parallel_processor(object):

    def __init__(self):
        self.spark = SparkSession \
            .builder \
            .appName("LibraryInsights") \
            .getOrCreate()

        # Add modules
        self.spark.sparkContext.addPyFile('process_file.py')
        self.spark.sparkContext.addPyFile('timestamp.py')

        self.bucket = "gauravdatabeamdata"
        # self.folder = notebooks_folder


    def NotebookUrlListToDF(self, file_list):
        url_list_schema = StructType([StructField("s3_url", StringType(), True)])
        url_list_rdd = self.spark.sparkContext.parallelize(file_list).map(lambda x: Row(x))
        files_urls_df = self.spark.createDataFrame(url_list_rdd, url_list_schema)
        return files_urls_df

    def AttachRepoID(self, files_urls_df, notebooks_folder_path):
        repo_df = self.spark.read.csv("s3a://gauravdatabeamdata/sample_data/data/csv/notebooks_sample.csv", header=True, multiLine=True, escape='"')
        # repo_df = self.spark.read.csv("s3a://gauravdatabeamdata/Summary_CSV_Data/csv/notebooks.csv", header=True, multiLine=True, escape='"')
        len_path = 6 + len(self.bucket) + 1 + len(notebooks_folder_path)
        # val result = df.withColumn("cutted", expr("substring(value, 1, length(value)-1)"))
        files_urls_df = files_urls_df.withColumn("nb_id", expr("substring(s3_url, " + str(len_path+4) + ", length(s3_url)-" + str(len_path) + "-9)"))
        files_urls_df.show(10   )
        files_urls_df = files_urls_df.join(repo_df,"nb_id")
        files_urls_df = files_urls_df.select([c for c in files_urls_df.columns if c in {'nb_id','s3_url','repo_id'}])
        return files_urls_df

    def NotebookMapper(self, files_urls_df):

        process_file = FileProcessor()


        print('got file df ..................................')

        # Farm out juoyter notebook files to Spark workers with a flatMap
        processed_rdd = files_urls_df.rdd.flatMap(process_file.ProcessEachFile) \
                        .filter(lambda x: x[0][0] != 'nolibrary') \
                        .reduceByKey(lambda n,m: n+m) \
                        .map(lambda x: (x[0][0],x[0][1],x[1]))


        processed_schema = StructType([StructField("library", StringType(), False),
                                         StructField("datetime", StringType(), False ),
                                         StructField("lib_counts", StringType(), False )])

        print('got processed rdd ..................................')

        processed_df = (
            processed_rdd \
            .map(lambda x: [x[0],x[1],x[2]]) \
            .toDF(processed_schema) \
            .select("library","datetime","lib_counts")
        )

        return processed_df

    def write_to_postgres(self, library_df, table_name, connector):
        print('Writing in Postgres Func ..................................')
        table = table_name
        mode = "append"
        connector.write(library_df, table, mode)

    def WriteTables(self, processed_df):
        libinfo_df = self.spark.read.csv("s3a://gauravdatabeamdata/LibraryInfo.csv", header=True, multiLine=True)
        libraries_list = libinfo_df.select(libinfo_df.Libraries).collect()

        print("Getting postgre connector..............................")
        connector = postgres.PostgresConnector()

        for lib_link in libraries_list:
            lib = lib_link.Libraries
            print(lib)
            lib_df = processed_df.where(processed_df.library==str(lib)).select("datetime","lib_counts")
            if  len(lib_df.head(1)) > 0:
                print("Saving table %s into Postgres........................" %lib)
                self.write_to_postgres(lib_df,str(lib),connector)
            else:
                continue
