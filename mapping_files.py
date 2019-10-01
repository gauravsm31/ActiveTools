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
        url_list_schema = StructType([StructField("s3_url", StringType(), True)])
        url_list_rdd = self.spark.sparkContext.parallelize(file_list).map(lambda x: Row(x))
        files_urls_df = self.spark.createDataFrame(url_list_rdd, url_list_schema)
        return files_urls_df

    def AttachRepoID(self, files_urls_df):
        repo_df = self.spark.read.csv("s3a://gauravdatabeamdata/sample_data/data/csv/notebooks_sample.csv", header=True, multiLine=True, escape='"')
        len_path = 6 + len(self.bucket) + 1 + len(self.folder)
        files_urls_df = files_urls_df.withColumn("nb_id", expr("substring(s3_url, " + str(len_path+4) + ", length(s3_url)-" + str(len_path) + "-9)"))
        files_urls_df = files_urls_df.join(repo_df,"nb_id")
        files_urls_df = files_urls_df.select([c for c in files_urls_df.columns if c in {'nb_id','s3_url','repo_id'}])
        return files_urls_df


    def AttachTimestamp(self, nbURL_ndID_repoID_df):
        nbURL_nbID_timestamp_df = self.spark.read.json("s3a://gauravdatabeamdata/sample_data/data/repository_metadata/*")
        nbURL_nbID_timestamp_df = nbURL_nbID_timestamp_df.join(nbURL_ndID_repoID_df, nbURL_nbID_timestamp_df.id == nbURL_ndID_repoID_df.repo_id)
        nbURL_nbID_timestamp_df = nbURL_nbID_timestamp_df.select([c for c in nbURL_nbID_timestamp_df.columns if c in {'nb_id','s3_url','updated_at'}])
        return nbURL_nbID_timestamp_df

    def NotebookMapper(self, files_urls_df):

        print('got file df ..................................')
        # Farm out juoyter notebook files to Spark workers with a flatMap
        processed_rdd = files_urls_df.rdd.flatMap(ProcessEachFile) \
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


    def run(self,notebooks_folder):

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
        processed_df = self.NotebookMapper(nbURL_nbID_timestamp_df)

        print("Splitting Into Library Tables.............................")
        self.WriteTables(processed_df)

        print("Saved To Postgres .......................................")



def find_imports(toCheck):
    """
    Given a filename, returns a list of modules imported by the program.
    This program does not run the code, so import statements
    in if/else or try/except blocks will always be included.
    """

    importedItems = []
    with open(toCheck, 'r') as pyFile:
        for line in pyFile:
            # ignore comments
            line = line.strip().strip(',').strip('"').strip('n').strip('\\').partition("#")[0].partition(" as ")[0].split(' ')
            if line[0] == "import":
                for imported in line[1:]:
                    # remove commas - this doesn't check for commas if
                    # they're supposed to be there!
                    imported = imported.strip(", ")
                    if "." in imported:
                        imported = imported.split('.')[0]
                    else:
                        pass
                    importedItems.append(imported)
            if line[0] == "from" and line[2] == "import":
                imported = line[1]
                if "." in imported:
                    imported = imported.split('.')[0]
                else:
                    pass
                importedItems.append(imported)
    importedItems = list(dict.fromkeys(importedItems))
    print(importedItems)

    return importedItems


def GetYearMonth(file_timestamp):
    d = datetime.datetime.strptime(str(file_timestamp),"%Y-%m-%dT%H:%M:%SZ")
    new_format = "%Y-%m"
    return d.strftime(new_format)



def ProcessEachFile(file_info):

    file_path = file_info.s3_url
    file_timestamp = file_info.updated_at
    file_date = GetYearMonth(file_timestamp)

    file_path = file_path.encode("utf-8")

    # strip off the starting s3a:// from the bucket
    current_bucket = os.path.dirname(str(file_path))[6:24]
    key = str(file_path)[25:]
    file_name = os.path.basename(str(file_path))
    notebook_id = os.path.splitext(file_name)[0][3:]

    s3_res = boto3.resource('s3')
    s3_res.Bucket(current_bucket).download_file(key,file_name)

    # Get Libraries to Analyse Trends
    LibInfoFile = 'LibraryInfo.csv'
    s3_res.Bucket(current_bucket).download_file(LibInfoFile,LibInfoFile)
    lib_df = pd.read_csv(LibInfoFile)

    importedItems = find_imports(file_name)

    return_lib_list = lib_df.Libraries[lib_df['Libraries'].isin(importedItems)].values.tolist()

    returndata = []
    if not return_lib_list:
        returndata.append((('nolibrary','nodate'),0))
    else:
        for library in return_lib_list:
            returndata.append(((library,file_date),1))

    return returndata



def main():
    notebooks_folder = "sample_data/data/notebooks/"
    proc = ProcessNotebookData(notebooks_folder)
    proc.run(notebooks_folder)

main()
