from __future__ import print_function
import sys
import os
import pyspark
import boto3
import pandas as pd
from timestamp import GetTimeStamp
from imports import GetImportedLibraries
from combinations import GetCollocatedLibraries

class FileProcessor(object):

    """
    1. Get jupyter notebook filepaths (.ipynb), notebook_id and
        repo_id to be processed
    2. Get timestamp for each notebook_id from it's repository metadata (.json)
        file
    3. Extract libraries used by each notebook_id
    4. Get information on libraries that need to be chosen from
        extracted libraries to store in the database from LibraryInfo.csv
    5. Get collocation (pairs of libraries) from chosen libraries
    6. Convert information into a list having elements of type:
        ('library','timestamp'),1)

    """

    def ProcessEachFile(self, file_info):

        returndata = []

        s3_res = boto3.resource('s3')
        file_path = file_info.s3_url
        file_path = file_path.encode("utf-8")
        # strip off the starting s3a:// from the bucket
        current_bucket = os.path.dirname(str(file_path))[6:24]
        # strip off the starting s3a://<bucket_name>/ the file path
        key = str(file_path)[25:]
        file_name = os.path.basename(str(file_path))
        notebook_id = os.path.splitext(file_name)[0][3:]

        # Get timestamp for each notebook
        year_month = GetTimeStamp()
        file_timestamp = year_month.AttachTimestamp(str(file_info.repo_id),s3_res,current_bucket)
        if file_timestamp == 'NoTimestamp':
            returndata.append((('nolibrary','nodate'),0))
            return returndata
        file_date = year_month.GetYearMonth(file_timestamp)

        # Get list of imported libraries for each notebook
        libs_imported = GetImportedLibraries()
        importedItems = libs_imported.find_imports(file_name,s3_res,current_bucket,key)

        # Get list of libraries from S3 for which you want activity trends
        LibInfoFile_local = os.path.basename('LibraryInfo.csv')
        LibInfoFile_remote = 'LibraryInfo.csv'
        s3_res.Bucket(current_bucket).download_file(LibInfoFile_remote,LibInfoFile_local)
        lib_df = pd.read_csv(LibInfoFile_local)

        # Pick out intended libraries from imported libraries to return to main processor
        return_lib_list = lib_df.Libraries[lib_df['Libraries'].isin(importedItems)].values.tolist()

        if not return_lib_list:
            returndata.append((('nolibrary','nodate'),0))
        else:
            # Add pairs of libraries used together (collocated) to individual libraries list
            if len(return_lib_list)>1:
                addcollocatedlibs = GetCollocatedLibraries()
                libs_ind_coll = addcollocatedlibs.GetLibraryPairs(return_lib_list)
            else:
                libs_ind_coll = return_lib_list

            #Add each individual library and collocated library pair to the final list
            for library in libs_ind_coll:
                returndata.append(((library,file_date),1))

        return returndata
