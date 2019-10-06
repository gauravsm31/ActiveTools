from __future__ import print_function
import sys
import os
import pyspark
import boto3
import pandas as pd
# import datetime
# import json
from timestamp import GetTimeStamp

class FileProcessor(object):

    def find_imports(self, toCheck):
        """
        Given a filename, returns a list of modules imported by the program.
        This program does not run the code, so import statements
        in if/else or try/except blocks will always be included.
        """

        importedItems = []
        with open(toCheck, 'r') as pyFile:
            for line in pyFile:
                if line == []:
                    pass
                else:
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
                    if len(line) > 2:
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


    # def AttachTimestamp(self, repo_id,s3_res,current_bucket):
    #     repo_metadata_path = "s3a://gauravdatabeamdata/sample_data/data/repository_metadata/repo_" + repo_id + ".json"
    #     # repo_metadata_path = "s3a://gauravdatabeamdata/repository_metadata/repo_" + repo_id + ".json"
    #     key = str(repo_metadata_path)[25:]
    #     file_name = "repo_" + repo_id + ".json"
    #     s3_res.Bucket(current_bucket).download_file(key,file_name)
    #     #repo_metadata_df = pd.read_json(file_name)
    #
    #     with open(file_name, 'r') as myfile:
    #         data=myfile.read()
    #
    #     obj = json.loads(data)
    #
    #     if 'updated_at' in obj:
    #         timestamp = str(obj['updated_at'])
    #         return timestamp
    #     else:
    #         return 'NoTimestamp'
    #
    #     #timestamp = repo_metadata_df["updated_at"].values[0]
    #
    #
    #
    # def GetYearMonth(self, file_timestamp):
    #     timestamp = str(file_timestamp)
    #     #timestamp = timestamp.split(".")[0]
    #     d = datetime.datetime.strptime(timestamp,'%Y-%m-%dT%H:%M:%SZ')
    #     new_format = "%Y-%m"
    #     return d.strftime(new_format)


    def ProcessEachFile(self, file_info):

        returndata = []
        s3_res = boto3.resource('s3')

        file_path = file_info.s3_url
        file_path = file_path.encode("utf-8")
        # strip off the starting s3a:// from the bucket
        current_bucket = os.path.dirname(str(file_path))[6:24]
        key = str(file_path)[25:]
        file_name = os.path.basename(str(file_path))
        notebook_id = os.path.splitext(file_name)[0][3:]

        year_month = GetTimeStamp()

        file_timestamp = year_month.AttachTimestamp(str(file_info.repo_id),s3_res,current_bucket)
        if file_timestamp == 'NoTimestamp':
            returndata.append((('nolibrary','nodate'),0))
            return returndata

        file_date = year_month.GetYearMonth(file_timestamp)

        s3_res.Bucket(current_bucket).download_file(key,file_name)
        importedItems = self.find_imports(file_name)

        # Get Libraries to Analyse Trends
        LibInfoFile_local = os.path.basename('LibraryInfo.csv')
        LibInfoFile_remote = os.path.basename('LibraryInfo.csv')
        s3_res.Bucket(current_bucket).download_file(LibInfoFile_remote,LibInfoFile_local)
        lib_df = pd.read_csv(LibInfoFile_local)

        # Pick out libraries from imported libraries to return to main processor
        return_lib_list = lib_df.Libraries[lib_df['Libraries'].isin(importedItems)].values.tolist()
        if not return_lib_list:
            returndata.append((('nolibrary','nodate'),0))
        else:
            for library in return_lib_list:
                returndata.append(((library,file_date),1))

        return returndata
