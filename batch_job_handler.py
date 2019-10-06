from __future__ import print_function
from pyspark.sql import SparkSession
from mapping_files import bucketextractor

class ProcessNotebookData(object):

    def run(self,parent_folder,notebooks_folder_names):

        print("batch_run_folder: ", parent_folder)

        info_compiler = bucketextractor()

        file_list = []
        for notebooks_folder in notebooks_folder_names:
            folder_path = parent_folder + notebooks_folder
            files_inFolder = info_compiler.getNotebookFileLocations(folder_path)
            file_list.extend(files_inFolder)


        # Get a dataframe with urls of filenames
        print("Converting file urls list to file urls dataframe .................................")
        files_urls_df = info_compiler.NotebookUrlListToDF(file_list)
        files_urls_df.show(10)

        print("Getting notebook id - repo id information ................................")
        print(folder_path)
        nbURL_nbID_repoID_df = info_compiler.AttachRepoID(files_urls_df,folder_path)

        # Process each file
        print("Sending files to process..................................")
        processed_df = info_compiler.NotebookMapper(nbURL_nbID_repoID_df)

        print("Splitting Into Library Tables.............................")
        info_compiler.WriteTables(processed_df)

        print("Saved To Postgres .......................................")
