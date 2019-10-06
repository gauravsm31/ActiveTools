from __future__ import print_function
from pyspark.sql import SparkSession
from mapping_files import parallel_processor
from listfilepaths import get_filepaths

class ProcessNotebookData(object):

    def run(self,parent_folder,notebooks_folder_names):

        print("batch_run_folder: ", parent_folder)

        fpaths_collector = get_filepaths()
        file_list = []
        for notebooks_folder in notebooks_folder_names:
            folder_path = parent_folder + notebooks_folder
            files_inFolder = fpaths_collector.getNotebookFileLocations(folder_path)
            file_list.extend(files_inFolder)

        parallel_compute = parallel_processor()

        # Get a dataframe with urls of filenames
        print("Converting file urls list to file urls dataframe .................................")
        files_urls_df = parallel_compute.NotebookUrlListToDF(file_list)
        files_urls_df.show(10)

        print("Getting notebook id - repo id information ................................")
        print(folder_path)
        nbURL_nbID_repoID_df = parallel_compute.AttachRepoID(files_urls_df,folder_path)

        # Process each file
        print("Sending files to process..................................")
        processed_df = parallel_compute.NotebookMapper(nbURL_nbID_repoID_df)

        print("Splitting Into Library Tables.............................")
        parallel_compute.WriteTables(processed_df)

        print("Saved To Postgres .......................................")
