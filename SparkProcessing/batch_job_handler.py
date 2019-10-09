from __future__ import print_function
from pyspark.sql import SparkSession
from mapping_files import parallel_processor
from listfilepaths import get_filepaths

class ProcessNotebookData(object):
    """
    1. Get jupyter notebook filepaths (.ipynb) to be processed in one batch
    2. Convert filepaths into a spark DataFrame
    3. Extract notebook_id from filepath, go to notebooks.csv to attach repo_id
        for each notebook_id
    4. Send files to where filepaths and repo_ids are are
        ditributed to different nodes for processing
    """

    def run(self,parent_folder,notebooks_folder_names):

        print("batch_run_folder: ", parent_folder)

        # Get list of filepaths from multiple folders to be processed in batch
        fpaths_collector = get_filepaths()
        file_list = []
        for notebooks_folder in notebooks_folder_names:
            folder_path = parent_folder + notebooks_folder
            files_inFolder = fpaths_collector.getNotebookFileLocations(folder_path)
            file_list.extend(files_inFolder)

        # Process files in filepaths in parallel on a spark cluster and
        # write processed data to postgresql database
        parallel_compute = parallel_processor()

        # Get a dataframe with urls of filenames
        print("Converting file urls list to file urls dataframe .................................")
        files_urls_df = parallel_compute.NotebookUrlListToDF(file_list)
        files_urls_df.show(10)

        # Get repository ID for each notebook ID
        print("Getting notebook id - repo id information ................................")
        print(folder_path)
        nbURL_nbID_repoID_df = parallel_compute.AttachRepoID(files_urls_df,folder_path)

        # Process each notebook file to get library,timestamp,users
        print("Sending files to process..................................")
        processed_df = parallel_compute.NotebookMapper(nbURL_nbID_repoID_df)

        # Write timestamp,users information for each library into the database
        print("Splitting Into Library Tables.............................")
        parallel_compute.WriteTables(processed_df)

        print("Saved To Postgres .......................................")
