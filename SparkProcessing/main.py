from batch_job_handler import ProcessNotebookData

def main():
    parent_folder = ''

    # notebooks_folder_names must have entries of same length
    # Enter folder/folders having less than a total of
    # 600000 (240GB) notebooks to avoid filling up memory
    notebooks_folder_names = ['notebooks_6/']

    proc = ProcessNotebookData()
    proc.run(parent_folder,notebooks_folder_names)

main()
