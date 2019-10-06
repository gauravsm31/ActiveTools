from batch_job_handler import ProcessNotebookData

def main():
    parent_folder = 'sample_data/data/'

    # notebooks_folder_names must have entries of same length
    notebooks_folder_names = ['test_notebooks_1/','test_notebooks_2/']

    # notebooks_folder = "notebooks_1/"
    proc = ProcessNotebookData()
    proc.run(parent_folder,notebooks_folder_names)

main()
