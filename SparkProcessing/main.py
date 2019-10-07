from batch_job_handler import ProcessNotebookData

def main():
    parent_folder = ''

    # notebooks_folder_names must have entries of same length
    notebooks_folder_names = ['notebooks_1/','notebooks_2/','notebooks_3/','notebooks_4/','notebooks_5/','notebooks_6/']

    # notebooks_folder = "notebooks_1/"
    proc = ProcessNotebookData()
    proc.run(parent_folder,notebooks_folder_names)

main()
