import boto3

class GetImportedLibraries():

    def find_imports(self,toCheck,s3_res,current_bucket,key):
        """
        Given a filename, returns a list of modules imported by the program.
        This program does not run the code, so import statements
        in if/else or try/except blocks will always be included.
        """
        s3_res.Bucket(current_bucket).download_file(key,toCheck)
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

        return importedItems
