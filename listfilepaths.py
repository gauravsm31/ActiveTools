import boto3

class get_filepaths(object):

    def __init__(self):
        self.bucket = "gauravdatabeamdata"

    def getNotebookFileLocations(self, folder_path):

        bucket_name = self.bucket
        prefix = folder_path
        s3_conn = boto3.client('s3')
        s3_result = s3_conn.list_objects_v2(Bucket=bucket_name, Prefix=prefix, Delimiter = "/")

        if 'Contents' not in s3_result:
            return []

        file_list = []
        file_list_1000 = []
        for key in s3_result['Contents']:
            file_list_1000.append("s3a://" + bucket_name + "/" + key['Key'])
        file_list.extend(file_list_1000[1:])
        print("List count = " + str(len(file_list)))

        while s3_result['IsTruncated']:
            continuation_key = s3_result['NextContinuationToken']
            s3_result = s3_conn.list_objects_v2(Bucket=bucket_name, Prefix=prefix, Delimiter="/", ContinuationToken=continuation_key)
            if 'Contents' not in s3_result:
                break
            else:
                file_list_1000 = []
                for key in s3_result['Contents']:
                    file_list_1000.append("s3a://" + bucket_name + "/" + key['Key'])
                file_list.extend(file_list_1000)
                print("List count = " + str(len(file_list)))

        return file_list
