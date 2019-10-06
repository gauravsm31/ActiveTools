import datetime
import json
import boto3

class GetTimeStamp(object):

    def AttachTimestamp(self, repo_id,s3_res,current_bucket):
        repo_metadata_path = "s3a://gauravdatabeamdata/sample_data/data/repository_metadata/repo_" + repo_id + ".json"
        # repo_metadata_path = "s3a://gauravdatabeamdata/repository_metadata/repo_" + repo_id + ".json"
        key = str(repo_metadata_path)[25:]
        file_name = "repo_" + repo_id + ".json"
        s3_res.Bucket(current_bucket).download_file(key,file_name)
        #repo_metadata_df = pd.read_json(file_name)

        with open(file_name, 'r') as myfile:
            data=myfile.read()

        obj = json.loads(data)

        if 'updated_at' in obj:
            timestamp = str(obj['updated_at'])
            return timestamp
        else:
            return 'NoTimestamp'


    def GetYearMonth(self, file_timestamp):
        timestamp = str(file_timestamp)
        #timestamp = timestamp.split(".")[0]
        d = datetime.datetime.strptime(timestamp,'%Y-%m-%dT%H:%M:%SZ')
        new_format = "%Y-%m"
        return d.strftime(new_format)
