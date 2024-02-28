import boto3
from utils.utils import *




def list_sns(file_path,session,region):
    client = session.client('sns',region_name=region)
    sts = session.client('sts')
    account_id = sts.get_caller_identity()["Account"]
    sns_list = client.list_topics()
    resources = []
    if len(sns_list['Topics']) != 0:
        for i in sns_list['Topics']:
            arn = i['TopicArn']
            resouce_object = extract_common_info(arn,i,region,account_id)
            resources.append(resouce_object)
        save_as_file_parquet(resources,file_path,generate_parquet_prefix(__file__,region,account_id))
        return resources