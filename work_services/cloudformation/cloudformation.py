import boto3
from utils.utils import *




def list_cloudformation(file_path,session,region):
    client = session.client('cloudformation',region_name=region)
    sts = session.client('sts')
    account_id = sts.get_caller_identity()["Account"]
    cloud_list = client.describe_stacks()
    resources = []
    if len(cloud_list['Stacks']) != 0:
        for i in cloud_list['Stacks']:
            arn = i['StackId']
            resouce_object = extract_common_info(arn,i,region,account_id)
            resources.append(resouce_object)
        save_as_file_parquet(resources,file_path,generate_parquet_prefix(__file__,region,account_id))
        return resources