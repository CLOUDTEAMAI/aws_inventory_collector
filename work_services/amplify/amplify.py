import boto3
from utils.utils import *

def list_amplify(file_path,session,region):
    client_boto = session.client('amplify',region_name=region)
    sts = session.client('sts')
    account_id = sts.get_caller_identity()["Account"]
    amplify_app_list = client_boto.list_apps()
    amplify_list = []
    region          = region
    if len(amplify_app_list['apps']) != 0:
        for i in amplify_app_list['apps']:
            arn = i['appArn']
            object_client = extract_common_info(arn,i,region,account_id)
            amplify_list.append(object_client)
        save_as_file_parquet(amplify_list,file_path,generate_parquet_prefix(__file__,region,account_id))
    