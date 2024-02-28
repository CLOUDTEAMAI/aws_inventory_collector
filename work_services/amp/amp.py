import boto3
from utils.utils import *

def list_amp(file_path,session,region):
    client_boto = session.client('amp',region_name=region)
    sts = session.client('sts')
    account_id = sts.get_caller_identity()["Account"]
    amp_list = client_boto.list_workspaces()
    amplist = []
    region          = region
    if len(amp_list['workspaces']) != 0:
        for i in amp_list['workspaces']:
            arn = i['workspaceArn']
            object_client = extract_common_info(arn,i,region,account_id)
            amplist.append(object_client)
        save_as_file_parquet(amplist,file_path,generate_parquet_prefix(__file__,region,account_id))
    return amplist
    