import boto3
from utils.utils import *



def list_wafv2(file_path,session,region):
    wafv2_client = session.client('wafv2',region_name=region)
    sts = session.client('sts')
    account_id = sts.get_caller_identity()["Account"]
    inventory_instances = []
    inventory = wafv2_client.list_web_acls(Scope='REGIONAL')
    if len(inventory['WebACLs']) != 0:
        for i in inventory['WebACLs']:
           arn = i['ARN']
           inventory_object = extract_common_info(arn,i,region,account_id)
           inventory_instances.append(inventory_object)
        save_as_file_parquet(inventory_instances,file_path,generate_parquet_prefix(__file__,region,account_id))
    return inventory_instances