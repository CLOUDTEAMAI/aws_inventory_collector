import boto3
from utils.utils import *



def list_wafv2(file_path,session,region,time_generated,account):
    wafv2_client = session.client('wafv2',region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ","_")
    inventory_instances = []
    inventory = wafv2_client.list_web_acls(Scope='REGIONAL')
    if len(inventory['WebACLs']) != 0:
        for i in inventory['WebACLs']:
           arn = i['ARN']
           inventory_object = extract_common_info(arn,i,region,account_id,time_generated,account_name)
           inventory_instances.append(inventory_object)
        save_as_file_parquet(inventory_instances,file_path,generate_parquet_prefix(__file__,region,account_id))
    return inventory_instances