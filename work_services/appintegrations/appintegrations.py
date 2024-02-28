import boto3
from utils.utils import *



def list_appintegrations(file_path,session,region):
    appintegrations = session.client('appintegrations',region_name=region)
    sts = session.client('sts')
    account_id = sts.get_caller_identity()["Account"]
    inventory_instances = []
    inventory = appintegrations.describe_fleets()
    if len(inventory['EventIntegrations']) != 0:
        for i in inventory['EventIntegrations']:
           arn = i['EventIntegrationArn']
           inventory_object = extract_common_info(arn,i,region,account_id)
           inventory_instances.append(inventory_object)
        save_as_file_parquet(inventory_instances,file_path,generate_parquet_prefix(__file__,region,account_id))
    return inventory_instances


