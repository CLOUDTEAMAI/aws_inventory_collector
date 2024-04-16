import boto3
from utils.utils import *



def list_appsync(file_path,session,region,time_generated,account):
    client = session.client('appsync',region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ","_")
    inventory_instances = []
    inventory = client.list_graphql_apis()
    if len(inventory['graphqlApis']) != 0:
        for i in inventory['graphqlApis']:
           arn = i['arn']
           inventory_object = extract_common_info(arn,i,region,account_id,time_generated,account_name)
           inventory_instances.append(inventory_object)
        save_as_file_parquet(inventory_instances,file_path,generate_parquet_prefix(__file__,region,account_id))
    return inventory_instances


