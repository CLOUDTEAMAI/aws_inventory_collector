import boto3
from utils.utils import *



def list_athena(file_path,session,region,time_generated,account):
    client = session.client('athena',region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ","_")
    inventory_instances = []
    inventory = client.list_named_queries()
    if len(inventory['NamedQueryIds']) != 0:
        for i in inventory['NamedQueryIds']:
           query_details = client.get_named_query(NamedQueryId=i)
           arn = f"arn:aws:athena:{region}:{account_id}:named_query/{i}"
           inventory_object = extract_common_info(arn,query_details,region,account_id,time_generated,account_name)
           inventory_instances.append(inventory_object)
        save_as_file_parquet(inventory_instances,file_path,generate_parquet_prefix(__file__,region,account_id))
    return inventory_instances


