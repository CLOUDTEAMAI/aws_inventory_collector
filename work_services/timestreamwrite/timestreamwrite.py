import boto3
from utils.utils import *



def list_timestreamwrite(file_path,session,region):
    timestream_write_client = session.client('timestream-write',region_name=region)
    sts = session.client('sts')
    account_id = sts.get_caller_identity()["Account"]
    inventory_instances = []
    inventory = timestream_write_client.list_databases()
    if len(inventory['Databases']) != 0:
        for i in inventory['Databases']:
           arn = i['Arn']
           inventory_object = extract_common_info(arn,i,region,account_id)
           inventory_instances.append(inventory_object)
        save_as_file_parquet(inventory_instances,file_path,generate_parquet_prefix(__file__,region,account_id))
    return inventory_instances