from utils.utils import *
import boto3




def list_appconfig(file_path,session,region):
    appconfig = session.client('appconfig',region_name=region)
    sts = session.client('sts')
    account_id = sts.get_caller_identity()["Account"]
    inventory_instances = []
    inventory = appconfig.list_applications()
    if len(inventory['Items']) != 0:
        for i in inventory['Items']:
           appId = i['Id']
           arn = f'arn:aws:appconfig:{region}:{account_id}:application/{appId}'
           arn = "test"
           inventory_object = extract_common_info(arn,i,region,account_id)
           inventory_instances.append(inventory_object)
        save_as_file_parquet(inventory_instances,file_path,generate_parquet_prefix(__file__,region,account_id))
    return inventory_instances

