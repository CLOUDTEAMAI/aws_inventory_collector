import boto3
from utils.utils import *



def list_autoscaling(file_path,session,region):
    client = session.client('autoscaling',region_name=region)
    sts = session.client('sts')
    account_id = sts.get_caller_identity()["Account"]
    inventory_instances = []
    inventory = client.describe_auto_scaling_groups()
    if len(inventory['AutoScalingGroups']) != 0:
        for i in inventory['AutoScalingGroups']:
           
           if 'CreatedTime' in i:
               i['CreatedTime'] = i['CreatedTime'].isoformat()
               
           arn = i['AutoScalingGroupARN']
           inventory_object = extract_common_info(arn,i,region,account_id)
           inventory_instances.append(inventory_object)
        save_as_file_parquet(inventory_instances,file_path,generate_parquet_prefix(__file__,region,account_id))
    return inventory_instances


