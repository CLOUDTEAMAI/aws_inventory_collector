import boto3
from utils.utils import *



def list_alexaforbusiness(file_path,session,region,time_generated):
    acm_client = session.client('alexaforbusiness',region_name=region)
    sts = session.client('sts')
    account_id = sts.get_caller_identity()["Account"]
    inventory_instances = []
    inventory = acm_client.list_skills()
    if len(inventory['Skills']) != 0:
        for i in inventory['Skills']:
           skillId = i['SkillId']
           arn = f"arn:aws:alexaforbusiness:{region}:{account_id}:skill/{skillId}"
           inventory_object = extract_common_info(arn,i,region,account_id,time_generated)
           inventory_instances.append(inventory_object)
        save_as_file_parquet(inventory_instances,file_path,generate_parquet_prefix(__file__,region,account_id))
    # return inventory_instances