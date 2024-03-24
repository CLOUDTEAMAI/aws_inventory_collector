import boto3
from utils.utils import *



def list_application_insights(file_path,session,region):
    application_insights = session.client('application-insights',region_name=region)
    sts = session.client('sts')
    account_id = sts.get_caller_identity()["Account"]
    inventory_instances = []
    inventory = application_insights.list_applications()
    if len(inventory['ApplicationInfoList']) != 0:
        for i in inventory['ApplicationInfoList']:
           name = i['ResourceGroupName']
           arn = f"arn:aws:application-insights:{region}:{account_id}:application/{name}"
           inventory_object = extract_common_info(arn,i,region,account_id)
           inventory_instances.append(inventory_object)
        save_as_file_parquet(inventory_instances,file_path,generate_parquet_prefix(__file__,region,account_id))
    return inventory_instances


