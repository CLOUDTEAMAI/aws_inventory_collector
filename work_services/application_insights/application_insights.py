import boto3
from utils.utils import *



def list_application_insights(file_path,session,region,time_generated,account):
    application_insights = session.client('application-insights',region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ","_")
    inventory_instances = []
    inventory = application_insights.list_applications()
    if len(inventory['ApplicationInfoList']) != 0:
        for i in inventory['ApplicationInfoList']:
           name = i['ResourceGroupName']
           arn = f"arn:aws:application-insights:{region}:{account_id}:application/{name}"
           inventory_object = extract_common_info(arn,i,region,account_id,time_generated,account_name)
           inventory_instances.append(inventory_object)
        save_as_file_parquet(inventory_instances,file_path,generate_parquet_prefix(__file__,region,account_id))
    # return inventory_instances


