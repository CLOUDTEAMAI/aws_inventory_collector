import boto3
from utils.utils import *



def list_appflow(file_path,session,region):
    appmesh = session.client('appflow',region_name=region)
    sts = session.client('sts')
    account_id = sts.get_caller_identity()["Account"]
    inventory_instances = []
    inventory = appmesh.list_flows()
    if len(inventory['flows']) != 0:
        for i in inventory['flows']:
           if 'createdAt' in i:
               i['createdAt'] = i['createdAt'].isoformat()
           if 'lastUpdatedAt' in i:
               i['lastUpdatedAt'] = i['lastUpdatedAt'].isoformat()
           if 'mostRecentExecutionTime' in i['lastRunExecutionDetails']:
               i['lastRunExecutionDetails']['mostRecentExecutionTime'] = i['lastRunExecutionDetails']['mostRecentExecutionTime'].isoformat()
           flow_name = i['flowName']
           arn = f"arn:aws:appflow:{region}:{account_id}:flow/{flow_name}"
           inventory_object = extract_common_info(arn,i,region,account_id)
           inventory_instances.append(inventory_object)
        save_as_file_parquet(inventory_instances,file_path,generate_parquet_prefix(__file__,region,account_id))
    return inventory_instances


