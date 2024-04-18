import boto3
from utils.utils import *



def list_appflow(file_path,session,region,time_generated,account):
    appmesh = session.client('appflow',region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ","_")
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
           inventory_object = extract_common_info(arn,i,region,account_id,time_generated,account_name)
           inventory_instances.append(inventory_object)
        save_as_file_parquet(inventory_instances,file_path,generate_parquet_prefix(__file__,region,account_id))
    # return inventory_instances


async def async_list_appflow(file_path, session, region, time_generated):
    try:
        client_list = []
        clinet_account =  session[1].client('sts')
        account_id = clinet_account.get_caller_identity()["Account"]
        async with session[0].client('appflow') as client_boto:
            paginator = client_boto.get_paginator('get_apis')
            async for page in paginator.paginate():
                    for i in page['flows']:
                        i['createdAt'] = i['createdAt'].isoformat()
                    if 'lastUpdatedAt' in i:
                        i['lastUpdatedAt'] = i['lastUpdatedAt'].isoformat()
                    if 'mostRecentExecutionTime' in i['lastRunExecutionDetails']:
                        i['lastRunExecutionDetails']['mostRecentExecutionTime'] = i['lastRunExecutionDetails']['mostRecentExecutionTime'].isoformat()
                    flow_name = i['flowName']
                    arn = f"arn:aws:appflow:{region}:{account_id}:flow/{flow_name}"
                    inventory_object = extract_common_info(arn, i, region, account_id, time_generated)
                    client_list.append(inventory_object)
    except Exception as ex:
            print(ex)
    finally:
        if len(client_list) != 0:
                save_as_file_parquet(client_list, file_path, generate_parquet_prefix(__file__, region, account_id))