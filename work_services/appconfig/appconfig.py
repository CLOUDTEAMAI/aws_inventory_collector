from utils.utils import *
import boto3




def list_appconfig(file_path,session,region,time_generated,account):
    appconfig = session.client('appconfig',region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ","_")
    inventory_instances = []
    inventory = appconfig.list_applications()
    if len(inventory['Items']) != 0:
        for i in inventory['Items']:
           appId = i['Id']
           arn = f'arn:aws:appconfig:{region}:{account_id}:application/{appId}'
           inventory_object = extract_common_info(arn,i,region,account_id,time_generated,account_name)
           inventory_instances.append(inventory_object)
        save_as_file_parquet(inventory_instances,file_path,generate_parquet_prefix(__file__,region,account_id))
    # return inventory_instances




async def async_list_appconfig(file_path, session, region, time_generated):
    try:
        client_list = []
        clinet_account =  session[1].client('sts')
        account_id = clinet_account.get_caller_identity()["Account"]
        async with session[0].client('appconfig') as client_boto:
            paginator = client_boto.get_paginator('list_applications')
            async for page in paginator.paginate():
                for i in page['items']:
                    appId = i['Id']
                    arn = f'arn:aws:appconfig:{region}:{account_id}:application/{appId}'
                    inventory_object = extract_common_info(arn, i, region, account_id, time_generated)
                    client_list.append(inventory_object)
    except Exception as ex:
            print(ex)
    finally:
        if len(client_list) != 0:
                save_as_file_parquet(client_list, file_path, generate_parquet_prefix(__file__, region, account_id))