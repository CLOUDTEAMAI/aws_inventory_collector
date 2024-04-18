import boto3
from utils.utils import *



def list_apigatewayv2(file_path,session,region,time_generated,account):
    apigatewayv2_client = session.client('apigatewayv2',region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ","_")
    inventory_instances = []
    inventory = apigatewayv2_client.get_apis()
    if len(inventory['Items']) != 0:
        for i in inventory['Items']:
           if 'CreatedDate' in i:
               i['CreatedDate'] = i['CreatedDate'].isoformat()
           api_id = i['ApiId']
           arn = f"arn:aws:apigateway:{region}::/apis/{api_id}"
           inventory_object = extract_common_info(arn,i,region,account_id,time_generated,account_name)
           inventory_instances.append(inventory_object)
        save_as_file_parquet(inventory_instances,file_path,generate_parquet_prefix(__file__,region,account_id))
    # return inventory_instances



async def async_list_apigatewayv2(file_path, session, region, time_generated):
    try:
        client_list = []
        clinet_account =  session[1].client('sts')
        account_id = clinet_account.get_caller_identity()["Account"]
        async with session[0].client('apigatewayv2') as client_boto:
            paginator = client_boto.get_paginator('get_apis')
            async for page in paginator.paginate():
                for i in page['Items']:
                    if 'CreatedDate' in i:
                        i['CreatedDate'] = i['CreatedDate'].isoformat()
                    api_id = i['ApiId']
                    arn = f"arn:aws:apigateway:{region}::/apis/{api_id}"
                    inventory_object = extract_common_info(arn, i, region, account_id, time_generated)
                    client_list.append(inventory_object)
    except Exception as ex:
            print(ex)
    finally:
        if len(client_list) != 0:
                save_as_file_parquet(client_list, file_path, generate_parquet_prefix(__file__, region, account_id))