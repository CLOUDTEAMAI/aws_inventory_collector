import boto3
import concurrent.futures
from datetime import datetime, timedelta
import pandas as pd
import json
from cloudwatch_logic import *
import os 
from utils.utils import *

   

def list_eip(file_path,session,region,time_generated,account):
    client = session.client('ec2',region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ","_")
    client_list_object = client.describe_addresses()
    client_list = []
    if len(client_list_object['Addresses']) != 0:
        for i in client_list_object['Addresses']:
            arn = f"arn:aws:ec2:{region}:{account_id}:eip/{i['AllocationId']}"
            client_object = extract_common_info(arn,i,region,account_id,time_generated,account_name)
            client_list.append(client_object)
        save_as_file_parquet(client_list,file_path,generate_parquet_prefix(__file__,region,account_id))
    # return client_list


async def async_list_eip(file_path, session, region, time_generated):
    try:
        client_list = []
        # client_session = session[1].client('ec2')
        clinet_account =  session[1].client('sts')
        account_id = clinet_account.get_caller_identity()["Account"]
        async with session[0].client('ec2') as elastic:
            paginator = await elastic.describe_addresses()
            for page in paginator['Addresses']:
                # for i in page['Addresses']:
                arn = f"arn:aws:ec2:{region}:{account_id}:eip/{page['AllocationId']}"
                inventory_object = extract_common_info(arn, page, region, account_id, time_generated)
                client_list.append(inventory_object)
    except Exception as ex:
            print(ex)
    finally:
        if len(client_list) != 0:
                save_as_file_parquet(client_list, file_path, generate_parquet_prefix(__file__, region, account_id))




