import boto3
import concurrent.futures
from datetime import datetime, timedelta
import pandas as pd
import json
from cloudwatch_logic import *
import os 
from utils.utils import *


def list_efs_file_systems(file_path,session,region,time_generated,account):
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ","_")
    efs = session.client('efs',region_name=region)
    file_systems = efs.describe_file_systems()
    efs_instances = []
    if len(file_systems['FileSystems']) != 0:
        for fs in file_systems['FileSystems']:
            if 'CreationTime' in fs:
                fs['CreationTime'] = fs['CreationTime'].isoformat()
            if 'SizeInBytes' in fs:
                fs['SizeInBytes']['Timestamp'] = fs['SizeInBytes']['Timestamp'].isoformat()
            arn = f"arn:aws:elasticfilesystem:{region}:{account_id}:file-system/{fs['FileSystemId']}"
            efs_object = extract_common_info(arn,fs,region,account_id,time_generated,account_name)
            efs_instances.append(efs_object)
        save_as_file_parquet(efs_instances,file_path,generate_parquet_prefix(__file__,region,account_id))
        # return efs_instances
    

async def async_list_efs_file_systems(file_path, session, region, time_generated):
    try:
        client_list = []
        clinet_account =  session[1].client('sts')
        account_id = clinet_account.get_caller_identity()["Account"]
        async with session[0].client('efs') as elastic:
            paginator = elastic.get_paginator('describe_file_systems')
            async for page in paginator.paginate():
                for i in page['FileSystems']:
                    if 'CreationTime' in i:
                        i['CreationTime'] = i['CreationTime'].isoformat()
                    if 'SizeInBytes' in i:
                        i['SizeInBytes']['Timestamp'] = i['SizeInBytes']['Timestamp'].isoformat()
                    arn = f"arn:aws:elasticfilesystem:{region}:{account_id}:file-system/{i['FileSystemId']}"
                    inventory_object = extract_common_info(arn, i, region, account_id, time_generated)
                    client_list.append(inventory_object)
    except Exception as ex:
            print(ex)
    finally:
        if len(client_list) != 0:
                save_as_file_parquet(client_list, file_path, generate_parquet_prefix(__file__, region, account_id))

