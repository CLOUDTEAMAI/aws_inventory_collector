import boto3
import concurrent.futures
from datetime import datetime, timedelta
import pandas as pd
import json
from cloudwatch_logic import *
import os 
from utils.utils import *
import gc



def list_ec2_snapshots(file_path,session,region,time_generated,account):
    ec2 = session.client('ec2',region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ","_")
    ec2_instances = []
    snapshots_client = ec2.describe_snapshots(OwnerIds=['self'])
    if len(snapshots_client['Snapshots']) != 0:
        for i in snapshots_client['Snapshots']:
            if 'StartTime' in i:
                i['StartTime'] = i['StartTime'].isoformat()
            if 'RestoreExpiryTime' in i:
                i['RestoreExpiryTime'] = i['RestoreExpiryTime'].isoformat()
            arn = f"arn:aws:ec2:{region}::{i['SnapshotId']}"
            inventory_object = extract_common_info(arn,i,region,account_id,time_generated,account_name)
            ec2_instances.append(inventory_object)
        save_as_file_parquet(ec2_instances,file_path,generate_parquet_prefix(__file__,region,account_id))
    del ec2,snapshots_client
    gc.collect()
    # return ec2_instances



async def async_list_snapshot(file_path, session, region, time_generated):
    try:
        client_list = []
        # client_session = session[1].client('ec2')
        clinet_account =  session[1].client('sts')
        account_id = clinet_account.get_caller_identity()["Account"]
        async with session[0].client('ec2') as elastic:
            paginator = await elastic.describe_snapshots(OwnerIds=['self'])
            for page in paginator['Snapshots']:
                # for i in page['Addresses']:
                if 'StartTime' in page:
                    page['StartTime'] = page['StartTime'].isoformat()
                if 'RestoreExpiryTime' in page:
                    page['RestoreExpiryTime'] = page['RestoreExpiryTime'].isoformat()
                arn = f"arn:aws:ec2:{region}::{page['SnapshotId']}"
                inventory_object = extract_common_info(arn, page, region, account_id, time_generated)
                client_list.append(inventory_object)
    except Exception as ex:
            print(ex)
    finally:
        if len(client_list) != 0:
                save_as_file_parquet(client_list, file_path, generate_parquet_prefix(__file__, region, account_id))

