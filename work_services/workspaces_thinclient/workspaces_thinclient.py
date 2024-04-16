import boto3
import concurrent.futures
from datetime import datetime, timedelta
import pandas as pd
import json
from utils.utils import *
import os 

# cloudwatch = boto3.client('cloudwatch')
# cost_explorer = boto3.client('ce')

def list_workspaces_thin_client(file_path,session,region,time_generated,account):
    work = session.client('workspaces-thin-client',region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ","_")
    inventory_instances = []
    inventory = work.list_devices()
    if len(inventory['devices']) != 0:
        for i in inventory['devices']:
            i['lastConnectedAt'] = i['lastConnectedAt'].isoformat()
            i['lastPostureAt'] = i['lastPostureAt'].isoformat()
            i['createdAt'] = i['createdAt'].isoformat()
            i['updatedAt'] = i['updatedAt'].isoformat()
            arn = i['arn']
            inventory_object = extract_common_info(arn,i,region,account_id,time_generated,account_name)
            inventory_instances.append(inventory_object)
        save_as_file_parquet(inventory_instances,file_path,generate_parquet_prefix(__file__,region,account_id))
    return inventory_instances


