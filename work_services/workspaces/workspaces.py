import boto3
import concurrent.futures
from datetime import datetime, timedelta
import pandas as pd
import json
from utils.utils import *
import os 

# cloudwatch = boto3.client('cloudwatch')
# cost_explorer = boto3.client('ce')

def list_workspaces(file_path,session,region,time_generated,account):
    work = session.client('workspaces',region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ","_")
    inventory_instances = []
    inventory = work.describe_workspaces()
    if len(inventory['Workspaces']) != 0:
        for i in inventory['Workspaces']:
            if 'StandbyWorkspacesProperties' in i:
                i['RecoverySnapshotTime'] = i['RecoverySnapshotTime'].isoformat()
            if 'DataReplicationSettings' in i:
                i['DataReplicationSettings'] = i['DataReplicationSettings'].isoformat()
            arn = f"arn:aws:workspaces:{region}:{account_id}:workspace/{i['WorkspaceId']}"
            inventory_object = extract_common_info(arn,i,region,account_id,time_generated,account_name)
            inventory_instances.append(inventory_object)
        save_as_file_parquet(inventory_instances,file_path,generate_parquet_prefix(__file__,region,account_id))
    # return inventory_instances


