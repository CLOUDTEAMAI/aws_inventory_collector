import boto3
import concurrent.futures
from datetime import datetime, timedelta
import pandas as pd
import json
from utils.utils import *
import os 

cloudwatch = boto3.client('cloudwatch')
cost_explorer = boto3.client('ce')

def list_workspaces_thin_client(file_path,session,region):
    work = session.client('workspaces-thin-client',region_name=region)
    sts = session.client('sts')
    account_id = sts.get_caller_identity()["Account"]
    inventory_instances = []
    inventory = work.list_devices()
    if len(inventory['devices']) != 0:
        for i in inventory['devices']:
            arn = i['arn']
            inventory_object = extract_common_info(arn,i,region,account_id)
            inventory_instances.append(inventory_object)
        save_as_file_parquet(inventory_instances,file_path,generate_parquet_prefix(__file__,region,account_id))
    return inventory_instances


