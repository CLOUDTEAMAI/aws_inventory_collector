import boto3
import concurrent.futures
from datetime import datetime, timedelta
import pandas as pd
import json
from cloudwatch_logic import *
import os 
from utils.utils import *




    
def list_ec2_snapshots(file_path,session,region):
    ec2 = session.client('ec2',region_name=region)
    sts = session.client('sts')
    account_id = sts.get_caller_identity()["Account"]
    ec2_instances = []
    snapshots_client = ec2.describe_snapshots(OwnerIds=['self'])
    if len(snapshots_client['Snapshots']) != 0:
        for i in snapshots_client['Snapshots']:
            if 'StartTime' in i:
                i['StartTime'] = i['StartTime'].isoformat()
            if 'RestoreExpiryTime' in i:
                i['RestoreExpiryTime'] = i['RestoreExpiryTime'].isoformat()
            arn = f"arn:aws:ec2:{region}::{i['SnapshotId']}"
            inventory_object = extract_common_info(arn,i,region,account_id)
            ec2_instances.append(inventory_object)
        save_as_file_parquet(ec2_instances,file_path,generate_parquet_prefix(__file__,region,account_id))
    return ec2_instances