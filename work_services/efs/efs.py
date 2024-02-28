import boto3
import concurrent.futures
from datetime import datetime, timedelta
import pandas as pd
import json
from cloudwatch_logic import *
import os 
from utils.utils import *


def list_efs_file_systems(file_path,session,region):
    sts = session.client('sts')
    account_id = sts.get_caller_identity()["Account"]
    efs = session.client('efs',region_name=region)
    file_systems = efs.describe_file_systems()
    if len(file_systems['FileSystems']) != 0:
        efs_instances = []
        for fs in file_systems['FileSystems']:
            arn = f"arn:aws:elasticfilesystem:{region}:{account_id}:file-system/{fs['FileSystemId']}"
            efs_object = extract_common_info(arn,fs,region,account_id)
            efs_instances.append(efs_object)
        save_as_file_parquet(efs_instances,file_path,generate_parquet_prefix(__file__,region,account_id))
        return efs_instances