import boto3
import concurrent.futures
from datetime import datetime, timedelta
import pandas as pd
import json
from utils.utils import *
import os 

# cloudwatch = boto3.client('cloudwatch')
# cost_explorer = boto3.client('ce')
# path_json_file = os.path.join(os.getcwd(), 'rds/metric.json')

# with open(path_json_file,'r') as file:
#     json_file = json.load(file)



def list_s3_buckets(file_path,session,region=None):
    sts = session.client('sts')
    account_id = sts.get_caller_identity()["Account"]
    s3 = session.client('s3')
    s3_instances = []
    buckets = s3.list_buckets()
    for i in buckets['Buckets']:
        arn = f"arn:aws:s3:::{i['Name']}"
        s3_object = extract_common_info(arn,i,s3.get_bucket_location(Bucket=i['Name'])['LocationConstraint'],account_id)
        s3_instances.append(s3_object)
    save_as_file_parquet(s3_instances,file_path,generate_parquet_prefix(__file__,'global',account_id))
    
    return s3_instances


