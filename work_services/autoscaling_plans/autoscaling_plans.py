import boto3
import concurrent.futures
from datetime import datetime, timedelta
import pandas as pd
import json
from cloudwatch_logic import *
import os 
from utils.utils import *

   

def list_autoscaling_plans(file_path,session,region,time_generated):
    client = session.client('autoscaling-plans',region_name=region)
    sts = session.client('sts')
    account_id = sts.get_caller_identity()["Account"]
    repositories = client.describe_scaling_plans()
    client_list = []
    if len(repositories['ScalingPlans']) != 0:
        for repo in repositories['ScalingPlans']:

            if 'StatusStartTime' in repo:
                repo['StatusStartTime'] = repo['StatusStartTime'].isoformat()

            if 'CreationTime' in repo:
                repo['CreationTime'] = repo['CreationTime'].isoformat()

                
            arn = repo['repositoryArn']
            client_object = extract_common_info(arn,repo,region,account_id,time_generated)
            client_list.append(client_object)
        save_as_file_parquet(client_list,file_path,generate_parquet_prefix(__file__,region,account_id))
    # return client_list


