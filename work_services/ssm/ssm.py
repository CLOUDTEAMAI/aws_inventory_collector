import boto3
import concurrent.futures
from datetime import datetime, timedelta
import pandas as pd
import json
from cloudwatch_logic import *
import os 
from utils.utils import *

   

def list_ssm(file_path,session,region,time_generated,account):
    client = session.client('ssm',region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ","_")
    client_list_object = client.describe_parameters()
    client_list = []
    if len(client_list_object['Parameters']) != 0:
        for i in client_list_object['Parameters']:
            i['LastModifiedDate'] = i['LastModifiedDate'].isoformat()
            arn = i['ARN']
            client_object = extract_common_info(arn,i,region,account_id,time_generated,account_name)
            client_list.append(client_object)
        save_as_file_parquet(client_list,file_path,generate_parquet_prefix(__file__,region,account_id))
    # return client_list


