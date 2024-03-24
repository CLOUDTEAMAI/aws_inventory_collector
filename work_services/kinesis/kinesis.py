import boto3
import concurrent.futures
from datetime import datetime, timedelta
import pandas as pd
import json
from cloudwatch_logic import *
import os 
from utils.utils import *

   

def list_kinesis(file_path,session,region):
    client = session.client('kinesis',region_name=region)
    sts = session.client('sts')
    account_id = sts.get_caller_identity()["Account"]
    client_list_stream = client.list_streams()
    
    client_list = []
    if len(client_list_stream['StreamNames']) != 0:
        for i in client_list_stream['StreamNames']:
            response_data = client.describe_stream(StreamName=i)['StreamDescription']
            if 'StreamCreationTimestamp' in response_data:
                response_data['StreamCreationTimestamp'] = response_data['StreamCreationTimestamp'].isoformat()
            arn = response_data['StreamARN']
            client_object = extract_common_info(arn,response_data,region,account_id)
            client_list.append(client_object)
        save_as_file_parquet(client_list,file_path,generate_parquet_prefix(__file__,region,account_id))
    return client_list


