import boto3
import concurrent.futures
from datetime import datetime, timedelta
import pandas as pd
import json
from cloudwatch_logic import *
import os 
from utils.utils import *

   

def list_storagegateway(file_path,session,region,time_generated):
    client = session.client('storagegateway',region_name=region)
    sts = session.client('sts')
    account_id = sts.get_caller_identity()["Account"]
    client_list_object = client.list_gateways()
    client_list = []
    if len(client_list_object['InternetGateways']) != 0:
        for i in client_list_object['InternetGateways']:
            arn = i['GatewayARN']
            client_object = extract_common_info(arn,i,region,account_id,time_generated)
            client_list.append(client_object)
        save_as_file_parquet(client_list,file_path,generate_parquet_prefix(__file__,region,account_id))
    return client_list


