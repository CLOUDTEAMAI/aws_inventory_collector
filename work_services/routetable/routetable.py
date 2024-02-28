import boto3
import concurrent.futures
from datetime import datetime, timedelta
import pandas as pd
import json
from cloudwatch_logic import *
import os 
from utils.utils import *

   

def list_routetable(file_path,session,region):
    client = session.client('ec2',region_name=region)
    sts = session.client('sts')
    account_id = sts.get_caller_identity()["Account"]
    client_list_object = client.describe_route_tables()
    
    client_list = []
    if len(client_list_object['RouteTables']) != 0:
        for i in client_list_object['RouteTables']:
            arn = f"arn:aws:ec2:{region}:{account_id}:route-table/{i['RouteTableId']}"
            client_object = extract_common_info(arn,i,region,account_id)
            client_list.append(client_object)
        save_as_file_parquet(client_list,file_path,generate_parquet_prefix(__file__,region,account_id))
    return client_list


