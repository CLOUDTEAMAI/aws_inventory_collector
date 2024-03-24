import boto3
import concurrent.futures
from datetime import datetime, timedelta
import pandas as pd
import json
from cloudwatch_logic import *
import os 
from utils.utils import *

   

def list_networkfirewall(file_path,session,region):
    client = session.client('network-firewall',region_name=region)
    sts = session.client('sts')
    account_id = sts.get_caller_identity()["Account"]
    client_list = client.list_firewalls()
    client_list = []
    if len(client_list['Firewalls']) != 0:
        for i in client_list['Firewalls']:
            arn = i['FirewallArn']
            firewall_description = client.describe_firewall(FirewallArn=arn)
            client_object = extract_common_info(arn,firewall_description,region,account_id)
            client_list.append(client_object)
        save_as_file_parquet(client_list,file_path,generate_parquet_prefix(__file__,region,account_id))
    return client_list


