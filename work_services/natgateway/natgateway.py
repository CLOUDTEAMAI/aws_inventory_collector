import boto3
import concurrent.futures
from datetime import datetime, timedelta
import pandas as pd
import json
from cloudwatch_logic import *
import os 
from utils.utils import *

   

def list_natgateway(file_path,session,region):
    client = session.client('ec2',region_name=region)
    sts = session.client('sts')
    account_id = sts.get_caller_identity()["Account"]
    client_list_object = client.describe_nat_gateways()
    client_list = []
    if len(client_list_object['NatGateways']) != 0:
        for i in client_list_object['NatGateways']:
            if 'CreateTime' in i:
                i['CreateTime'] = i['CreateTime'].isoformat()
            if 'DeleteTime' in i:
                i['DeleteTime'] = i['DeleteTime'].isoformat()
            if 'ProvisionedBandwidth' in i:
                i['ProvisionedBandwidth']['ProvisionTime']  = i['ProvisionedBandwidth']['ProvisionTime'].isoformat()
                i['ProvisionedBandwidth']['RequestTime']    = i['ProvisionedBandwidth']['RequestTime'].isoformat()

            
            arn = f"arn:aws:ec2:{region}:{account_id}:natgateway/{i['NatGatewayId']}"
            client_object = extract_common_info(arn,i,region,account_id)
            client_list.append(client_object)
        save_as_file_parquet(client_list,file_path,generate_parquet_prefix(__file__,region,account_id))
    return client_list


