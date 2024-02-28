import boto3
import concurrent.futures
from datetime import datetime, timedelta
import pandas as pd
import json
from cloudwatch_logic import *
import os 
from utils.utils import *

   

def list_rekognition(file_path,session,region):
    client = session.client('rekognition',region_name=region)
    sts = session.client('sts')
    account_id = sts.get_caller_identity()["Account"]
    client_list = client.list_collections()
    
    client_list = []
    if len(client_list['CollectionIds']) != 0:
        for i in client_list['CollectionIds']:
            describe_response = client.describe_collection(CollectionId=i)
            arn = describe_response['CollectionARN']
            client_object = extract_common_info(arn,describe_response,region,account_id)
            client_list.append(client_object)
        save_as_file_parquet(client_list,file_path,generate_parquet_prefix(__file__,region,account_id))
    return client_list


