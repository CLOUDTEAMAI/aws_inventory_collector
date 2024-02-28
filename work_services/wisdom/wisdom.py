import boto3
import concurrent.futures
from datetime import datetime, timedelta
import pandas as pd
import json
from utils.utils import *
import os 

cloudwatch = boto3.client('cloudwatch')
cost_explorer = boto3.client('ce')


def list_wisdom(file_path,session,region):
    try:
        wisdom = session.client('wisdom',region_name=region)
        sts = session.client('sts')
        account_id = sts.get_caller_identity()["Account"]
        inventory_instances = []
        inventory = wisdom.list_knowledge_bases()['knowledgeBaseSummaries']
        if len(inventory) != 0:
            for i in inventory:
                arn = i['knowledgeBaseArn']
                inventory_object = extract_common_info(arn,i,region,account_id)
                inventory_instances.append(inventory_object)
            save_as_file_parquet(inventory_instances,file_path,generate_parquet_prefix(__file__,region,account_id))
            return inventory_instances
    except Exception as error:
        #print(f'cannot bring data of wisdom {error}')
        print(f'{error}')


