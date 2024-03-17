import boto3
import concurrent.futures
from datetime import datetime, timedelta
import pandas as pd
import json
from utils.utils import *
import os 

# cloudwatch = boto3.client('cloudwatch')
# cost_explorer = boto3.client('ce')


def list_sqs(file_path,session,region):
    sts = session.client('sts')
    account_id = sts.get_caller_identity()["Account"]
    sqs = session.client('sqs',region_name=region)
    inventory_instances = []
    inventory = sqs.list_queues()
    if  'QueueUrls' in inventory:
        if len(inventory['QueueUrls']) != 0:
            for i in inventory['QueueUrls']:
                attributes       = sqs.get_queue_attributes(QueueUrl=i, AttributeNames=['All'])['Attributes']
                arn = attributes['QueueArn']
                inventory_object = extract_common_info(arn,attributes,region,account_id)
                inventory_instances.append(inventory_object)
            save_as_file_parquet(inventory_instances,file_path,generate_parquet_prefix(__file__,region,account_id))
    return inventory_instances


