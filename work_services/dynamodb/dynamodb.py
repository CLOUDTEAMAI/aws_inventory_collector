import boto3
import concurrent.futures
from datetime import datetime, timedelta
import pandas as pd
import json
from cloudwatch_logic import *
import os 
from utils.utils import *

def list_dynamo(file_path,session,region):
    dynamodb = session.client('dynamodb',region_name=region)
    sts = session.client('sts')
    account_id = sts.get_caller_identity()["Account"]
    dynamodb_tables = dynamodb.list_tables()
    dynamodb_list   = []
    if len(dynamodb_tables['TableNames']) != 0:
        for i in dynamodb_tables['TableNames']:


            info_tables     = dynamodb.describe_table(TableName=i)['Table']
            if 'CreationDateTime' in info_tables:
                info_tables['CreationDateTime'] = info_tables['CreationDateTime'].isoformat()
            
            if 'ProvisionedThroughput' in info_tables:
                if 'LastIncreaseDateTime' in info_tables['ProvisionedThroughput']:
                    info_tables['ProvisionedThroughput']['LastIncreaseDateTime'] = info_tables['ProvisionedThroughput']['LastIncreaseDateTime'].isoformat()
                if 'LastDecreaseDateTime' in info_tables['ProvisionedThroughput']:
                    info_tables['ProvisionedThroughput']['LastDecreaseDateTime'] = info_tables['ProvisionedThroughput']['LastDecreaseDateTime'].isoformat()


            if 'BillingModeSummary' in info_tables:
                info_tables['BillingModeSummary']['LastUpdateToPayPerRequestDateTime'] =info_tables['BillingModeSummary']['LastUpdateToPayPerRequestDateTime'].isoformat()
            arn             = info_tables['TableArn']
            object_client   = extract_common_info(arn,info_tables,region,account_id)
            dynamodb_list.append(object_client)
        save_as_file_parquet(dynamodb_list,file_path,generate_parquet_prefix(__file__,region,account_id))
    return dynamodb_list









