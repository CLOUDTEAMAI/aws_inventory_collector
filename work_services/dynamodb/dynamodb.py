import boto3
import concurrent.futures
from datetime import datetime, timedelta
import pandas as pd
import json
from cloudwatch_logic import *
import os 
from utils.utils import *

def list_dynamo(file_path,session,region,time_generated,account):
    dynamodb = session.client('dynamodb',region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ","_")
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
            object_client   = extract_common_info(arn,info_tables,region,account_id,time_generated,account_name)
            dynamodb_list.append(object_client)
        save_as_file_parquet(dynamodb_list,file_path,generate_parquet_prefix(__file__,region,account_id))
    return dynamodb_list







async def async_list_dynamo(file_path, session, region, time_generated):
    try:
        client_list = []
        clinet_account =  session[1].client('sts')
        clinet =  session[1].client('dynamodb')
        account_id = clinet_account.get_caller_identity()["Account"]
        async with session[0].client('dynamodb') as elastic:
            paginator = elastic.get_paginator('list_tables')
            async for page in paginator.paginate():
                for i in page['TableNames']:
                    info_tables     = clinet.describe_table(TableName=i)['Table']
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
                    inventory_object = extract_common_info(arn, info_tables, region, account_id, time_generated)
                    client_list.append(inventory_object)
    except Exception as ex:
            print(ex)
    finally:
        if len(client_list) != 0:
                save_as_file_parquet(client_list, file_path, generate_parquet_prefix(__file__, region, account_id))



