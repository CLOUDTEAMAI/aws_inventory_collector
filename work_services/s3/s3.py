import boto3
import concurrent.futures
from datetime import datetime, timedelta
import pandas as pd
import json
from utils.utils import *
import os 
from cloudwatch_logic import * 

# cloudwatch = boto3.client('cloudwatch')
# cost_explorer = boto3.client('ce')
# path_json_file = os.path.join(os.getcwd(), 'rds/metric.json')

# with open(path_json_file,'r') as file:
#     json_file = json.load(file)

path_json_file =  os.path.dirname(os.path.abspath(__file__))
file = open(f'{path_json_file}/metric.json','r')
json_file = json.load(file)
storage_types = [
        'StandardStorage'
        # ,
        # {'Name':'StorageType','Value':'IntelligentTieringStorage'},
        # {'Name':'StorageType','Value':'StandardIAStorage'},
        # {'Name':'StorageType','Value':'OneZoneIAStorage'},
        # {'Name':'StorageType','Value':'GlacierStorage'},
        # {'Name':'StorageType','Value':'DeepArchiveStorage'}
    ]

def list_s3_buckets(file_path,session,region=None,time_generated=None,account=None):
    if session:
        account_id = account['account_id']
        account_name = str(account['account_name']).replace(" ","_")
        s3 = session.client('s3')
        s3_instances = []
        try:
            buckets = s3.list_buckets()
        except Exception as ex:
            buckets = []
        if len(buckets) != 0:
            for i in buckets['Buckets']:
                i['CreationDate'] = i['CreationDate'].isoformat()
                arn = f"arn:aws:s3:::{i['Name']}"
                try:
                    bucket_location = s3.get_bucket_location(Bucket=i['Name'])['LocationConstraint']
                    s3_object = extract_common_info(arn,i,bucket_location,account_id,time_generated,account_name)
                    s3_instances.append(s3_object)
                except Exception as ex:
                    print(ex)
                    s3_object = extract_common_info(arn,i,None,account_id,time_generated,account_name)
                    s3_instances.append(s3_object)
            save_as_file_parquet(s3_instances,file_path,generate_parquet_prefix(__file__,'global',account_id))
            return None
        
        # return s3_instances





async def async_list_s3_buket(file_path, session, time_generated):
    try:
        client_list = []
        clinet_account =  session[1].client('sts')
        client =  session[1].client('s3')
        account_id = clinet_account.get_caller_identity()["Account"]
        async with session[0].client('s3') as client_boto:
            paginator = await client_boto.list_buckets()
            for i in paginator['Buckets']:
                i['CreationDate'] = i['CreationDate'].isoformat()
                arn = f"arn:aws:s3:::{i['Name']}"
                region = client.get_bucket_location(Bucket=i['Name'])['LocationConstraint']
                # bucket_size,storage_tiers =  get_bucket_details(client,i['Name'])
                # i["BucketSize"] = bucket_size
                # i["StorageTypes"] = storage_tiers
                inventory_object = extract_common_info(arn, i, region, account_id, time_generated)
                client_list.append(inventory_object)
    except Exception as ex:
            print(ex)
    finally:
        if len(client_list) != 0:
                save_as_file_parquet(client_list, file_path, generate_parquet_prefix(__file__, 'global', account_id))




def metrics_utill_s3(file_path,session):
    client = session.client('s3')
    sts = session.client('sts')
    account_id = sts.get_caller_identity()["Account"]
    client_instances = []
    client_instances_metrics = []
    client_items = client.list_buckets()
    for i in client_items['Buckets']:
      client_instances.append(i['Name'])
    if len(client_instances) != 0:
        for j in client_instances:
            for metric in json_file['metrics']:
            # item_object = extract_common_info()
                item = get_resource_utilization_metric(session,j,metric['metricname'],metric['statistics'],metric['unit'],metric['nameDimensions'],metric['serviceType'])
                client_instances_metrics.append(item)
        save_as_file_parquet(client_instances_metrics,file_path,generate_parquet_prefix(__file__,'Metric',f'{account_id}-metrics'))




def metrics_s3(file_path,session,account):
    client = session.client('s3')
    sts = session.client('sts')
    account_id = sts.get_caller_identity()["Account"]
    client_instances = []
    client_instances_metrics = []
    client_items = client.list_buckets()
    for i in client_items['Buckets']:
      client_instances.append(i['Name'])
    if len(client_instances) != 0:
        for j in client_instances:
            for metric in json_file['metrics']:
                for k in storage_types:
            # item_object = extract_common_info()
                    item = get_resource_utilization_metric(
                        session=session,ids=j,
                        metricname=metric['metricname'],statistics=metric['statistics'],
                        unit=metric['unit'],name_dimensions=metric['nameDimensions'],
                        serviceType=metric['serviceType'],storageType=k,account=account)
                    client_instances_metrics.append(item)
        save_as_file_parquet(client_instances_metrics,file_path,generate_parquet_prefix(__file__,'Metric',f'{account_id}-metrics'))


# def get_bucket_details(s3_client, bucket_name):
#     # paginator = s3_client.get_paginator('list_objects_v2')
#     paginator = s3_client.list_objects_v2(Bucket=bucket_name)
#     bucket_size = 0
#     storage_tiers = set()
#     try:
#         if len(paginator['Contents']) != 0:
#             for obj in paginator['Contents']:
#                 if 'Contents' in obj:
#                     bucket_size += obj['Size']
#                     storage_tiers.add(obj['StorageClass'])

#         # async for page in paginator.paginate(Bucket=bucket_name):
#         #     if 'Contents' in page:
#         #         for obj in page['Contents']:
#         #             bucket_size += obj['Size']
#         #             storage_tiers.add(obj['StorageClass'])

#         return bucket_size/(1024 ** 3), list(storage_tiers)
#     except Exception as ex:
#          print(ex)