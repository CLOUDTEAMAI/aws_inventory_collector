import boto3
import concurrent.futures
from datetime import datetime, timedelta
import pandas as pd
import json
from cloudwatch_logic import *
import os 
from utils.utils import *


# cloudwatch = boto3.client('cloudwatch')

# cost_explorer = boto3.client('ce')
# path_json_file = os.path.join(os.getcwd(), 'ec2/metric.json')
# with open(path_json_file,'r') as file:
#     json_file = json.load(file)


def list_eks(file_path,session,region,time_generated,account):
    eks = session.client('eks',region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ","_")
    eks_instances = []
    eks_clusters = eks.list_clusters()
    if len(eks_clusters['clusters']) != 0:
        for i in eks_clusters['clusters']:
            eks_describe = eks.describe_cluster(name=i)['cluster']
            if 'createdAt' in eks_describe:
                eks_describe['createdAt'] = eks_describe['createdAt'].isoformat()
            
            if 'connectorConfig' in eks_describe:
                eks_describe['connectorConfig']['activationExpiry'] = eks_describe['connectorConfig']['activationExpiry'].isoformat()
            
            arn = f"{eks_describe['arn']}"
            inventory_object = extract_common_info(arn,eks_describe,region,account_id,time_generated,account_name)
            eks_instances.append(inventory_object)
        save_as_file_parquet(eks_instances,file_path,generate_parquet_prefix(__file__,region,account_id))
    # return eks_instances



async def async_list_eks(file_path, session, region, time_generated):
    try:
        client_list = []
        eks_client = session[1].client('eks',region_name=region)
        clinet_account =  session[1].client('sts')
        account_id = clinet_account.get_caller_identity()["Account"]
        async with session[0].client('eks') as boto3_client:
            paginator = boto3_client.get_paginator('list_clusters')
            async for page in paginator.paginate():
                for i in page['clusters']:
                    eks_describe = eks_client.describe_cluster(name=i)['cluster']
                    if 'createdAt' in eks_describe:
                        eks_describe['createdAt'] = eks_describe['createdAt'].isoformat()
                    if 'connectorConfig' in eks_describe:
                        eks_describe['connectorConfig']['activationExpiry'] = eks_describe['connectorConfig']['activationExpiry'].isoformat()
                    arn = f"{eks_describe['arn']}"
                    inventory_object = extract_common_info(arn, eks_describe, region, account_id, time_generated)
                    client_list.append(inventory_object)
    except Exception as ex:
            print(ex)
    finally:
        if len(client_list) != 0:
                save_as_file_parquet(client_list, file_path, generate_parquet_prefix(__file__, region, account_id))




# def ec2_covrage(ec2):
#     instance_coverage = []
#     end_time = datetime.utcnow()
#     start_time = end_time - timedelta(days=30)
#     time_period = {
#         'Start': start_time.strftime('%Y-%m-%d'),
#         'End': end_time.strftime('%Y-%m-%d')
#     }
#     coverage_response = cost_explorer.get_reservation_coverage(
#         TimePeriod=time_period,
#         Granularity='MONTHLY',
#         Metrics=["Hour"],
#     )
#     # savings_plans_utilization = cost_explorer.get_savings_plans_utilization(TimePeriod=time_period,Granularity='DAILY')
#     # print(time_period)
#     # savings_plans_utilization_detailes = cost_explorer.get_savings_plans_utilization_details(TimePeriod=time_period)
   
#     print(coverage_response)

#     for reservation in ec2:
#         for instance in reservation['Instances']:
#             instance_id = instance['InstanceId']
#             print(f"Checking coverage for Instance ID: {instance_id}")
#             instance_coverage.append(instance_id)

#     return instance_coverage

# def ec2_utiliztion(ec2):
#   ec2_instance_metric = []
#   for instance in json_file['metrics']:
#       print(instance['metricname'])
#       ec2_instance_metric.append(get_resource_utilization_parallel(ec2,metricname=instance['metricname'],name='InstanceId',statistics=instance['statistics'],unit=instance['unit']))
#   return  ec2_instance_metric #get_resource_utilization(ec2,metricname='CPUUtilization',name='InstanceId',satistics=['Average','Maximum'],unit='Percent')
    

