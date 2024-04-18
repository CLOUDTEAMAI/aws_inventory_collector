import boto3
import concurrent.futures
from datetime import datetime, timedelta
import pandas as pd
import json
from cloudwatch_logic import *
import os 
from utils.utils import *
import gc
from memory_profiler import profile

# cloudwatch = boto3.client('cloudwatch')
# cost_explorer = boto3.client('ce')

path_json_file =  os.path.dirname(os.path.abspath(__file__))
with open(f'{path_json_file}/metric.json','r') as file:
    json_file = json.load(file)


def list_ec2(file_path,session,region,time_generated,account):
    ec2 = session.client('ec2',region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ","_")
    ec2_instances = []
    vm = ec2.describe_instances()
    if len(vm['Reservations']) != 0:
        for i in vm['Reservations']:
            for instance in i['Instances']:
                instance['LaunchTime'] = instance['LaunchTime'].isoformat()
                if 'UsageOperationUpdateTime' in instance:
                    instance['UsageOperationUpdateTime'] = instance['UsageOperationUpdateTime'].isoformat()

                for device in instance.get('BlockDeviceMappings', []):
                    if 'Ebs' in device and 'AttachTime' in device['Ebs']:
                        device['Ebs']['AttachTime'] = device['Ebs']['AttachTime'].isoformat()


                for interface in instance.get('NetworkInterfaces', []):
                    if 'Attachment' in interface and 'AttachTime' in interface['Attachment']:
                        interface['Attachment']['AttachTime'] = interface['Attachment']['AttachTime'].isoformat()

                arn = f"arn:aws:ec2:{region}:{account_id}:instance/{instance['InstanceId']}"
                inventory_object = extract_common_info(arn,i,region,account_id,time_generated,account_name)
            ec2_instances.append(inventory_object)
        save_as_file_parquet(ec2_instances,file_path,generate_parquet_prefix(__file__,region,account_id))
    del ec2,vm
    gc.collect() 
    # return ec2_instances


async def async_list_ec2(file_path, session, region, time_generated):
    try:
        client_list = []
        clinet_account =  session[1].client('sts')
        account_id = clinet_account.get_caller_identity()["Account"]
        async with session[0].client('ec2') as ec2:
            paginator = ec2.get_paginator('describe_instances')
            async for page in paginator.paginate():
                for reservation in page['Reservations']:
                    for instance in reservation['Instances']:
                        arn = f"arn:aws:ec2:{region}:{account_id}:instance/{instance['InstanceId']}"
                        inventory_object = extract_common_info(arn, instance, region, account_id, time_generated)
                        client_list.append(inventory_object)
    except Exception as ex:
            print(ex)
    finally:
        if len(client_list) != 0:
                save_as_file_parquet(client_list, file_path, generate_parquet_prefix(__file__, region, account_id))



def metrics_utill_ec2(file_path,session,region,account):
    instance_ids = []
    if session is not None:
        ec2 = session.client('ec2',region_name=region)
        try:
            account_id = account['account_id']
            ec2_instances = []
            vm = ec2.describe_instances()
            for i in vm['Reservations']:
                for instance in i['Instances']:
                    instance_ids.append(instance['InstanceId'])
        except Exception as ex:
            print(ex)
    

    if len(instance_ids) != 0:
        for j in instance_ids:
            for metric in json_file['metrics']:
            # item_object = extract_common_info()
                item = get_resource_utilization_metric(session,j,metric['metricname'],metric['statistics'],metric['unit'],metric['nameDimensions'],metric['serviceType'],account)
                ec2_instances.append(item)
        save_as_file_parquet(ec2_instances,file_path,generate_parquet_prefix(__file__,region,f'{account_id}-metrics'))


    #         item = get_resource_utilization(session,instance_ids,metric['metricname'],metric['statistics'],metric['unit'],metric['nameDimensions'],metric['serviceType'])
    #         ec2_instances.append(item)
    #     for z in ec2_instances:
    #         if isinstance(z, dict):
    #             for key, value in z.items():
    #     resources_list = [value for key, value in ec2_instances.items() if value is not None]
    #     json_output = json.dumps(resources_list, indent=4)
    #     save_as_file_parquet_metric(json_output,file_path,generate_parquet_prefix(__file__,region,f'{account_id}-metrics'))






# ec2,metricname='CPUUtilization',name='InstanceId',satistics=['Average','Maximum'],unit='Percent')
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
    

#  Metric that throw error still need to debug those
# {"metricname":"CPUCreditUsage"                  ,"statistics":["Average","Sum","Minimum","Maximum"],"unit":"Credits (vCPU-minutes)"},
# {"metricname":"CPUCreditBalance"                ,"statistics":["Average","Sum","Minimum","Maximum"],"unit":"Credits (vCPU-minutes)"},
# {"metricname":"CPUSurplusCreditBalance"         ,"statistics":["Average","Sum","Minimum","Maximum"],"unit":"Credits (vCPU-minutes)"},
# {"metricname":"CPUSurplusCreditsCharged"        ,"statistics":["Average","Sum","Minimum","Maximum"],"unit":"Credits (vCPU-minutes)"},
# {"metricname":"MetadataNoToken"                 ,"statistics":["Percentiles","Sum"],"unit":"Count"}

# inventory_object = {
#                 'arn'           : f"arn:aws:ec2:{region}:{account_id}:instance/{instance['InstanceId']}",
#                 'account_id'    : account_id,
#                 'region'        : region,
#                 'properties'    : i,
#                 'timegenerated' : today
#             }