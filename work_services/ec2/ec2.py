import boto3
import concurrent.futures
from datetime import datetime, timedelta
import pandas as pd
import json
from cloudwatch_logic import *
import os 
from utils.utils import *


cloudwatch = boto3.client('cloudwatch')

cost_explorer = boto3.client('ce')
# path_json_file = os.path.join(os.getcwd(), 'ec2/metric.json')
# with open(path_json_file,'r') as file:
#     json_file = json.load(file)


    
def list_ec2(file_path,session,region):
    ec2 = session.client('ec2',region_name=region)
    sts = session.client('sts')
    account_id = sts.get_caller_identity()["Account"]
    ec2_instances = []
    # session = boto3.session.Session()
    vm = ec2.describe_instances()
    if len(vm['Reservations']) != 0:
        for i in vm['Reservations']:
            for instance in i['Instances']:
                arn = f"arn:aws:ec2:{region}:{account_id}:instance/{instance['InstanceId']}"
                inventory_object = extract_common_info(arn,i,region,account_id)
            ec2_instances.append(inventory_object)
        save_as_file_parquet(ec2_instances,file_path,generate_parquet_prefix(__file__,region,account_id))
    return ec2_instances








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