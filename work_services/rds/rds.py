import boto3
import concurrent.futures
from datetime import datetime, timedelta
import pandas as pd
import json
from utils.utils import *
import os 
from cloudwatch_logic import *

cloudwatch = boto3.client('cloudwatch')
cost_explorer = boto3.client('ce')
path_json_file = os.path.join(os.getcwd(), 'rds/metric.json')

# with open(path_json_file,'r') as file:
#     json_file = json.load(file)



def list_rds(file_path,session,region):
    rds = session.client('rds',region_name=region)
    sts = session.client('sts')
    account_id = sts.get_caller_identity()["Account"]
    rds_instances = []
    rds_list = rds.describe_db_instances()
    if len(rds_list['DBInstances']) != 0:
        for i in rds_list['DBInstances']:
            if 'InstanceCreateTime' in i:
                i['InstanceCreateTime'] = i['InstanceCreateTime'].isoformat()
            if 'LatestRestorableTime' in i:
                i['LatestRestorableTime'] = i['LatestRestorableTime'].isoformat()
            if 'ValidTill' in i['CertificateDetails']:
                i['CertificateDetails']['ValidTill'] = i['CertificateDetails']['ValidTill'].isoformat()
            rds_object = extract_common_info(i['DBInstanceArn'],i,region,account_id)
            rds_instances.append(rds_object)
        save_as_file_parquet(rds_instances,file_path,generate_parquet_prefix(__file__,region,account_id))
    return rds_instances


# def rds_utiliztion(rds):
#   rds_instance_metric = []
#   for instance in json_file['metrics']:
#       print(instance['metricname'])
#       rds_instance_metric.append(get_resource_utilization_parallel(rds,metricname=instance['metricname'],name='DBInstanceIdentifier',statistics=instance['statistics'],unit=instance['unit']))
#   return  rds_instance_metric 

#  {"metricname":  "CPUCreditUsage"                   ,"statistics":["Average","Sum","Minimum","Maximum"],"unit":"Credits (vCPU-minutes)"},
#         {"metricname":  "CPUCreditBalance"                 ,"statistics":["Average","Sum","Minimum","Maximum"],"unit":"Credits (vCPU-minutes)"},
#         {"metricname":  "CPUSurplusCreditBalance"          ,"statistics":["Average","Sum","Minimum","Maximum"],"unit":"Credits (vCPU-minutes)"},
#         {"metricname":  "CPUSurplusCreditsCharged"         ,"statistics":["Average","Sum","Minimum","Maximum"],"unit":"Credits (vCPU-minutes)"},
# {"metricname":  "DatabaseConnections"              ,"statistics":["Average","Sum","Minimum","Maximum"],"unit":"Count"},
#         {"metricname":  "DiskQueueDepth"                   ,"statistics":["Average","Sum","Minimum","Maximum"],"unit":"Count"},
#         {"metricname":  "DiskQueueDepthLogVolume"          ,"statistics":["Average","Sum","Minimum","Maximum"],"unit":"Count"},
#         {"metricname":  "EBSByteBalance%"                  ,"statistics":["Average","Sum","Minimum","Maximum"],"unit":"Percentage"},
#         {"metricname":  "EBSIOBalance%"                    ,"statistics":["Average","Sum","Minimum","Maximum"],"unit":"Percentage"},
#         {"metricname":  "FailedSQLServerAgentJobsCount"    ,"statistics":["Average","Sum","Minimum","Maximum"],"unit":"Count per minute"},
#         {"metricname":  "FreeableMemory"                   ,"statistics":["Average","Sum","Minimum","Maximum"],"unit":"Bytes"},
#         {"metricname":  "FreeStorageSpace"                 ,"statistics":["Average","Sum","Minimum","Maximum"],"unit":"Bytes"},
#         {"metricname":  "FreeStorageSpaceLogVolume"        ,"statistics":["Average","Sum","Minimum","Maximum"],"unit":"Bytes"}