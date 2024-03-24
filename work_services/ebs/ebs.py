import boto3
import concurrent.futures
from datetime import datetime, timedelta
import pandas as pd
import json
from cloudwatch_logic import *
import os 
from utils.utils import *
# from cloudwatch_logic import *


underutilized_volumes = []
# cost_explorer = boto3.client('ce')
# path_json_file = os.path.join(os.getcwd(), 'ebs/metric.json')

# with open(path_json_file,'r') as file:
#     json_file = json.load(file)

def list_volumes(file_path,session,region):
    ec2 = session.client('ec2',region_name=region)
    sts = session.client('sts')
    account_id = sts.get_caller_identity()["Account"]
    volumes = ec2.describe_volumes()['Volumes']
    volumes_list = []
    for i in volumes:

        for vol in i.get('Attachments',[]):
            vol['AttachTime'] = vol['AttachTime'].isoformat()

        i['CreateTime'] = i['CreateTime'].isoformat()
        
        arn = f"arn:aws:ec2:{region}:{account_id}:volume/{i['VolumeId']}"
        object_volume = extract_common_info(arn,i,region,account_id)
        volumes_list.append(object_volume)
    save_as_file_parquet(volumes_list,file_path,generate_parquet_prefix(__file__,region,account_id))
    return volumes_list

def list_unattched_volumes(volumes):
    unattached_volumes = []
    for volume in volumes:
        if len(volume['Attachments']) == 0:
            unattached_info = {
                'Id': volume['VolumeId'],
                'VolumeType': volume['VolumeType'],
                'Size': volume['Size'],  # Size in GiB
                'Iops': volume['Iops'],
                'State': volume['State'],
                'tags': volume['Tags'],
                'serviceType': 'EBS'
            }   
            unattached_volumes.append(unattached_info)
    return unattached_volumes

def is_candidate_for_upgrade(volumes):
    """Check if a gp2 volume is a candidate for upgrading to gp3."""
    gp2_volumes = []
    for volume in volumes:
        if(volume['VolumeType'] == 'gp2'):
            if len(volume['Attachments']) == 0:
                unattached_info = {
                'VolumeId': volume['VolumeId'],
                'VolumeType': volume['VolumeType'],
                'Size': volume['Size'],  # Size in GiB
                'Iops': volume['Iops'],
                'State': volume['State'],
                'tags': volume['Tags'],
                'serviceType': 'EBS'
            }   
                gp2_volumes.append(unattached_info)
                
            else:
                volume_object = {
                'VolumeId'  : volume['VolumeId'],
                'VolumeType': volume['VolumeType'] ,
                'Size'      : volume['Size'],
                'Iops'      : volume['Iops'],
                'state'     : volume['State'],
                'InstanceId': volume['Attachments'][0]['InstanceId'] if volume['Attachments'] else 'N/A',
                'tags'      : volume['Tags'],
                'serviceType': 'EBS'
                 }
                gp2_volumes.append(volume_object)
    return gp2_volumes

# def ebs_utiliztion(volumes):
  
#   ebs_metric_list = []
# #   ebs_nitro_utiliztion = []
#   for ebs in json_file['metrics']:
#     ebs_metric_list.append(get_resource_utilization_parallel(volumes,metricname=ebs['metricname'],name='VolumeId',statistics=ebs['statistics'],unit=ebs['unit']))
#   return  ebs_metric_list
    









