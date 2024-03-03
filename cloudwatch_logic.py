import boto3
import concurrent.futures
from datetime import datetime, timedelta
import pandas as pd
import json
import time
from concurrent.futures import ThreadPoolExecutor
import pytz
import re
from utils.utils import extract_common_info_metrics


def get_resource_utilization(session,ids: list,metricname:str,statistics: list,unit:str,name_dimensions: str,serviceType: str,days=60):
    cloudwatch = session.client('cloudwatch')
    start_timer = datetime.now()
    end_time = datetime.utcnow()
    start_time = end_time - timedelta(days)
    metrics_resources = []

    for resource in ids:
        metrics = cloudwatch.get_metric_statistics(
        Namespace  = 'AWS/' + serviceType , 
        MetricName = metricname , 
        Dimensions =[{'Name': name_dimensions ,'Value': resource}],
        StartTime=start_time,
        EndTime=end_time,
        Period=86400,  
        Statistics= statistics ,
        Unit= unit
    )
        print(metrics['Datapoints'])
        item = extract_common_info_metrics(resource,metrics['Datapoints'],metrics['Label'])
        metrics_resources.append(item)
    stop_timer = datetime.now()
    runtime = (stop_timer - start_timer).total_seconds()
    print(f"{runtime % 60 :.3f}")
    # print(metrics_resources)
    return metrics_resources

def get_resource_utilization_metric(session,ids: str,metricname:str,statistics: list,unit:str,name_dimensions: str,serviceType: str,days=60):
    cloudwatch = session.client('cloudwatch')
    start_timer = datetime.now()
    end_time = datetime.utcnow()
    start_time = end_time - timedelta(days)

    metrics = cloudwatch.get_metric_statistics(
    Namespace  = 'AWS/' + serviceType , 
    MetricName = metricname , 
    Dimensions =[{'Name': name_dimensions ,'Value': ids}],
    StartTime=start_time,
    EndTime=end_time,
    Period=86400,  
    Statistics= statistics ,
    Unit= unit
    )
    for i in metrics['Datapoints']:
        i['Timestamp'] = i['Timestamp'].isoformat()

    item = extract_common_info_metrics(ids,metrics['Datapoints'],metrics['Label'])
    stop_timer = datetime.now()
    runtime = (stop_timer - start_timer).total_seconds()
    print(f"{runtime % 60 :.3f}")
    return item

# def get_resource_utilization_test(resources,metricname,statistics,unit,name,days=60):
#     end_time = datetime.utcnow()
#     start_time = end_time - timedelta(days)
#     metrics_resources = []

#     metrics = cloudwatch.get_metric_statistics(
#     Namespace  = 'AWS/' + resources['serviceType'] ,
#     MetricName = metricname ,
#     Dimensions =[{'Name': name ,'Value': resources['Id']}],
#     StartTime=start_time,
#     EndTime=end_time,
#     Period=86400,  
#     Statistics= statistics ,
#     Unit= unit#'Seconds'
#     )
#     # return metrics_resources
#     return metrics['Datapoints']

# def get_resource_utilization_parallel(resources, metricname, statistics, unit, name):
#     metrics_resources = []
#     start_timer = datetime.now()
#     with ThreadPoolExecutor(max_workers=10) as executor:
#         futures = [
#             executor.submit(
#                 get_resource_utilization_test,
#                 resource, metricname, statistics, unit, name
#             ) for resource in resources
#         ]
#         for future in concurrent.futures.as_completed(futures):
#             metrics_resources.append(future.result())
#     stop_timer = datetime.now()
#     runtime = (stop_timer - start_timer).total_seconds()
#     print(f"{runtime % 60 :.3f}")
#     return metrics_resources