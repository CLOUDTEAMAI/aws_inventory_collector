import boto3
import concurrent.futures
from datetime import datetime, timedelta
import pandas as pd
import json
import time
from concurrent.futures import ThreadPoolExecutor

cloudwatch = boto3.client('cloudwatch')

def get_resource_utilization(resources,metricname,statistics,unit,name,days=60):
    start_timer = datetime.now()
    end_time = datetime.utcnow()
    start_time = end_time - timedelta(days)
    metrics_resources = []

    for resource in resources:
        # print(resource['Id'])
        metrics = cloudwatch.get_metric_statistics(
        Namespace  = 'AWS/' + resource['serviceType'] , 
        MetricName = metricname , 
        Dimensions =[{'Name': name ,'Value': resource['Id']}],
        StartTime=start_time,
        EndTime=end_time,
        Period=86400,  
        Statistics= statistics ,
        Unit= unit
    )
        metrics_resources.append(metrics['Datapoints'])
    stop_timer = datetime.now()
    runtime = (stop_timer - start_timer).total_seconds()
    print(f"{runtime % 60 :.3f}")
    return metrics_resources


def get_resource_utilization_test(resources,metricname,statistics,unit,name,days=60):
    end_time = datetime.utcnow()
    start_time = end_time - timedelta(days)
    metrics_resources = []

    metrics = cloudwatch.get_metric_statistics(
    Namespace  = 'AWS/' + resources['serviceType'] ,
    MetricName = metricname ,
    Dimensions =[{'Name': name ,'Value': resources['Id']}],
    StartTime=start_time,
    EndTime=end_time,
    Period=86400,  
    Statistics= statistics ,
    Unit= unit#'Seconds'
    )
    # return metrics_resources
    return metrics['Datapoints']

def get_resource_utilization_parallel(resources, metricname, statistics, unit, name):
    metrics_resources = []
    start_timer = datetime.now()
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [
            executor.submit(
                get_resource_utilization_test,
                resource, metricname, statistics, unit, name
            ) for resource in resources
        ]
        for future in concurrent.futures.as_completed(futures):
            metrics_resources.append(future.result())
    stop_timer = datetime.now()
    runtime = (stop_timer - start_timer).total_seconds()
    print(f"{runtime % 60 :.3f}")
    return metrics_resources