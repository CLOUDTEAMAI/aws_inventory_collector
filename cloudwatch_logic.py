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
import ast


# def get_resource_utilization(session,ids: list,metricname:str,statistics: list,unit:str,name_dimensions: str,serviceType: str,days=60):
#     cloudwatch = session.client('cloudwatch')
#     start_timer = datetime.now()
#     end_time = datetime.utcnow()
#     start_time = end_time - timedelta(days)
#     metrics_resources = []

#     for resource in ids:
#         metrics = cloudwatch.get_metric_statistics(
#         Namespace  = 'AWS/' + serviceType , 
#         MetricName = metricname , 
#         Dimensions =[{'Name': name_dimensions ,'Value': resource}],
#         StartTime=start_time,
#         EndTime=end_time,
#         Period=86400,  
#         Statistics= statistics ,
#         Unit= unit
#     )
#         print(metrics['Datapoints'])
#         item = extract_common_info_metrics(resource,metrics['Datapoints'],metrics['Label'])
#         metrics_resources.append(item)
#     stop_timer = datetime.now()
#     runtime = (stop_timer - start_timer).total_seconds()
#     print(f"{runtime % 60 :.3f}")
#     # print(metrics_resources)
#     return metrics_resources

def get_resource_utilization_metric(session,ids: str,metricname:str,statistics: list,unit:str,name_dimensions: str,serviceType: str,days=60):
    cloudwatch = session.client('cloudwatch')
    sts = session.client('sts')
    account_id = sts.get_caller_identity()["Account"]    
    start_timer = datetime.now()
    end_time = datetime.utcnow()
    start_time = end_time - timedelta(days)

    metrics = cloudwatch.get_metric_statistics(
    Namespace  = 'AWS/' + serviceType , 
    MetricName = metricname , 
    Dimensions =[{'Name': name_dimensions ,'Value': ids}],
    StartTime=start_time,
    EndTime=end_time,
    Period=3600,  
    Statistics= statistics ,
    Unit= unit
    )
    for i in metrics['Datapoints']:
        i['Timestamp'] = i['Timestamp'].isoformat()

    item = extract_common_info_metrics(account_id,ids,metrics['Datapoints'],metrics['Label'])
    stop_timer = datetime.now()
    runtime = (stop_timer - start_timer).total_seconds()
    print(f"{runtime % 60 :.3f}")
    return item



def calculate_statistics(path_file,output_path,output_path2):
    df = pd.read_parquet(path_file)
    df2= pd.read_parquet(path_file)
    cpu_df = df[df['label'] == 'CPUUtilization']
    results = {}
    results2 = {}
    for index, row in cpu_df.iterrows():
        # Deserialize the 'properties' field from a string to a list of dictionaries
        # print(row['properties']) 
        # properties = json.loads(row['properties'])
        properties = ast.literal_eval(row['properties'])
        
        
        # Calculate the statistics if properties is not empty
        # if properties:
        #     avg_average = sum(item["Average"] for item in properties) / len(properties)
        #     min_average = sum(item["Minimum"] for item in properties) / len(properties)
        #     max_average = sum(item["Maximum"] for item in properties) / len(properties)

        #     max_avg_value = max(item["Average"] for item in properties)
        #     rounded_max_avg_value = round(max_avg_value, 2)

        # # Do similarly for 'Minimum' and 'Maximum' if needed
        #     max_min_value = max(item["Minimum"] for item in properties)
        #     rounded_max_min_value = round(max_min_value, 2)

        #     max_max_value = max(item["Maximum"] for item in properties)
        #     rounded_max_max_value = round(max_max_value, 2)


        #     results[row['id']] = [avg_average, min_average, max_average]
        #     df = pd.DataFrame(list(results.items()), columns=['id', 'statistics'])
        #     df[['average', 'minimum', 'maximum']] = pd.DataFrame(df['statistics'].tolist(), index=df.index)
        #     df.drop(columns=['statistics'], inplace=True)
        #     df.to_parquet(output_path, index=False)

    for index, row in cpu_df.iterrows():
        # Deserialize the 'properties' field from a string to a list of dictionaries
        # print(row['properties']) 
        # properties = json.loads(row['properties'])
        properties = ast.literal_eval(row['properties'])
        
        
        # Calculate the statistics if properties is not empty
        if properties:
            avg_average = sum(item["Average"] for item in properties) / len(properties)
            min_average = sum(item["Minimum"] for item in properties) / len(properties)
            max_average = sum(item["Maximum"] for item in properties) / len(properties)

            max_avg_entry = max(properties, key=lambda x: x["Average"])
            max_avg_value, timestamp_max_avg = max_avg_entry["Average"], max_avg_entry["Timestamp"]
            rounded_max_avg_value = round(max_avg_value, 2)

        # Do similarly for 'Minimum' and 'Maximum' if needed
            max_min_entry = max(properties, key=lambda x: x["Minimum"])
            max_min_value, timestamp_max_min = max_min_entry["Minimum"], max_min_entry["Timestamp"]
            rounded_max_min_value = round(max_min_value, 2)

            # max_max_value = max(item["Maximum"] for item in properties)
            max_max_entry = max(properties, key=lambda x: x["Maximum"])
            max_max_value, timestamp_max_max = max_max_entry["Maximum"], max_max_entry["Timestamp"]
            rounded_max_max_value = round(max_max_value, 2)




            results[row['id']] = [rounded_max_avg_value,timestamp_max_avg, rounded_max_min_value,timestamp_max_min, rounded_max_max_value,timestamp_max_max]
            df2 = pd.DataFrame(list(results.items()), columns=['id', 'statistics'])
            df2[['average_max', 'date_avg','minimum','date_min', 'maximum_max','date']] = pd.DataFrame(df2['statistics'].tolist(), index=df2.index)
            df2.drop(columns=['statistics'], inplace=True)
            df2.to_parquet(output_path2, index=False)
           
        #     results2[row['id']] = {
        #     'max_avg_value': rounded_max_avg_value,
        #     'max_min_value': rounded_max_min_value,
        #     'max_max_value': rounded_max_max_value
        # }
            


            # res = pd.DataFrame(list(results2.items()), columns=['id', 'statistics'])
            # res[['average', 'minimum', 'maximum']] = pd.DataFrame(df['statistics'].tolist(), index=df.index)
            # res.drop(columns=['statistics'], inplace=True)
            # res.to_parquet(output_path, index=False)
            # results_df = pd.DataFrame.from_dict(results2, orient='index').reset_index()
            # results_df.rename(columns={'index': 'id'}, inplace=True)

            
            # df.to_parquet(f'{output_path2}', index=False)
    # print(results)
    # return results


    
    

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