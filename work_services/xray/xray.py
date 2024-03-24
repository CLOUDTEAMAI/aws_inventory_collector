import boto3
import concurrent.futures
from datetime import datetime, timedelta
import pandas as pd
import json
from utils.utils import *
import os 

cloudwatch = boto3.client('cloudwatch')


def list_xray(file_path,session,region):
    xray = session.client('xray',region_name=region)
    sts = session.client('sts')
    account_id = sts.get_caller_identity()["Account"]
    end_time = datetime.utcnow()
    start_time = end_time - timedelta(hours=24)
    region = session.region_name
    inventory_instances = []
    try:
        response = xray.get_trace_summaries(
        StartTime=start_time,
        EndTime=end_time,
        Sampling=False  # Set to True to get a subset of traces if you're dealing with a high volume
    )
        trace_summaries = response['TraceSummaries']
        if len(trace_summaries) != 0:
            for i in trace_summaries:
                attributes                    = i['Id']
                i['StartTime']                = i['StartTime'].isoformat()
                i['MatchedEventTime']         = i['MatchedEventTime'].isoformat()
                inventory_object = extract_common_info(attributes,i,region,account_id)
                inventory_instances.append(inventory_object)
            save_as_file_parquet(inventory_instances,file_path,generate_parquet_prefix(__file__,region,account_id))
        return inventory_instances
    except Exception as e :
        print(e)
    
   
