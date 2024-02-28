import boto3
import concurrent.futures
from datetime import datetime, timedelta
import pandas as pd
import json
from cloudwatch_logic import *
import os 
from utils.utils import *



def list_ecs_clusters(file_path,session,region):
    ecs = session.client('ecs',region_name=region)
    sts = session.client('sts')
    account_id = sts.get_caller_identity()["Account"]
    clusters_list = ecs.list_clusters()
    ecs_clusters = []
    if len(clusters_list['clusterArns']) != 0:
        for cluster_arn in clusters_list['clusterArns']:
            clusters_info = ecs.describe_clusters(clusters=[cluster_arn])
            ecs_object = extract_common_info(cluster_arn,clusters_info,region,account_id)
            ecs_clusters.append(ecs_object)
        save_as_file_parquet(ecs_clusters,file_path,generate_parquet_prefix(__file__,region,account_id))
    return ecs_clusters
