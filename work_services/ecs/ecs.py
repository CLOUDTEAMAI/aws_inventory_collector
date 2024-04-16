import boto3
import concurrent.futures
from datetime import datetime, timedelta
import pandas as pd
import json
from cloudwatch_logic import *
import os 
from utils.utils import *



def list_ecs_clusters(file_path,session,region,time_generated,account):
    ecs = session.client('ecs',region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ","_")
    clusters_list = ecs.list_clusters()
    ecs_clusters = []
    if len(clusters_list['clusterArns']) != 0:
        for cluster_arn in clusters_list['clusterArns']:
            clusters_info = ecs.describe_clusters(clusters=[cluster_arn])
            ecs_object = extract_common_info(cluster_arn,clusters_info,region,account_id,time_generated,account_name)
            ecs_clusters.append(ecs_object)
        save_as_file_parquet(ecs_clusters,file_path,generate_parquet_prefix(__file__,region,account_id))
    return ecs_clusters




async def async_list_ecs_clusters(file_path, session, region, time_generated):
    try:
        client_list = []
        clinet_account =  session[1].client('sts')
        ecs = session[1].client('ecs',region_name=region)
        account_id = clinet_account.get_caller_identity()["Account"]
        async with session[0].client('ecs') as client_boto:
            paginator = client_boto.get_paginator('list_clusters')
            async for page in paginator.paginate():
                for i in page['clusterArns']:
                    if i:
                        # describe_ecs_test = client_boto.get_paginator('describe_clusters',clusters=[i])
                        describe_ecs = ecs.describe_clusters(clusters=[i])
                        for cluster_info in describe_ecs['clusters']:
                            ecs_object = extract_common_info(cluster_info['clusterArn'],cluster_info,region,account_id,time_generated)
                            client_list.append(ecs_object)
    except Exception as ex:
            print(ex)
    finally:
        if len(client_list) != 0:
                save_as_file_parquet(client_list, file_path, generate_parquet_prefix(__file__, region, account_id))