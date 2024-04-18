import boto3
import concurrent.futures
from datetime import datetime, timedelta
import pandas as pd
import json
from cloudwatch_logic import *
import os 
from utils.utils import *

   

def list_ecr_repositories(file_path,session,region,time_generated,account):
    ecr = session.client('ecr',region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ","_")
    repositories = ecr.describe_repositories()
    ecr_repos = []
    if len(repositories['repositories']) != 0:
        for repo in repositories['repositories']:
            if 'createdAt' in repo:
                repo['createdAt'] = repo['createdAt'].isoformat()
            repo_arn = repo['repositoryArn']
            ecr_object = extract_common_info(repo_arn,repo,region,account_id,time_generated,account_name)
            ecr_repos.append(ecr_object)
        save_as_file_parquet(ecr_repos,file_path,generate_parquet_prefix(__file__,region,account_id))
    # return ecr_repos


async def async_list_ecr(file_path, session, region, time_generated):
    try:
        client_list = []
        clinet_account =  session[1].client('sts')
        account_id = clinet_account.get_caller_identity()["Account"]
        async with session[0].client('ecr') as ecr:
            paginator = ecr.get_paginator('describe_repositories')
            async for page in paginator.paginate():
                for repo in page['repositories']:
                    arn = repo['repositoryArn']
                    inventory_object = extract_common_info(arn, repo, region, account_id, time_generated)
                    client_list.append(inventory_object)
    except Exception as ex:
            print(ex)
    finally:
        if len(client_list) != 0:
                save_as_file_parquet(client_list, file_path, generate_parquet_prefix(__file__, region, account_id))