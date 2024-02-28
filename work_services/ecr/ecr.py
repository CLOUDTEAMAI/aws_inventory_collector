import boto3
import concurrent.futures
from datetime import datetime, timedelta
import pandas as pd
import json
from cloudwatch_logic import *
import os 
from utils.utils import *

   

def list_ecr_repositories(file_path,session,region):
    ecr = session.client('ecr',region_name=region)
    sts = session.client('sts')
    account_id = sts.get_caller_identity()["Account"]
    repositories = ecr.describe_repositories()
    ecr_repos = []
    if len(repositories['repositories']) != 0:
        for repo in repositories['repositories']:
            repo_arn = repo['repositoryArn']
            ecr_object = extract_common_info(repo_arn,repo,region,account_id)
            ecr_repos.append(ecr_object)
        save_as_file_parquet(ecr_repos,file_path,generate_parquet_prefix(__file__,region,account_id))
    return ecr_repos


