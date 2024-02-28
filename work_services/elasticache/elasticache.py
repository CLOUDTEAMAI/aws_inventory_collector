import boto3
from utils.utils import *

def list_cache(file_path,session,region):
    client = session.client('elasticache',region_name=region)
    sts = session.client('sts')
    account_id = sts.get_caller_identity()["Account"]
    cache_list = client.describe_cache_clusters()
    resources = []
    if len(cache_list['CacheClusters']) != 0:
        for i in cache_list['CacheClusters']:
            arn = i['ARN']
            resouce_object = extract_common_info(arn,i,region,account_id)
            resources.append(resouce_object)
        save_as_file_parquet(resources,file_path,generate_parquet_prefix(__file__,region,account_id))
        return resources