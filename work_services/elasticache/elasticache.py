import boto3
from utils.utils import *

def list_cache(file_path,session,region,time_generated,account):
    client = session.client('elasticache',region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ","_")
    cache_list = client.describe_cache_clusters()
    resources = []
    if len(cache_list['CacheClusters']) != 0:
        for i in cache_list['CacheClusters']:
            if 'CacheClusterCreateTime' in i:
                i['CacheClusterCreateTime'] = i['CacheClusterCreateTime'].isoformat()
            for nodes in i.get('CacheNodes',[]):
                nodes['CacheNodeCreateTime'] = nodes['CacheNodeCreateTime'].isoformat()
            arn = i['ARN']
            resouce_object = extract_common_info(arn,i,region,account_id,time_generated,account_name)
            resources.append(resouce_object)
        save_as_file_parquet(resources,file_path,generate_parquet_prefix(__file__,region,account_id))
        return resources
    

async def async_list_elasticahe(file_path, session, region, time_generated):
    try:
        client_list = []
        clinet_account =  session[1].client('sts')
        account_id = clinet_account.get_caller_identity()["Account"]
        async with session[0].client('elasticache') as ecr:
            paginator = ecr.get_paginator('describe_cache_clusters')
            async for page in paginator.paginate():
                for i in page['CacheClusters']:
                    if 'CacheClusterCreateTime' in i:
                        i['CacheClusterCreateTime'] = i['CacheClusterCreateTime'].isoformat()
                    for nodes in i.get('CacheNodes',[]):
                        nodes['CacheNodeCreateTime'] = nodes['CacheNodeCreateTime'].isoformat()
                    arn = i['ARN']
                    inventory_object = extract_common_info(arn, i, region, account_id, time_generated)
                    client_list.append(inventory_object)
    except Exception as ex:
            print(ex)
    finally:
        if len(client_list) != 0:
                save_as_file_parquet(client_list, file_path, generate_parquet_prefix(__file__, region, account_id))