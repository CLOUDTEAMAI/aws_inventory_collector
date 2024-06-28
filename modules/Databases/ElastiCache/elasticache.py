from inspect import stack
from utils.utils import extract_common_info, save_as_file_parquet, generate_parquet_prefix


def list_cache(file_path, session, region, time_generated, account):
    """
    This Python function retrieves information about cache clusters using the AWS Elasticache client and
    saves the data in Parquet format.

    :param file_path: The `file_path` parameter in the `list_cache` function is the path where the cache
    cluster inventory data will be saved as a Parquet file. This parameter should be a string
    representing the file path where you want to save the data
    :param session: The `session` parameter is an object that represents the current session. It is
    typically used to create clients for AWS services. In the provided code snippet, the `session`
    object is used to create a client for the Amazon ElastiCache service in a specific AWS region
    :param region: The `region` parameter in the `list_cache` function is used to specify the AWS region
    where the Elasticache clusters are located. This parameter is required to create a client session
    for the Elasticache service in the specified region and to retrieve information about the cache
    clusters in that region
    :param time_generated: Time when the cache cluster inventory is generated. It is used to timestamp
    the inventory objects
    :param account: The `account` parameter in the `list_cache` function seems to be a dictionary
    containing information about an account. It likely includes keys such as 'account_id' and
    'account_name'. This information is used within the function to extract account-specific details and
    process cache cluster data accordingly
    """
    next_token = None
    idx = 0
    client = session.client('elasticache', region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.describe_cache_clusters(
                Marker=next_token) if next_token else client.describe_cache_clusters()
            for resource in response.get('CacheClusters', []):
                if 'CacheClusterCreateTime' in resource:
                    resource['CacheClusterCreateTime'] = resource['CacheClusterCreateTime'].isoformat(
                    )
                for nodes in resource.get('CacheNodes', []):
                    nodes['CacheNodeCreateTime'] = nodes['CacheNodeCreateTime'].isoformat()
                if 'AuthTokenLastModifiedDate' in resource:
                    resource['AuthTokenLastModifiedDate'] = resource['AuthTokenLastModifiedDate'].isoformat(
                    )
                arn = resource['ARN']
                inventory_object = extract_common_info(
                    arn, resource, region, account_id, time_generated, account_name)
                inventory.append(inventory_object)
            save_as_file_parquet(inventory, file_path, generate_parquet_prefix(
                str(stack()[0][3]), region, account_id, idx))
            next_token = response.get('Marker', None)
            idx = idx + 1
            if not next_token:
                break
        except Exception as e:
            print(e)
            break


def list_cache_sizing(file_path, session, region, time_generated, account):
    next_token = None
    idx = 0
    client = session.client('elasticache', region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            clusters = client.describe_cache_clusters(
                Marker=next_token) if next_token else client.describe_cache_clusters()
            for resource in clusters.get('CacheClusters', []):
                response = client.list_allowed_node_type_modifications(
                    CacheClusterId=resource.get('CacheClusterId')) if not resource.get('ReplicationGroupId') else client.list_allowed_node_type_modifications(
                    ReplicationGroupId=resource.get('ReplicationGroupId'))
                arn = f"{resource['ARN']}!!scaling-options"
                inventory_object = extract_common_info(
                    arn, response, region, account_id, time_generated, account_name)
                inventory.append(inventory_object)
            save_as_file_parquet(inventory, file_path, generate_parquet_prefix(
                str(stack()[0][3]), region, account_id, idx))
            next_token = clusters.get('Marker', None)
            idx = idx + 1
            if not next_token:
                break
        except Exception as e:
            print(e)
            break
