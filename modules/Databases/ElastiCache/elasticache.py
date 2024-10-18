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


def list_cache_serverless(file_path, session, region, time_generated, account):
    """
    This Python function lists and caches serverless resources using AWS Elasticache client.

    :param file_path: The `file_path` parameter in the `list_cache_serverless` function is the path
    where the inventory data will be saved as a file. It is the location where the function will write
    the inventory information in Parquet format
    :param session: The `session` parameter in the `list_cache_serverless` function is typically an
    instance of a boto3 session that allows you to create service clients for AWS services. It is used
    to interact with AWS Elasticache in the specified region
    :param region: The `region` parameter in the `list_cache_serverless` function is used to specify the
    AWS region in which the Elasticache serverless caches are located. This parameter is required to
    create a client session for the Elasticache service in the specified region and to retrieve
    information about the serverless caches in
    :param time_generated: The `time_generated` parameter in the `list_cache_serverless` function is
    used to specify the timestamp or time at which the inventory data is generated or collected. This
    timestamp is important for tracking when the inventory information was retrieved and can be used for
    various purposes such as auditing, monitoring, or analysis
    :param account: The `list_cache_serverless` function takes several parameters:
    """
    next_token = None
    idx = 0
    client = session.client('elasticache', region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.describe_serverless_caches(
                NextToken=next_token) if next_token else client.describe_serverless_caches()
            for resource in response.get('ServerlessCaches', []):
                if 'CreateTime' in resource:
                    resource['CreateTime'] = resource['CreateTime'].isoformat(
                    )
                arn = resource['ARN']
                inventory_object = extract_common_info(
                    arn, resource, region, account_id, time_generated, account_name)
                inventory.append(inventory_object)
            save_as_file_parquet(inventory, file_path, generate_parquet_prefix(
                str(stack()[0][3]), region, account_id, idx))
            next_token = response.get('NextToken', None)
            idx = idx + 1
            if not next_token:
                break
        except Exception as e:
            print(e)
            break


def list_cache_sizing(file_path, session, region, time_generated, account):
    """
    This Python function retrieves information about cache clusters and their node type modifications,
    saves the data as Parquet files, and handles exceptions during the process.

    :param file_path: The `file_path` parameter is the file path where the cache sizing information will
    be saved. It is the location where the output data will be stored, typically in a file format like
    Parquet
    :param session: The `session` parameter in the `list_cache_sizing` function is an object that
    represents the connection to the AWS services. It is typically created using the `boto3.Session`
    class and is used to make API calls to AWS services like Elasticache in this case. The `session`
    :param region: The `region` parameter in the `list_cache_sizing` function is used to specify the AWS
    region where the ElastiCache client will be created. This region is where the ElastiCache service
    will be accessed to retrieve information about cache clusters and their configurations. It is
    important to provide the correct
    :param time_generated: Time when the cache sizing information is generated
    :param account: The `account` parameter seems to be a dictionary containing information about an
    account. It includes keys such as 'account_id' and 'account_name'. This information is used within
    the `list_cache_sizing` function to extract relevant details for cache clusters and their node type
    modifications. If you have a
    """
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


def list_cache_snapshots(file_path, session, region, time_generated, account):
    """
    This Python function retrieves and processes cache snapshots from an AWS ElastiCache instance,
    saving the information to a Parquet file.

    :param file_path: The `file_path` parameter in the `list_cache_snapshots` function is the path where
    the cache snapshots inventory will be saved as a file. It is the location where the Parquet file
    containing the cache snapshots information will be stored
    :param session: The `session` parameter in the `list_cache_snapshots` function is typically an AWS
    session object that is used to create clients for various AWS services. It is used to interact with
    the AWS Elasticache service in the specified region
    :param region: The `region` parameter in the `list_cache_snapshots` function is used to specify the
    AWS region where the ElastiCache snapshots are located. This parameter is passed to the
    `session.client` method to create a client for the Elasticache service in the specified region
    :param time_generated: The `time_generated` parameter in the `list_cache_snapshots` function likely
    represents the timestamp or datetime when the cache snapshots are being generated or listed. This
    parameter is used in the function to capture the time at which the cache snapshots are being
    processed or retrieved. It is important for tracking and recording
    :param account: The `account` parameter in the `list_cache_snapshots` function seems to be a
    dictionary containing information about an account. It likely includes keys such as 'account_id' and
    'account_name' which are used within the function to extract specific details for processing cache
    snapshots
    """
    next_token = None
    idx = 0
    client = session.client('elasticache', region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.describe_snapshots(
                Marker=next_token) if next_token else client.describe_snapshots()
            for resource in response.get('Snapshots', []):
                if 'CacheClusterCreateTime' in resource:
                    resource['CacheClusterCreateTime'] = resource['CacheClusterCreateTime'].isoformat(
                    )
                counter = 0
                for node in resource.get('NodeSnapshots', []):
                    if 'CacheNodeCreateTime' in node:
                        resource['NodeSnapshots'][counter]['CacheNodeCreateTime'] = node['CacheNodeCreateTime'].isoformat(
                        )
                    if 'SnapshotCreateTime' in node:
                        resource['NodeSnapshots'][counter]['SnapshotCreateTime'] = node['SnapshotCreateTime'].isoformat(
                        )
                    counter = counter + 1
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


def list_cache_serverless_snapshots(file_path, session, region, time_generated, account):
    """
    This Python function lists and caches serverless snapshots from an Elasticache client using provided
    parameters.

    :param file_path: The `file_path` parameter in the `list_cache_serverless_snapshots` function is the
    path where the snapshots inventory data will be saved as a file. It should be a valid file path on
    the system where the function is being executed
    :param session: The `session` parameter in the `list_cache_serverless_snapshots` function is
    typically an AWS session object that is used to create clients for AWS services. It is commonly
    created using the `boto3.Session` class and is used to interact with AWS services in a specific
    region using specific credentials
    :param region: Region is a string representing the AWS region where the Elasticache serverless cache
    snapshots are located. It specifies the geographical area where the resources are deployed, such as
    "us-east-1" for US East (N. Virginia) or "eu-west-1" for EU (Ireland)
    :param time_generated: The `time_generated` parameter in the `list_cache_serverless_snapshots`
    function is used to specify the timestamp or time at which the cache snapshots are being generated
    or processed. This parameter is typically a datetime object or a string representing a specific time
    in a standardized format (e.g., ISO
    :param account: The `account` parameter in the `list_cache_serverless_snapshots` function seems to
    be a dictionary containing information about an account. It likely includes the keys 'account_id'
    and 'account_name', which are used within the function to extract specific details for processing
    serverless cache snapshots
    """
    next_token = None
    idx = 0
    client = session.client('elasticache', region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.describe_serverless_cache_snapshots(
                NextToken=next_token) if next_token else client.describe_serverless_cache_snapshots()
            for resource in response.get('ServerlessCacheSnapshots', []):
                if 'CreateTime' in resource:
                    resource['CreateTime'] = resource['CreateTime'].isoformat(
                    )
                if 'ExpiryTime' in resource:
                    resource['ExpiryTime'] = resource['ExpiryTime'].isoformat(
                    )
                arn = resource['ARN']
                inventory_object = extract_common_info(
                    arn, resource, region, account_id, time_generated, account_name)
                inventory.append(inventory_object)
            save_as_file_parquet(inventory, file_path, generate_parquet_prefix(
                str(stack()[0][3]), region, account_id, idx))
            next_token = response.get('NextToken', None)
            idx = idx + 1
            if not next_token:
                break
        except Exception as e:
            print(e)
            break
