from inspect import stack
from utils.utils import save_as_file_parquet_metrics, generate_parquet_prefix, get_resource_utilization_metric


def elasticache_metrics(file_path, session, region, account, metrics, time_generated, boto_config):
    """
    The function `elasticache_metrics` retrieves metrics for Amazon ElastiCache clusters and saves them
    to a file.

    :param file_path: The `file_path` parameter in the `elasticache_metrics` function is a string that
    represents the path where the metrics data will be saved or stored. This could be a file path on the
    local file system or a cloud storage location, depending on how you plan to store the metrics data
    :param session: The `session` parameter in the `elasticache_metrics` function is typically an
    instance of a boto3 session that allows you to create service clients for AWS services. It is used
    to interact with AWS Elasticache service in the specified region
    :param region: Region is a string representing the AWS region where the ElastiCache clusters are
    located. It specifies the geographical area where the resources are deployed, such as 'us-east-1'
    for US East (N. Virginia) or 'eu-west-1' for EU (Ireland)
    :param account: The `account` parameter in the `elasticache_metrics` function seems to be a
    dictionary containing information about the AWS account. It likely includes keys such as
    'account_id' and 'account_name'. The function uses the 'account_id' to retrieve the account's ID and
    the 'account_name'
    :param metrics: Metrics is a list of metrics that you want to collect for the Elasticache clusters.
    These metrics could include CPU utilization, memory utilization, network throughput, etc. The
    function `elasticache_metrics` is designed to retrieve these metrics for the specified Elasticache
    clusters and save them to a file in Parquet
    :param time_generated: The `time_generated` parameter in the `elasticache_metrics` function
    represents the timestamp or time at which the metrics are generated or collected. This parameter is
    used to track when the metrics data was obtained, allowing for time-based analysis and monitoring of
    Elasticache clusters. It is typically a datetime object or
    """
    next_token = None
    idx = 0
    client = session.client(
        'elasticache', region_name=region, config=boto_config)
    account_id = account['account_id']
    account_name = str(account.get('account_name', '')).replace(" ", "_")
    while True:
        try:
            clusters_idx = 0
            inventory = []
            addons = {"type": "elasticache"}
            addons['nodes'] = []
            response = client.describe_cache_clusters(
                Marker=next_token) if next_token else client.describe_cache_clusters()
            for resource in response.get('CacheClusters', []):
                inventory.append(resource['CacheClusterId'])
                addons['nodes'].append(
                    {"CacheClusterId": resource['CacheClusterId'], "nodes": []})
                if resource.get('NumCacheNodes', 1) <= 1:
                    addons['nodes'][clusters_idx]['nodes'].append('0001')
                else:
                    for node in resource.get('CacheNodes', []):
                        addons['nodes'][clusters_idx]['nodes'].append(
                            node.get('CacheNodeId', ''))
                clusters_idx = clusters_idx + 1
            if inventory:
                metrics = get_resource_utilization_metric(
                    session, region, inventory, account, metrics, time_generated, addons)
                save_as_file_parquet_metrics(metrics, file_path, generate_parquet_prefix(
                    str(stack()[0][3]), region, f'{account_id}-metrics', idx))
            next_token = response.get('Marker', None)
            idx = idx + 1
            if not next_token:
                break
        except Exception as e:
            print(e)
            break
