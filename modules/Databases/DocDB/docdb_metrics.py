from inspect import stack
from utils.utils import save_as_file_parquet_metrics, generate_parquet_prefix, get_resource_utilization_metric


def docdb_metrics(file_path, session, region, account, metrics, time_generated):
    """
    This Python function retrieves metrics for Amazon DocumentDB clusters and saves them to a Parquet
    file.

    :param file_path: The `file_path` parameter in the `docdb_metrics` function is a string that
    represents the path where the metrics data will be saved as a file. This could be a file path on the
    local file system or a cloud storage location, depending on the implementation
    :param session: The `session` parameter in the `docdb_metrics` function is typically an instance of
    a boto3 session that allows you to create service clients for AWS services. It is used to interact
    with the Amazon DocumentDB service in the specified AWS region
    :param region: The `region` parameter in the `docdb_metrics` function refers to the AWS region where
    the Amazon DocumentDB clusters are located. This parameter is used to specify the region when
    creating a client session for interacting with the DocumentDB service in that specific region
    :param account: The `account` parameter seems to be a dictionary containing information about an
    account. It likely includes the account ID among other details. In the provided code snippet, the
    account ID is accessed using `account['account_id']`
    :param metrics: The `metrics` parameter in the `docdb_metrics` function likely refers to a data
    structure or object that stores information related to resource utilization metrics for Amazon
    DocumentDB clusters. This could include metrics such as CPU utilization, memory usage, disk I/O,
    network traffic, etc. The function seems to
    :param time_generated: The `time_generated` parameter in the `docdb_metrics` function represents the
    timestamp or time at which the metrics are generated or collected. This parameter is used to track
    when the metrics data was obtained and can be helpful for analyzing trends over time or for
    troubleshooting issues related to the metrics
    """
    next_token = None
    idx = 0
    client = session.client('docdb', region_name=region)
    account_id = account['account_id']
    while True:
        try:
            inventory = []
            response = client.describe_db_clusters(
                Marker=next_token) if next_token else client.describe_db_clusters()
            for resource in response.get('DBClusters', []):
                inventory.append(resource['DBClusterIdentifier'])
            if inventory:
                metrics = get_resource_utilization_metric(
                    session, region, inventory, account, metrics, time_generated)
                save_as_file_parquet_metrics(metrics, file_path, generate_parquet_prefix(
                    str(stack()[0][3]), region, f'{account_id}-metrics', idx))
            next_token = response.get('Marker', None)
            idx = idx + 1
            if not next_token:
                break
        except Exception as e:
            print(e)
            break
