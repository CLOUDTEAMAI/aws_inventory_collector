from inspect import stack
from utils.utils import save_as_file_parquet_metrics, generate_parquet_prefix, get_resource_utilization_metric


def rds_instances_metrics(file_path, session, region, account, metrics, time_generated):
    """
    This Python function retrieves metrics for RDS instances, saves them as Parquet files, and handles
    exceptions during the process.

    :param file_path: The `file_path` parameter is a string that represents the file path where the
    metrics data will be saved. It is the location where the Parquet file containing the metrics will be
    stored
    :param session: The `session` parameter is typically an instance of a boto3 session that is used to
    interact with AWS services. It is used to create clients for specific AWS services, such as RDS in
    this case. The session stores configuration information, such as credentials and region, that are
    used when making API
    :param region: The `region` parameter in the `rds_instances_metrics` function is used to specify the
    AWS region where the RDS instances are located. This parameter is important for establishing a
    connection to the AWS RDS service in the specified region and retrieving information about the RDS
    instances within that region
    :param account: The `account` parameter in the `rds_instances_metrics` function seems to be a
    dictionary containing information about the account. It likely includes details such as the account
    ID, which can be accessed using `account['account_id']`. This information is used within the
    function to interact with AWS services specific
    :param metrics: The `metrics` parameter in the `rds_instances_metrics` function likely refers to a
    list or dictionary that contains the metrics you want to collect for the RDS instances. These
    metrics could include performance indicators such as CPU utilization, storage usage, memory usage,
    and so on. The function seems to
    :param time_generated: The `time_generated` parameter in the `rds_instances_metrics` function is
    used to specify the timestamp or time at which the metrics are generated or collected for the RDS
    instances. This parameter is likely used to track when the metrics data was collected and can be
    helpful for analysis, monitoring, or
    """
    next_token = None
    idx = 0
    client = session.client('rds', region_name=region)
    account_id = account['account_id']
    while True:
        try:
            inventory = []
            response = client.describe_db_instances(
                Marker=next_token) if next_token else client.describe_db_instances()
            for resource in response.get('DBInstances', []):
                inventory.append(resource['DBInstanceIdentifier'])
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


def rds_proxies_metrics(file_path, session, region, account, metrics, time_generated):
    """
    This Python function retrieves metrics for RDS proxies and saves them to a Parquet file.

    :param file_path: The `file_path` parameter is a string that represents the file path where the
    metrics data will be saved. It is the location where the metrics data will be stored in a file,
    typically in a specific format like Parquet
    :param session: The `session` parameter is typically an instance of boto3.Session class that
    represents the connection to AWS services. It is used to create clients for AWS services like RDS in
    this case
    :param region: Region is the geographical area where the AWS resources are located. It is a required
    parameter for AWS API calls to specify the region where the resources are located or where the
    operations should be performed. Examples of regions include 'us-east-1', 'eu-west-1', 'ap-southeast-
    :param account: The `account` parameter in the `rds_proxies_metrics` function seems to be a
    dictionary containing account information. It likely includes details such as the account ID, which
    can be accessed using `account['account_id']`. This information is used within the function to
    identify the account associated with the
    :param metrics: The `metrics` parameter in the `rds_proxies_metrics` function likely refers to a
    list or dictionary that stores the metrics related to RDS proxies. These metrics could include
    information such as CPU utilization, memory usage, network traffic, etc. The function seems to be
    iterating over RDS proxies
    :param time_generated: The `time_generated` parameter in the `rds_proxies_metrics` function likely
    represents the timestamp or time at which the metrics were generated or collected. This parameter is
    used to track the time when the metrics data was obtained for the RDS proxies. It could be a
    datetime object, timestamp,
    """
    next_token = None
    idx = 0
    client = session.client('rds', region_name=region)
    account_id = account['account_id']
    while True:
        try:
            inventory = []
            response = client.describe_db_proxies(
                Marker=next_token) if next_token else client.describe_db_proxies()
            for resource in response.get('DBProxies', []):
                inventory.append(resource['DBProxyName'])
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
