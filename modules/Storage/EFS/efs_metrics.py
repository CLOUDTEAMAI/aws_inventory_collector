from inspect import stack
from utils.utils import save_as_file_parquet_metrics, generate_parquet_prefix, get_resource_utilization_metric


def efs_filesystem_metrics(file_path, session, region, account, metrics, time_generated, boto_config):
    """
    This Python function retrieves metrics for EFS file systems and saves them in a Parquet file.
    
    :param file_path: The `file_path` parameter in the `efs_filesystem_metrics` function is the path
    where the metrics data will be saved as a file. It is the location where the Parquet file containing
    the metrics will be stored
    :param session: The `session` parameter in the `efs_filesystem_metrics` function is typically an
    instance of a boto3 session that allows you to create service clients and resources for AWS
    services. It is used to interact with the AWS Elastic File System (EFS) service in this function
    :param region: Region is a string representing the AWS region where the EFS file system is located.
    It specifies the geographical area where the resources are deployed
    :param account: The `account` parameter in the `efs_filesystem_metrics` function seems to represent
    an account object that likely contains information about the account, such as the account ID. It is
    used to extract the account ID from the account object using `account['account_id']` in the function
    :param metrics: Metrics is a dictionary that stores the metrics data related to the EFS filesystems.
    It is used to track and store various performance and utilization metrics of the EFS filesystems
    :param time_generated: The `time_generated` parameter in the `efs_filesystem_metrics` function is
    used to specify the timestamp or time at which the metrics are generated or collected. This
    timestamp is important for tracking when the metrics data was collected and can be used for analysis
    or comparison purposes. It helps in understanding the time
    :param boto_config: The `boto_config` parameter in the `efs_filesystem_metrics` function is used to
    provide additional configuration options for the AWS SDK for Python (Boto3) client that is created
    within the function. This configuration can include settings such as retries, timeouts, and other
    client-specific configurations
    """
    next_token = None
    idx = 0
    client = session.client('efs', region_name=region, config=boto_config)
    account_id = account['account_id']
    while True:
        try:
            inventory = []
            response = client.describe_file_systems(
                Marker=next_token) if next_token else client.describe_file_systems()
            for resource in response.get('FileSystems', []):
                inventory.append(resource['FileSystemId'])
            if inventory:
                metrics = get_resource_utilization_metric(
                    session, region, inventory, account, metrics, time_generated)
                save_as_file_parquet_metrics(metrics, file_path, generate_parquet_prefix(
                    str(stack()[0][3]), region, f'{account_id}-metrics', idx))
            next_token = response.get('NextMarker', None)
            idx = idx + 1
            if not next_token:
                break
        except Exception as e:
            print(e)
            break
