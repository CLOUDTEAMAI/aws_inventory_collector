from inspect import stack
from utils.utils import save_as_file_parquet_metrics, generate_parquet_prefix, get_resource_utilization_metric


def s3_metrics(file_path, session, region, account, metrics, time_generated, boto_config):
    """
    This Python function retrieves S3 bucket inventory, calculates resource utilization metrics, and
    saves the metrics to a Parquet file.

    :param file_path: The `file_path` parameter in the `s3_metrics` function is the path where the
    metrics data will be saved as a file. It is a string that represents the location where the metrics
    data will be stored, such as a file path on the local filesystem or a cloud storage location
    :param session: The `session` parameter in the `s3_metrics` function is typically an instance of a
    boto3 session that allows you to create service clients for AWS services. It is used to interact
    with AWS services like S3 in this case
    :param region: Region is a string representing the AWS region where the S3 buckets are located. It
    specifies the geographical area in which the resources exist, such as 'us-east-1' for US East (N.
    Virginia) or 'eu-west-1' for EU (Ireland)
    :param account: The `account` parameter in the `s3_metrics` function seems to represent an account
    object that likely contains information about the account, such as the account ID. It is used to
    extract the account ID from the account object and assign it to the `account_id` variable in the
    function
    :param metrics: The `metrics` parameter in the `s3_metrics` function likely represents a list or
    dictionary that stores the metrics related to S3 buckets. These metrics could include information
    such as bucket size, number of objects, access patterns, or any other relevant data that you want to
    track or analyze for the
    :param time_generated: Time when the metrics were generated
    :param boto_config: The `boto_config` parameter in the `s3_metrics` function is used to provide
    additional configuration options for the Boto3 client that interacts with AWS services. This
    parameter allows you to customize the behavior of the Boto3 client, such as setting custom retry
    settings, timeouts, or other
    """
    next_token = None
    idx = 0
    client = session.client('s3', region_name=region, config=boto_config)
    account_id = account['account_id']
    while True:
        try:
            inventory = []
            response = client.list_buckets()
            for resource in response.get('Buckets', []):
                inventory.append(resource['Name'])
            if inventory:
                metrics = get_resource_utilization_metric(
                    session, region, inventory, account, metrics, time_generated)
                save_as_file_parquet_metrics(metrics, file_path, generate_parquet_prefix(
                    str(stack()[0][3]), region, f'{account_id}-metrics', idx))
            next_token = response.get('ContinuationToken', None)
            idx = idx + 1
            if not next_token:
                break
        except Exception as e:
            print(e)
            break
