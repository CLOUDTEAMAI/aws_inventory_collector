from inspect import stack
from utils.utils import save_as_file_parquet_metrics, generate_parquet_prefix, get_resource_utilization_metric


def functions_metrics(file_path, session, region, account, metrics, time_generated):
    """
    This Python function retrieves metrics for AWS Lambda functions and saves them in a Parquet file.

    :param file_path: The `file_path` parameter in the `functions_metrics` function represents the file
    path where the metrics data will be saved. It is a string that specifies the location where the
    metrics will be stored, such as a file path on the local system or a cloud storage location
    :param session: The `session` parameter in the `functions_metrics` function is typically an instance
    of a boto3 session that allows you to create service clients. It is used to interact with AWS
    services like Lambda in this case
    :param region: Region is a string representing the AWS region where the resources are located. It
    specifies the geographical area in which the AWS resources will be deployed or where the operations
    will be performed. Examples of region names include 'us-east-1', 'eu-west-2', 'ap-southeast-2',
    :param account: The `account` parameter in the `functions_metrics` function seems to be a dictionary
    containing information about an AWS account. It likely includes the account ID as one of its keys
    :param metrics: The `metrics` parameter in the `functions_metrics` function is used to store the
    metrics data related to the Lambda functions. It is passed as an argument to the function and is
    updated with the resource utilization metrics for each Lambda function listed in the inventory. The
    `get_resource_utilization_metric` function
    :param time_generated: The `time_generated` parameter in the `functions_metrics` function represents
    the timestamp or time at which the metrics are generated or collected. This parameter is used to
    track when the metrics data was obtained for the functions listed in the inventory. It helps in
    maintaining a record of when the metrics were collected for
    """
    next_token = None
    idx = 0
    client = session.client('lambda', region_name=region)
    account_id = account['account_id']
    while True:
        try:
            inventory = []
            response = client.list_functions(
                Marker=next_token) if next_token else client.list_functions()
            for resource in response.get('Functions', []):
                inventory.append(resource['FunctionName'])
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
