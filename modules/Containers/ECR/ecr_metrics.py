from inspect import stack
from utils.utils import save_as_file_parquet_metrics, generate_parquet_prefix, get_resource_utilization_metric


def ecr_metrics(file_path, session, region, account, metrics, time_generated):
    """
    This Python function retrieves ECR repository information, generates utilization metrics, and saves
    the metrics to a Parquet file.

    :param file_path: The `file_path` parameter in the `ecr_metrics` function represents the path where
    the metrics data will be saved as a file. It is a string that specifies the location or directory
    where the metrics data will be stored
    :param session: The `session` parameter in the `ecr_metrics` function is typically an instance of a
    boto3 session that allows you to interact with AWS services. It is used to create a client for
    Amazon Elastic Container Registry (ECR) in the specified region
    :param region: Region is a string representing the AWS region where the ECR (Elastic Container
    Registry) is located. It specifies the geographical area where the ECR resources are provisioned and
    managed. Examples of region names include 'us-east-1', 'eu-west-2', 'ap-southeast-
    :param account: The `account` parameter in the `ecr_metrics` function seems to represent an account
    object that contains information about the account, such as the account ID. It is used to extract
    the account ID from the account object and pass it to other parts of the function for processing
    :param metrics: The `metrics` parameter in the `ecr_metrics` function seems to be a variable that is
    being passed to the function. It is likely used to store or accumulate some kind of metrics data
    related to ECR repositories. The function appears to iterate over ECR repositories, retrieve some
    metrics data for
    :param time_generated: The `time_generated` parameter in the `ecr_metrics` function likely
    represents the timestamp or time at which the metrics are being generated or collected. This
    parameter is used to track when the metrics data was generated and can be helpful for analyzing
    trends over time or for auditing purposes. It is important to
    """
    next_token = None
    idx = 0
    client = session.client('ecr', region_name=region)
    account_id = account['account_id']
    while True:
        try:
            inventory = []
            response = client.describe_repositories(
                nextToken=next_token) if next_token else client.describe_repositories()
            for resource in response.get('repositories', []):
                inventory.append(resource['repositoryName'])
            if inventory:
                metrics = get_resource_utilization_metric(
                    session, region, inventory, account, metrics, time_generated)
                save_as_file_parquet_metrics(metrics, file_path, generate_parquet_prefix(
                    str(stack()[0][3]), region, f'{account_id}-metrics', idx))
            next_token = response.get('nextToken', None)
            idx = idx + 1
            if not next_token:
                break
        except Exception as e:
            print(e)
            break
