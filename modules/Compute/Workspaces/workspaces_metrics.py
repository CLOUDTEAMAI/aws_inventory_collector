from inspect import stack
from utils.utils import save_as_file_parquet_metrics, generate_parquet_prefix, get_resource_utilization_metric


def workspaces_metrics(file_path, session, region, account, metrics, time_generated):
    """
    This Python function retrieves metrics for WorkSpaces resources using the AWS SDK, saves the metrics
    to a Parquet file, and handles pagination for large result sets.

    :param file_path: The `file_path` parameter in the `workspaces_metrics` function is a string that
    represents the path where the metrics data will be saved as a Parquet file. This file will contain
    the metrics information for the Workspaces resources
    :param session: The `session` parameter in the `workspaces_metrics` function is typically an
    instance of a boto3 session that allows you to create service clients for AWS services. It is used
    to interact with the AWS WorkSpaces service in this function
    :param region: The `region` parameter in the `workspaces_metrics` function refers to the AWS region
    where the Amazon WorkSpaces are located. This parameter specifies the geographical region where the
    WorkSpaces resources are managed and deployed. Examples of AWS regions include 'us-east-1',
    'eu-west-1', '
    :param account: The `account` parameter in the `workspaces_metrics` function seems to be a
    dictionary containing information about an account. It likely includes the account ID as
    `account['account_id']`. This parameter is used to extract the account ID for further processing
    within the function
    :param metrics: The `metrics` parameter in the `workspaces_metrics` function likely refers to a data
    structure or object that stores information related to metrics for the workspaces being processed.
    This could include metrics such as resource utilization, performance data, or any other relevant
    statistics that are being collected and analyzed for the work
    :param time_generated: The `time_generated` parameter in the `workspaces_metrics` function is used
    to specify the time at which the metrics are generated or collected for the workspaces. This
    parameter likely represents a timestamp or datetime value indicating when the metrics data was
    obtained. It is used within the function to track the time
    """
    next_token = None
    idx = 0
    account_id = account['account_id']
    while True:
        try:
            client = session.client('workspaces', region_name=region)
            inventory = []
            response = client.describe_workspaces(
                NextToken=next_token) if next_token else client.describe_workspaces()
            for resource in response.get('Workspaces', []):
                inventory.append(resource['WorkspaceId'])
            if inventory:
                metrics = get_resource_utilization_metric(
                    session, region, inventory, account, metrics, time_generated)
                save_as_file_parquet_metrics(metrics, file_path, generate_parquet_prefix(
                    str(stack()[0][3]), region, f'{account_id}-metrics', idx))
            next_token = response.get('NextToken', None)
            idx = idx + 1
            if not next_token:
                break
        except Exception as e:
            print(e)
            break
