from inspect import stack
from utils.utils import save_as_file_parquet_metrics, generate_parquet_prefix, get_resource_utilization_metric


def amplify_metrics(file_path, session, region, account, metrics, time_generated, boto_config):
    """
    The function `amplify_metrics` retrieves resource utilization metrics for Amplify apps and saves
    them as Parquet files.

    :param file_path: The `file_path` parameter is a string that represents the path where the metrics
    data will be saved as a file. It should specify the location and name of the file where the metrics
    data will be stored
    :param session: The `session` parameter in the `amplify_metrics` function is typically an AWS
    session object that allows you to interact with AWS services using the credentials and configuration
    provided. It is used to create a client for the AWS Amplify service in the specified region
    :param region: Region is the geographical area where the resources are located. It is typically a
    string representing a specific region where the AWS services are deployed, such as 'us-east-1' for
    US East (N. Virginia) or 'eu-west-1' for EU (Ireland)
    :param account: The `account` parameter in the `amplify_metrics` function seems to represent an
    account object with an 'account_id' attribute. It is used to extract the account ID for further
    processing within the function. If you have any specific questions or need assistance with this
    parameter or any other part of
    :param metrics: The `metrics` parameter in the `amplify_metrics` function likely refers to a list of
    specific metrics that you want to collect or analyze for the Amplify apps listed in the inventory.
    These metrics could include performance metrics, usage statistics, error rates, or any other
    relevant data points that you
    :param time_generated: Time_generated is a parameter that represents the timestamp or time at which
    the metrics were generated or collected. It is typically a datetime object or a string representing
    a specific point in time when the metrics data was recorded
    """
    next_token = None
    idx = 0
    client = session.client('amplify', region_name=region, config=boto_config)
    account_id = account['account_id']
    while True:
        try:
            inventory = []
            response = client.list_apps(
                NextToken=next_token) if next_token else client.list_apps()
            for resource in response.get('apps', []):
                inventory.append(resource.get('appId'))
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
