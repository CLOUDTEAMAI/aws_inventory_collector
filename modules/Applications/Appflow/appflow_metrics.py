from inspect import stack
from utils.utils import save_as_file_parquet_metrics, generate_parquet_prefix, get_resource_utilization_metric


def appflow_metrics(file_path, session, region, account, metrics, time_generated):
    """
    This Python function retrieves metrics for AppFlow resources, saves them as Parquet files, and
    handles pagination for listing flows.

    :param file_path: The `file_path` parameter is a string that represents the path where the metrics
    data will be saved as a file. It should specify the location and name of the file where the metrics
    will be stored
    :param session: The `session` parameter is typically an instance of a boto3 session that allows you
    to create service clients for AWS services. It is used to interact with AWS services like AppFlow in
    this case
    :param region: The `region` parameter in the `appflow_metrics` function refers to the AWS region
    where the AppFlow service is located. This parameter specifies the region name where the AWS AppFlow
    client will be created to interact with the AppFlow service in that specific region. It is used to
    define the geographical
    :param account: The `account` parameter in the `appflow_metrics` function seems to be a dictionary
    containing information about an account. It likely includes details such as the account ID, which
    can be accessed using `account['account_id']`. This parameter is used within the function to extract
    the account ID for further
    :param metrics: The `metrics` parameter in the `appflow_metrics` function likely refers to a list of
    specific metrics that you want to collect or analyze for the flows listed in the AppFlow service.
    These metrics could include performance indicators, resource utilization data, or any other relevant
    data points that you want to track
    :param time_generated: The `time_generated` parameter in the `appflow_metrics` function likely
    represents the timestamp or time at which the metrics are generated or collected. This parameter is
    used to track when the metrics data was generated and can be helpful for analyzing the data over
    time or for troubleshooting purposes. It is important to
    """
    next_token = None
    idx = 0
    account_id = account['account_id']
    while True:
        try:
            client = session.client('appflow', region_name=region)
            inventory = []
            response = client.list_flows(
                nextToken=next_token) if next_token else client.list_flows()
            for resource in response.get('flows', []):
                inventory.append(resource['flowName'].split(':')[-1])
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
