from inspect import stack
from utils.utils import save_as_file_parquet_metrics, generate_parquet_prefix, get_resource_utilization_metric


def sns_metrics(file_path, session, region, account, metrics, time_generated):
    """
    This Python function retrieves SNS topics, collects metrics for each topic, and saves the metrics to
    a Parquet file.

    :param file_path: The `file_path` parameter is the file path where the metrics data will be saved.
    It should be a string representing the location where the metrics data will be stored, such as a
    file path on the local filesystem or a cloud storage location
    :param session: The `session` parameter in the `sns_metrics` function is typically an instance of a
    boto3 session that is used to create clients for AWS services. It allows you to interact with AWS
    services using the credentials and configuration provided in the session
    :param region: The `region` parameter in the `sns_metrics` function refers to the AWS region where
    the SNS (Simple Notification Service) resources are located. This parameter is used to specify the
    region when creating an SNS client in the AWS SDK and when listing topics in that region. It is
    important to
    :param account: The `account` parameter in the `sns_metrics` function seems to be a dictionary
    containing information about the account. It likely includes the account ID as
    `account['account_id']`
    :param metrics: The `metrics` parameter in the `sns_metrics` function likely refers to a list of
    specific metrics that you want to collect for the SNS (Simple Notification Service) topics in the
    specified AWS account and region. These metrics could include data such as message delivery rates,
    message publishing rates, or any
    :param time_generated: Time_generated is a parameter that represents the timestamp or time at which
    the metrics were generated or collected. It is typically a datetime object or a string representing
    a specific point in time when the metrics data was obtained. This parameter is used in the function
    to track the time at which the metrics were generated and
    """
    next_token = None
    idx = 0
    account_id = account['account_id']
    while True:
        try:
            client = session.client('sns', region_name=region)
            inventory = []
            response = client.list_topics(
                NextToken=next_token) if next_token else client.list_topics()
            for resource in response.get('Topics', []):
                inventory.append(resource['TopicArn'].split(':')[-1])
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
