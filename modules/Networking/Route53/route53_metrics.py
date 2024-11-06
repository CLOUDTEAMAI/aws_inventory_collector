from inspect import stack
from utils.utils import save_as_file_parquet_metrics, generate_parquet_prefix, get_resource_utilization_metric


def route53_metrics(file_path, session, region, account, metrics, time_generated):
    """
    The function `route53_metrics` retrieves Route 53 hosted zones, collects resource utilization
    metrics, and saves the metrics to a Parquet file.

    :param file_path: The `file_path` parameter in the `route53_metrics` function is the file path where
    the metrics data will be saved. It is the location where the metrics will be stored in a file,
    typically in a specific format like Parquet
    :param session: The `session` parameter in the `route53_metrics` function is typically an instance
    of a boto3 session that is used to create a client for interacting with AWS services. It allows you
    to configure credentials, region, and other settings for making API calls to AWS services like Route
    53
    :param region: Region is a string representing the AWS region where the Route 53 metrics will be
    collected from. It specifies the geographical area where the resources are located, such as
    'us-east-1' for US East (N. Virginia) or 'eu-west-1' for EU (Ireland)
    :param account: The `account` parameter in the `route53_metrics` function seems to be a dictionary
    containing account information. It likely includes details such as the account ID, which can be
    accessed using `account['account_id']`. This information is used within the function to interact
    with AWS Route 53 services on
    :param metrics: The `metrics` parameter in the `route53_metrics` function seems to be a variable
    that is being passed to the function. It is likely used to store or accumulate some kind of metrics
    data related to Route 53 hosted zones. The function appears to be iterating over hosted zones,
    retrieving some metrics
    :param time_generated: The `time_generated` parameter in the `route53_metrics` function represents
    the timestamp or time at which the metrics are generated or collected. This parameter is used to
    track when the metrics data was obtained, allowing for better analysis and monitoring of Route 53
    resources over time. It is typically a datetime
    """
    next_token = None
    idx = 0
    client = session.client('route53', region_name=region)
    account_id = account['account_id']
    while True:
        try:
            inventory = []
            response = client.list_hosted_zones(
                Marker=next_token) if next_token else client.list_hosted_zones()
            for resource in response.get('HostedZones', []):
                inventory.append(resource['Id'].split('/')[-1])
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


def route53_resolver_metrics(file_path, session, region, account, metrics, time_generated):
    """
    The function `route53_resolver_metrics` retrieves Route 53 Resolver endpoints, collects resource
    utilization metrics, and saves the metrics to a Parquet file.

    :param file_path: The `file_path` parameter in the `route53_resolver_metrics` function is a string
    that represents the path where the metrics data will be saved as a file. This could be a file path
    on the local file system or a cloud storage location, depending on where you want to store the
    metrics data
    :param session: The `session` parameter in the `route53_resolver_metrics` function is typically an
    instance of a boto3 session that is used to create a client for AWS Route 53 Resolver service. This
    session allows you to make API calls to AWS services using the credentials and configuration
    provided
    :param region: Region is a string representing the AWS region where the Route 53 Resolver service is
    located. It specifies the geographical area where the resources will be provisioned and where the
    operations will be performed. Examples of region names include 'us-east-1', 'eu-west-1',
    'ap-southeast
    :param account: The `account` parameter in the `route53_resolver_metrics` function seems to be a
    dictionary containing information about the account. It likely includes the account ID as one of its
    keys
    :param metrics: The `metrics` parameter in the `route53_resolver_metrics` function seems to be a
    variable that is being passed to the function. It is likely used to store or accumulate some kind of
    metrics data related to Route 53 Resolver endpoints. The function appears to be iterating over a
    list of Resolver endpoints
    :param time_generated: The `time_generated` parameter in the `route53_resolver_metrics` function
    represents the timestamp or datetime when the metrics are generated or collected. This parameter is
    used to track when the metrics data was obtained, which can be helpful for analysis, monitoring, and
    troubleshooting purposes. It is typically in a format
    """
    next_token = None
    idx = 0
    client = session.client('route53resolver', region_name=region)
    account_id = account['account_id']
    while True:
        try:
            inventory = []
            response = client.list_resolver_endpoints(
                NextToken=next_token) if next_token else client.list_resolver_endpoints()
            for resource in response.get('ResolverEndpoints', []):
                inventory.append(resource.get('Id', ''))
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
