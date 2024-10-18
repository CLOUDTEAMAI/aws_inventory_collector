from inspect import stack
from utils.utils import save_as_file_parquet_metrics, generate_parquet_prefix, get_resource_utilization_metric


def vpn_metrics(file_path, session, region, account, metrics, time_generated):
    """
    This Python function retrieves VPN connection information, extracts tunnel IP addresses, fetches
    resource utilization metrics, and saves the metrics to a Parquet file.

    :param file_path: The `file_path` parameter in the `vpn_metrics` function is the file path where the
    metrics data will be saved. It is the location where the metrics will be stored in a file, typically
    in a specific format like Parquet
    :param session: The `session` parameter in the `vpn_metrics` function is typically an instance of a
    boto3 session that is used to create a client for interacting with AWS services. It is used to make
    API calls to AWS services like EC2 in this case
    :param region: The `region` parameter in the `vpn_metrics` function is used to specify the AWS
    region in which the VPN connections are located. This parameter is essential for creating an EC2
    client in the specified region and for describing the VPN connections within that region. It helps
    in ensuring that the function operates within
    :param account: The `account` parameter in the `vpn_metrics` function is a dictionary containing
    information about the account. It likely includes details such as the account ID, which can be
    accessed using `account['account_id']`. This information is used within the function to identify the
    account associated with the VPN metrics being
    :param metrics: Metrics is a variable that likely stores information related to resource utilization
    metrics for VPN connections. It is used within the function to store and update these metrics as the
    function iterates through VPN connections
    :param time_generated: The `time_generated` parameter in the `vpn_metrics` function represents the
    timestamp or time at which the metrics are generated or collected. This parameter is used to track
    when the metrics data was obtained, allowing for analysis and monitoring of VPN connections over
    time. It is typically a datetime object or timestamp indicating
    """
    next_token = None
    idx = 0
    client = session.client('ec2', region_name=region)
    account_id = account['account_id']
    while True:
        try:
            inventory = []
            response = client.describe_vpn_connections(
                NextToken=next_token) if next_token else client.describe_vpn_connections()
            for resource in response.get('VpnConnections', []):
                for tunnel in resource['Options']['TunnelOptions']:
                    inventory.append(tunnel['OutsideIpAddress'])
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


def clientvpn_metrics(file_path, session, region, account, metrics, time_generated):
    """
    This Python function retrieves metrics for client VPN endpoints and saves them to a Parquet file.

    :param file_path: The `file_path` parameter in the `clientvpn_metrics` function is a string that
    represents the path where the metrics data will be saved as a file. This could be a file path on the
    local file system or a network location where the file will be stored
    :param session: The `session` parameter in the `clientvpn_metrics` function is typically an instance
    of a boto3 session that allows you to create service clients. It is used to interact with AWS
    services like EC2 in the specified region
    :param region: The `region` parameter in the `clientvpn_metrics` function is used to specify the AWS
    region in which the Client VPN endpoints are located. This parameter is required to create an EC2
    client session in the specified region and to retrieve information about the Client VPN endpoints in
    that region
    :param account: The `account` parameter in the `clientvpn_metrics` function likely represents an
    object or dictionary containing information about the AWS account. It seems to have a key
    `account_id` which stores the account's unique identifier. This parameter is used to retrieve the
    account ID and utilize it within the function for
    :param metrics: Metrics is a list of metrics that you want to collect for the Client VPN endpoints.
    It could include metrics such as connection count, data transfer rate, latency, etc
    :param time_generated: The `time_generated` parameter in the `clientvpn_metrics` function represents
    the timestamp or time at which the metrics are generated or collected. This parameter is used to
    track when the metrics data was obtained for the Client VPN endpoints. It is typically a datetime
    object or a string representing the time of data
    """
    next_token = None
    idx = 0
    client = session.client('ec2', region_name=region)
    account_id = account['account_id']
    while True:
        try:
            inventory = []
            response = client.describe_client_vpn_endpoints(
                NextToken=next_token) if next_token else client.describe_client_vpn_endpoints()
            for resource in response.get('ClientVpnEndpoints', []):
                inventory.append(resource['ClientVpnEndpointId'])
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
