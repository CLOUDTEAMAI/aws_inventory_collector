from inspect import stack
from utils.utils import save_as_file_parquet_metrics, generate_parquet_prefix, get_resource_utilization_metric


def natgateway_metrics(file_path, session, region, account, metrics, time_generated, boto_config):
    """
    The function `natgateway_metrics` retrieves and saves metrics for NAT gateways in a specified region
    and account.
    
    :param file_path: The `file_path` parameter is the file path where the metrics data will be saved.
    It is a string that represents the location where the metrics will be stored, such as a file path on
    the local system or a cloud storage location
    :param session: The `session` parameter in the `natgateway_metrics` function is typically an
    instance of a boto3 session that is used to create clients and resources for AWS services. It allows
    you to configure credentials, region, and other settings for interacting with AWS services
    :param region: Region is a string representing the AWS region where the NAT gateways are located. It
    specifies the geographical area where the resources are deployed, such as 'us-east-1' for US East
    (N. Virginia) or 'eu-west-1' for EU (Ireland)
    :param account: The `account` parameter in the `natgateway_metrics` function seems to be a
    dictionary containing information about the AWS account. It likely includes details such as the
    account ID, which can be accessed using `account['account_id']`. This information is used within the
    function to identify the account associated with
    :param metrics: The `metrics` parameter in the `natgateway_metrics` function likely refers to a data
    structure or object that stores information related to metrics for NAT gateways. This could include
    metrics such as network utilization, data transfer rates, error rates, or any other relevant
    performance indicators for NAT gateways. The
    :param time_generated: The `time_generated` parameter in the `natgateway_metrics` function
    represents the timestamp or time at which the metrics are generated or collected. This parameter is
    used to track when the metrics data was obtained for the NAT gateways in the specified AWS account
    and region. It helps in organizing and analyzing the
    :param boto_config: Boto3 configuration that includes settings such as retries, timeouts, and other
    configurations for the AWS SDK for Python (Boto3). It allows you to customize the behavior of the
    Boto3 client when making API calls to AWS services. You can pass this configuration to the Boto3
    client to
    """
    next_token = None
    idx = 0
    client = session.client('ec2', region_name=region, config=boto_config)
    account_id = account['account_id']
    while True:
        try:
            inventory = []
            response = client.describe_nat_gateways(
                Marker=next_token) if next_token else client.describe_nat_gateways()
            for resource in response.get('NatGateways', []):
                inventory.append(resource['NatGatewayId'])
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
