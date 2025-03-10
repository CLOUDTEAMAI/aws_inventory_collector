from inspect import stack
from utils.utils import save_as_file_parquet_metrics, generate_parquet_prefix, get_resource_utilization_metric


def ebs_volumes_metrics(file_path, session, region, account, metrics, time_generated, boto_config):
    """
    This Python function retrieves EBS volume metrics, saves them as Parquet files, and handles
    pagination for large volumes of data.

    :param file_path: The `file_path` parameter in the `ebs_volumes_metrics` function is a string that
    represents the path where the metrics data will be saved as a file. It is the location where the
    Parquet file containing the metrics will be stored
    :param session: The `session` parameter in the `ebs_volumes_metrics` function is typically an
    instance of a boto3 session that is used to create clients for AWS services. It allows you to make
    API calls to AWS services using the credentials and configuration provided in the session
    :param region: The `region` parameter in the `ebs_volumes_metrics` function is used to specify the
    AWS region in which the EBS volumes are located. This parameter is required for creating an EC2
    client in the specified region and for retrieving information about EBS volumes in that region
    :param account: The `account` parameter in the `ebs_volumes_metrics` function seems to be a
    dictionary containing information about the account. It likely includes keys such as 'account_id'
    and 'account_name'. The 'account_id' is used to identify the account, and the 'account_name' is
    :param metrics: The `metrics` parameter in the `ebs_volumes_metrics` function likely refers to a
    list or dictionary that stores the metrics related to EBS volumes. These metrics could include
    information such as volume size, performance metrics, utilization data, or any other relevant
    statistics that you want to collect and analyze
    :param time_generated: The `time_generated` parameter in the `ebs_volumes_metrics` function is used
    to specify the timestamp or time at which the metrics are generated or collected for the EBS
    volumes. This parameter is likely used to track when the metrics data was collected and can be
    helpful for analysis, monitoring,
    """
    next_token = None
    idx = 0
    client = session.client('ec2', region_name=region, config=boto_config)
    account_id = account['account_id']
    account_name = str(account.get('account_name', '')).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.describe_volumes(
                NextToken=next_token) if next_token else client.describe_volumes()
            for resource in response.get('Volumes', []):
                inventory.append(resource['VolumeId'])
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
