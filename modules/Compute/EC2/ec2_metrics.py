from inspect import stack
from utils.utils import save_as_file_parquet_metrics, generate_parquet_prefix, get_resource_utilization_metric


def ec2_instances_metrics(file_path, session, region, account, metrics, time_generated, boto_config):
    """
    This Python function retrieves EC2 instance metrics, saves them to a Parquet file, and handles
    pagination for large result sets.

    :param file_path: The `file_path` parameter is a string that represents the file path where the
    metrics data will be saved. It is the location where the output file will be stored after processing
    the EC2 instances metrics
    :param session: The `session` parameter in the `ec2_instances_metrics` function is typically an
    instance of a boto3 session that allows you to create service clients for AWS services. It is used
    to interact with AWS services like EC2 in this case. You can create a session using `boto3.Session
    :param region: The `region` parameter in the `ec2_instances_metrics` function is used to specify the
    AWS region where the EC2 instances are located. This parameter is required to create an EC2 client
    in the specified region and to retrieve information about the EC2 instances in that region
    :param account: The `account` parameter in the `ec2_instances_metrics` function seems to be a
    dictionary containing information about the AWS account. It likely includes the account ID as one of
    its keys
    :param metrics: The `metrics` parameter in the `ec2_instances_metrics` function likely refers to a
    list or dictionary that stores the metrics related to EC2 instances. These metrics could include
    information such as CPU utilization, memory usage, network traffic, etc. The function seems to be
    iterating over EC2 instances,
    :param time_generated: The `time_generated` parameter in the `ec2_instances_metrics` function likely
    represents the timestamp or time at which the metrics are being generated or collected for the EC2
    instances. This parameter is used to track when the metrics were collected and can be used for
    various purposes such as monitoring, analysis,
    """
    next_token = None
    idx = 0
    client = session.client('ec2', region_name=region, config=boto_config)
    account_id = account['account_id']
    while True:
        try:
            inventory = []
            response = client.describe_instances(
                NextToken=next_token) if next_token else client.describe_instances()
            for resource in response.get('Reservations', []):
                for instance in resource['Instances']:
                    inventory.append(instance['InstanceId'])
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


def ec2_instances_cwagent_metrics(file_path, session, region, account, metrics, time_generated, boto_config):
    """
    This Python function retrieves EC2 instance metrics using CloudWatch Agent and saves them to a file
    in Parquet format.

    :param file_path: The `file_path` parameter in the `ec2_instances_cwagent_metrics` function
    represents the file path where the metrics data will be saved. It is the location where the metrics
    will be stored in a file format, such as Parquet
    :param session: The `session` parameter in the `ec2_instances_cwagent_metrics` function is typically
    an instance of a boto3 session that is used to create a client for interacting with AWS services. It
    allows you to configure credentials, region, and other settings for making API calls to AWS
    services. You
    :param region: The `region` parameter in the `ec2_instances_cwagent_metrics` function refers to the
    AWS region where the CloudWatch metrics are being retrieved from. This parameter specifies the
    geographical region where the Amazon EC2 instances are located and where the CloudWatch metrics are
    being collected. It is used to
    :param account: The `account` parameter in the `ec2_instances_cwagent_metrics` function seems to be
    a dictionary containing account information. It likely includes details such as the account ID,
    which can be accessed using `account['account_id']`. This information is used within the function to
    retrieve account-specific metrics
    :param metrics: The `metrics` parameter in the `ec2_instances_cwagent_metrics` function likely
    refers to a list or dictionary that stores information about CloudWatch metrics related to EC2
    instances. This information could include metrics such as CPU utilization, disk usage, memory usage,
    network traffic, etc. The function
    :param time_generated: Time_generated is a parameter that represents the time at which the metrics
    were generated or collected. It is typically a timestamp indicating when the data was recorded. This
    parameter is used in the function `ec2_instances_cwagent_metrics` to help track and manage the
    metrics data for EC2 instances monitored by
    """
    next_token = None
    idx = 0
    client = session.client(
        'cloudwatch', region_name=region, config=boto_config)
    account_id = account['account_id']
    while True:
        try:
            inventory = []
            response = client.list_metrics(
                NextToken=next_token, Namespace='CWAgent') if next_token else client.list_metrics(Namespace='CWAgent')
            for resource in response.get('Metrics', []):
                if resource.get('MetricName') in ('disk_used_percent', 'swap_used_percent', 'mem_used_percent'):
                    for stat in ('Maximum', 'Average'):
                        tmp_resource = resource
                        tmp_resource['Stat'] = stat
                        inventory.append(tmp_resource.copy())
            if inventory:
                metrics = get_resource_utilization_metric(
                    session, region, inventory, account, metrics, time_generated, metrics_list=inventory)
                save_as_file_parquet_metrics(metrics, file_path, generate_parquet_prefix(
                    str(stack()[0][3]), region, f'{account_id}-metrics', idx))
            next_token = response.get('NextToken', None)
            idx = idx + 1
            if not next_token:
                break
        except Exception as e:
            print(e)
            break
