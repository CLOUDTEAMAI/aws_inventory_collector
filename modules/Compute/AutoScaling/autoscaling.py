from inspect import stack
from utils.utils import extract_common_info, save_as_file_parquet, generate_parquet_prefix


def list_autoscaling(file_path, session, region, time_generated, account, boto_config):
    """
    The function `list_autoscaling` retrieves information about auto-scaling groups and saves it to a
    Parquet file.

    :param file_path: The `file_path` parameter in the `list_autoscaling` function is the path where the
    output file will be saved. It is the location where the Parquet file containing the inventory
    information will be stored
    :param session: The `session` parameter in the `list_autoscaling` function is typically an instance
    of a boto3 session that allows you to create service clients for AWS services. It is used to
    interact with the AWS Auto Scaling service in this particular function
    :param region: Region is a string representing the geographical region where the autoscaling groups
    are located. It is used to specify the AWS region for the autoscaling client to interact with
    :param time_generated: Time when the inventory is generated
    :param account: The `account` parameter in the `list_autoscaling` function seems to be a dictionary
    containing information about the account. It likely includes keys such as 'account_id' and
    'account_name'. This information is used within the function to extract details related to the
    account for further processing
    """
    next_token = None
    idx = 0
    client = session.client(
        'autoscaling', region_name=region, config=boto_config)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.describe_auto_scaling_groups(
                NextToken=next_token) if next_token else client.describe_auto_scaling_groups()
            for resource in response.get('AutoScalingGroups', []):
                if 'CreatedTime' in resource:
                    resource['CreatedTime'] = resource['CreatedTime'].isoformat()
                arn = resource['AutoScalingGroupARN']
                inventory_object = extract_common_info(
                    arn, resource, region, account_id, time_generated, account_name)
                inventory.append(inventory_object)
            save_as_file_parquet(inventory, file_path, generate_parquet_prefix(
                str(stack()[0][3]), region, account_id, idx))
            next_token = response.get('NextToken', None)
            idx = idx + 1
            if not next_token:
                break
        except Exception as e:
            print(e)
            break


def list_autoscaling_plans(file_path, session, region, time_generated, account, boto_config):
    """
    The function `list_autoscaling_plans` retrieves and processes autoscaling plans information and
    saves it as Parquet files.

    :param file_path: The `file_path` parameter in the `list_autoscaling_plans` function is the path
    where the output files will be saved. It is the location where the Parquet files containing the
    autoscaling plans information will be stored
    :param session: The `session` parameter in the `list_autoscaling_plans` function is typically an
    instance of a boto3 session that allows you to create service clients for AWS services. It is used
    to create a client for the AWS Auto Scaling Plans service in the specified region
    :param region: The `region` parameter in the `list_autoscaling_plans` function refers to the AWS
    region where the Auto Scaling plans are to be listed. This parameter specifies the geographical area
    in which the resources will be managed and deployed. Examples of AWS regions include `us-east-1`,
    `us
    :param time_generated: The `time_generated` parameter in the `list_autoscaling_plans` function
    represents the timestamp indicating when the autoscaling plans inventory is being generated. This
    timestamp is used to track when the inventory data was collected or processed
    :param account: The `list_autoscaling_plans` function takes in several parameters:
    """
    next_token = None
    idx = 0
    client = session.client('autoscaling-plans',
                            region_name=region, config=boto_config)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.describe_scaling_plans(
                NextToken=next_token) if next_token else client.describe_scaling_plans()
            for repo in response.get('ScalingPlans', []):
                if 'StatusStartTime' in repo:
                    repo['StatusStartTime'] = repo['StatusStartTime'].isoformat()
                if 'CreationTime' in repo:
                    repo['CreationTime'] = repo['CreationTime'].isoformat()
                arn = repo['repositoryArn']
                inventory_object = extract_common_info(
                    arn, repo, region, account_id, time_generated, account_name)
                inventory.append(inventory_object)
            save_as_file_parquet(inventory, file_path, generate_parquet_prefix(
                str(stack()[0][3]), region, account_id, idx))
            next_token = response.get('NextToken', None)
            idx = idx + 1
            if not next_token:
                break
        except Exception as e:
            print(e)
            break
