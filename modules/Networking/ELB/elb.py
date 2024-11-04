from inspect import stack
from utils.utils import extract_common_info, save_as_file_parquet, generate_parquet_prefix


def list_elb(file_path, session, region, time_generated, account):
    """
    This Python function lists Elastic Load Balancers (ELBs) and saves the information to a Parquet
    file.

    :param file_path: The `file_path` parameter in the `list_elb` function is the path where the output
    file will be saved. It is the location on the file system where the Parquet file containing the
    extracted information about Elastic Load Balancers (ELBs) will be stored
    :param session: The `session` parameter in the `list_elb` function is typically an instance of a
    boto3 session that is used to create clients for AWS services. It allows you to make API calls to
    AWS services using the credentials and configuration provided in the session
    :param region: Region is a string representing the AWS region where the Elastic Load Balancers
    (ELBs) are located. It specifies the geographical area where the ELBs are deployed, such as
    'us-east-1' for US East (N. Virginia) or 'eu-west-1' for EU (I
    :param time_generated: Time when the inventory is generated
    :param account: The `account` parameter in the `list_elb` function seems to be a dictionary
    containing information about the account. It likely has the following structure:
    """
    next_token = None
    idx = 0
    client = session.client('elb', region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.describe_load_balancers(
                Marker=next_token) if next_token else client.describe_load_balancers()
            for response in response.get('LoadBalancerDescriptions', []):
                if 'CreatedTime' in response:
                    response['CreatedTime'] = response['CreatedTime'].isoformat()
                arn = response['LoadBalancerArn']
                inventory_object = extract_common_info(
                    arn, response, region, account_id, time_generated, account_name)
                inventory.append(inventory_object)
            save_as_file_parquet(inventory, file_path, generate_parquet_prefix(
                str(stack()[0][3]), region, account_id, idx))
            next_token = response.get('NextMarker', None)
            idx = idx + 1
            if not next_token:
                break
        except Exception as e:
            print(e)
            break


def list_elbv2(file_path, session, region, time_generated, account):
    """
    This Python function lists Elastic Load Balancers (ELBv2) and saves the information as Parquet
    files.

    :param file_path: The `file_path` parameter in the `list_elbv2` function is the path where the
    output file will be saved. It is the location on the file system where the function will write the
    data retrieved from the AWS Elastic Load Balancing service
    :param session: The `session` parameter in the `list_elbv2` function is typically an AWS session
    object that is used to create clients for various AWS services. It is commonly created using the
    `boto3.Session` class and is used to interact with AWS services in a specific region using specific
    credentials
    :param region: Region is a string representing the AWS region where the Elastic Load Balancers (ELB)
    are located. It is used to specify the region in which the AWS SDK client will operate
    :param time_generated: The `time_generated` parameter in the `list_elbv2` function is used to
    specify the timestamp or time at which the inventory data is generated or collected. This timestamp
    is important for tracking when the inventory information was retrieved and can be used for various
    purposes such as auditing, monitoring, or historical
    :param account: The `account` parameter in the `list_elbv2` function seems to be a dictionary
    containing information about an AWS account. It likely has the following structure:
    """
    next_token = None
    idx = 0
    client = session.client('elbv2', region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.describe_load_balancers(
                Marker=next_token) if next_token else client.describe_load_balancers()
            for resource in response.get('LoadBalancers', []):
                if 'CreatedTime' in resource:
                    resource['CreatedTime'] = resource['CreatedTime'].isoformat()
                arn = resource['LoadBalancerArn']
                inventory_object = extract_common_info(
                    arn, resource, region, account_id, time_generated, account_name)
                inventory.append(inventory_object)
            save_as_file_parquet(inventory, file_path, generate_parquet_prefix(
                str(stack()[0][3]), region, account_id, idx))
            next_token = response.get('NextMarker', None)
            idx = idx + 1
            if not next_token:
                break
        except Exception as e:
            print(e)
            break


def list_elbv2_target_group(file_path, session, region, time_generated, account):
    """
    This Python function lists Elastic Load Balancing target groups and saves the information to a
    Parquet file.

    :param file_path: The `file_path` parameter in the `list_elbv2_target_group` function is the path
    where the inventory data will be saved as a Parquet file. It is the location where the output file
    will be stored on the file system
    :param session: The `session` parameter in the `list_elbv2_target_group` function is typically an
    instance of a boto3 session that allows you to create service clients for AWS services. It is used
    to interact with the Elastic Load Balancing (ELB) v2 service in the specified AWS region
    :param region: Region is a string representing the AWS region where the Elastic Load Balancing (ELB)
    resources are located. It specifies the geographical area in which the resources are deployed, such
    as 'us-east-1' for US East (N. Virginia) or 'eu-west-1' for EU (
    :param time_generated: The `time_generated` parameter in the `list_elbv2_target_group` function is
    used to specify the timestamp or time at which the inventory data is generated or collected. This
    timestamp is typically used for tracking when the inventory information was retrieved and can be
    helpful for auditing or monitoring purposes
    :param account: The `account` parameter in the `list_elbv2_target_group` function seems to be a
    dictionary containing information about an AWS account. It likely includes keys such as 'account_id'
    and 'account_name' which are used within the function to extract specific details for processing
    """
    next_token = None
    idx = 0
    client = session.client('elbv2', region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.describe_target_groups(
                Marker=next_token) if next_token else client.describe_target_groups()
            for resource in response.get('TargetGroups', []):
                arn = resource['TargetGroupArn']
                inventory_object = extract_common_info(
                    arn, resource, region, account_id, time_generated, account_name)
                inventory.append(inventory_object)
            save_as_file_parquet(inventory, file_path, generate_parquet_prefix(
                str(stack()[0][3]), region, account_id, idx))
            next_token = response.get('NextMarker', None)
            idx = idx + 1
            if not next_token:
                break
        except Exception as e:
            print(e)
            break


def list_elbv2_listeners(file_path, session, region, time_generated, account):
    """
    This Python function retrieves information about listeners associated with Elastic Load Balancers
    and saves the data in a Parquet file.

    :param file_path: The `file_path` parameter in the `list_elbv2_listeners` function is the path where
    the output file will be saved. It is the location on the file system where the Parquet file
    containing the extracted information will be stored
    :param session: The `session` parameter in the `list_elbv2_listeners` function is typically an
    instance of a boto3 session that allows you to create service clients for AWS services. It is used
    to interact with the Elastic Load Balancing (ELB) service in the specified AWS region. You can
    :param region: The `region` parameter in the `list_elbv2_listeners` function is used to specify the
    AWS region in which the Elastic Load Balancer resources are located. This parameter is passed to the
    `session.client` method to create a client for the Elastic Load Balancing service (`elbv2
    :param time_generated: Time when the data was generated
    :param account: The `account` parameter in the `list_elbv2_listeners` function seems to be a
    dictionary containing information about an AWS account. It likely includes the account ID
    (`account_id`) and the account name (`account_name`). The function uses this account information to
    perform operations related to AWS Elastic Load
    """
    next_token = None
    idx = 0
    listener_idx = 0
    client = session.client('elbv2', region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.describe_load_balancers(
                Marker=next_token) if next_token else client.describe_load_balancers()
            for resource in response.get('LoadBalancers', []):
                listener_next_token = None
                while True:
                    try:
                        listener_response = client.describe_listeners(
                            Marker=listener_next_token, LoadBalancerArn=resource['LoadBalancerArn']) if listener_next_token else client.describe_listeners(LoadBalancerArn=resource['LoadBalancerArn'])
                        for listener in listener_response.get('Listeners'):
                            arn = listener['ListenerArn']
                            inventory_object = extract_common_info(
                                arn, resource, region, account_id, time_generated, account_name)
                            inventory.append(inventory_object)
                        listener_next_token = listener_response.get(
                            'NextMarker', None)
                        listener_idx = listener_idx + 1
                        if not next_token:
                            break
                    except Exception as e:
                        print(e)
                        break
            save_as_file_parquet(inventory, file_path, generate_parquet_prefix(
                str(stack()[0][3]), region, account_id, idx))
            next_token = response.get('NextMarker', None)
            idx = idx + 1
            if not next_token:
                break
        except Exception as e:
            print(e)
            break
