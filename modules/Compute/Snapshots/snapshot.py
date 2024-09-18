from inspect import stack
from utils.utils import extract_common_info, save_as_file_parquet, generate_parquet_prefix


def list_ec2_snapshots(file_path, session, region, time_generated, account):
    """
    The function `list_ec2_snapshots` retrieves information about EC2 snapshots, formats the data, and
    saves it to a Parquet file.

    :param file_path: The `file_path` parameter in the `list_ec2_snapshots` function refers to the path
    where the output files will be saved. This should be a valid file path on your system where the
    function will save the EC2 snapshots information in Parquet format
    :param session: The `session` parameter in the `list_ec2_snapshots` function is typically an
    instance of `boto3.Session` class that represents your AWS credentials and configuration. It is used
    to create an EC2 client in the specified region for interacting with AWS services. You can create a
    session using
    :param region: The `region` parameter in the `list_ec2_snapshots` function refers to the AWS region
    where the EC2 snapshots are located. This parameter is used to specify the region in which the AWS
    SDK client will operate and retrieve the EC2 snapshots. It is important to provide the correct AWS
    region
    :param time_generated: The `time_generated` parameter in the `list_ec2_snapshots` function is used
    to specify the timestamp or time at which the snapshots are being generated or processed. This
    parameter is likely used to track when the snapshot data was retrieved or processed for further
    analysis or reporting purposes
    :param account: The `account` parameter in the `list_ec2_snapshots` function seems to be a
    dictionary containing information about an AWS account. It likely includes keys such as 'account_id'
    and 'account_name' which are used within the function to retrieve specific details related to the
    account for processing EC2
    """
    next_token = None
    idx = 0
    client = session.client('ec2', region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.describe_snapshots(NextToken=next_token, OwnerIds=[
                'self']) if next_token else client.describe_snapshots(OwnerIds=['self'])
            for resource in response.get('Snapshots', []):
                if 'StartTime' in resource:
                    resource['StartTime'] = resource['StartTime'].isoformat()
                if 'RestoreExpiryTime' in resource:
                    resource['RestoreExpiryTime'] = resource['RestoreExpiryTime'].isoformat(
                    )
                arn = f"arn:aws:ec2:{region}:{account_id}:snapshot/{resource['SnapshotId']}"
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


def list_ec2_snapshots_fsr(file_path, session, region, time_generated, account):
    """
    This Python function lists EC2 snapshots with fast snapshot restores, extracts common information,
    and saves the data as Parquet files.

    :param file_path: The `file_path` parameter in the `list_ec2_snapshots_fsr` function represents the
    path where the output file will be saved. It is the location on the file system where the function
    will write the results of the EC2 snapshot inventory. This parameter should be a string that
    specifies the
    :param session: The `session` parameter in the `list_ec2_snapshots_fsr` function is typically an AWS
    session object that is used to create a client for the EC2 service in a specific region. This
    session object is usually created using the `boto3` library in Python and is used to
    :param region: The `region` parameter in the `list_ec2_snapshots_fsr` function is used to specify
    the AWS region in which the EC2 snapshots and fast snapshot restores are being described. This
    parameter helps the function to make API calls to the specified region to retrieve information about
    fast snapshot restores associated with
    :param time_generated: The `time_generated` parameter in the `list_ec2_snapshots_fsr` function
    likely represents the timestamp or datetime when the operation is being executed or when the
    snapshots are being processed. This parameter is used in the function to capture the time at which
    certain events related to Fast Snapshot Restores (
    :param account: The `account` parameter in the `list_ec2_snapshots_fsr` function seems to be a
    dictionary containing information about the AWS account. It likely includes keys such as
    'account_id' and 'account_name' to identify the AWS account associated with the session. This
    information is used within the
    """
    next_token = None
    idx = 0
    client = session.client('ec2', region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.describe_fast_snapshot_restores(
                NextToken=next_token) if next_token else client.describe_fast_snapshot_restores()
            for resource in response.get('FastSnapshotRestores', []):
                if 'EnablingTime' in resource:
                    resource['EnablingTime'] = resource['EnablingTime'].isoformat()
                if 'OptimizingTime' in resource:
                    resource['OptimizingTime'] = resource['OptimizingTime'].isoformat(
                    )
                if 'DisablingTime' in resource:
                    resource['DisablingTime'] = resource['DisablingTime'].isoformat(
                    )
                if 'EnabledTime' in resource:
                    resource['EnabledTime'] = resource['EnabledTime'].isoformat()
                if 'DisabledTime' in resource:
                    resource['DisabledTime'] = resource['DisabledTime'].isoformat(
                    )
                arn = f"arn:aws:ec2:{region}:{account_id}:snapshot/{resource['SnapshotId']}-FSR"
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
