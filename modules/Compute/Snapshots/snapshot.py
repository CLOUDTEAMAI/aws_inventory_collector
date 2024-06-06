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
