from inspect import stack
from utils.utils import extract_common_info, save_as_file_parquet, generate_parquet_prefix


def list_workspaces(file_path, session, region, time_generated, account):
    """
    This Python function lists workspaces using the AWS WorkSpaces client and saves the inventory
    information as Parquet files.

    :param file_path: The `file_path` parameter in the `list_workspaces` function is the path where the
    output file will be saved. It is the location on your file system where the function will store the
    workspace information in a Parquet file format
    :param session: The `session` parameter in the `list_workspaces` function is typically an instance
    of the `boto3.Session` class, which is used to create service clients for AWS services. It allows
    you to make API requests to AWS services using the credentials and configuration provided in the
    session
    :param region: The `region` parameter in the `list_workspaces` function is used to specify the AWS
    region in which the WorkSpaces are located. This parameter is required to create a client for the
    WorkSpaces service in the specified region and to retrieve information about the WorkSpaces in that
    region
    :param time_generated: The `time_generated` parameter in the `list_workspaces` function is used to
    specify the timestamp or time at which the workspace inventory is being generated or collected. This
    timestamp is typically used for tracking and auditing purposes to know when the workspace
    information was retrieved
    :param account: The `account` parameter seems to be a dictionary containing information about an AWS
    account. It likely includes keys such as 'account_id' and 'account_name'. This dictionary is used to
    extract the account ID and account name for further processing within the `list_workspaces` function
    """
    next_token = None
    idx = 0
    client = session.client('workspaces', region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.describe_workspaces(
                NextToken=next_token) if next_token else client.describe_workspaces()
            for resource in response.get('Workspaces'):
                if 'StandbyWorkspacesProperties' in resource:
                    resource['RecoverySnapshotTime'] = resource['RecoverySnapshotTime'].isoformat(
                    )
                if 'DataReplicationSettings' in resource:
                    resource['DataReplicationSettings'] = resource['DataReplicationSettings'].isoformat(
                    )
                arn = f"arn:aws:workspaces:{region}:{account_id}:workspace/{resource['WorkspaceId']}"
                inventory_object = extract_common_info(
                    arn, resource, region, account_id, time_generated, account_name)
                inventory.append(inventory_object)
            save_as_file_parquet(inventory, file_path,
                                 generate_parquet_prefix(str(stack()[0][3]), region, account_id, idx))
            next_token = response.get('NextToken', None)
            idx = idx + 1
            if not next_token:
                break
        except Exception as e:
            print(e)
            break


def list_workspaces_thin_client(file_path, session, region, time_generated, account):
    """
    This Python function retrieves information about thin clients in AWS WorkSpaces and saves it as
    Parquet files.

    :param file_path: The `file_path` parameter in the `list_workspaces_thin_client` function is the
    path where the inventory data will be saved as a Parquet file. It is the location where the function
    will write the output data
    :param session: The `session` parameter in the `list_workspaces_thin_client` function is typically
    an instance of a boto3 session that allows you to create service clients. It is used to create a
    client for the AWS WorkSpaces service in the specified region. You can create a session using the `b
    :param region: The `region` parameter in the `list_workspaces_thin_client` function is used to
    specify the AWS region where the WorkSpaces are located. This parameter is important for making API
    calls to the AWS WorkSpaces service in the specified region to retrieve information about the
    WorkSpaces devices
    :param time_generated: The `time_generated` parameter in the `list_workspaces_thin_client` function
    is used to specify the timestamp or time at which the inventory data is being generated or
    collected. This timestamp is important for tracking when the inventory information was retrieved and
    can be used for various purposes such as auditing, monitoring
    :param account: The `account` parameter in the `list_workspaces_thin_client` function seems to be a
    dictionary containing information about an account. It likely has the following structure:
    """
    next_token = None
    idx = 0
    client = session.client('workspaces-thin-client', region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.list_devices(
                nextToken=next_token) if next_token else client.list_devices()
            for resource in response.get('devices', []):
                resource['lastConnectedAt'] = resource['lastConnectedAt'].isoformat()
                resource['lastPostureAt'] = resource['lastPostureAt'].isoformat()
                resource['createdAt'] = resource['createdAt'].isoformat()
                resource['updatedAt'] = resource['updatedAt'].isoformat()
                arn = resource['arn']
                inventory_object = extract_common_info(
                    arn, resource, region, account_id, time_generated, account_name)
                inventory.append(inventory_object)
            save_as_file_parquet(inventory, file_path, generate_parquet_prefix(
                str(stack()[0][3]), region, account_id, idx))
            next_token = response.get('nextToken', None)
            idx = idx + 1
            if not next_token:
                break
        except Exception as e:
            print(e)
            break
