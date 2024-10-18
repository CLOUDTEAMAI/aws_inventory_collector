from inspect import stack
from utils.utils import extract_common_info, save_as_file_parquet, generate_parquet_prefix


def list_directconnect_connections(file_path, session, region, time_generated, account):
    """
    This Python function retrieves and saves information about Direct Connect connections to a file in
    Parquet format.

    :param file_path: The `file_path` parameter is the path where the output file will be saved. It
    should be a string representing the file path including the file name and extension where the data
    will be stored
    :param session: The `session` parameter in the `list_directconnect_connections` function is
    typically an instance of the `boto3.Session` class that represents your AWS session. It is used to
    create a client for the AWS Direct Connect service in the specified region. This session object
    allows you to make API calls
    :param region: The `region` parameter in the `list_directconnect_connections` function is used to
    specify the AWS region where the Direct Connect connections are located. This parameter is required
    to create a client for the Direct Connect service in the specified region and to retrieve
    information about the Direct Connect connections in that region
    :param time_generated: Time when the inventory is generated
    :param account: The `account` parameter in the `list_directconnect_connections` function seems to be
    a dictionary containing information about an AWS account. It likely includes keys such as
    'account_id' and 'account_name' to identify the account
    """
    next_token = None
    idx = 0
    client = session.client('directconnect', region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.describe_connections(
                NextToken=next_token) if next_token else client.describe_connections()
            for resource in response.get('connections', []):
                arn = resource.get(
                    'Arn', f'arn:aws:directconnect:{region}:{account_id}:dxcon/{resource.get("connectionId")}')
                if 'loaIssueTime' in resource:
                    resource['loaIssueTime'] = resource['loaIssueTime'].isoformat()
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


def list_directconnect_interfaces(file_path, session, region, time_generated, account):
    """
    This Python function retrieves and saves information about Direct Connect interfaces for a specified
    account and region.

    :param file_path: The `file_path` parameter in the `list_directconnect_interfaces` function is the
    path where the output file will be saved. It is the location on the file system where the Parquet
    file containing the extracted Direct Connect interface information will be stored
    :param session: The `session` parameter in the `list_directconnect_interfaces` function is typically
    an instance of the `boto3.Session` class that is used to create a client for AWS services. It is
    used to make API calls to the AWS Direct Connect service in this case. The `session.client('
    :param region: The `region` parameter in the `list_directconnect_interfaces` function is used to
    specify the AWS region in which the Direct Connect interfaces are located. This parameter is
    required to create a client for the Direct Connect service in the specified region and to retrieve
    information about the virtual interfaces in that region
    :param time_generated: The `time_generated` parameter in the `list_directconnect_interfaces`
    function is used to specify the timestamp or time at which the inventory data is generated or
    collected. This timestamp is typically used for tracking when the data was retrieved and can be
    helpful for auditing or monitoring purposes. It is passed as an
    :param account: The `account` parameter in the `list_directconnect_interfaces` function seems to be
    a dictionary containing information about an AWS account. It likely includes keys such as
    'account_id' and 'account_name' which are used within the function to identify the account and its
    resources
    """
    next_token = None
    idx = 0
    client = session.client('directconnect', region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.describe_virtual_interfaces(
                NextToken=next_token) if next_token else client.describe_virtual_interfaces()
            for resource in response.get('virtualInterfaces', []):
                arn = resource.get(
                    'Arn', f'arn:aws:directconnect:{region}:{account_id}:dxvif/{resource.get("virtualInterfaceId")}')
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
