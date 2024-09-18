from inspect import stack
from utils.utils import extract_common_info, save_as_file_parquet, generate_parquet_prefix


def list_account_name(file_path, session, region, time_generated, account):
    next_token = None
    idx = 0
    client = session.client('iam')
    account_id = account['account_id']
    account_name = str(account.get('account_name')).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.list_account_aliases()
            arn = f"account/{account_id}"
            if response:
                response = response.get('AccountAliases', [])
                resource = {
                    "id": account_id,
                    "name": response[0]
                }
                client_object = extract_common_info(
                    arn, resource, region, account_id, time_generated, account_name)
                inventory.append(client_object)
            save_as_file_parquet(inventory, file_path, generate_parquet_prefix(
                str(stack()[0][3]), 'global', account_id, idx))
            next_token = response.get('NextToken', None)
            idx = idx + 1
            if not next_token:
                break
        except Exception as e:
            print(e)
            break


def list_accounts(file_path, session, region, time_generated, account):
    """
    The function `list_accounts` retrieves account information from AWS Organizations, processes it, and
    saves it as a Parquet file.

    :param file_path: The `file_path` parameter in the `list_accounts` function is the path where the
    output file will be saved. It should be a string representing the file path where you want to save
    the output data. For example, it could be something like "/path/to/output/file.parquet"
    :param session: The `session` parameter in the `list_accounts` function is typically an instance of
    the `boto3.Session` class, which is used to create service clients for AWS services. It allows you
    to make API requests to AWS services using the credentials and configuration provided
    :param region: Region is the geographical area where the AWS resources are located. It is typically
    a string representing the region code, such as 'us-east-1' for US East (N. Virginia) or 'eu-west-1'
    for EU (Ireland)
    :param time_generated: The `time_generated` parameter is likely a timestamp or datetime object that
    represents the time when the accounts list is being generated. It is used in the function
    `list_accounts` to capture the time at which the account information is being processed or
    retrieved. This parameter is important for tracking when the data was
    :param account: The `account` parameter seems to be a dictionary containing information about an AWS
    account. It likely includes keys such as 'account_id' and 'account_name' to identify the account
    """
    next_token = None
    idx = 0
    client = session.client('organizations')
    account_id = account['account_id']
    account_name = str(account.get('account_name')).replace(" ", "_")
    global_arn = f'arn:aws:organizations::{account_id}:account/'
    while True:
        try:
            inventory = []
            response = client.list_accounts(
                NextToken=next_token) if next_token else client.list_accounts()
            for resource in response['Accounts']:
                arn = resource.get('Arn', global_arn)
                client_object = extract_common_info(
                    arn, resource, region, account_id, time_generated, account_name)
                inventory.append(client_object)
            save_as_file_parquet(inventory, file_path, generate_parquet_prefix(
                str(stack()[0][3]), 'global', account_id, idx))
            next_token = response.get('NextToken', None)
            idx = idx + 1
            if not next_token:
                break
        except Exception as e:
            print(e)
            break
