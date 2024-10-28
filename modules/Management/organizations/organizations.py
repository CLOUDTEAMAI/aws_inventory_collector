from inspect import stack
from utils.utils import extract_common_info, save_as_file_parquet, generate_parquet_prefix


def list_org(file_path, session, region, time_generated, account):
    """
    The function `list_org` retrieves information about an organization using the AWS Organizations API
    and saves it to a Parquet file.

    :param file_path: The `file_path` parameter in the `list_org` function is the path where the output
    files will be saved. It is the location on the file system where the Parquet files containing the
    organization information will be stored
    :param session: The `session` parameter in the `list_org` function is typically an AWS session
    object that is used to create a client for the AWS Organizations service. This session object is
    used to make API calls to interact with the AWS Organizations service in the specified region
    :param region: The `region` parameter in the `list_org` function is used to specify the AWS region
    in which the organization is located. This information is important for making API calls to the AWS
    Organizations service in the specified region. The region parameter is typically a string
    representing the region code (e.g., '
    :param time_generated: The `time_generated` parameter in the `list_org` function is used to specify
    the timestamp or time at which the organization data is being generated or processed. This parameter
    is important for tracking when the organization data was retrieved or updated. It helps in
    maintaining a record of the timing of organization-related operations
    :param account: The `list_org` function takes in several parameters:
    """
    next_token = None
    idx = 0
    client = session.client('organizations')
    account_id = account['account_id']
    account_name = str(account.get('account_name')).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.describe_organization(
                NextToken=next_token) if next_token else client.describe_organization()
            response = response.get('Organization', '')
            arn = response.get('Arn')
            client_object = extract_common_info(
                arn, response, region, account_id, time_generated, account_name)
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
