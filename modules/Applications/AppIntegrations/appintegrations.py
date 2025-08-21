from inspect import stack
from utils.utils import extract_common_info, save_as_file_parquet, generate_parquet_prefix


def list_appintegrations(file_path, session, region, time_generated, account, boto_config):
    """
    The function `list_appintegrations` retrieves a list of applications from an AWS account and saves
    the information in a Parquet file.

    :param file_path: The `file_path` parameter is the path where the output file will be saved. It
    should be a string representing the file path including the file name and extension where the
    inventory data will be stored
    :param session: The `session` parameter is typically an instance of a boto3 session that allows you
    to interact with AWS services. It is used to create a client for the `appintegrations` service in
    the specified `region`
    :param region: Region is a string representing the geographical region where the resources are
    located. It is used to specify the AWS region for the AppIntegrations client
    :param time_generated: The `time_generated` parameter in the `list_appintegrations` function is used
    to specify the timestamp or time at which the inventory data is generated or collected. This
    timestamp is typically used for tracking when the inventory information was retrieved and can be
    helpful for auditing or monitoring purposes
    :param account: The `account` parameter in the `list_appintegrations` function seems to be a
    dictionary containing information about an account. It likely has keys such as 'account_id' and
    'account_name' which are used within the function to extract specific details for processing
    """
    next_token = None
    idx = 0
    client = session.client(
        'appintegrations', region_name=region, config=boto_config)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.list_applications(
                NextToken=next_token) if next_token else client.list_applications()
            for resource in response.get('Applications', []):
                arn = resource['Arn']
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
