from inspect import stack
from utils.utils import extract_common_info, save_as_file_parquet, generate_parquet_prefix


def list_apprunner(file_path, session, region, time_generated, account, boto_config):
    """
    The function `list_apprunner` retrieves information about App Runner services and saves it to a
    Parquet file.

    :param file_path: The `file_path` parameter in the `list_apprunner` function is the path where the
    output file will be saved. It is the location on the file system where the Parquet file containing
    the inventory data will be stored
    :param session: The `session` parameter in the `list_apprunner` function is an object that
    represents the current session with AWS services. It is typically created using the `boto3.Session`
    class and is used to create clients and resources for interacting with AWS services
    :param region: The `region` parameter in the `list_apprunner` function refers to the AWS region in
    which the App Runner service is being used. This parameter is used to specify the region where the
    AWS App Runner client will be created to interact with the service in that specific region
    :param time_generated: Time when the inventory is generated. It is used to track when the inventory
    data was collected
    :param account: The `account` parameter in the `list_apprunner` function seems to be a dictionary
    containing information about an account. It likely includes keys such as 'account_id' and
    'account_name' which are used within the function to extract specific information for processing App
    Runner services
    """
    next_token = None
    idx = 0
    client = session.client(
        'apprunner', region_name=region, config=boto_config)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.list_services(
                NextToken=next_token) if next_token else client.list_services()
            for resource in response.get('ServiceSummaryList', []):
                arn = resource['ServiceArn']
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
