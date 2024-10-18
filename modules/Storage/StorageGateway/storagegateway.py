from inspect import stack
from utils.utils import extract_common_info, save_as_file_parquet, generate_parquet_prefix


def list_storagegateway(file_path, session, region, time_generated, account):
    """
    The function `list_storagegateway` retrieves information about storage gateways and saves it in a
    Parquet file.

    :param file_path: The `file_path` parameter is the path where the output file will be saved. It
    should be a string representing the file path on the system where the code is running
    :param session: The `session` parameter in the `list_storagegateway` function is typically an
    instance of a boto3 session that is used to create a client for AWS services. It allows you to make
    API calls to AWS services using the credentials and configuration provided in the session
    :param region: The `region` parameter in the `list_storagegateway` function is used to specify the
    AWS region in which the Storage Gateway resources are located. This parameter is required to create
    a client session for the AWS Storage Gateway service in the specified region
    :param time_generated: The `time_generated` parameter in the `list_storagegateway` function is used
    to specify the timestamp or time at which the inventory data is generated or collected. This
    timestamp is typically used for tracking when the inventory information was retrieved and can be
    helpful for auditing or monitoring purposes. It is important to ensure
    :param account: The `account` parameter in the `list_storagegateway` function seems to be a
    dictionary containing information about an account. It likely has the following structure:
    """
    next_token = None
    idx = 0
    client = session.client('storagegateway', region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.list_gateways(
                Marker=next_token) if next_token else client.list_gateways()
            for resource in response.get('Gateways', []):
                arn = resource['GatewayARN']
                inventory_object = extract_common_info(
                    arn, resource, region, account_id, time_generated)
                inventory.append(inventory_object)
            save_as_file_parquet(inventory, file_path, generate_parquet_prefix(
                str(stack()[0][3]), region, account_id, idx))
            next_token = response.get('Marker', None)
            idx = idx + 1
            if not next_token:
                break
        except Exception as e:
            print(e)
            break
