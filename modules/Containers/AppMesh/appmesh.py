from inspect import stack
from utils.utils import extract_common_info, save_as_file_parquet, generate_parquet_prefix


def list_appmesh(file_path, session, region, time_generated, account, boto_config):
    """
    The function `list_appmesh` retrieves information about App Mesh resources and saves it to a Parquet
    file.

    :param file_path: The `file_path` parameter in the `list_appmesh` function is the path where the
    output files will be saved. It is the location where the inventory data for App Mesh will be stored
    in Parquet format
    :param session: The `session` parameter in the `list_appmesh` function is typically an instance of a
    boto3 session that is used to create a client for AWS AppConfig service. It allows you to make API
    calls to AWS AppConfig in a specific region with the necessary credentials and configuration
    :param region: The `region` parameter in the `list_appmesh` function is used to specify the AWS
    region in which the App Mesh resources will be listed. This parameter is required for creating the
    client session and accessing the App Mesh service in the specified region
    :param time_generated: Time when the inventory is generated in a specific format, such as
    'YYYY-MM-DD HH:MM:SS'
    :param account: The `account` parameter in the `list_appmesh` function seems to be a dictionary
    containing information about an account. It likely has keys such as 'account_id' and 'account_name'
    which are used within the function to extract specific information
    """
    next_token = None
    idx = 0
    client = session.client('appmesh', region_name=region, config=boto_config)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.list_meshes(
                nextToken=next_token) if next_token else client.list_meshes()
            for resource in response.get('meshes', []):
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
