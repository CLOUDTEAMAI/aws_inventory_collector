from inspect import stack
from utils.utils import extract_common_info, save_as_file_parquet, generate_parquet_prefix


def list_appconfig(file_path, session, region, time_generated, account, boto_config):
    """
    The function `list_appconfig` retrieves a list of applications from Alexa for Business and saves the
    information in a Parquet file.

    :param file_path: The `file_path` parameter in the `list_appconfig` function is the path where the
    output file will be saved. It is the location on the file system where the Parquet file containing
    the inventory information will be stored
    :param session: The `session` parameter in the `list_appconfig` function is an object that
    represents the current session with AWS services. It is typically created using the
    `boto3.Session()` method and is used to create clients and resources for interacting with AWS
    services
    :param region: The `region` parameter in the `list_appconfig` function refers to the AWS region
    where the Alexa for Business service is located. This parameter is used to specify the region in
    which the AWS client will operate and interact with the Alexa for Business service APIs. It is
    important to provide the correct region
    :param time_generated: Time when the configuration data was generated. It is used as a reference for
    the inventory objects created during the listing process
    :param account: The `account` parameter in the `list_appconfig` function seems to be a dictionary
    containing information about an AWS account. It likely includes keys such as 'account_id' and
    'account_name' which are used within the function to retrieve specific details related to the
    account
    """
    next_token = None
    idx = 0
    client = session.client(
        'appconfig', region_name=region, config=boto_config)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.list_applications(
                NextToken=next_token) if next_token else client.list_applications()
            for resource in response.get('Items', []):
                arn = f'arn:aws:appconfig:{region}:{account_id}:application/{resource.get("Id", "")}'
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
