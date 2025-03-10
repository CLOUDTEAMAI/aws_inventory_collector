from inspect import stack
from utils.utils import extract_common_info, save_as_file_parquet, generate_parquet_prefix


def list_appsync(file_path, session, region, time_generated, account, boto_config):
    """
    The function `list_appsync` retrieves information about GraphQL APIs using the AWS AppSync client
    and saves the data to a Parquet file.

    :param file_path: The `file_path` parameter in the `list_appsync` function is the path where the
    inventory data will be saved as a Parquet file. This parameter should be a string representing the
    file path where you want to save the inventory data
    :param session: The `session` parameter in the `list_appsync` function is an object representing the
    session used to interact with AWS services. It is typically created using the `boto3.Session` class
    and contains the necessary credentials and configuration to make API requests to AWS services
    :param region: Region is a string representing the geographical region where the AWS resources are
    located. It is used to specify the AWS region in which the AppSync client will operate
    :param time_generated: The `time_generated` parameter in the `list_appsync` function is used to
    specify the timestamp or time at which the inventory data is generated or collected. This timestamp
    is important for tracking when the inventory information was retrieved and can be useful for
    auditing or monitoring purposes. It helps in associating the
    :param account: The `account` parameter in the `list_appsync` function seems to be a dictionary
    containing information about an AWS account. It likely has the following structure:
    """
    next_token = None
    idx = 0
    client = session.client('appsync', region_name=region, config=boto_config)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.list_graphql_apis(
                nextToken=next_token) if next_token else client.list_graphql_apis()
            for resource in response.get('graphqlApis', []):
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
