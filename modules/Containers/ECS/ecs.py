from inspect import stack
from utils.utils import extract_common_info, save_as_file_parquet, generate_parquet_prefix


def list_ecs_clusters(file_path, session, region, time_generated, account):
    """
    The function `list_ecs_clusters` retrieves information about ECS clusters, processes the data, and
    saves it as a Parquet file.

    :param file_path: The `file_path` parameter is a string that represents the path where the output
    file will be saved. It should include the file name and extension (e.g., "output_file.csv")
    :param session: The `session` parameter in the `list_ecs_clusters` function is an AWS session object
    that is used to create a client for the ECS (Elastic Container Service) service in a specific AWS
    region. This session object is typically created using the `boto3` library in Python and is
    :param region: Region is a string representing the AWS region where the ECS clusters are located. It
    specifies the geographical area where the resources are deployed. Examples of region names include
    'us-east-1', 'eu-west-1', 'ap-southeast-2', etc
    :param time_generated: The `time_generated` parameter typically refers to the timestamp or datetime
    when the data was generated or when the function is being executed. It is used to track when the
    data was collected or processed, which can be helpful for auditing, logging, or data analysis
    purposes
    :param account: The `account` parameter is a dictionary containing information about the AWS
    account. It typically includes the `account_id` and `account_name` keys, which provide the AWS
    account ID and the name of the account, respectively. In the provided code snippet, the `account`
    dictionary is used to extract
    """
    next_token = None
    idx = 0
    client = session.client('ecs', region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.describe_clusters()
            for resource in response.get('clusters', []):
                arn = resource.get('clusterArn', '')
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
