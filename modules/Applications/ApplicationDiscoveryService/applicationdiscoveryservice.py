from inspect import stack
from utils.utils import extract_common_info, save_as_file_parquet, generate_parquet_prefix


def list_ads_agents(file_path, session, region, time_generated, account):
    """
    The function `list_ads_agents` retrieves information about AWS Discovery agents and saves it to a
    Parquet file.

    :param file_path: The `file_path` parameter in the `list_ads_agents` function is the path where the
    inventory data will be saved as a Parquet file. It is the location on the file system where the
    output file will be stored
    :param session: The `session` parameter in the `list_ads_agents` function is typically an instance
    of the `boto3.Session` class that is used to create service clients. It allows you to make API calls
    to AWS services. You can create a session by providing your AWS credentials and specifying the
    region where
    :param region: The `region` parameter in the `list_ads_agents` function is used to specify the AWS
    region in which the AWS Discovery service is being accessed. This parameter is required to create a
    client for the AWS Discovery service and to construct the ARN (Amazon Resource Name) for the agents
    being described
    :param time_generated: Time when the inventory data was generated. It is used to timestamp the
    inventory objects
    :param account: The `account` parameter in the `list_ads_agents` function seems to be a dictionary
    containing information about an AWS account. It likely includes keys such as 'account_id' and
    'account_name' which are used within the function to retrieve specific details related to the
    account
    """
    next_token = None
    idx = 0
    client = session.client('discovery', region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.describe_agents(
                nextToken=next_token) if next_token else client.describe_agents()
            for resource in response.get('agentsInfo', []):
                arn = f"arn:aws:discovery:{region}:{account_id}:agent/{resource.get('agentId', '')}"
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
