from inspect import stack
from utils.utils import extract_common_info, save_as_file_parquet, generate_parquet_prefix


def list_savingsplans(file_path, session, region, time_generated, account):
    """
    This Python function retrieves and saves information about savings plans for a specific account in a
    given region.

    :param file_path: The `file_path` parameter is the file path where the savings plans inventory data
    will be saved. It is the location where the output file will be stored
    :param session: The `session` parameter in the `list_savingsplans` function is typically an AWS
    session object that is used to create a client for the AWS service. It is used to make API calls to
    the AWS service, in this case, the Savings Plans service. The session object is usually created
    using
    :param region: Region is the geographical area where the AWS resources are located. It is used to
    specify the region in which the AWS service client will operate. Examples of regions include
    'us-east-1', 'eu-west-1', 'ap-southeast-2', etc
    :param time_generated: Time when the savings plans inventory is generated. It is used to track when
    the inventory was last updated
    :param account: The `account` parameter in the `list_savingsplans` function seems to be a dictionary
    containing information about an account. It likely includes keys such as 'account_id' and
    'account_name' which are used within the function to identify the account and its details
    """
    next_token = None
    idx = 0
    client = session.client('savingsplans', region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.describe_savings_plans(
                nextToken=next_token) if next_token else client.describe_savings_plans()
            for resource in response.get('savingsPlans', []):
                arn = resource.get('savingsPlanArn', '')
                inventory_object = extract_common_info(
                    arn, resource, region, account_id, time_generated, account_name)
                inventory.append(inventory_object)
            save_as_file_parquet(inventory, file_path,
                                 generate_parquet_prefix(str(stack()[0][3]), region, account_id, idx))
            next_token = response.get('nextToken', None)
            idx = idx + 1
            if not next_token:
                break
        except Exception as e:
            print(e)
            break
