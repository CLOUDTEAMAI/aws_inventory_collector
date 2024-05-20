from inspect import stack
from utils.utils import extract_common_info, save_as_file_parquet, generate_parquet_prefix


def list_athena(file_path, session, region, time_generated, account):
    """
    The function `list_athena` retrieves and saves information about named queries in AWS Athena for a
    specific account.

    :param file_path: The `file_path` parameter is the path where the output file will be saved. It
    should be a string representing the directory and filename where the file will be stored
    :param session: The `session` parameter is typically an instance of `boto3.Session` that represents
    your AWS credentials and configuration. It is used to create clients for AWS services like Athena in
    this case. You can create a session using `boto3.Session()` and pass it to the `list_ath
    :param region: The `region` parameter in the `list_athena` function refers to the AWS region where
    the Athena service is located. This parameter is used to specify the region name when creating an
    Athena client in the provided session. It is important to set the correct region to ensure that the
    client interacts with
    :param time_generated: Time when the function is being executed
    :param account: The `account` parameter in the `list_athena` function seems to be a dictionary
    containing information about an AWS account. It likely has the following structure:
    """
    next_token = None
    idx = 0
    client = session.client('athena', region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.list_named_queries()
            for resource in response['NamedQueryIds']:
                query_details = client.get_named_query(NamedQueryId=resource)
                arn = f"arn:aws:athena:{region}:{account_id}:named_query/{resource}"
                inventory_object = extract_common_info(
                    arn, query_details.get('NamedQuery', {}), region, account_id, time_generated, account_name)
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
