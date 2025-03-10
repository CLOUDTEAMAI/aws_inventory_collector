from inspect import stack
from utils.utils import extract_common_info, save_as_file_parquet, generate_parquet_prefix


def list_accessanalyzer(file_path, session, region, time_generated, account, boto_config):
    """
    The function `list_accessanalyzer` retrieves and processes data from AWS Access Analyzer analyzers
    and saves it to a Parquet file.

    :param file_path: The `file_path` parameter in the `list_accessanalyzer` function is the path where
    the output file will be saved. It is the location on the file system where the Parquet file
    containing the inventory data will be stored
    :param session: The `session` parameter in the `list_accessanalyzer` function is typically an AWS
    session object that is used to create clients for AWS services. It is used to interact with the AWS
    Access Analyzer service in the specified region
    :param region: The `region` parameter in the `list_accessanalyzer` function is used to specify the
    AWS region where the Access Analyzer is located. This parameter determines the region where the AWS
    SDK client will be created to interact with the Access Analyzer service in that specific region
    :param time_generated: Time_generated is a parameter that represents the timestamp or datetime when
    the function is being executed or when the data is being generated. It is used to track when the
    data was collected or processed
    :param account: The `account` parameter in the `list_accessanalyzer` function seems to be a
    dictionary containing information about the account. It likely includes keys such as 'account_id'
    and 'account_name' which are used within the function to extract relevant information for processing
    by the Access Analyzer client
    """
    next_token = None
    idx = 0
    client = session.client(
        'accessanalyzer', region_name=region, config=boto_config)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.list_analyzers(
                nextToken=next_token) if next_token else client.list_analyzers()
            for resource in response.get('analyzers', []):
                if 'lastResourceAnalyzedAt' in resource:
                    resource['lastResourceAnalyzedAt'] = resource['lastResourceAnalyzedAt'].isoformat(
                    )
                if 'createdAt' in resource:
                    resource['createdAt'] = resource['createdAt'].isoformat()
                arn = resource['arn']
                client_object = extract_common_info(
                    arn, resource, region, account_id, time_generated, account_name)
                inventory.append(client_object)
            save_as_file_parquet(inventory, file_path, generate_parquet_prefix(
                str(stack()[0][3]), region, account_id, idx))
            next_token = response.get('nextToken', None)
            idx = idx + 1
            if not next_token:
                break
        except Exception as e:
            print(e)
            break
