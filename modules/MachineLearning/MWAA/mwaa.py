from inspect import stack
from utils.utils import extract_common_info, save_as_file_parquet, generate_parquet_prefix


def list_mwaa(file_path, session, region, time_generated, account, boto_config):
    """
    This Python function retrieves and processes information about MWAA environments and saves the data
    in a Parquet file.

    :param file_path: The `file_path` parameter is the path where the output file will be saved. It
    should be a string representing the file path on the system where you want to save the file
    :param session: The `session` parameter in the `list_mwaa` function is an AWS session object that is
    used to create a client for the Amazon Managed Workflows for Apache Airflow (MWAA) service. This
    session object is typically created using the `boto3` library in Python and is
    :param region: The `region` parameter in the `list_mwaa` function is used to specify the AWS region
    where the Amazon Managed Workflows for Apache Airflow (MWAA) resources are located. This parameter
    is required to create a client for the MWAA service in the specified region and to list and
    :param time_generated: The `time_generated` parameter in the `list_mwaa` function is used to specify
    the timestamp or time at which the inventory data is being generated or collected. This timestamp is
    important for tracking when the inventory information was retrieved and can be used for various
    purposes such as auditing, monitoring, or
    :param account: The `account` parameter in the `list_mwaa` function seems to be a dictionary
    containing information about the account. It likely includes keys such as 'account_id' and
    'account_name' which are used within the function to retrieve specific details related to the
    account for processing
    """
    next_token = None
    idx = 0
    client = session.client('mwaa', region_name=region, config=boto_config)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.list_environments(
                NextToken=next_token) if next_token else client.list_environments()
            for resource in response.get('Environments', []):
                resource_response = client.get_environment(
                    Name=resource
                )
                environment = resource_response.get('Environment')
                if 'CreatedAt' in environment:
                    environment['CreatedAt'] = environment['CreatedAt'].isoformat()
                if 'LastUpdate' in environment:
                    if 'CreatedAt' in environment['LastUpdate']:
                        environment['LastUpdate']['CreatedAt'] = environment['LastUpdate']['CreatedAt'].isoformat(
                        )
                arn = environment.get('Arn')
                inventory_object = extract_common_info(
                    arn, environment, region, account_id, time_generated, account_name)
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
