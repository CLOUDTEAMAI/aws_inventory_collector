from inspect import stack
from utils.utils import extract_common_info, save_as_file_parquet, generate_parquet_prefix


def list_lambda(file_path, session, region, time_generated, account, boto_config):
    """
    This Python function retrieves a list of Lambda functions using the AWS SDK, processes the data, and
    saves it as a Parquet file.

    :param file_path: The `file_path` parameter in the `list_lambda` function represents the path where
    the inventory data will be saved as a file. This could be a file path on your local machine or a
    cloud storage location where you want to store the inventory information
    :param session: The `session` parameter is typically an instance of `boto3.Session` that represents
    your AWS credentials and configuration. It is used to create clients for AWS services like Lambda in
    this case
    :param region: The `region` parameter in the `list_lambda` function represents the AWS region where
    the Lambda functions are located. This parameter is used to specify the region when creating a
    client for the AWS Lambda service and when extracting information about Lambda functions in that
    specific region
    :param time_generated: Time_generated is a timestamp indicating when the data was generated. It is
    used in the function to capture the time at which the inventory data is being collected
    :param account: The `account` parameter seems to be a dictionary containing information about an
    account. It has keys like 'account_id' and 'account_name'. The function `list_lambda` uses this
    account information to extract specific details and perform operations related to AWS Lambda
    functions
    """
    next_token = None
    idx = 0
    client = session.client('lambda', region_name=region, config=boto_config)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.list_functions(
                Marker=next_token) if next_token else client.list_functions()
            for resource in response.get('Functions'):
                if 'ApplyOn' in resource['SnapStart'] and resource['SnapStart']['ApplyOn'] is None:
                    resource['SnapStart']['ApplyOn'] = "null"
                arn = resource.get('FunctionArn', '')
                inventory_object = extract_common_info(
                    arn, resource, region, account_id, time_generated, account_name)
                inventory.append(inventory_object)
            save_as_file_parquet(inventory, file_path, generate_parquet_prefix(
                str(stack()[0][3]), region, account_id, idx))
            next_token = response.get('NextMarker', None)
            idx = idx + 1
            if not next_token:
                break
        except Exception as e:
            print(e)
            break
