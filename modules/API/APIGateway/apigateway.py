from inspect import stack
from utils.utils import extract_common_info, save_as_file_parquet, generate_parquet_prefix


def list_apigateway(file_path, session, region, time_generated, account, boto_config):
    """
    This Python function lists API Gateway resources and saves the inventory as a Parquet file.

    :param file_path: The `file_path` parameter in the `list_apigateway` function is the path where you
    want to save the inventory data as a file. It should be a string representing the file path
    including the file name and extension where you want to store the data. For example, it could be
    something
    :param session: The `session` parameter in the `list_apigateway` function is typically an instance
    of a boto3 session that allows you to connect to AWS services. It is used to create a client for the
    API Gateway in the specified region. This client is then used to interact with
    the
    :param region: The `region` parameter in the `list_apigateway` function refers to the AWS region
    where the Amazon API Gateway resources are located. This parameter is used to specify the region in
    which the API Gateway client will operate and retrieve information about the API Gateway resources
    in that specific region
    :param time_generated: Time when the function is being executed
    :param account: The `account` parameter is a dictionary containing information about the AWS
    account. It typically includes the `account_id` and `account_name` keys, which provide the unique
    identifier and name of the AWS account, respectively. In the provided code snippet, the `account`
    dictionary is used to extract the
    """
    idx = 0
    client = session.client(
        'apigateway', region_name=region, config=boto_config)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    response = client.get_rest_apis(limit=500)
    inventory = []
    for resource in response.get('items', []):
        api_id = resource['id']
        arn = f"arn:aws:apigateway:{region}::/apis/{api_id}"
        inventory_api = client.get_resources(restApiId=api_id, limit=500)[
            'items']
        inventory_object = extract_common_info(
            arn, inventory_api, region, account_id, time_generated, account_name)
        inventory.append(inventory_object)
    save_as_file_parquet(inventory, file_path,
                         generate_parquet_prefix(str(stack()[0][3]), region, account_id, idx))


def list_apigatewayv2(file_path, session, region, time_generated, account, boto_config):
    """
    This Python function lists API Gateway v2 resources and saves the information to a Parquet file.

    :param file_path: The `file_path` parameter in the `list_apigatewayv2` function is the path where
    the inventory data will be saved as a file. It is the location where the Parquet file containing the
    API Gateway v2 inventory information will be stored
    :param session: The `session` parameter in the `list_apigatewayv2` function is an object
    representing the current session. It is typically created using the `boto3.Session` class from the
    AWS SDK for Python (Boto3). This session object stores configuration state and allows you to create
    service
    :param region: The `region` parameter in the `list_apigatewayv2` function is used to specify the AWS
    region in which the Amazon API Gateway V2 resources will be listed. This parameter determines the
    region where the AWS SDK client will operate and retrieve the API Gateway V2 resources from. It is
    :param time_generated: The `time_generated` parameter in the `list_apigatewayv2` function is used to
    specify the timestamp when the inventory data is generated. This timestamp is typically used for
    tracking and auditing purposes to know when the inventory information was collected or updated
    :param account: The `list_apigatewayv2` function takes in several parameters:
    """
    next_token = None
    idx = 0
    client = session.client(
        'apigatewayv2', region_name=region, config=boto_config)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.get_apis(
                NextToken=next_token) if next_token else client.get_apis()
            for resource in response.get('Items', []):
                if 'CreatedDate' in resource:
                    resource['CreatedDate'] = resource['CreatedDate'].isoformat()
                api_id = resource['ApiId']
                arn = f"arn:aws:apigateway:{region}::/apis/{api_id}"
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
