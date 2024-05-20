from inspect import stack
from utils.utils import extract_common_info, save_as_file_parquet, generate_parquet_prefix


def list_rekognition(file_path, session, region, time_generated, account):
    """
    This Python function lists Rekognition collections and saves the information in a Parquet file.

    :param file_path: The `file_path` parameter in the `list_rekognition` function is the path where the
    inventory data will be saved as a file. It should be a string representing the file path where the
    inventory data will be stored
    :param session: The `session` parameter in the `list_rekognition` function is typically an instance
    of a boto3 session that allows you to create service clients for AWS services. It is used to create
    a client for the Amazon Rekognition service in the specified region
    :param region: The `region` parameter in the `list_rekognition` function refers to the AWS region
    where the Amazon Rekognition service will be accessed. This parameter specifies the geographical
    region where the Rekognition resources, such as collections, will be listed and described. It is
    used to configure the client
    :param time_generated: The `time_generated` parameter in the `list_rekognition` function is used to
    specify the timestamp when the inventory is generated. This timestamp is used in creating the
    inventory object and saving it to a file. It helps in tracking when the inventory was generated and
    can be useful for auditing or tracking
    :param account: The `account` parameter in the `list_rekognition` function seems to be a dictionary
    containing information about an account. It likely includes keys such as 'account_id' and
    'account_name'. This information is used within the function to extract details and perform
    operations related to Rekognition collections for
    """
    next_token = None
    idx = 0
    client = session.client('rekognition', region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.list_collections(
                NextToken=next_token) if next_token else client.list_collections()
            for resource in response.get('CollectionIds', []):
                describe_response = client.describe_collection(
                    CollectionId=resource)
                arn = describe_response['CollectionARN']
                resource['CreationTimestamp'] = resource['CreationTimestamp'].isoformat()
                inventory_object = extract_common_info(
                    arn, describe_response, region, account_id, time_generated, account_name)
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
