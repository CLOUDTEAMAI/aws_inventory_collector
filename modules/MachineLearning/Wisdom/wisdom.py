from inspect import stack
from utils.utils import extract_common_info, save_as_file_parquet, generate_parquet_prefix


def list_wisdom(file_path, session, region, time_generated, account):
    """
    This function retrieves a list of knowledge bases using the AWS Wisdom service and saves the
    information to a Parquet file.

    :param file_path: The `file_path` parameter is the path where the output file will be saved. It
    should be a string representing the file path including the file name and extension where the data
    will be stored
    :param session: The `session` parameter is typically an instance of a boto3 session that allows you
    to create service clients for AWS services. It is used to interact with the AWS services in your
    Python code
    :param region: The `region` parameter in the `list_wisdom` function refers to the AWS region where
    the `wisdom` service client will be created. This parameter specifies the geographical region where
    the AWS resources will be managed and operated. It is used to define the region-specific
    configurations for the AWS client
    :param time_generated: The `time_generated` parameter in the `list_wisdom` function is used to
    specify the timestamp when the inventory data is generated. This timestamp is used in the function
    to include the time of generation in the inventory object that is created for each knowledge base
    resource
    :param account: The `account` parameter in the `list_wisdom` function seems to be a dictionary
    containing information about an account. It likely includes keys such as 'account_id' and
    'account_name'. This information is used within the function to extract details needed for
    processing knowledge bases
    """
    next_token = None
    idx = 0
    client = session.client('wisdom', region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.list_knowledge_bases(
                nextToken=next_token) if next_token else client.list_knowledge_bases()
            for resource in response.get('knowledgeBaseSummaries', []):
                arn = resource['knowledgeBaseArn']
                resource['updatedAt'] = resource['updatedAt'].isoformat()
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
