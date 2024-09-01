from inspect import stack
from utils.utils import extract_common_info, save_as_file_parquet, generate_parquet_prefix


def list_secrets(file_path, session, region, time_generated, account):
    """
    The function `list_secrets` retrieves a list of secrets from AWS Secrets Manager, processes the
    data, and saves it as a Parquet file.

    :param file_path: The `file_path` parameter is the path where the output file will be saved. It
    should be a string representing the file path on the filesystem where you want to save the data
    extracted from the Secrets Manager
    :param session: The `session` parameter in the `list_secrets` function is typically an instance of a
    boto3 session that is used to create a client for AWS Secrets Manager service. It allows you to make
    API calls to AWS services using the credentials and configuration provided in the session
    :param region: The `region` parameter in the `list_secrets` function is used to specify the AWS
    region where the Secrets Manager client will be created. This region is where the Secrets Manager
    service will be accessed to list the secrets. It is important to provide the correct region where
    the secrets are stored in order
    :param time_generated: Time when the secrets inventory is generated
    :param account: The `account` parameter in the `list_secrets` function seems to be a dictionary
    containing information about an account. It likely includes keys such as 'account_id' and
    'account_name'. This information is used within the function to extract common information and
    generate a file path for saving the inventory data
    """
    next_token = None
    idx = 0
    client = session.client('secretsmanager', region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.list_secrets(
                NextToken=next_token) if next_token else client.list_secrets()
            for resource in response.get('SecretList', []):
                if 'LastRotatedDate' in resource:
                    resource['LastRotatedDate'] = resource['LastRotatedDate'].isoformat()
                if 'LastChangedDate' in resource:
                    resource['LastChangedDate'] = resource['LastChangedDate'].isoformat()
                if 'LastAccessedDate' in resource:
                    resource['LastAccessedDate'] = resource['LastAccessedDate'].isoformat()
                if 'DeletedDate' in resource:
                    resource['DeletedDate'] = resource['DeletedDate'].isoformat()
                if 'NextRotationDate' in resource:
                    resource['NextRotationDate'] = resource['NextRotationDate'].isoformat()
                if 'CreatedDate' in resource:
                    resource['CreatedDate'] = resource['CreatedDate'].isoformat()
                arn = resource['ARN']
                inventory_object = extract_common_info(
                    arn, resource, region, account_id, time_generated, account_name)
                inventory.append(inventory_object)
            save_as_file_parquet(inventory, file_path,
                                 generate_parquet_prefix(str(stack()[0][3]), region, account_id, idx))
            next_token = response.get('NextToken', None)
            idx = idx + 1
            if not next_token:
                break
        except Exception as e:
            print(e)
            break
