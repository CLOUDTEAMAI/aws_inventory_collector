from inspect import stack
from utils.utils import extract_common_info, save_as_file_parquet, generate_parquet_prefix


def list_eks(file_path, session, region, time_generated, account):
    """
    The function `list_eks` retrieves information about EKS clusters and saves it to a Parquet file.

    :param file_path: The `file_path` parameter in the `list_eks` function is the path where the
    inventory data will be saved as a file. It is the location where the Parquet file containing the
    extracted EKS cluster information will be stored
    :param session: The `session` parameter in the `list_eks` function is typically an instance of
    `boto3.Session` that is used to create clients for AWS services. It allows you to make API calls to
    AWS services using the credentials and configuration provided in the session object
    :param region: Region is a string representing the AWS region where the Amazon EKS clusters are
    located. It is used to specify the region for the AWS client session
    :param time_generated: Time when the inventory is generated
    :param account: The `account` parameter in the `list_eks` function seems to be a dictionary
    containing information about the AWS account. It likely includes the account ID (`account_id`) and
    the account name (`account_name`). The function uses this information to retrieve and process data
    related to Amazon EKS clusters within
    """
    next_token = None
    idx = 0
    client = session.client('eks', region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.list_clusters(
                nextToken=next_token) if next_token else client.list_clusters()
            for i in response['clusters']:
                resource = client.describe_cluster(name=i)['cluster']
                if 'createdAt' in resource:
                    resource['createdAt'] = resource['createdAt'].isoformat()

                if 'connectorConfig' in resource:
                    resource['connectorConfig']['activationExpiry'] = resource['connectorConfig']['activationExpiry'].isoformat()

                arn = f"{resource['arn']}"
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
