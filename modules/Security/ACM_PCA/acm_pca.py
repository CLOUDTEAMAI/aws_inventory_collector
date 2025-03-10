from inspect import stack
from utils.utils import extract_common_info, save_as_file_parquet, generate_parquet_prefix


def list_acm_pca(file_path, session, region, time_generated, account, boto_config):
    """
    This Python function lists ACM PCA certificate authorities and saves the information to a Parquet
    file.

    :param file_path: The `file_path` parameter in the `list_acm_pca` function is the path where the
    output file will be saved. It should be a string representing the file path where the Parquet file
    will be stored
    :param session: The `session` parameter in the `list_acm_pca` function is typically an instance of a
    boto3 session that is used to create a client for the AWS ACM PCA (Amazon Certificate Manager
    Private Certificate Authority) service. This session allows you to make API calls to the ACM PCA
    service in
    :param region: The `region` parameter in the `list_acm_pca` function refers to the AWS region where
    the ACM PCA (Amazon Certificate Manager Private Certificate Authority) service is located. This
    parameter is used to specify the region in which the function will interact with the ACM PCA service
    to list certificate authorities
    :param time_generated: Time when the inventory is generated
    :param account: The `account` parameter in the `list_acm_pca` function seems to be a dictionary
    containing information about an account. It likely has the following structure:
    """
    next_token = None
    idx = 0
    client = session.client('acm-pca', region_name=region, config=boto_config)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.list_certificate_authorities(
                NextToken=next_token) if next_token else client.list_certificate_authorities()
            for resource in response.get('CertificateAuthorities', []):
                arn = resource['Arn']
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
