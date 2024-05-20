from inspect import stack
from utils.utils import extract_common_info, save_as_file_parquet, generate_parquet_prefix


def list_acm(file_path, session, region, time_generated, account):
    """
    This Python function lists ACM certificates, extracts common information, and saves the data in
    Parquet format.

    :param file_path: The `file_path` parameter in the `list_acm` function represents the path where the
    output data will be saved as a file. It is the location where the Parquet file containing the
    inventory information will be stored
    :param session: The `session` parameter in the `list_acm` function is an object that represents the
    current session with AWS services. It is typically created using the `boto3.Session` class and is
    used to create clients and resources for interacting with AWS services
    :param region: The `region` parameter in the `list_acm` function refers to the AWS region where the
    ACM (AWS Certificate Manager) service is located. This parameter is used to specify the region in
    which the ACM client will operate and retrieve certificate information. It is important to provide
    the correct AWS region where
    :param time_generated: The `time_generated` parameter in the `list_acm` function is used to specify
    the timestamp when the inventory data is generated. This timestamp is used in creating the inventory
    object for each certificate in the ACM (AWS Certificate Manager) service. It helps in tracking when
    the inventory data was collected or
    :param account: The `account` parameter in the `list_acm` function seems to be a dictionary
    containing information about an AWS account. It likely includes the account ID (`account_id`) and
    the account name (`account_name`). The function uses this account information to retrieve and
    process ACM (AWS Certificate Manager) certificates
    """
    next_token = None
    idx = 0
    client = session.client('acm', region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.list_certificates(
                NextToken=next_token) if next_token else client.list_certificates()
            for resource in response.get('CertificateSummaryList', []):
                if 'NotBefore' in resource:
                    resource['NotBefore'] = resource['NotBefore'].isoformat()
                if 'NotAfter' in resource:
                    resource['NotAfter'] = resource['NotAfter'].isoformat()
                if 'CreatedAt' in resource:
                    resource['CreatedAt'] = resource['CreatedAt'].isoformat()
                if 'IssuedAt' in resource:
                    resource['IssuedAt'] = resource['IssuedAt'].isoformat()
                if 'ImportedAt' in resource:
                    resource['ImportedAt'] = resource['ImportedAt'].isoformat()
                if 'RevokedAt' in resource:
                    resource['RevokedAt'] = resource['RevokedAt'].isoformat()
                arn = resource['CertificateArn']
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
