from inspect import stack
from utils.utils import extract_common_info, save_as_file_parquet, save_as_file_parquet_metrics, generate_parquet_prefix, get_resource_utilization_metric


def list_efs_file_systems(file_path, session, region, time_generated, account):
    """
    This Python function lists Elastic File System (EFS) file systems and saves the information in a
    Parquet file.

    :param file_path: The `file_path` parameter in the `list_efs_file_systems` function represents the
    path where the output files will be saved. This is the location where the function will store the
    information about the Elastic File Systems (EFS) that it retrieves during its execution
    :param session: The `session` parameter in the `list_efs_file_systems` function is typically an
    instance of `boto3.Session` that represents your AWS credentials and configuration. It is used to
    create a client for the Amazon Elastic File System (EFS) service in the specified region
    :param region: The `region` parameter in the `list_efs_file_systems` function refers to the AWS
    region where the Amazon Elastic File System (EFS) resources are located. This parameter is used to
    specify the region when creating a client for the EFS service and when constructing the ARN (Amazon
    :param time_generated: The `time_generated` parameter in the `list_efs_file_systems` function likely
    represents the timestamp or datetime when the file system inventory is being generated. This
    parameter is used to capture the time at which the inventory data is collected or processed. It can
    be helpful for tracking when the inventory was
    :param account: The `account` parameter in the `list_efs_file_systems` function seems to be a
    dictionary containing information about the AWS account. It likely includes keys such as
    'account_id' and 'account_name' which are used within the function to retrieve specific details
    related to the account for processing E
    """
    next_token = None
    idx = 0
    client = session.client('efs', region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.describe_file_systems(
                Marker=next_token) if next_token else client.describe_file_systems()
            for resource in response.get('FileSystems', []):
                if 'CreationTime' in resource:
                    resource['CreationTime'] = resource['CreationTime'].isoformat()
                if 'SizeInBytes' in resource:
                    resource['SizeInBytes']['Timestamp'] = resource['SizeInBytes']['Timestamp'].isoformat(
                    )
                arn = f"arn:aws:elasticfilesystem:{region}:{account_id}:file-system/{resource['FileSystemId']}"
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
