from inspect import stack
from utils.utils import extract_common_info, save_as_file_parquet, generate_parquet_prefix


def list_neptune(file_path, session, region, time_generated, account):
    """
    This Python function retrieves information about Neptune database clusters and saves it in Parquet
    format.

    :param file_path: The `file_path` parameter is the path where the output file will be saved. It
    should be a string representing the file path including the file name and extension where the
    inventory data will be stored
    :param session: The `session` parameter in the `list_neptune` function is an object that represents
    the connection to the AWS service. It is typically created using the `boto3.Session` class and is
    used to make API calls to AWS services like Neptune in this case
    :param region: The `region` parameter in the `list_neptune` function is used to specify the AWS
    region where the Neptune database clusters are located. This parameter is required to create a
    client for the Neptune service in the specified region and to retrieve information about the Neptune
    clusters in that region
    :param time_generated: Time when the inventory is generated
    :param account: The `account` parameter in the `list_neptune` function seems to be a dictionary
    containing information about an AWS account. It likely includes keys such as 'account_id' and
    'account_name' which are used within the function to extract common information for generating an
    inventory of Neptune database clusters
    """
    next_token = None
    idx = 0
    client = session.client('neptune', region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.describe_db_clusters(
                Marker=next_token) if next_token else client.describe_db_clusters()
            for resource in response.get('DBClusters', []):
                if 'EarliestRestorableTime' in resource:
                    resource['EarliestRestorableTime'] = resource['EarliestRestorableTime'].isoformat(
                    )
                if 'LatestRestorableTime' in resource:
                    resource['LatestRestorableTime'] = resource['LatestRestorableTime'].isoformat(
                    )
                if 'ClusterCreateTime' in resource:
                    resource['ClusterCreateTime'] = resource['ClusterCreateTime'].isoformat(
                    )
                if 'AutomaticRestartTime' in resource:
                    resource['AutomaticRestartTime'] = resource['AutomaticRestartTime'].isoformat(
                    )
                if 'IOOptimizedNextAllowedModificationTime' in resource:
                    resource['IOOptimizedNextAllowedModificationTime'] = resource['IOOptimizedNextAllowedModificationTime'].isoformat(
                    )
                arn = resource.get('DBClusterArn', '')
                inventory_object = extract_common_info(
                    arn, resource, region, account_id, time_generated, account_name)
                inventory.append(inventory_object)
            save_as_file_parquet(inventory, file_path,
                                 generate_parquet_prefix(str(stack()[0][3]), region, account_id, idx))
            next_token = response.get('Marker', None)
            idx = idx + 1
            if not next_token:
                break
        except Exception as e:
            print(e)
            break


def list_neptune_instances(file_path, session, region, time_generated, account):
    """
    This Python function lists Neptune instances, retrieves their information, and saves it in a Parquet
    file.

    :param file_path: The `file_path` parameter is the file path where the Neptune instances inventory
    will be saved. It should be a valid file path on the system where the script is running
    :param session: The `session` parameter in the `list_neptune_instances` function is an object
    representing the current session. It is typically created using the `boto3.Session` class and is
    used to interact with AWS services. This object stores configuration options such as credentials,
    region, and other settings needed to
    :param region: The `region` parameter in the `list_neptune_instances` function is used to specify
    the AWS region where the Neptune instances are located. This parameter is required to create a
    client for the Neptune service in the specified region and to retrieve information about the Neptune
    instances in that region
    :param time_generated: The `time_generated` parameter in the `list_neptune_instances` function is
    used to specify the timestamp or time at which the inventory of Neptune instances is being
    generated. This timestamp is used in the function to record the time when the inventory data was
    collected for each Neptune instance
    :param account: The `account` parameter seems to be a dictionary containing information about an AWS
    account. It likely includes keys such as 'account_id' and 'account_name'. This information is used
    within the `list_neptune_instances` function to retrieve Neptune instances associated with the
    specified AWS account
    """
    next_token = None
    idx = 0
    client = session.client('neptune', region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.describe_db_instances(
                Marker=next_token) if next_token else client.describe_db_instances()
            for resource in response.get('DBInstances', []):
                if 'InstanceCreateTime' in resource:
                    resource['InstanceCreateTime'] = resource['InstanceCreateTime'].isoformat(
                    )
                if 'LatestRestorableTime' in resource:
                    resource['LatestRestorableTime'] = resource['LatestRestorableTime'].isoformat(
                    )
                arn = resource.get('DBInstanceArn', '')
                inventory_object = extract_common_info(
                    arn, resource, region, account_id, time_generated, account_name)
                inventory.append(inventory_object)
            save_as_file_parquet(inventory, file_path,
                                 generate_parquet_prefix(str(stack()[0][3]), region, account_id, idx))
            next_token = response.get('Marker', None)
            idx = idx + 1
            if not next_token:
                break
        except Exception as e:
            print(e)
            break
