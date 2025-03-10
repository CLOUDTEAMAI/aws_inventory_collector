from inspect import stack
from utils.utils import extract_common_info, save_as_file_parquet, generate_parquet_prefix


def list_memorydb(file_path, session, region, time_generated, account, boto_config):
    """
    This Python function retrieves information about MemoryDB clusters and saves it to a Parquet file.

    :param file_path: The `file_path` parameter in the `list_memorydb` function is the path where the
    inventory data will be saved as a Parquet file. This parameter should be a string representing the
    file path where you want to store the data
    :param session: The `session` parameter in the `list_memorydb` function is an object representing
    the session used to interact with AWS services. It is typically created using the `boto3.Session`
    class and contains the necessary credentials and configuration to make API calls to AWS services
    :param region: The `region` parameter in the `list_memorydb` function is used to specify the AWS
    region where the MemoryDB clusters are located. This parameter is required to create a client for
    the MemoryDB service in the specified region and to retrieve information about MemoryDB clusters and
    their shards within that region
    :param time_generated: The `time_generated` parameter in the `list_memorydb` function is used to
    specify the timestamp or time at which the inventory data is generated or collected. This timestamp
    is typically used for tracking when the inventory information was retrieved and can be helpful for
    auditing or monitoring purposes
    :param account: The `account` parameter in the `list_memorydb` function seems to be a dictionary
    containing information about an account. It likely includes keys such as 'account_id' and
    'account_name'. The function uses the account information to extract specific details and perform
    operations related to MemoryDB clusters
    """
    next_token = None
    idx = 0
    client = session.client('memorydb', region_name=region, config=boto_config)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.describe_clusters(
                NextToken=next_token, ShowShardDetails=True) if next_token else client.describe_clusters(ShowShardDetails=True)
            for resource in response.get('Clusters', []):
                counter = 0
                arn = resource.get('ARN', '')
                for shard in response.get('Shards', []):
                    nodes_counter = 0
                    for node in shard.get('Nodes', []):
                        if 'CreateTime' in node:
                            resource['Shards'][counter]['Nodes'][nodes_counter]['CreateTime'] = node['CreateTime'].isoformat(
                            )
                        nodes_counter = nodes_counter + 1
                    counter = counter + 1
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


def list_memorydb_snapshots(file_path, session, region, time_generated, account, boto_config):
    """
    This Python function retrieves and processes memoryDB snapshots, saving the information in a Parquet
    file.

    :param file_path: The `file_path` parameter is the file path where the snapshots will be saved. It
    is the location on the file system where the function will write the snapshot data
    :param session: The `session` parameter in the `list_memorydb_snapshots` function is typically an
    instance of a boto3 session that allows you to create service clients for AWS services. It is used
    to create a client for the MemoryDB service in the specified region. This client is then used to
    interact with
    :param region: Region is a string representing the geographical region where the MemoryDB snapshots
    are located. It is used to specify the AWS region for the client session
    :param time_generated: Time when the snapshots were generated
    :param account: The `account` parameter in the `list_memorydb_snapshots` function seems to be a
    dictionary containing information about an account. It likely includes keys such as 'account_id' and
    'account_name' which are used within the function to extract relevant information for processing
    MemoryDB snapshots
    """
    next_token = None
    idx = 0
    client = session.client('memorydb', region_name=region, config=boto_config)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.describe_snapshots(
                NextToken=next_token, ShowDetail=True) if next_token else client.describe_snapshots(ShowDetail=True)
            for resource in response.get('Snapshots', []):
                counter = 0
                arn = resource.get('ARN', '')
                for shard in response.get('Shards', []):
                    if 'SnapshotCreationTime' in shard:
                        resource['Shards'][counter]['SnapshotCreationTime'] = shard['SnapshotCreationTime'].isoformat(
                        )
                    counter = counter + 1
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
