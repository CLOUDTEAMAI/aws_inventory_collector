from inspect import stack
from utils.utils import extract_common_info, save_as_file_parquet, generate_parquet_prefix


def list_memorydb(file_path, session, region, time_generated, account):
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
    client = session.client('memorydb', region_name=region)
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
