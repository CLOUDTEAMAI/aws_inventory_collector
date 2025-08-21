from inspect import stack
from utils.utils import extract_common_info, save_as_file_parquet, generate_parquet_prefix


def list_dynamodb(file_path, session, region, time_generated, account, boto_config):
    """
    The function `list_dynamo` retrieves information about DynamoDB tables and saves it to a file in
    Parquet format.

    :param file_path: The `file_path` parameter is the path where the output file will be saved. It is
    the location on your file system where the function will write the data it collects during the
    execution
    :param session: The `session` parameter in the `list_dynamo` function is typically an instance of
    `boto3.Session` class, which is used to create service clients for AWS services. It allows you to
    configure credentials, region, and other settings for making API calls to AWS services. You can
    create
    :param region: The `region` parameter in the `list_dynamo` function is used to specify the AWS
    region where the DynamoDB tables are located. This parameter is required to create a DynamoDB client
    in the specified region and to retrieve information about the tables in that region
    :param time_generated: The `time_generated` parameter in the `list_dynamo` function is used to
    specify the timestamp or datetime when the inventory data is generated or collected. This
    information is important for tracking when the inventory information was retrieved and can be useful
    for auditing or monitoring purposes. The `time_generated` parameter is
    :param account: The `account` parameter in the `list_dynamo` function seems to be a dictionary
    containing information about an AWS account. It likely includes the keys 'account_id' and
    'account_name', which are used within the function to extract account-specific details for DynamoDB
    tables
    """
    next_token = None
    idx = 0
    client = session.client('dynamodb', region_name=region, config=boto_config)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.list_tables(
                ExclusiveStartTableName=next_token) if next_token else client.list_tables()
            for i in response.get('TableNames', []):
                info_tables = client.describe_table(TableName=i)['Table']
                if 'CreationDateTime' in info_tables:
                    info_tables['CreationDateTime'] = info_tables['CreationDateTime'].isoformat(
                    )

                if 'ProvisionedThroughput' in info_tables:
                    if 'LastIncreaseDateTime' in info_tables['ProvisionedThroughput']:
                        info_tables['ProvisionedThroughput']['LastIncreaseDateTime'] = info_tables[
                            'ProvisionedThroughput']['LastIncreaseDateTime'].isoformat()
                    if 'LastDecreaseDateTime' in info_tables['ProvisionedThroughput']:
                        info_tables['ProvisionedThroughput']['LastDecreaseDateTime'] = info_tables[
                            'ProvisionedThroughput']['LastDecreaseDateTime'].isoformat()

                if 'BillingModeSummary' in info_tables:
                    info_tables['BillingModeSummary']['LastUpdateToPayPerRequestDateTime'] = info_tables[
                        'BillingModeSummary']['LastUpdateToPayPerRequestDateTime'].isoformat()

                if 'GlobalSecondaryIndexes' in info_tables:
                    counter = 0
                    for index in info_tables.get('GlobalSecondaryIndexes', []):
                        info_tables['GlobalSecondaryIndexes'][counter]['ProvisionedThroughput']['LastIncreaseDateTime'] = index['ProvisionedThroughput']['LastIncreaseDateTime'].isoformat(
                        )
                        info_tables['GlobalSecondaryIndexes'][counter]['ProvisionedThroughput']['LastDecreaseDateTime'] = index[
                            'ProvisionedThroughput']['LastDecreaseDateTime'].isoformat()
                        counter = counter + 1

                if 'Replicas' in info_tables:
                    counter = 0
                    for index in info_tables.get('Replicas', []):
                        info_tables['Replicas'][counter]['ReplicaInaccessibleDateTime'] = index['ReplicaInaccessibleDateTime'].isoformat(
                        )
                        info_tables['Replicas'][counter]['ReplicaTableClassSummary']['LastUpdateDateTime'] = index[
                            'ReplicaTableClassSummary']['LastUpdateDateTime'].isoformat()
                        counter = counter + 1

                if 'RestoreSummary' in info_tables:
                    info_tables['RestoreSummary']['RestoreDateTime'] = info_tables['RestoreSummary']['RestoreDateTime'].isoformat(
                    )
                if 'SSEDescription' in info_tables:
                    info_tables['SSEDescription']['InaccessibleEncryptionDateTime'] = info_tables['SSEDescription']['InaccessibleEncryptionDateTime'].isoformat(
                    )
                if 'ArchivalSummary' in info_tables:
                    info_tables['ArchivalSummary']['ArchivalDateTime'] = info_tables['ArchivalSummary']['ArchivalDateTime'].isoformat(
                    )
                if 'TableClassSummary' in info_tables:
                    info_tables['TableClassSummary']['LastUpdateDateTime'] = info_tables['TableClassSummary']['LastUpdateDateTime'].isoformat(
                    )
                arn = info_tables.get(
                    'TableArn', f"arn:aws:dynamodb:{region}:{account_id}:{info_tables.get('ResourceId')}")
                inventory_object = extract_common_info(
                    arn, info_tables, region, account_id, time_generated, account_name)
                inventory.append(inventory_object)
            save_as_file_parquet(inventory, file_path, generate_parquet_prefix(
                str(stack()[0][3]), region, account_id, idx))
            next_token = response.get(
                'LastEvaluatedTableName', None)
            idx = idx + 1
            if not next_token:
                break
        except Exception as e:
            print(e)
            break


def list_dynamodb_streams(file_path, session, region, time_generated, account, boto_config):
    """
    The function `list_dynamodb_streams` retrieves and processes information about DynamoDB streams,
    saving the data as Parquet files.

    :param file_path: The `file_path` parameter is the file path where the output will be saved. It is
    the location where the function will save the data retrieved from DynamoDB streams
    :param session: The `session` parameter in the `list_dynamodb_streams` function is an object that
    represents the current session. It is typically created using the `boto3.Session` class from the AWS
    SDK for Python (Boto3). This session object stores configuration information such as credentials,
    region, and
    :param region: The `region` parameter in the `list_dynamodb_streams` function is used to specify the
    AWS region where the DynamoDB streams are located. This parameter is required to create a client
    session for the DynamoDB Streams service in the specified region
    :param time_generated: The `time_generated` parameter in the `list_dynamodb_streams` function is
    used to specify the time at which the inventory information is generated. This parameter is likely
    used to timestamp the inventory objects created during the execution of the function
    :param account: The `list_dynamodb_streams` function takes in several parameters:
    """
    next_token = None
    idx = 0
    client = session.client(
        'dynamodbstreams', region_name=region, config=boto_config)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    streams = []
    while True:
        try:
            inventory = []
            response = client.list_streams(
                ExclusiveStartStreamArn=next_token) if next_token else client.list_streams()
            for stream in response.get('Streams', []):
                streams.append(stream['StreamArn'])
            next_token = response.get(
                'LastEvaluatedStreamArn', None)
            if not next_token:
                break
        except Exception as e:
            print(e)
            break

    for stream in streams:
        while True:
            try:
                next_token = None
                inventory = []
                response = client.describe_stream(
                    ExclusiveStartShardId=next_token) if next_token else client.describe_stream()
                for resource in response.get('StreamDescription', []):
                    if 'CreationRequestDateTime' in resource:
                        resource['CreationRequestDateTime'] = resource['CreationRequestDateTime'].isoformat(
                        )
                    arn = resource['StreamArn']
                    inventory_object = extract_common_info(
                        arn, resource, region, account_id, time_generated, account_name)
                    inventory.append(inventory_object)
                save_as_file_parquet(inventory, file_path, generate_parquet_prefix(
                    str(stack()[0][3]), region, account_id, idx))
                next_token = response.get(
                    'LastEvaluatedShardId', None)
                idx = idx + 1
                if not next_token:
                    break
            except Exception as e:
                print(e)
                break


def list_dax(file_path, session, region, time_generated, account, boto_config):
    """
    The function `list_dax` retrieves information about DAX clusters, formats the data, and saves it to
    a Parquet file.

    :param file_path: The `file_path` parameter is the path where the output file will be saved. It is a
    string that represents the location where the generated data will be stored
    :param session: The `session` parameter in the `list_dax` function is used to create a session with
    the AWS service using the AWS SDK for Python (Boto3). It is typically an instance of `boto3.Session`
    class that manages the configuration state and allows you to create service clients and
    :param region: Region is the geographical area in which the AWS resources are located. It is a
    required parameter for AWS services to know where to deploy resources and perform operations.
    Examples of regions include us-east-1, eu-west-1, ap-southeast-2, etc
    :param time_generated: Time when the inventory is generated. It is used to timestamp the inventory
    items
    :param account: The `list_dax` function takes several parameters:
    """
    next_token = None
    idx = 0
    client = session.client('dax', region_name=region, config=boto_config)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            nodes_counter = 0
            response = client.describe_clusters(
                NextToken=next_token) if next_token else client.describe_clusters()
            for resource in response.get('Clusters', []):
                for node in resource.get('Nodes', []):
                    resource['Nodes'][nodes_counter]['NodeCreateTime'] = node['NodeCreateTime'].isoformat(
                    )
                arn = resource.get('ClusterArn', '')
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
