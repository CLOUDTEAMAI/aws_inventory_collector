from inspect import stack
from utils.utils import extract_common_info, save_as_file_parquet, generate_parquet_prefix


def list_keyspaces(file_path, session, region, time_generated, account, boto_config):
    """
    This Python function lists keyspaces using the AWS Keyspaces client and saves the inventory
    information as Parquet files.

    :param file_path: The `file_path` parameter is a string that represents the file path where the
    output will be saved. It is the location where the list of keyspaces will be stored in a file
    :param session: The `session` parameter in the `list_keyspaces` function is typically an instance of
    a boto3 session that is used to create a client for interacting with AWS services. It allows you to
    make API calls to AWS services like keyspaces in this case
    :param region: The `region` parameter in the `list_keyspaces` function is used to specify the AWS
    region in which the Amazon Keyspaces (managed Apache Cassandra-compatible database service) is
    located. This parameter is required to create a client for the Keyspaces service and to list the
    keyspaces within that region
    :param time_generated: The `time_generated` parameter in the `list_keyspaces` function is used to
    specify the timestamp or time at which the inventory of keyspaces is generated. This timestamp is
    typically used for tracking and auditing purposes to know when the inventory data was collected
    :param account: The `account` parameter in the `list_keyspaces` function seems to be a dictionary
    containing information about an account. It likely includes keys such as 'account_id' and
    'account_name'. This information is used within the function to extract details related to the
    account for further processing
    """
    next_token = None
    idx = 0
    client = session.client(
        'keyspaces', region_name=region, config=boto_config)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.list_keyspaces(
                nextToken=next_token) if next_token else client.list_keyspaces()
            for resource in response.get('keyspaces', []):
                arn = resource.get('resourceArn')
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


def list_keyspaces_tables(file_path, session, region, time_generated, account, boto_config):
    """
    The function `list_keyspaces_tables` retrieves information about keyspaces and tables from a
    Keyspaces service and saves the data in Parquet format.

    :param file_path: The `file_path` parameter in the `list_keyspaces_tables` function is the path
    where the output file will be saved. It should be a string representing the file path where the
    Parquet file will be stored
    :param session: The `session` parameter in the `list_keyspaces_tables` function is typically an
    instance of a boto3 session that is used to create a client for interacting with AWS services. It
    allows you to configure credentials, region, and other settings for making API calls to AWS services
    like Amazon Keyspaces in
    :param region: The `region` parameter in the `list_keyspaces_tables` function is used to specify the
    AWS region where the Amazon Keyspaces (for Apache Cassandra) service is located. This region is
    where the function will interact with the Keyspaces service to list keyspaces and tables, retrieve
    information about them,
    :param time_generated: The `time_generated` parameter in the `list_keyspaces_tables` function is
    used to specify the timestamp when the inventory data is generated. This timestamp is used in the
    function to include the time information in the inventory objects created during the listing of
    keyspaces and tables
    :param account: The `account` parameter seems to be a dictionary containing information about an AWS
    account. It likely includes keys such as 'account_id' and 'account_name'
    """
    next_token = None
    sub_next_token = None
    idx = 0
    client = session.client(
        'keyspaces', region_name=region, config=boto_config)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.list_keyspaces(
                nextToken=next_token) if next_token else client.list_keyspaces()
            for resource in response.get('keyspaces', []):
                while True:
                    try:
                        tables = client.list_tables(
                            nextToken=sub_next_token, keyspaceName=resource.get('keyspaceName')) if sub_next_token else client.list_tables(keyspaceName=resource.get('keyspaceName'))
                        for table in tables.get('tables', []):
                            table_resource = client.get_table(keyspaceName=resource.get(
                                'keyspaceName'), tableName=table.get('tableName'))
                            arn = table_resource.get('resourceArn')
                            if 'creationTimestamp' in table_resource:
                                table_resource['creationTimestamp'] = table_resource['creationTimestamp'].isoformat(
                                )
                            if 'capacitySpecification' in table_resource:
                                if 'lastUpdateToPayPerRequestTimestamp' in table_resource.get('capacitySpecification', {}):
                                    table_resource['capacitySpecification']['lastUpdateToPayPerRequestTimestamp'] = table_resource[
                                        'capacitySpecification']['lastUpdateToPayPerRequestTimestamp'].isoformat()
                            if 'pointInTimeRecovery' in table_resource:
                                if 'earliestRestorableTimestamp' in table_resource.get('pointInTimeRecovery', {}):
                                    table_resource['pointInTimeRecovery']['earliestRestorableTimestamp'] = table_resource[
                                        'pointInTimeRecovery']['earliestRestorableTimestamp'].isoformat()
                            rs_idx = 0
                            for replicaSpecification in table_resource.get('replicaSpecifications', []):
                                if 'capacitySpecification' in replicaSpecification:
                                    if 'lastUpdateToPayPerRequestTimestamp' in replicaSpecification.get('capacitySpecification', {}):
                                        table_resource['replicaSpecifications'][rs_idx]['capacitySpecification']['lastUpdateToPayPerRequestTimestamp'] = replicaSpecification.get(
                                            'capacitySpecification', {})['lastUpdateToPayPerRequestTimestamp'].isoformat()
                                rs_idx = rs_idx + 1
                        inventory_object = extract_common_info(
                            arn, table_resource, region, account_id, time_generated, account_name)
                        inventory.append(inventory_object)
                        sub_next_token = response.get('nextToken', None)
                        if not sub_next_token:
                            break
                    except Exception as e:
                        print(e)
                        break
            save_as_file_parquet(inventory, file_path, generate_parquet_prefix(
                str(stack()[0][3]), region, account_id, idx))
            next_token = response.get('nextToken', None)
            idx = idx + 1
            if not next_token:
                break
        except Exception as e:
            print(e)
            break


def list_keyspaces_tables_autoscaling(file_path, session, region, time_generated, account, boto_config):
    next_token = None
    sub_next_token = None
    idx = 0
    client = session.client(
        'keyspaces', region_name=region, config=boto_config)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.list_keyspaces(
                nextToken=next_token) if next_token else client.list_keyspaces()
            for resource in response.get('keyspaces', []):
                while True:
                    try:
                        tables = client.list_tables(
                            nextToken=sub_next_token, keyspaceName=resource.get('keyspaceName')) if sub_next_token else client.list_tables(keyspaceName=resource.get('keyspaceName'))
                        for table in tables.get('tables', []):
                            table_resource = client.get_table_auto_scaling_settings(keyspaceName=resource.get(
                                'keyspaceName'), tableName=table.get('tableName'))
                            arn = table_resource.get('resourceArn')
                            rs_idx = rs_idx + 1
                        inventory_object = extract_common_info(
                            arn, table_resource, region, account_id, time_generated, account_name)
                        inventory.append(inventory_object)
                        sub_next_token = response.get('nextToken', None)
                        if not sub_next_token:
                            break
                    except Exception as e:
                        print(e)
                        break
            save_as_file_parquet(inventory, file_path, generate_parquet_prefix(
                str(stack()[0][3]), region, account_id, idx))
            next_token = response.get('nextToken', None)
            idx = idx + 1
            if not next_token:
                break
        except Exception as e:
            print(e)
            break
