from inspect import stack
from utils.utils import extract_common_info, save_as_file_parquet, generate_parquet_prefix


def list_timestream_influxdb_instances(file_path, session, region, time_generated, account):
    """
    This Python function lists Timestream InfluxDB instances and saves the information to a Parquet
    file.

    :param file_path: The `file_path` parameter is the path where the output file will be saved. It
    should be a string representing the file path including the file name and extension where the data
    will be stored
    :param session: The `session` parameter in the `list_timestream_influxdb_instances` function is
    typically an instance of a boto3 session that is used to create a client for the AWS service. It
    allows you to make API calls to the AWS Timestream InfluxDB service in the specified region
    :param region: The `region` parameter in the `list_timestream_influxdb_instances` function refers to
    the AWS region where the Amazon Timestream InfluxDB instances are located. This parameter is used to
    specify the region when creating a client for the Timestream InfluxDB service in the provided
    :param time_generated: The `time_generated` parameter in the `list_timestream_influxdb_instances`
    function is used to specify the timestamp or time at which the inventory information is being
    generated or collected. This timestamp is associated with the inventory objects created for each
    database instance listed in the Timestream InfluxDB
    :param account: The `account` parameter in the `list_timestream_influxdb_instances` function seems
    to be a dictionary containing information about an account. It likely includes keys such as
    'account_id' and 'account_name'
    """
    next_token = None
    idx = 0
    client = session.client('timestream-influxdb', region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.list_db_instances(
                nextToken=next_token) if next_token else client.list_db_instances()
            for resource in response.get('items', []):
                arn = resource.get('arn')
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


def list_timestream_write_databases(file_path, session, region, time_generated, account):
    """
    This Python function lists databases in Amazon Timestream, extracts common information, and saves
    the inventory as Parquet files.

    :param file_path: The `file_path` parameter is the file path where the output will be saved. It is
    the location on the file system where the Parquet files containing the extracted information will be
    stored
    :param session: The `session` parameter in the `list_timestream_write_databases` function is
    typically an AWS session object that is used to create a client for interacting with AWS Timestream
    Write service in a specific region. It is used to make API calls to AWS Timestream Write service
    within the
    :param region: Region is the AWS region where the Amazon Timestream database is located. It
    specifies the geographical area where the resources are provisioned. Examples of regions include
    'us-east-1', 'eu-west-1', 'ap-southeast-2', etc
    :param time_generated: The `time_generated` parameter in the `list_timestream_write_databases`
    function represents the timestamp indicating when the operation is being executed or when the data
    is being generated. This timestamp is used in the function to track the time at which the inventory
    information is being collected for the Timestream
    :param account: The `account` parameter in the `list_timestream_write_databases` function seems to
    be a dictionary containing information about an account. It likely includes keys such as
    'account_id' and 'account_name' which are used within the function to extract relevant information
    for processing Timestream databases
    """
    next_token = None
    idx = 0
    client = session.client('timestream-write', region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.list_databases(
                NextToken=next_token) if next_token else client.list_databases()
            for resource in response.get('Databases', []):
                arn = resource.get('Arn')
                if 'CreationTime' in resource:
                    resource['CreationTime'] = resource['CreationTime'].isoformat()
                if 'LastUpdatedTime' in resource:
                    resource['LastUpdatedTime'] = resource['LastUpdatedTime'].isoformat()
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


def list_timestream_write_tables(file_path, session, region, time_generated, account):
    """
    This Python function iterates through TimeStream databases and tables, extracts information, and
    saves it as a Parquet file.

    :param file_path: The `file_path` parameter is the path where the output file will be saved. It
    should be a string representing the file path on your system where you want to save the output file
    :param session: The `session` parameter in the `list_timestream_write_tables` function is typically
    an AWS session object that is used to create a client for the AWS Timestream Write service. This
    session object is usually created using the `boto3` library in Python and contains the necessary
    credentials and
    :param region: Region is the geographical area where the AWS resources are located. It is used to
    specify the AWS region where the Amazon Timestream database is located. Examples of regions include
    'us-east-1', 'us-west-2', 'eu-central-1', etc
    :param time_generated: Time when the function is executed
    :param account: The `account` parameter in the `list_timestream_write_tables` function seems to be a
    dictionary containing information about an account. It likely includes keys such as 'account_id' and
    'account_name'. This information is used within the function to extract common information and
    generate inventory objects for Timest
    """
    next_token = None
    sub_next_token = None
    idx = 0
    client = session.client('timestream-write', region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.list_databases(
                NextToken=next_token) if next_token else client.list_databases()
            for resource in response.get('Databases', []):
                while True:
                    try:
                        resource_response = client.list_databases(
                            NextToken=next_token, DatabaseName=resource.get('DatabaseName')) if next_token else client.list_databases(DatabaseName=resource.get('DatabaseName'))
                        for table in resource_response.get('Tables', []):
                            arn = table.get('Arn')
                            if 'CreationTime' in table:
                                table['CreationTime'] = table['CreationTime'].isoformat(
                                )
                            if 'LastUpdatedTime' in table:
                                table['LastUpdatedTime'] = table['LastUpdatedTime'].isoformat(
                                )
                            inventory_object = extract_common_info(
                                arn, table, region, account_id, time_generated, account_name)
                            inventory.append(inventory_object)
                        sub_next_token = response.get('NextToken', None)
                        idx = idx + 1
                        if not sub_next_token:
                            break
                    except Exception as e:
                        print(e)
                        break
            save_as_file_parquet(inventory, file_path, generate_parquet_prefix(
                str(stack()[0][3]), region, account_id, idx))
            next_token = response.get('NextToken', None)
            idx = idx + 1
            if not next_token:
                break
        except Exception as e:
            print(e)
            break
