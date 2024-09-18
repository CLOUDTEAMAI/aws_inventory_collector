from inspect import stack
from utils.utils import extract_common_info, save_as_file_parquet, generate_parquet_prefix


def list_ec2_reservations(file_path, session, region, time_generated, account):
    """
    This Python function lists EC2 reservations for a specific account in a given region and saves the
    information in a Parquet file.

    :param file_path: The `file_path` parameter in the `list_ec2_reservations` function represents the
    file path where the output data will be saved. This should be a string that specifies the location
    and name of the file where the EC2 reservations information will be stored. For example, it could be
    something like
    :param session: The `session` parameter in the `list_ec2_reservations` function is typically an
    instance of `boto3.Session` class that represents your AWS credentials and configuration. It is used
    to create an EC2 client in the specified region
    :param region: The `region` parameter in the `list_ec2_reservations` function refers to the AWS
    region where the EC2 instances are located. This parameter is used to specify the region in which
    the AWS SDK client will operate and retrieve information about the reserved EC2 instances. It is
    important to provide the
    :param time_generated: The `time_generated` parameter in the `list_ec2_reservations` function
    represents the timestamp or datetime when the inventory data is being generated or collected. This
    parameter is used to track when the inventory information was retrieved and can be helpful for
    auditing or tracking purposes. It is typically a datetime object or
    :param account: The `account` parameter in the `list_ec2_reservations` function seems to be a
    dictionary containing information about the AWS account. It likely includes the account ID
    (`account_id`) and the account name (`account_name`). This information is used within the function
    to construct ARNs, file names,
    """
    next_token = None
    idx = 0
    client = session.client('ec2', region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.describe_reserved_instances(
                NextToken=next_token) if next_token else client.describe_reserved_instances()
            for resource in response.get('ReservedInstances', []):
                arn = f"arn:aws:ec2:{region}:{account_id}:reserved-instances/{resource.get('ReservedInstancesId', '')}"
                if 'End' in resource:
                    resource['End'] = resource['End'].isoformat()
                if 'Start' in resource:
                    resource['Start'] = resource['Start'].isoformat()
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


def list_rds_reservations(file_path, session, region, time_generated, account):
    """
    This Python function retrieves information about reserved RDS instances and saves it in a Parquet
    file.

    :param file_path: The `file_path` parameter in the `list_rds_reservations` function is the path
    where the output file will be saved. It should be a string representing the file path where you want
    to save the results of the RDS reservations listing
    :param session: The `session` parameter in the `list_rds_reservations` function is typically an
    instance of the `boto3.Session` class that represents your AWS session. It is used to create an RDS
    client in the specified region for interacting with AWS services
    :param region: The `region` parameter in the `list_rds_reservations` function is used to specify the
    AWS region where the RDS (Relational Database Service) resources are located. This parameter is
    required to create a client for the RDS service in the specified region and to retrieve information
    about reserved DB
    :param time_generated: The `time_generated` parameter in the `list_rds_reservations` function is
    used to specify the timestamp or datetime when the inventory data is generated. This information is
    important for tracking when the data was collected and can be useful for auditing or analysis
    purposes
    :param account: The `account` parameter in the `list_rds_reservations` function seems to be a
    dictionary containing information about the AWS account. It likely includes the account ID
    (`account_id`) and the account name (`account_name`). This information is used within the function
    to construct ARNs, extract common information
    """
    next_token = None
    idx = 0
    client = session.client('rds', region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.describe_reserved_db_instances(
                Marker=next_token) if next_token else client.describe_reserved_db_instances()
            for resource in response.get('ReservedDBInstances', []):
                arn = f"arn:aws:rds:{region}:{account_id}:reserved-instances/{resource.get('ReservedDBInstanceId', '')}"
                if 'End' in resource:
                    resource['End'] = resource['End'].isoformat()
                if 'Start' in resource:
                    resource['Start'] = resource['Start'].isoformat()
                inventory_object = extract_common_info(
                    arn, resource, region, account_id, time_generated, account_name)
                inventory.append(inventory_object)
            save_as_file_parquet(inventory, file_path, generate_parquet_prefix(
                str(stack()[0][3]), region, account_id, idx))
            next_token = response.get('Marker', None)
            idx = idx + 1
            if not next_token:
                break
        except Exception as e:
            print(e)
            break


def list_opensearch_reservations(file_path, session, region, time_generated, account):
    """
    This Python function retrieves and saves OpenSearch reservations information to a file in Parquet
    format.

    :param file_path: The `file_path` parameter is the path where the output file will be saved. It
    should be a string representing the file path including the file name and extension where the data
    will be stored
    :param session: The `session` parameter in the `list_opensearch_reservations` function is typically
    an instance of the `boto3.Session` class that is used to create a client for AWS services. It is
    used to interact with AWS services like Amazon OpenSearch Service in this case. The `session
    :param region: Region is the AWS region where the OpenSearch service is located. It is used to
    specify the region for the AWS client session
    :param time_generated: The `time_generated` parameter in the `list_opensearch_reservations` function
    is used to specify the timestamp or time at which the inventory data is being generated or
    collected. This timestamp is important for tracking when the inventory information was retrieved and
    can be used for various purposes such as auditing, monitoring
    :param account: The `account` parameter in the `list_opensearch_reservations` function seems to be a
    dictionary containing information about the AWS account. It likely includes the account ID
    (`account_id`) and the account name (`account_name`). The function uses this information to
    construct ARNs and filenames for the resources
    """
    next_token = None
    idx = 0
    client = session.client('opensearch', region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.describe_reserved_instances(
                NextToken=next_token) if next_token else client.describe_reserved_instances()
            for resource in response.get('ReservedInstances', []):
                arn = f"arn:aws:opensearch:{region}:{account_id}:reserved-instances/{resource.get('ReservedInstanceId', '')}"
                if 'StartTime' in resource:
                    resource['StartTime'] = resource['StartTime'].isoformat()
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


def list_elasticsearch_reservations(file_path, session, region, time_generated, account):
    """
    This Python function lists Elasticsearch reservations for a specific account in a given region and
    saves the information to a Parquet file.

    :param file_path: The `file_path` parameter is the file path where the output of the function will
    be saved. It is a string that represents the location where the function will write the results of
    listing Elasticsearch reservations
    :param session: The `session` parameter in the `list_elasticsearch_reservations` function is
    typically an AWS session object that allows you to create service clients for AWS services. It is
    used to interact with AWS services like Amazon Elasticsearch Service (ES) in this case. The session
    object is usually created using the `
    :param region: The `region` parameter in the `list_elasticsearch_reservations` function is used to
    specify the AWS region where the Elasticsearch service is located. This parameter is required to
    create an AWS client for the Elasticsearch service in the specified region
    :param time_generated: The `time_generated` parameter in the `list_elasticsearch_reservations`
    function is used to specify the timestamp when the inventory data is generated. This timestamp is
    used in creating the inventory objects for each reserved Elasticsearch instance
    :param account: The `account` parameter in the `list_elasticsearch_reservations` function seems to
    be a dictionary containing information about the AWS account. It likely includes the keys
    `account_id` and `account_name`. The `account_id` is the unique identifier for the AWS account, and
    `account_name`
    """
    next_token = None
    idx = 0
    client = session.client('es', region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.describe_reserved_elasticsearch_instances(
                NextToken=next_token) if next_token else client.describe_reserved_elasticsearch_instances()
            for resource in response.get('ReservedElasticsearchInstances', []):
                arn = f"arn:aws:elasticsearch:{region}:{account_id}:reserved-instances/{resource.get('ReservedElasticsearchInstanceId', '')}"
                if 'StartTime' in resource:
                    resource['StartTime'] = resource['StartTime'].isoformat()
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


def list_elasticcache_reservations(file_path, session, region, time_generated, account):
    """
    This Python function retrieves and saves information about reserved cache nodes in Amazon
    ElastiCache.

    :param file_path: The `file_path` parameter in the `list_elasticcache_reservations` function
    represents the file path where the output of the function will be saved. This could be a local file
    path or an S3 path depending on where you want to store the output data
    :param session: The `session` parameter in the `list_elasticcache_reservations` function is
    typically an AWS session object that is used to create clients for AWS services. It is used to
    interact with AWS Elasticache service in a specific region
    :param region: Region is the AWS region where the Elasticache resources are located. It is used to
    specify the region for the AWS client session
    :param time_generated: The `time_generated` parameter in the `list_elasticcache_reservations`
    function is used to specify the timestamp or time at which the inventory data is generated or
    collected. This timestamp is important for tracking when the data was retrieved and can be useful
    for auditing or monitoring purposes. It is typically in
    :param account: The `account` parameter in the `list_elasticcache_reservations` function seems to be
    a dictionary containing information about the AWS account. It likely includes the keys 'account_id'
    and 'account_name', which are used within the function to retrieve specific details related to the
    account, such as the
    """
    next_token = None
    idx = 0
    client = session.client('elasticache', region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.describe_reserved_cache_nodes(
                Marker=next_token) if next_token else client.describe_reserved_cache_nodes()
            for resource in response.get('ReservedCacheNodes', []):
                arn = resource.get(
                    'ReservationARN', f"arn:aws:rds:{region}:{account_id}:reserved-instances/{resource.get('ReservedCacheNodeId', '')}")
                if 'StartTime' in resource:
                    resource['StartTime'] = resource['StartTime'].isoformat()
                inventory_object = extract_common_info(
                    arn, resource, region, account_id, time_generated, account_name)
                inventory.append(inventory_object)
            save_as_file_parquet(inventory, file_path, generate_parquet_prefix(
                str(stack()[0][3]), region, account_id, idx))
            next_token = response.get('Marker', None)
            idx = idx + 1
            if not next_token:
                break
        except Exception as e:
            print(e)
            break


def list_memorydb_reservations(file_path, session, region, time_generated, account):
    """
    This Python function retrieves and saves information about reserved nodes in MemoryDB to a file in
    Parquet format.

    :param file_path: The `file_path` parameter in the `list_memorydb_reservations` function is the path
    where the output file will be saved. It should be a string representing the file path where the
    function will save the data retrieved from the MemoryDB service
    :param session: The `session` parameter in the `list_memorydb_reservations` function is typically an
    instance of a boto3 session that allows you to create service clients for AWS services. It is used
    to create a client for the AWS MemoryDB service in the specified region
    :param region: Region is the AWS region where the MemoryDB service is located. It is used to specify
    the region for the AWS client session
    :param time_generated: The `time_generated` parameter in the `list_memorydb_reservations` function
    is used to specify the timestamp or time at which the inventory data is generated or collected. This
    timestamp is important for tracking when the data was retrieved and can be useful for auditing or
    monitoring purposes. It is typically in ISO
    :param account: The `account` parameter in the `list_memorydb_reservations` function seems to be a
    dictionary containing information about an AWS account. It likely includes keys such as 'account_id'
    and 'account_name' which are used within the function to identify the account for which the MemoryDB
    reservations are being
    """
    next_token = None
    idx = 0
    client = session.client('memorydb', region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.describe_reserved_nodes(
                NextToken=next_token) if next_token else client.describe_reserved_nodes()
            for resource in response.get('ReservedNodes', []):
                arn = resource.get(
                    'ARN', f"arn:aws:memorydb:{region}:{account_id}:reserved-instances/{resource.get('ReservedNodeId', '')}")
                if 'StartTime' in resource:
                    resource['StartTime'] = resource['StartTime'].isoformat()
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


def list_redshift_reservations(file_path, session, region, time_generated, account):
    """
    This Python function retrieves and saves information about Redshift reservations to a file in
    Parquet format.

    :param file_path: The `file_path` parameter in the `list_redshift_reservations` function is the path
    where the output file will be saved. It should be a string representing the file path where the
    Parquet file will be stored
    :param session: The `session` parameter in the `list_redshift_reservations` function is an AWS
    session object that is used to create a client for interacting with AWS services. It is typically
    created using the `boto3` library in Python and contains the necessary credentials and configuration
    to make API calls to AWS
    :param region: Region is the AWS region where the Redshift resources are located. It is a string
    value representing the geographical area where the resources are deployed, such as 'us-east-1' for
    US East (N. Virginia) or 'eu-west-1' for EU (Ireland)
    :param time_generated: The `time_generated` parameter in the `list_redshift_reservations` function
    is used to specify the timestamp or time at which the inventory data is generated or collected. This
    timestamp is important for tracking when the data was retrieved and can be used for various purposes
    such as auditing, monitoring, or analysis
    :param account: The `account` parameter in the `list_redshift_reservations` function seems to be a
    dictionary containing information about the AWS account. It likely includes the account ID
    (`account_id`) and the account name (`account_name`)
    """
    next_token = None
    idx = 0
    client = session.client('redshift', region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.describe_reserved_nodes(
                Marker=next_token) if next_token else client.describe_reserved_nodes()
            for resource in response.get('ReservedNodes', []):
                arn = resource.get(
                    'ARN', f"arn:aws:redshift:{region}:{account_id}:reserved-instances/{resource.get('ReservedNodeId', '')}")
                if 'StartTime' in resource:
                    resource['StartTime'] = resource['StartTime'].isoformat()
                inventory_object = extract_common_info(
                    arn, resource, region, account_id, time_generated, account_name)
                inventory.append(inventory_object)
            save_as_file_parquet(inventory, file_path, generate_parquet_prefix(
                str(stack()[0][3]), region, account_id, idx))
            next_token = response.get('Marker', None)
            idx = idx + 1
            if not next_token:
                break
        except Exception as e:
            print(e)
            break
