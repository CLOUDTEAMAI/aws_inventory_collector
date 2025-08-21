from inspect import stack
from utils.utils import extract_common_info, save_as_file_parquet, generate_parquet_prefix


def list_rds(file_path, session, region, time_generated, account, boto_config):
    """
    This Python function lists RDS clusters and saves the information in a Parquet file.

    :param file_path: The `file_path` parameter in the `list_rds` function is the path where the output
    file will be saved. It is the location on the file system where the Parquet file containing the RDS
    inventory data will be stored
    :param session: The `session` parameter in the `list_rds` function is an AWS session object that is
    used to create a client for interacting with the AWS RDS service in a specific region
    :param region: The `region` parameter in the `list_rds` function is used to specify the AWS region
    in which the RDS (Relational Database Service) resources are located. This parameter is required to
    create a client for the RDS service in the specified region and to retrieve information about RDS
    clusters
    :param time_generated: The `time_generated` parameter in the `list_rds` function is used to specify
    the timestamp or time at which the inventory data is being generated or collected. This timestamp is
    typically used for tracking when the inventory information was retrieved and can be helpful for
    auditing or monitoring purposes
    :param account: The `account` parameter in the `list_rds` function seems to be a dictionary
    containing information about the account. It likely includes keys such as 'account_id' and
    'account_name'. This information is used within the function to extract common information and
    generate file names for saving the RDS inventory
    """
    next_token = None
    idx = 0
    client = session.client('rds', region_name=region, config=boto_config)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.describe_db_clusters(
                Marker=next_token) if next_token else client.describe_db_clusters()
            for resource in response.get('DBClusters', []):
                if 'AutomaticRestartTime' in resource:
                    resource['AutomaticRestartTime'] = resource['AutomaticRestartTime'].isoformat(
                    )
                if 'EarliestRestorableTime' in resource:
                    resource['EarliestRestorableTime'] = resource['EarliestRestorableTime'].isoformat(
                    )
                if 'LatestRestorableTime' in resource:
                    resource['LatestRestorableTime'] = resource['LatestRestorableTime'].isoformat(
                    )

                if 'ClusterCreateTime' in resource:
                    resource['ClusterCreateTime'] = resource['ClusterCreateTime'].isoformat(
                    )

                if 'EarliestBacktrackTime' in resource:
                    resource['EarliestBacktrackTime'] = resource['EarliestBacktrackTime'].isoformat(
                    )
                if resource.get('PendingModifiedValues', {}).get('CertificateDetails', {}).get('ValidTill', None):
                    resource['PendingModifiedValues']['CertificateDetails']['ValidTill'] = resource['PendingModifiedValues']['CertificateDetails']['ValidTill'].isoformat(
                    )
                if 'IOOptimizedNextAllowedModificationTime' in resource:
                    resource['IOOptimizedNextAllowedModificationTime'] = resource['IOOptimizedNextAllowedModificationTime'].isoformat(
                    )

                inventory_object = extract_common_info(
                    resource['DBClusterArn'], resource, region, account_id, time_generated, account_name)
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


def list_rds_global(file_path, session, region, time_generated, account, boto_config):
    """
    This Python function lists RDS clusters and saves the information in a Parquet file.

    :param file_path: The `file_path` parameter in the `list_rds` function is the path where the output
    file will be saved. It is the location on the file system where the Parquet file containing the RDS
    inventory data will be stored
    :param session: The `session` parameter in the `list_rds` function is an AWS session object that is
    used to create a client for interacting with the AWS RDS service in a specific region
    :param region: The `region` parameter in the `list_rds` function is used to specify the AWS region
    in which the RDS (Relational Database Service) resources are located. This parameter is required to
    create a client for the RDS service in the specified region and to retrieve information about RDS
    clusters
    :param time_generated: The `time_generated` parameter in the `list_rds` function is used to specify
    the timestamp or time at which the inventory data is being generated or collected. This timestamp is
    typically used for tracking when the inventory information was retrieved and can be helpful for
    auditing or monitoring purposes
    :param account: The `account` parameter in the `list_rds` function seems to be a dictionary
    containing information about the account. It likely includes keys such as 'account_id' and
    'account_name'. This information is used within the function to extract common information and
    generate file names for saving the RDS inventory
    """
    next_token = None
    idx = 0
    client = session.client('rds', region_name=region, config=boto_config)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.describe_global_clusters(
                Marker=next_token) if next_token else client.describe_global_clusters()
            for resource in response.get('GlobalClusters', []):
                inventory_object = extract_common_info(
                    resource['GlobalClusterArn'], resource, region, account_id, time_generated, account_name)
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


def list_rds_instances(file_path, session, region, time_generated, account, boto_config):
    """
    This Python function lists RDS instances, extracts common information, and saves the data in Parquet
    format.

    :param file_path: The `file_path` parameter is a string that represents the file path where the RDS
    instances information will be saved. This could be a local file path or a path in a cloud storage
    service like Amazon S3
    :param session: The `session` parameter in the `list_rds_instances` function is typically an
    instance of `boto3.Session` that is used to create a client for the AWS service. It allows you to
    make API calls to AWS services like RDS (Relational Database Service) in this case
    :param region: The `region` parameter in the `list_rds_instances` function is used to specify the
    AWS region where the RDS instances are located. This parameter is required to create a client for
    the RDS service in the specified region and to retrieve information about the RDS instances in that
    region
    :param time_generated: The `time_generated` parameter in the `list_rds_instances` function is used
    to specify the timestamp or time at which the inventory of RDS instances is being generated. This
    timestamp is typically used for tracking and auditing purposes to know when the inventory data was
    collected or generated. It helps in maintaining
    :param account: The `account` parameter in the `list_rds_instances` function seems to be a
    dictionary containing information about the AWS account. It likely includes keys such as
    'account_id' and 'account_name' which are used within the function to extract specific details for
    processing RDS instances
    """
    next_token = None
    idx = 0
    client = session.client('rds', region_name=region, config=boto_config)
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
                if 'ValidTill' in resource['CertificateDetails']:
                    resource['CertificateDetails']['ValidTill'] = resource['CertificateDetails']['ValidTill'].isoformat(
                    )
                inventory_object = extract_common_info(
                    resource['DBInstanceArn'], resource, region, account_id, time_generated, account_name)
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


def list_rds_snapshots(file_path, session, region, time_generated, account, boto_config):
    """
    This Python function lists RDS snapshots for a given account and region, saving the information to a
    file in Parquet format.

    :param file_path: The `file_path` parameter is the path where the RDS snapshots inventory data will
    be saved. This should be a valid file path on the system where the script is running
    :param session: The `session` parameter in the `list_rds_snapshots` function is typically an
    instance of a boto3 session that is used to create clients and resources for AWS services. It allows
    you to make API calls to AWS services using the credentials and configuration provided in the
    session
    :param region: The `region` parameter in the `list_rds_snapshots` function is used to specify the
    AWS region in which the RDS (Relational Database Service) snapshots are to be listed. This parameter
    determines the geographical location where the RDS resources are located or where the operation will
    be performed
    :param time_generated: Time when the snapshots were generated. It is used to track when the
    snapshots were created
    :param account: The `account` parameter in the `list_rds_snapshots` function seems to be a
    dictionary containing information about the account. It likely includes keys such as 'account_id'
    and 'account_name'. This parameter is used to extract the account ID and account name to be used in
    the function for
    """
    next_token = None
    idx = 0
    client = session.client('rds', region_name=region, config=boto_config)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.describe_db_cluster_snapshots(
                Marker=next_token) if next_token else client.describe_db_cluster_snapshots()
            for resource in response.get('DBClusterSnapshots', []):
                arn = resource.get('DBClusterSnapshotArn', '')
                if 'SnapshotCreateTime' in resource:
                    resource['SnapshotCreateTime'] = resource['SnapshotCreateTime'].isoformat(
                    )
                if 'ClusterCreateTime' in resource:
                    resource['ClusterCreateTime'] = resource['ClusterCreateTime'].isoformat(
                    )
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


def list_rds_proxies(file_path, session, region, time_generated, account, boto_config):
    """
    This Python function lists RDS proxies, extracts common information, and saves the data as Parquet
    files.

    :param file_path: The `file_path` parameter is the path where the output file will be saved. It
    should be a string representing the file path including the file name and extension where the data
    will be stored
    :param session: The `session` parameter in the `list_rds_proxies` function is an object representing
    the current session. It is typically created using the `boto3.Session` class and is used to interact
    with AWS services. This object stores configuration information such as credentials, region, and
    other settings needed
    :param region: The `region` parameter in the `list_rds_proxies` function is used to specify the AWS
    region where the RDS proxies are located. This parameter is required to create a client session for
    the RDS service in the specified region and to retrieve information about the RDS proxies in that
    region
    :param time_generated: The `time_generated` parameter in the `list_rds_proxies` function is used to
    specify the timestamp or datetime when the inventory of RDS proxies is being generated. This
    timestamp is typically used for tracking and auditing purposes to know when the inventory data was
    collected
    :param account: The `account` parameter in the `list_rds_proxies` function seems to be a dictionary
    containing information about an AWS account. It likely includes the account ID and account name
    """
    next_token = None
    idx = 0
    client = session.client('rds', region_name=region, config=boto_config)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.describe_db_proxies(
                Marker=next_token) if next_token else client.describe_db_proxies()
            for resource in response.get('DBProxies', []):
                arn = resource.get('DBProxyArn', '')
                if 'CreatedDate' in resource:
                    resource['CreatedDate'] = resource['CreatedDate'].isoformat(
                    )
                if 'UpdatedDate' in resource:
                    resource['UpdatedDate'] = resource['UpdatedDate'].isoformat(
                    )
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


def list_rds_proxy_endpoints(file_path, session, region, time_generated, account, boto_config):
    """
    This Python function retrieves and saves information about RDS proxy endpoints to a file in Parquet
    format.

    :param file_path: The `file_path` parameter is the file path where the output data will be saved. It
    is the location where the Parquet file containing the RDS proxy endpoints information will be stored
    :param session: The `session` parameter in the `list_rds_proxy_endpoints` function is typically an
    instance of `boto3.Session` that is used to create clients for AWS services. It allows you to make
    API calls to AWS services using the credentials and configuration provided in the session
    :param region: The `region` parameter in the `list_rds_proxy_endpoints` function is used to specify
    the AWS region in which the RDS (Relational Database Service) proxy endpoints should be listed. This
    parameter determines the geographical location where the RDS proxy endpoints are located or managed
    within the AWS infrastructure
    :param time_generated: The `time_generated` parameter in the `list_rds_proxy_endpoints` function is
    used to specify the timestamp or time at which the inventory data is being generated or collected.
    This timestamp is typically used for tracking and auditing purposes to know when the inventory
    information was retrieved. It is important to ensure
    :param account: The `account` parameter in the `list_rds_proxy_endpoints` function seems to be a
    dictionary containing information about an account. It likely includes keys such as 'account_id' and
    'account_name' which are used within the function to identify the account and its name
    """
    next_token = None
    idx = 0
    client = session.client('rds', region_name=region, config=boto_config)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.describe_db_proxy_endpoints(
                Marker=next_token) if next_token else client.describe_db_proxy_endpoints()
            for resource in response.get('DBProxyEndpoints', []):
                arn = resource.get('DBProxyEndpointArn', '')
                if 'CreatedDate' in resource:
                    resource['CreatedDate'] = resource['CreatedDate'].isoformat(
                    )
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


def list_rds_sizing(file_path, session, region, time_generated, account, boto_config):
    """
    The function `list_rds_sizing` retrieves information about available RDS engine versions and
    instance options, and saves the data to a file in Parquet format.

    :param file_path: The `file_path` parameter in the `list_rds_sizing` function is the file path where
    the output data will be saved. It is the location where the function will save the inventory
    information about RDS instance types and their sizing options
    :param session: The `session` parameter in the `list_rds_sizing` function is typically an instance
    of `boto3.Session` class that represents your AWS session. It is used to create clients for AWS
    services like RDS in a specific region. You can create a session using `boto3
    :param region: The `region` parameter in the `list_rds_sizing` function is used to specify the AWS
    region where the RDS instances are located. This parameter is important for creating the RDS client
    in the specified region and for generating ARNs (Amazon Resource Names) specific to that region
    :param time_generated: The `time_generated` parameter in the `list_rds_sizing` function is used to
    specify the timestamp or time at which the RDS sizing information is generated or collected. This
    parameter helps in tracking when the data was retrieved and can be useful for auditing or monitoring
    purposes. It is typically a
    :param account: The `account` parameter seems to be a dictionary containing information about an AWS
    account. It likely includes keys such as 'account_id' and 'account_name'. This information is used
    within the `list_rds_sizing` function to retrieve data related to RDS instances for the specified
    AWS account
    """
    next_token = None
    idx = 0
    client = session.client('rds', region_name=region, config=boto_config)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    engines = []
    engines_default_list = ['aurora-mysql', 'aurora-postgresql', 'db2-ae', 'db2-se',
                            'mariadb', 'mysql', 'oracle-ee', 'oracle-ee-cdb', 'oracle-se2', 'oracle-se2-cdb', 'postgres', 'sqlserver-ee', 'sqlserver-se', 'sqlserver-ex', 'sqlserver-web']
    while True:
        try:
            response = client.describe_db_engine_versions(
                Marker=next_token) if next_token else client.describe_db_engine_versions()
            for resource in response.get('DBEngineVersions', []):
                engines.append(resource.get('Engine', ''))
            next_token = response.get('Marker', None)
            if not next_token:
                break
        except Exception as e:
            print(e)
            break
    engines_list = list(set(engines_default_list + engines))
    for engine in engines_list:
        next_token = None
        inventory = []
        engine_instances = []
        while True:
            try:
                response = client.describe_orderable_db_instance_options(
                    Engine=engine, Marker=next_token) if next_token else client.describe_orderable_db_instance_options(Engine=engine)
                for resource in response.get('OrderableDBInstanceOptions', []):
                    engine_instances.append(
                        resource.get('DBInstanceClass'))
                next_token = response.get('Marker', None)
                if not next_token:
                    break
            except Exception as e:
                print(e)
                break
        arn = f"arn:aws:rds:{region}:{account_id}:scaling-options/{engine}"
        inventory_object = extract_common_info(
            arn, {"instance_types": list(set(engine_instances))}, region, account_id, time_generated, account_name)
        inventory.append(inventory_object)
        save_as_file_parquet(inventory, file_path, generate_parquet_prefix(
            str(stack()[0][3]), region, account_id, idx))
        idx = idx + 1
