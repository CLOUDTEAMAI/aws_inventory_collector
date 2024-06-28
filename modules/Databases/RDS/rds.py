from inspect import stack
from utils.utils import extract_common_info, save_as_file_parquet, generate_parquet_prefix


def list_rds(file_path, session, region, time_generated, account):
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
    client = session.client('rds', region_name=region)
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


def list_rds_global(file_path, session, region, time_generated, account):
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
    client = session.client('rds', region_name=region)
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


def list_rds_instances(file_path, session, region, time_generated, account):
    next_token = None
    idx = 0
    client = session.client('rds', region_name=region)
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


def list_rds_snapshots(file_path, session, region, time_generated, account):
    next_token = None
    idx = 0
    client = session.client('rds', region_name=region)
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


def list_rds_proxies(file_path, session, region, time_generated, account):
    next_token = None
    idx = 0
    client = session.client('rds', region_name=region)
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


def list_rds_proxy_endpoints(file_path, session, region, time_generated, account):
    next_token = None
    idx = 0
    client = session.client('rds', region_name=region)
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


def list_rds_sizing(file_path, session, region, time_generated, account):
    next_token = None
    idx = 0
    client = session.client('rds', region_name=region)
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
