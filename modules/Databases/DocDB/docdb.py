from inspect import stack
from utils.utils import extract_common_info, save_as_file_parquet, generate_parquet_prefix


def list_docdb_global(file_path, session, region, time_generated, account, boto_config):
    """
    This Python function lists global clusters in Amazon DocumentDB and saves the information to a
    Parquet file.

    :param file_path: The `file_path` parameter in the `list_docdb_global` function is the path where
    the output file will be saved. It is the location on the file system where the function will write
    the results of the `describe_global_clusters` API call for Amazon DocumentDB
    :param session: The `session` parameter in the `list_docdb_global` function is typically an instance
    of `boto3.Session` that is used to create a client for interacting with AWS services. It is used to
    make API calls to Amazon DocumentDB (DocDB) in the specified AWS region
    :param region: The `region` parameter in the `list_docdb_global` function is used to specify the AWS
    region where the Amazon DocumentDB (DocDB) clusters are located. This parameter is required to
    create a client session for the DocDB service in the specified region
    :param time_generated: The `time_generated` parameter in the `list_docdb_global` function is used to
    specify the timestamp or time at which the inventory data is generated or collected. This timestamp
    is important for tracking when the inventory information was retrieved and can be useful for
    auditing or monitoring purposes. It helps in identifying the
    :param account: The `account` parameter in the `list_docdb_global` function seems to be a dictionary
    containing information about an account. It likely includes keys such as 'account_id' and
    'account_name'. This information is used within the function to extract details needed for
    processing global clusters in Amazon DocumentDB
    """
    next_token = None
    idx = 0
    client = session.client('docdb', region_name=region, config=boto_config)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.describe_global_clusters(
                Marker=next_token) if next_token else client.describe_global_clusters()
            for response in response.get('GlobalClusters', []):
                arn = response['GlobalClusterArn']
                inventory_object = extract_common_info(
                    arn, response, region, account_id, time_generated, account_name)
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


def list_docdb(file_path, session, region, time_generated, account, boto_config):
    """
    This Python function lists information about DocumentDB clusters and saves it to a Parquet file.

    :param file_path: The `file_path` parameter in the `list_docdb` function is the path where the
    output file will be saved. It should be a valid file path on the system where the function is being
    executed
    :param session: The `session` parameter in the `list_docdb` function is typically an instance of
    `boto3.Session` that is used to create a client for interacting with AWS services. It allows you to
    configure credentials, region, and other settings for making API calls to AWS services like Amazon
    DocumentDB
    :param region: The `region` parameter in the `list_docdb` function is used to specify the AWS region
    where the Amazon DocumentDB (DocDB) clusters are located. This parameter is required for creating a
    client session to interact with the DocDB service in the specified region
    :param time_generated: The `time_generated` parameter in the `list_docdb` function is used to
    specify the timestamp or datetime when the inventory data is generated or collected. This parameter
    is important for tracking when the inventory information was retrieved and can be useful for
    auditing or monitoring purposes. It helps in maintaining a record of
    :param account: The `account` parameter in the `list_docdb` function seems to be a dictionary
    containing information about an account. It likely includes the keys 'account_id' and
    'account_name', which are used within the function to extract specific values for further processing
    """
    next_token = None
    idx = 0
    client = session.client('docdb', region_name=region, config=boto_config)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.describe_db_clusters(
                Marker=next_token) if next_token else client.describe_db_clusters()
            for response in response.get('DBClusters', []):
                if 'EarliestRestorableTime' in response:
                    response['EarliestRestorableTime'] = response['EarliestRestorableTime'].isoformat(
                    )
                if 'LatestRestorableTime' in response:
                    response['LatestRestorableTime'] = response['LatestRestorableTime'].isoformat(
                    )
                if 'ClusterCreateTime' in response:
                    response['ClusterCreateTime'] = response['ClusterCreateTime'].isoformat(
                    )
                arn = response['DBClusterArn']
                inventory_object = extract_common_info(
                    arn, response, region, account_id, time_generated, account_name)
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


def list_docdb_instances(file_path, session, region, time_generated, account, boto_config):
    """
    This Python function lists Amazon DocumentDB instances, extracts relevant information, and saves it
    in a Parquet file.

    :param file_path: The `file_path` parameter is a string that represents the path where the output
    file will be saved. It should specify the directory and filename where the output will be stored
    :param session: The `session` parameter in the `list_docdb_instances` function is typically an
    instance of a boto3 session that allows you to create service clients for AWS services. It is used
    to create a client for Amazon DocumentDB (docdb) service in the specified AWS region
    :param region: The `region` parameter in the `list_docdb_instances` function is used to specify the
    AWS region where the Amazon DocumentDB (DocDB) instances are located. This parameter is required to
    create a client session for interacting with the DocDB service in the specified region
    :param time_generated: The `time_generated` parameter in the `list_docdb_instances` function is used
    to specify the timestamp or time at which the inventory data is generated or collected. This
    parameter is important for tracking when the inventory information was retrieved and can be useful
    for auditing or monitoring purposes. It helps in maintaining a
    :param account: The `account` parameter in the `list_docdb_instances` function seems to be a
    dictionary containing information about the account. It likely includes keys such as 'account_id'
    and 'account_name'
    """
    next_token = None
    idx = 0
    client = session.client('docdb', region_name=region, config=boto_config)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.describe_db_instances(
                Marker=next_token) if next_token else client.describe_db_instances()
            for response in response.get('DBInstances', []):
                if 'InstanceCreateTime' in response:
                    response['InstanceCreateTime'] = response['InstanceCreateTime'].isoformat(
                    )
                if 'LatestRestorableTime' in response:
                    response['LatestRestorableTime'] = response['LatestRestorableTime'].isoformat(
                    )
                if 'CertificateDetails' in response:
                    if 'ValidTill' in response.get('CertificateDetails'):
                        response['CertificateDetails']['ValidTill'] = response['CertificateDetails']['ValidTill'].isoformat(
                        )
                arn = response['DBInstanceArn']
                inventory_object = extract_common_info(
                    arn, response, region, account_id, time_generated, account_name)
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


def list_docdb_cluster_snapshots(file_path, session, region, time_generated, account, boto_config):
    """
    This Python function lists DocDB cluster snapshots and saves the information to a Parquet file.

    :param file_path: The `file_path` parameter is a string that represents the file path where the
    output data will be saved. It should point to the location where you want to store the results of
    listing DocDB cluster snapshots
    :param session: The `session` parameter in the `list_docdb_cluster_snapshots` function is typically
    an instance of a boto3 session that allows you to create service clients for AWS services. It is
    used to create a client for the Amazon DocumentDB service in the specified region. This client is
    then used to
    :param region: Region is the geographical area where the AWS resources are located. It is a required
    parameter for AWS SDK operations to specify the region where the resources are located or where the
    operations should be performed. Examples of regions include 'us-east-1', 'eu-west-1', 'ap-southeast-
    :param time_generated: The `time_generated` parameter in the `list_docdb_cluster_snapshots` function
    likely represents the timestamp or datetime when the function is being executed or when the
    snapshots are being generated. This parameter is used to capture the time at which the inventory of
    DocDB cluster snapshots is being collected or processed
    :param account: The `account` parameter in the `list_docdb_cluster_snapshots` function seems to be a
    dictionary containing information about an account. It likely includes keys such as 'account_id' and
    'account_name'. This parameter is used to extract specific details from the account dictionary to be
    used in the function
    """
    next_token = None
    idx = 0
    client = session.client('docdb', region_name=region, config=boto_config)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.describe_db_cluster_snapshots(
                Marker=next_token) if next_token else client.describe_db_cluster_snapshots()
            for response in response.get('DBClusterSnapshots', []):
                if 'SnapshotCreateTime' in response:
                    response['SnapshotCreateTime'] = response['SnapshotCreateTime'].isoformat(
                    )
                if 'ClusterCreateTime' in response:
                    response['ClusterCreateTime'] = response['ClusterCreateTime'].isoformat(
                    )
                arn = response['DBClusterSnapshotArn']
                inventory_object = extract_common_info(
                    arn, response, region, account_id, time_generated, account_name)
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


def list_docdb_elastic_cluster(file_path, session, region, time_generated, account, boto_config):
    """
    This Python function lists DocDB Elastic clusters, retrieves cluster information, and saves the data
    to a Parquet file.

    :param file_path: The `file_path` parameter in the `list_docdb_elastic_cluster` function is the path
    where the inventory data will be saved as a file. It should be a string representing the file path
    where the data will be stored
    :param session: The `session` parameter in the `list_docdb_elastic_cluster` function is typically an
    instance of a boto3 session that allows you to create service clients for AWS services. It is used
    to create a client for the Amazon DocumentDB (DocDB) service in a specific AWS region
    :param region: The `region` parameter in the `list_docdb_elastic_cluster` function refers to the AWS
    region where the Amazon DocumentDB (DocDB) cluster is located. This parameter is used to specify the
    region when creating a client for the DocDB service and when extracting information about the
    clusters in that
    :param time_generated: The `time_generated` parameter in the `list_docdb_elastic_cluster` function
    is used to specify the timestamp or time at which the inventory data is generated or collected. This
    timestamp is typically used for tracking when the inventory information was retrieved and can be
    helpful for auditing or monitoring purposes
    :param account: The `account` parameter in the `list_docdb_elastic_cluster` function seems to be a
    dictionary containing information about an account. It likely includes the keys `account_id` and
    `account_name`, which are used within the function to extract specific details for processing
    """
    next_token = None
    idx = 0
    client = session.client(
        'docdb-elastic', region_name=region, config=boto_config)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.list_clusters(
                nextToken=next_token) if next_token else client.list_clusters()
            for response in response.get('clusters', []):
                resource_response = client.get_cluster(
                    clusterArn=response.get('clusterArn'))
                resource_response = resource_response.get('cluster')
                arn = resource_response.get('clusterArn')
                inventory_object = extract_common_info(
                    arn, response, region, account_id, time_generated, account_name)
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


def list_docdb_elastic_cluster_snapshots(file_path, session, region, time_generated, account, boto_config):
    """
    This Python function lists DocDB Elastic cluster snapshots and saves the information to a file in
    Parquet format.

    :param file_path: The `file_path` parameter in the `list_docdb_elastic_cluster_snapshots` function
    is the path where the output files will be saved. It is the location on the file system where the
    Parquet files containing the cluster snapshots information will be stored
    :param session: The `session` parameter in the `list_docdb_elastic_cluster_snapshots` function is
    typically an instance of a boto3 session that allows you to create service clients for AWS services.
    It is used to create a client for the Amazon DocumentDB (DocDB) service in a specific region
    :param region: The `region` parameter in the `list_docdb_elastic_cluster_snapshots` function refers
    to the AWS region where the Amazon DocumentDB (DocDB) cluster snapshots are located. This parameter
    is used to specify the region when creating a client session to interact with the DocDB service in
    that specific
    :param time_generated: The `time_generated` parameter in the `list_docdb_elastic_cluster_snapshots`
    function likely represents the timestamp or time at which the operation is being executed or the
    snapshots are being listed. This parameter is used to track when the snapshots were generated or
    when the function is being called. It could
    :param account: The `account` parameter in the `list_docdb_elastic_cluster_snapshots` function seems
    to be a dictionary containing information about an account. It likely includes keys such as
    'account_id' and 'account_name'
    """
    next_token = None
    idx = 0
    client = session.client(
        'docdb-elastic', region_name=region, config=boto_config)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.list_cluster_snapshots(
                nextToken=next_token) if next_token else client.list_cluster_snapshots()
            for response in response.get('snapshots', []):
                arn = response['snapshotArn']
                inventory_object = extract_common_info(
                    arn, response, region, account_id, time_generated, account_name)
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
