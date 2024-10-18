from inspect import stack
from utils.utils import extract_common_info, save_as_file_parquet, generate_parquet_prefix


def list_redshift(file_path, session, region, time_generated, account):
    """
    The function `list_redshift` retrieves information about Redshift clusters, formats the data, and
    saves it as Parquet files.

    :param file_path: The `file_path` parameter in the `list_redshift` function is the path where the
    output file will be saved. It is the location on the file system where the Parquet file containing
    the Redshift cluster inventory will be stored
    :param session: The `session` parameter in the `list_redshift` function is an AWS session object
    that is used to create a client for interacting with the Amazon Redshift service. It is typically
    created using the `boto3.Session` class and contains the necessary credentials and configuration to
    make API requests to AWS
    :param region: The `region` parameter in the `list_redshift` function refers to the AWS region where
    the Redshift clusters are located. This parameter is used to specify the region when creating a
    Redshift client session in the AWS SDK. It helps to ensure that the client interacts with the
    Redshift service in
    :param time_generated: The `time_generated` parameter in the `list_redshift` function is used to
    specify the timestamp or time at which the inventory data is being generated or collected. This
    timestamp is used in creating the inventory object and is passed along with other information to the
    `extract_common_info` function for further processing
    :param account: The `account` parameter in the `list_redshift` function seems to be a dictionary
    containing information about an AWS account. It likely includes keys such as 'account_id' and
    'account_name'. This information is used within the function to extract details and generate
    inventory objects related to Redshift clusters for
    """
    next_token = None
    idx = 0
    client = session.client('redshift', region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.describe_clusters(
                Marker=next_token) if next_token else client.describe_clusters()
            for resource in response.get('Clusters', []):
                if 'ClusterCreateTime' in resource:
                    resource['ClusterCreateTime'] = resource['ClusterCreateTime'].isoformat(
                    )
                if 'ExpectedNextSnapshotScheduleTime' in resource:
                    resource['ExpectedNextSnapshotScheduleTime'] = resource['ExpectedNextSnapshotScheduleTime'].isoformat()
                if 'NextMaintenanceWindowStartTime' in resource:
                    resource['NextMaintenanceWindowStartTime'] = resource['NextMaintenanceWindowStartTime'].isoformat(
                    )
                for time in resource.get('DeferredMaintenanceWindows', []):
                    if 'DeferMaintenanceStartTime' in time:
                        time['DeferMaintenanceStartTime'] = time['DeferMaintenanceStartTime'].isoformat(
                        )
                    if 'DeferMaintenanceEndTime' in time:
                        time['DeferMaintenanceEndTime'] = time['DeferMaintenanceEndTime'].isoformat(
                        )
                if 'ReservedNodeExchangeStatus' in resource and 'RequestTime' in resource['ReservedNodeExchangeStatus']:
                    resource['ReservedNodeExchangeStatus']['RequestTime'] = resource['ReservedNodeExchangeStatus']['RequestTime'].isoformat()
                if 'CustomDomainCertificateExpiryDate' in resource:
                    resource['CustomDomainCertificateExpiryDate'] = resource['CustomDomainCertificateExpiryDate'].isoformat()

                arn = resource['ClusterNamespaceArn']
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


def list_redshift_serverless(file_path, session, region, time_generated, account):
    """
    This Python function lists Redshift serverless workgroups and saves the information to a Parquet
    file.

    :param file_path: The `file_path` parameter is the path where the output file will be saved. It
    should be a string representing the file path on the system where you want to save the output data
    :param session: The `session` parameter in the `list_redshift_serverless` function is typically an
    instance of a boto3 session that is used to create a client for the Redshift Serverless service in
    AWS. It allows you to make API calls to the Redshift Serverless service in the specified AWS region
    :param region: Region is the AWS region where the Redshift Serverless service is located. It
    specifies the geographical area where the resources will be deployed and managed. Examples of AWS
    regions include 'us-east-1' for US East (N. Virginia) and 'eu-west-1' for EU (Ireland
    :param time_generated: The `time_generated` parameter in the `list_redshift_serverless` function is
    used to specify the timestamp or time at which the inventory data is generated or collected. This
    parameter is important for tracking when the inventory information was retrieved and can be used for
    various purposes such as auditing, monitoring, or
    :param account: The `account` parameter in the `list_redshift_serverless` function seems to be a
    dictionary containing information about an account. It likely includes keys such as 'account_id' and
    'account_name' which are used within the function to extract specific information for processing
    Redshift serverless workgroups
    """
    next_token = None
    idx = 0
    client = session.client('redshift-serverless', region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.list_workgroups(
                nextToken=next_token) if next_token else client.list_workgroups()
            for resource in response.get('workgroups', []):
                if 'customDomainCertificateExpiryTime' in resource:
                    resource['customDomainCertificateExpiryTime'] = resource['customDomainCertificateExpiryTime'].isoformat(
                    )

                arn = resource['workgroupArn']
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


def list_redshift_cluster_snapshots(file_path, session, region, time_generated, account):
    """
    This Python function retrieves and processes Redshift cluster snapshots, saving the information in a
    Parquet file.

    :param file_path: The `file_path` parameter in the `list_redshift_cluster_snapshots` function
    represents the path where the output files will be saved. It should be a string that specifies the
    directory or file path where the generated data will be stored
    :param session: The `session` parameter in the `list_redshift_cluster_snapshots` function is an AWS
    session object that is used to create a Redshift client in a specific region. It is typically
    created using the `boto3.Session` class and is used for making API calls to AWS services
    :param region: The `region` parameter in the `list_redshift_cluster_snapshots` function refers to
    the AWS region where the Redshift cluster snapshots are located. This parameter is used to specify
    the region in which the AWS SDK client will operate and retrieve the cluster snapshots. It is
    important to provide the correct region
    :param time_generated: The `time_generated` parameter in the `list_redshift_cluster_snapshots`
    function likely represents the timestamp or time at which the operation is being executed or the
    time at which the snapshots are being generated. This parameter is used within the function to
    capture the time information related to the cluster snapshots being processed
    :param account: The `account` parameter in the `list_redshift_cluster_snapshots` function seems to
    be a dictionary containing information about an account. It likely includes keys such as
    'account_id' and 'account_name'. This information is used within the function to extract common
    information and generate a file name for saving
    """
    next_token = None
    idx = 0
    client = session.client('redshift', region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.describe_cluster_snapshots(
                Marker=next_token) if next_token else client.describe_cluster_snapshots()
            for resource in response.get('Snapshots', []):
                if 'ClusterCreateTime' in resource:
                    resource['ClusterCreateTime'] = resource['ClusterCreateTime'].isoformat(
                    )
                if 'SnapshotCreateTime' in resource:
                    resource['SnapshotCreateTime'] = resource['SnapshotCreateTime'].isoformat(
                    )
                if 'SnapshotRetentionStartTime' in resource:
                    resource['SnapshotRetentionStartTime'] = resource['SnapshotRetentionStartTime'].isoformat(
                    )
                arn = resource['SnapshotArn']
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


def list_redshift_serverless_snapshots(file_path, session, region, time_generated, account):
    """
    This Python function lists Redshift Serverless snapshots and saves the information to a file
    in Parquet format.

    :param file_path: The `file_path` parameter in the `list_redshift_serverless_snapshots`
    function is the path where the output snapshots inventory will be saved as a Parquet file. It should
    be a valid file path on the system where the code is running
    :param session: The `session` parameter in the `list_redshift_serverless_snapshots` function
    is typically an instance of a boto3 session that is used to create a client for the AWS service. It
    allows you to make API calls to the Redshift Serverless service in the specified region
    :param region: The `region` parameter in the `list_redshift_serverless_snapshots` function
    refers to the AWS region where the Redshift Serverless snapshots are located. This parameter
    is used to specify the region in which the AWS SDK client will operate and retrieve the snapshots.
    It should be a string
    :param time_generated: The `time_generated` parameter in the
    `list_redshift_serverless_snapshots` function likely represents the timestamp or time at
    which the function is being executed or the time at which the snapshots are being generated. This
    parameter is used in the function to capture the time at which the inventory objects are
    :param account: The `account` parameter in the `list_redshift_serverless_snapshots` function
    seems to be a dictionary containing information about the account. It likely includes keys such as
    'account_id' and 'account_name' which are used within the function to extract relevant information
    for processing Redshift serverless
    """
    next_token = None
    idx = 0
    client = session.client('redshift-serverless', region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.list_snapshots(
                nextToken=next_token) if next_token else client.list_snapshots()
            for resource in response.get('snapshots', []):
                if 'SnapshotCreateTime' in resource:
                    resource['SnapshotCreateTime'] = resource['SnapshotCreateTime'].isoformat(
                    )
                if 'SnapshotRetentionStartTime' in resource:
                    resource['SnapshotRetentionStartTime'] = resource['SnapshotRetentionStartTime'].isoformat(
                    )
                arn = resource['snapshotArn']
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
