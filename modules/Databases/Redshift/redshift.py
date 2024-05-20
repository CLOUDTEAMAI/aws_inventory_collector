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
