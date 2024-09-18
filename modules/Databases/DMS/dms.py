from inspect import stack
from utils.utils import extract_common_info, save_as_file_parquet, generate_parquet_prefix


def list_dms_tasks(file_path, session, region, time_generated, account):
    """
    This Python function lists DMS tasks, retrieves their details, processes the data, and saves it as a
    Parquet file.

    :param file_path: The `file_path` parameter is the file path where the output data will be saved. It
    is the location on the file system where the Parquet file containing the DMS tasks information will
    be stored
    :param session: The `session` parameter in the `list_dms_tasks` function is typically an instance of
    `boto3.Session` that is used to create a client for the AWS DMS (Database Migration Service). This
    session is used to interact with AWS services and resources in a specific region
    :param region: Region is a string representing the AWS region where the DMS tasks are located. It
    specifies the geographical area in which the resources are deployed, such as 'us-east-1' for US East
    (N. Virginia) or 'eu-west-1' for EU (Ireland)
    :param time_generated: The `time_generated` parameter is used to specify the timestamp when the task
    was generated or created. It is a datetime value that is used in the function `list_dms_tasks` to
    extract information about replication tasks and save them as Parquet files
    :param account: The `account` parameter in the `list_dms_tasks` function seems to be a dictionary
    containing information about an account. It likely includes keys such as 'account_id' and
    'account_name'. This information is used within the function to extract common information and
    generate a file path for saving the inventory
    """
    next_token = None
    idx = 0
    client = session.client('dms', region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.describe_replication_tasks(
                Marker=next_token) if next_token else client.describe_replication_tasks()
            for resource in response.get('ReplicationTasks', []):
                arn = resource.get('ReplicationTaskArn', '')
                if 'ReplicationTaskCreationDate' in resource:
                    resource['ReplicationTaskCreationDate'] = resource['ReplicationTaskCreationDate'].isoformat(
                    )

                if 'ReplicationTaskStartDate' in resource:
                    resource['ReplicationTaskStartDate'] = resource['ReplicationTaskStartDate'].isoformat(
                    )

                if 'FreshStartDate' in resource.get('ReplicationTaskStats', {}):
                    resource['ReplicationTaskStats']['FreshStartDate'] = resource['ReplicationTaskStats']['FreshStartDate'].isoformat()

                if 'StartDate' in resource.get('ReplicationTaskStats', {}):
                    resource['ReplicationTaskStats']['StartDate'] = resource['ReplicationTaskStats']['StartDate'].isoformat()

                if 'StopDate' in resource.get('ReplicationTaskStats', {}):
                    resource['ReplicationTaskStats']['StopDate'] = resource['ReplicationTaskStats']['StopDate'].isoformat()

                if 'FullLoadStartDate' in resource.get('ReplicationTaskStats', {}):
                    resource['ReplicationTaskStats']['FullLoadStartDate'] = resource['ReplicationTaskStats']['FullLoadStartDate'].isoformat()

                if 'FullLoadFinishDate' in resource.get('ReplicationTaskStats', {}):
                    resource['ReplicationTaskStats']['FullLoadFinishDate'] = resource['ReplicationTaskStats']['FullLoadFinishDate'].isoformat()
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


def list_dms_instances(file_path, session, region, time_generated, account):
    """
    This Python function lists AWS DMS instances, extracts common information, and saves the data as a
    Parquet file.

    :param file_path: The `file_path` parameter is a string that represents the path where the output
    file will be saved. It should include the file name and extension (e.g., "output.csv" or
    "data/output.json")
    :param session: The `session` parameter in the `list_dms_instances` function is typically an
    instance of a boto3 session that is used to create a client for the AWS DMS service. This session is
    used to make API calls to AWS services
    :param region: The `region` parameter in the `list_dms_instances` function is used to specify the
    AWS region in which the AWS DMS (Database Migration Service) instances are located. This parameter
    is required for creating a client session in the specified region and for describing the replication
    instances in that region. It
    :param time_generated: The `time_generated` parameter in the `list_dms_instances` function is used
    to specify the timestamp or time at which the inventory data is being generated or collected. This
    parameter is important for tracking when the inventory information was retrieved and can be useful
    for auditing and monitoring purposes
    :param account: The `account` parameter in the `list_dms_instances` function seems to be a
    dictionary containing information about an account. It likely includes keys such as 'account_id' and
    'account_name'. This parameter is used to extract specific information related to the account when
    listing DMS instances
    """
    next_token = None
    idx = 0
    client = session.client('dms', region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.describe_replication_instances(
                Marker=next_token) if next_token else client.describe_replication_instances()
            for resource in response.get('ReplicationInstances', []):
                arn = resource.get('ReplicationInstanceArn', '')
                if 'InstanceCreateTime' in resource:
                    resource['InstanceCreateTime'] = resource['InstanceCreateTime'].isoformat(
                    )

                if 'FreeUntil' in resource:
                    resource['FreeUntil'] = resource['FreeUntil'].isoformat(
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
