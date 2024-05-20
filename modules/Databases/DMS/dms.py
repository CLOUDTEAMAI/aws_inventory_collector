from inspect import stack
from utils.utils import extract_common_info, save_as_file_parquet, generate_parquet_prefix


def list_dms_tasks(file_path, session, region, time_generated, account):
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
