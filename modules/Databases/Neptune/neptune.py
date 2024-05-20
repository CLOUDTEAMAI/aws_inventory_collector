from inspect import stack
from utils.utils import extract_common_info, save_as_file_parquet, generate_parquet_prefix


def list_neptune(file_path, session, region, time_generated, account):
    next_token = None
    idx = 0
    client = session.client('neptune', region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            nodes_counter = 0
            response = client.describe_db_clusters(
                Marker=next_token) if next_token else client.describe_db_clusters()
            for resource in response.get('DBClusters', []):
                if 'EarliestRestorableTime' in resource:
                    resource['EarliestRestorableTime'] = resource['EarliestRestorableTime'].isoformat(
                    )
                if 'LatestRestorableTime' in resource:
                    resource['LatestRestorableTime'] = resource['LatestRestorableTime'].isoformat(
                    )
                if 'ClusterCreateTime' in resource:
                    resource['ClusterCreateTime'] = resource['ClusterCreateTime'].isoformat(
                    )
                if 'AutomaticRestartTime' in resource:
                    resource['AutomaticRestartTime'] = resource['AutomaticRestartTime'].isoformat(
                    )
                if 'IOOptimizedNextAllowedModificationTime' in resource:
                    resource['IOOptimizedNextAllowedModificationTime'] = resource['IOOptimizedNextAllowedModificationTime'].isoformat(
                    )
                arn = resource.get('DBClusterArn', '')
                inventory_object = extract_common_info(
                    arn, resource, region, account_id, time_generated, account_name)
                inventory.append(inventory_object)
            save_as_file_parquet(inventory, file_path,
                                 generate_parquet_prefix(str(stack()[0][3]), region, account_id, idx))
            next_token = response.get('Marker', None)
            idx = idx + 1
            if not next_token:
                break
        except Exception as e:
            print(e)
            break


def list_neptune_instances(file_path, session, region, time_generated, account):
    next_token = None
    idx = 0
    client = session.client('neptune', region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            nodes_counter = 0
            response = client.describe_db_instances(
                Marker=next_token) if next_token else client.describe_db_instances()
            for resource in response.get('DBInstances', []):
                if 'InstanceCreateTime' in resource:
                    resource['InstanceCreateTime'] = resource['InstanceCreateTime'].isoformat(
                    )
                if 'LatestRestorableTime' in resource:
                    resource['LatestRestorableTime'] = resource['LatestRestorableTime'].isoformat(
                    )
                arn = resource.get('DBInstanceArn', '')
                inventory_object = extract_common_info(
                    arn, resource, region, account_id, time_generated, account_name)
                inventory.append(inventory_object)
            save_as_file_parquet(inventory, file_path,
                                 generate_parquet_prefix(str(stack()[0][3]), region, account_id, idx))
            next_token = response.get('Marker', None)
            idx = idx + 1
            if not next_token:
                break
        except Exception as e:
            print(e)
            break
