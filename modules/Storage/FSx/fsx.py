from inspect import stack
from utils.utils import extract_common_info, save_as_file_parquet, generate_parquet_prefix


def list_fsx_filesystems(file_path, session, region, time_generated, account, boto_config):
    next_token = None
    idx = 0
    client = session.client('fsx', region_name=region, config=boto_config)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.describe_file_systems(
                NextToken=next_token) if next_token else client.describe_file_systems()
            AdministrativeActionCounter = 0
            for resource in response.get('FileSystems', []):
                arn = resource.get('ResourceARN')
                if 'CreationTime' in resource:
                    resource['CreationTime'] = resource['CreationTime'].isoformat()
                for AdministrativeAction in resource.get('AdministrativeActions', []):
                    resource['AdministrativeActions'][AdministrativeActionCounter][
                        'RequestTime'] = AdministrativeAction['RequestTime'].isoformat()
                    resource['AdministrativeActions'][AdministrativeActionCounter]['TargetVolumeValues'][
                        'CreationTime'] = AdministrativeAction['TargetVolumeValues']['CreationTime'].isoformat()
                    resource['AdministrativeActions'][AdministrativeActionCounter]['TargetSnapshotValues'][
                        'CreationTime'] = AdministrativeAction['TargetSnapshotValues']['CreationTime'].isoformat()
                    AdministrativeActionCounter = AdministrativeActionCounter + 1
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


def list_fsx_volumes(file_path, session, region, time_generated, account, boto_config):
    next_token = None
    idx = 0
    client = session.client('fsx', region_name=region, config=boto_config)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.describe_volumes(
                NextToken=next_token) if next_token else client.describe_volumes()
            AdministrativeActionCounter = 0
            for resource in response.get('Volumes', []):
                arn = resource.get('ResourceARN')
                if 'CreationTime' in resource:
                    resource['CreationTime'] = resource['CreationTime'].isoformat()
                for AdministrativeAction in resource.get('AdministrativeActions', []):
                    resource['AdministrativeActions'][AdministrativeActionCounter][
                        'RequestTime'] = AdministrativeAction['RequestTime'].isoformat()
                    resource['AdministrativeActions'][AdministrativeActionCounter]['TargetFileSystemValues'][
                        'CreationTime'] = AdministrativeAction['TargetFileSystemValues']['CreationTime'].isoformat()
                    resource['AdministrativeActions'][AdministrativeActionCounter]['TargetSnapshotValues'][
                        'CreationTime'] = AdministrativeAction['TargetSnapshotValues']['CreationTime'].isoformat()
                    AdministrativeActionCounter = AdministrativeActionCounter + 1
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


def list_fsx_filecache(file_path, session, region, time_generated, account, boto_config):
    next_token = None
    idx = 0
    client = session.client('fsx', region_name=region, config=boto_config)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.describe_file_caches(
                NextToken=next_token) if next_token else client.describe_file_caches()
            for resource in response.get('FileCaches', []):
                arn = resource.get('ResourceARN')
                if 'CreationTime' in resource:
                    resource['CreationTime'] = resource['CreationTime'].isoformat()
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
