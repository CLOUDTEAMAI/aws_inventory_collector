from inspect import stack
from utils.utils import save_as_file_parquet_metrics, generate_parquet_prefix, get_resource_utilization_metric


def rds_instances_metrics(file_path, session, region, account, metrics, time_generated):
    next_token = None
    idx = 0
    client = session.client('rds', region_name=region)
    account_id = account['account_id']
    while True:
        try:
            inventory = []
            response = client.describe_db_instances(
                Marker=next_token) if next_token else client.describe_db_instances()
            for resource in response.get('DBInstances', []):
                inventory.append(resource['DBInstanceIdentifier'])
            if inventory:
                metrics = get_resource_utilization_metric(
                    session, region, inventory, account, metrics, time_generated)
                save_as_file_parquet_metrics(metrics, file_path, generate_parquet_prefix(
                    str(stack()[0][3]), region, f'{account_id}-metrics', idx))
            next_token = response.get('Marker', None)
            idx = idx + 1
            if not next_token:
                break
        except Exception as e:
            print(e)
            break


def rds_proxies_metrics(file_path, session, region, account, metrics, time_generated):
    next_token = None
    idx = 0
    client = session.client('rds', region_name=region)
    account_id = account['account_id']
    while True:
        try:
            inventory = []
            response = client.describe_db_proxy_endpoints(
                Marker=next_token) if next_token else client.describe_db_proxy_endpoints()
            for resource in response.get('DBProxies', []):
                inventory.append(resource['DBProxyName'])
            if inventory:
                metrics = get_resource_utilization_metric(
                    session, region, inventory, account, metrics, time_generated)
                save_as_file_parquet_metrics(metrics, file_path, generate_parquet_prefix(
                    str(stack()[0][3]), region, f'{account_id}-metrics', idx))
            next_token = response.get('Marker', None)
            idx = idx + 1
            if not next_token:
                break
        except Exception as e:
            print(e)
            break
